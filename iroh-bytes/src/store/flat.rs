//! A flat file database implementation.
//!
//! This is a simple database implementation that stores all data in the file system.
//! It is used by the iroh binary.
//!
//! # File format
//!
//! The flat file database stores data and outboards in a directory structure.
//! Partial and complete entries can be stored in the same directory, or in different
//! directories. The purpose of a file is always clear from the file name.
//!
//! Currently a single directory is used to store all entries, but
//! in the future we might want to use a directory tree for file systems that don't
//! support a large number of files in a single directory.
//!
//! ## Files
//!
//! ### Complete data files
//!
//! Complete files have as name the hex encoded blake3 hash of the data, and the extension
//! `.data`. There can only ever be one complete file for a given hash. If the file does
//! not contain the data corresponding to the hash, this is considered an error that should
//! be reported during validation.
//!
//! They will not *change* during the lifetime of the database, but might be deleted.
//!
//! These files can become quite large and make up the vast majority of the disk usage.
//!
//! ### Complete outboard files
//!
//! Complete outboard files have as name the hex encoded blake3 hash of the data, and the
//! extension `.obao4`. `obao` stands for pre-order bao, and `4` describes the block size.
//! So `obao4` means that the outboard data is stored in a pre-order bao tree with a block
//! size of 1024*2^4=16384 bytes, which is the default block size for iroh.
//!
//! They will not *change* during the lifetime of the database, but might be deleted.
//!
//! The first 8 bytes of the file are the little endian encoded size of the data.
//!
//! In the future we might support other block sizes as well as in-order or post-order
//! encoded trees. The file extension will then change accordingly. E.g. `obao` for
//! pre-order outboard files with a block size of 1024*2^0=1024 bytes.
//!
//! For files that are smaller than the block size, the outboard file would just contain
//! the size. Storing these outboard files is not necessary, and therefore they are not
//! stored.
//!
//! ### Partial data files
//!
//! There can be multiple partial data files for a given hash. E.g. you could have one
//! partial data file containing valid bytes 0..16384 of a file, and another containing
//! valid bytes 32768..49152 of the same file.
//!
//! To allow for this, partial data files have as name the hex encoded blake3 hash of the
//! complete data, followed by a -, followed by a hex encoded 16 byte random uuid, followed
//! by the extension `.data`.
//!
//! ### Partial outboard files
//!
//! There can be multiple partial outboard files for a given hash. E.g. you could have one
//! partial outboard file containing the outboard for blocks 0..2 of a file and a second
//! partial outboard file containing the outboard for blocks 2..4 of the same file.
//!
//! To allow for this, partial outboard files have as name the hex encoded blake3 hash of
//! the complete data, followed by a -, followed by a hex encoded 16 byte random uuid,
//!
//! Partial outboard files are not stored for small files, since the outboard is just the
//! size of the data.
//!
//! Pairs of partial data and partial outboard files belong together, and are correlated
//! by the uuid.
//!
//! It is unusual but not impossible to have multiple partial data files for the same
//! hash. In that case the best partial data file should be chosen on startup.
//!
//! ### Temp files
//!
//! When copying data into the database, we first copy the data into a temporary file to
//! ensure that the data is not modified while we compute the outboard. These files have
//! just a hex encoded 16 byte random uuid as name, and the extension `.temp`.
//!
//! We don't know the hash of the data yet. These files are fully ephemeral, and can
//! be deleted on restart.
//!
//! # File lifecycle
//!
//! ## Import from local storage
//!
//! When a file is imported from local storage in copy mode, the file in question is first
//! copied to a temporary file. The temporary file is then used to compute the outboard.
//!
//! Once the outboard is computed, the temporary file is renamed to the final data file,
//! and the outboard is written to the final outboard file.
//!
//! When importing in reference mode, the outboard is computed directly from the file in
//! question. Once the outboard is computed, the file path is added to the paths file,
//! and the outboard is written to the outboard file.
//!
//! ## Download from the network
//!
//! When a file is downloaded from the network, a pair of partial data and partial outboard
//! files is created. The partial data file is filled with the downloaded data, and the
//! partial outboard file is filled at the same time. Note that a partial data file is
//! worthless without the corresponding partial outboard file, since only the outboard
//! can be used to verify the downloaded parts of the data.
//!
//! Once the download is complete, the partial data and partial outboard files are renamed
//! to the final partial data and partial outboard files.
#![allow(clippy::mutable_key_type)]
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    io::{self, BufReader},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex, RwLock},
    time::SystemTime,
};

use bao_tree::{
    io::{
        outboard::{PostOrderMemOutboard, PreOrderOutboard},
        outboard_size,
        sync::ReadAt,
    },
    BaoTree, ByteNum, ChunkRanges,
};
use bytes::Bytes;
use futures::{
    future::{self, BoxFuture},
    Future, FutureExt, Stream, StreamExt,
};
use iroh_io::{AsyncSliceReader, AsyncSliceWriter, File};
use redb::{Database, ReadableTable, RedbValue, TableDefinition, WriteTransaction};
use serde::{Deserialize, Serialize};
use tokio::{io::AsyncWriteExt, sync::mpsc};
use tracing::trace_span;

use crate::{
    store::{
        flatten_to_io, mem::MutableMemFile, new_uuid, temp_name, DbIter, EntryStatus, ExportMode,
        ImportMode, ImportProgress, Map, MapEntry, PartialMap, PartialMapEntry,
        PossiblyPartialEntry, ReadableStore, TempCounterMap, ValidateProgress,
    },
    util::{
        progress::{IdGenerator, IgnoreProgressSender, ProgressSender},
        {LivenessTracker, Tag},
    },
    BlobFormat, Hash, HashAndFormat, TempTag, IROH_BLOCK_SIZE,
};

type BoxIoFut<'a, T> = futures::future::BoxFuture<'a, io::Result<T>>;

#[derive(Debug, Default)]
struct State {
    // in memory tracking of live set
    live: BTreeSet<Hash>,
    // temp tags
    temp: TempCounterMap,
    // transient partial entries
    partial: BTreeMap<Hash, TransientPartialEntryData>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct CompleteEntry {
    // size of the data
    size: u64,
    // true means we own the data, false means it is stored externally
    owned_data: bool,
    // external storage locations
    external: BTreeSet<PathBuf>,
}

impl RedbValue for CompleteEntry {
    type SelfType<'a> = Self;

    type AsBytes<'a> = Vec<u8>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        postcard::from_bytes(data).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        postcard::to_stdvec(value).unwrap()
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new(std::any::type_name::<Self>())
    }
}

impl CompleteEntry {
    fn external_path(&self) -> Option<&PathBuf> {
        self.external.iter().next()
    }

    // create a new complete entry with the given size
    //
    // the generated entry will have no data or outboard data yet
    fn new_default(size: u64) -> Self {
        Self {
            owned_data: true,
            external: Default::default(),
            size,
        }
    }

    /// create a new complete entry with the given size and path
    ///
    /// the generated entry will have no data or outboard data yet
    fn new_external(size: u64, path: PathBuf) -> Self {
        Self {
            owned_data: false,
            external: [path].into_iter().collect(),
            size,
        }
    }

    #[allow(dead_code)]
    fn is_valid(&self) -> bool {
        !self.external.is_empty() || self.owned_data
    }

    fn union_with(&mut self, new: CompleteEntry) -> io::Result<()> {
        if self.size != 0 && self.size != new.size {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "size mismatch"));
        }
        self.size = new.size;
        self.owned_data |= new.owned_data;
        self.external.extend(new.external);
        Ok(())
    }
}

/// Data about a long lived partial entry.
#[derive(Debug, Clone, Default)]
struct PartialEntryData {
    // size of the data
    #[allow(dead_code)]
    size: u64,
    // unique id for this entry
    uuid: [u8; 16],
}

type PartialEntryDataRaw<'a> = (u64, &'a [u8; 16]);

impl RedbValue for PartialEntryData {
    type SelfType<'a> = Self;

    type AsBytes<'a> = <PartialEntryDataRaw<'a> as RedbValue>::AsBytes<'a>;

    fn fixed_width() -> Option<usize> {
        <PartialEntryDataRaw as RedbValue>::fixed_width()
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let (size, uuid) = <PartialEntryDataRaw as RedbValue>::from_bytes(data);
        Self { size, uuid: *uuid }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        <PartialEntryDataRaw as RedbValue>::as_bytes(&(value.size, &value.uuid))
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new(std::any::type_name::<Self>())
    }
}

impl PartialEntryData {
    fn new(size: u64, uuid: [u8; 16]) -> Self {
        Self { size, uuid }
    }
}

/// Data about a transient partial entry.
#[derive(Debug)]
struct TransientPartialEntryData {
    // size of the data
    size: u64,
    // data
    data: MutableMemFile,
}

impl TransientPartialEntryData {
    fn new(size: u64) -> Self {
        Self {
            size,
            data: MutableMemFile::default(),
        }
    }
}

impl MapEntry<Store> for PartialEntry {
    fn hash(&self) -> Hash {
        self.hash
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn available_ranges(&self) -> BoxIoFut<ChunkRanges> {
        futures::future::ok(ChunkRanges::all()).boxed()
    }

    fn outboard(&self) -> BoxIoFut<PreOrderOutboard<WriteableBlob>> {
        async move {
            let data = if let Some(outboard) = &self.outboard {
                WriteableBlob::File(outboard.open_read().await?)
            } else {
                WriteableBlob::Mem(Bytes::from(self.size.to_le_bytes().to_vec()))
            };
            Ok(PreOrderOutboard {
                root: self.hash.into(),
                tree: BaoTree::new(ByteNum(self.size), IROH_BLOCK_SIZE),
                data,
            })
        }
        .boxed()
    }

    fn data_reader(&self) -> BoxIoFut<WriteableBlob> {
        self.data.open_read().boxed()
    }

    fn is_complete(&self) -> bool {
        false
    }
}

impl PartialMapEntry<Store> for PartialEntry {
    fn outboard_mut(&self) -> Option<BoxIoFut<PreOrderOutboard<WriteableBlob>>> {
        let hash = self.hash;
        let size = self.size;
        let tree = BaoTree::new(ByteNum(size), IROH_BLOCK_SIZE);
        if let Some(outboard) = self.outboard.clone() {
            Some(
                async move {
                    let mut writer = WriteableBlob::File(outboard.open_write().await?);
                    writer.write_at(0, &size.to_le_bytes()).await?;
                    Ok(PreOrderOutboard {
                        root: hash.into(),
                        tree,
                        data: writer,
                    })
                }
                .boxed(),
            )
        } else {
            None
        }
    }

    fn data_writer(&self) -> BoxIoFut<WriteableBlob> {
        self.data.open_write().boxed()
    }
}

#[derive(Debug)]
struct Options {
    complete_path: PathBuf,
    partial_path: PathBuf,
    meta_path: PathBuf,
    move_threshold: u64,
    outboard_inline_threshold: u64,
}

impl Options {
    fn partial_data_path(&self, hash: Hash, uuid: &[u8; 16]) -> PathBuf {
        self.partial_path
            .join(FileName::PartialData(hash, *uuid).to_string())
    }

    fn partial_outboard_path(&self, hash: Hash, uuid: &[u8; 16]) -> PathBuf {
        self.partial_path
            .join(FileName::PartialOutboard(hash, *uuid).to_string())
    }

    fn owned_data_path(&self, hash: &Hash) -> PathBuf {
        self.complete_path.join(FileName::Data(*hash).to_string())
    }

    fn owned_outboard_path(&self, hash: &Hash) -> PathBuf {
        self.complete_path
            .join(FileName::Outboard(*hash).to_string())
    }
}

#[derive(Debug)]
struct Inner {
    options: Options,
    state: RwLock<State>,
    // mutex for async access to complete files
    //
    // complete files are never written to. They come into existence when a partial
    // entry is completed, and are deleted as a whole.
    complete_io_mutex: Mutex<()>,
    db: Database,
}

/// Table: Partial Index
const PARTIAL_TABLE: TableDefinition<Hash, PartialEntryData> =
    TableDefinition::new("partial-index-0");

/// Table: Full Index
const COMPLETE_TABLE: TableDefinition<Hash, CompleteEntry> =
    TableDefinition::new("complete-index-0");

/// Table: Inlined blobs
const BLOBS_TABLE: TableDefinition<Hash, &[u8]> = TableDefinition::new("blobs-0");

/// Table: Inlined outboards
const OUTBOARDS_TABLE: TableDefinition<Hash, &[u8]> = TableDefinition::new("outboards-0");

/// Table: Tags
const TAGS_TABLE: TableDefinition<Tag, HashAndFormat> = TableDefinition::new("tags-0");

/// Table: Metadata such as version
///
/// Version is stored as a be encoded u64, under the key "version".
const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("meta-0");

/// Key for the version, value is a be encoded u64.
///
/// Version 0 is where there were 3 separate directories for partial, complete and meta.
/// Version 1 moved these into a single directory.
/// Version 2 added the redb database for metadata.
const VERSION_KEY: &str = "version";

/// A generic enum for any resource that can come either from file or memory.
#[derive(Debug, Clone)]
enum MemOrFile<M, F> {
    Mem(M),
    File(F),
}

/// Flat file database implementation.
///
/// This
#[derive(Debug, Clone)]
pub struct Store(Arc<Inner>);
/// The [MapEntry] implementation for [Store].
#[derive(Debug, Clone)]
pub struct Entry {
    /// the hash is not part of the entry itself
    hash: Hash,
    entry: EntryData,
    is_complete: bool,
}

impl MapEntry<Store> for Entry {
    fn hash(&self) -> Hash {
        self.hash
    }

    fn size(&self) -> u64 {
        match &self.entry.data {
            MemOrFile::Mem(bytes) => bytes.len() as u64,
            MemOrFile::File((_, size)) => *size,
        }
    }

    fn available_ranges(&self) -> BoxIoFut<ChunkRanges> {
        futures::future::ok(ChunkRanges::all()).boxed()
    }

    fn outboard(&self) -> BoxIoFut<PreOrderOutboard<WriteableBlob>> {
        async move {
            let size = self.entry.size();
            let data = self.entry.outboard_reader().await?;
            Ok(PreOrderOutboard {
                root: self.hash.into(),
                tree: BaoTree::new(ByteNum(size), IROH_BLOCK_SIZE),
                data,
            })
        }
        .boxed()
    }

    fn data_reader(&self) -> BoxIoFut<WriteableBlob> {
        self.entry.data_reader().boxed()
    }

    fn is_complete(&self) -> bool {
        self.is_complete
    }
}

/// A [`Store`] entry.
///
/// This is either stored externally in the file system, or internally in the database.
///
/// Internally stored entries are stored in the iroh home directory when the database is
/// persisted.
#[derive(Debug, Clone)]
struct EntryData {
    /// The data itself.
    data: MemOrFile<Bytes, (PathBuf, u64)>,
    /// The bao outboard data.
    outboard: MemOrFile<Bytes, PathBuf>,
}

/// A writeable blob for data or outboard data.
///
/// It can be backed by a file or by memory. For the memory case, it can be
/// immutable or mutable.
#[derive(Debug)]
pub enum WriteableBlob {
    /// We got it all in memory
    Mem(Bytes),
    /// We got it all in memory, but it is mutable
    MemMut(MutableMemFile),
    /// An iroh_io::File
    File(File),
}

fn immutable_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "can't write to an immutable mem file",
    )
}

impl AsyncSliceWriter for WriteableBlob {
    type WriteBytesAtFuture<'a> = futures::future::Either<
        <MutableMemFile as AsyncSliceWriter>::WriteBytesAtFuture<'a>,
        <File as AsyncSliceWriter>::WriteBytesAtFuture<'a>,
    >;

    fn write_bytes_at(&mut self, offset: u64, data: Bytes) -> Self::WriteBytesAtFuture<'_> {
        match self {
            Self::Mem(_) => future::err(immutable_error()).left_future(),
            Self::MemMut(mem) => mem.write_bytes_at(offset, data).left_future(),
            Self::File(file) => file.write_bytes_at(offset, data).right_future(),
        }
    }

    type WriteAtFuture<'a> = futures::future::Either<
        <MutableMemFile as AsyncSliceWriter>::WriteAtFuture<'a>,
        <File as AsyncSliceWriter>::WriteAtFuture<'a>,
    >;

    fn write_at<'a>(&'a mut self, offset: u64, data: &'a [u8]) -> Self::WriteAtFuture<'a> {
        match self {
            Self::Mem(_) => future::err(immutable_error()).left_future(),
            Self::MemMut(mem) => mem.write_at(offset, data).left_future(),
            Self::File(file) => file.write_at(offset, data).right_future(),
        }
    }

    type SetLenFuture<'a> = futures::future::Either<
        <MutableMemFile as AsyncSliceWriter>::SetLenFuture<'a>,
        <File as AsyncSliceWriter>::SetLenFuture<'a>,
    >;

    fn set_len(&mut self, len: u64) -> Self::SetLenFuture<'_> {
        match self {
            Self::Mem(_) => future::err(immutable_error()).left_future(),
            Self::MemMut(mem) => mem.set_len(len).left_future(),
            Self::File(file) => file.set_len(len).right_future(),
        }
    }

    type SyncFuture<'a> = futures::future::Either<
        <MutableMemFile as AsyncSliceWriter>::SyncFuture<'a>,
        <File as AsyncSliceWriter>::SyncFuture<'a>,
    >;

    fn sync(&mut self) -> Self::SyncFuture<'_> {
        match self {
            Self::Mem(_) => future::err(immutable_error()).left_future(),
            Self::MemMut(mem) => mem.sync().left_future(),
            Self::File(file) => file.sync().right_future(),
        }
    }
}

impl AsyncSliceReader for WriteableBlob {
    type ReadAtFuture<'a> = futures::future::Either<
        <Bytes as AsyncSliceReader>::ReadAtFuture<'a>,
        <File as AsyncSliceReader>::ReadAtFuture<'a>,
    >;

    fn read_at(&mut self, offset: u64, len: usize) -> Self::ReadAtFuture<'_> {
        match self {
            Self::Mem(mem) => mem.read_at(offset, len).left_future(),
            Self::MemMut(mem) => mem.read_at(offset, len).left_future(),
            Self::File(file) => file.read_at(offset, len).right_future(),
        }
    }

    type LenFuture<'a> = futures::future::Either<
        <Bytes as AsyncSliceReader>::LenFuture<'a>,
        <File as AsyncSliceReader>::LenFuture<'a>,
    >;

    fn len(&mut self) -> Self::LenFuture<'_> {
        match self {
            Self::Mem(mem) => mem.len().left_future(),
            Self::MemMut(mem) => mem.len().left_future(),
            Self::File(file) => file.len().right_future(),
        }
    }
}

impl EntryData {
    /// Get the outboard data for this entry, as a `Bytes`.
    pub fn outboard_reader(&self) -> impl Future<Output = io::Result<WriteableBlob>> + 'static {
        let outboard = self.outboard.clone();
        async move {
            Ok(match outboard {
                MemOrFile::Mem(mem) => WriteableBlob::Mem(mem),
                MemOrFile::File(path) => WriteableBlob::File(File::open(path).await?),
            })
        }
    }

    /// A reader for the data.
    pub fn data_reader(&self) -> impl Future<Output = io::Result<WriteableBlob>> + 'static {
        let data = self.data.clone();
        async move {
            Ok(match data {
                MemOrFile::Mem(mem) => WriteableBlob::Mem(mem),
                MemOrFile::File((path, _)) => WriteableBlob::File(File::open(path).await?),
            })
        }
    }

    /// Returns the size of the blob
    pub fn size(&self) -> u64 {
        match &self.data {
            MemOrFile::Mem(mem) => mem.len() as u64,
            MemOrFile::File((_, size)) => *size,
        }
    }
}

fn needs_outboard(size: u64) -> bool {
    size > (IROH_BLOCK_SIZE.bytes() as u64)
}

#[derive(Debug, Clone)]
enum MemOrFileHandle {
    Mem(MutableMemFile),
    File(FileHandle),
}

impl MemOrFileHandle {
    async fn open_read(&self) -> io::Result<WriteableBlob> {
        Ok(match self {
            Self::Mem(mem) => WriteableBlob::Mem(mem.snapshot()),
            Self::File(file) => WriteableBlob::File(file.open_read().await?),
        })
    }

    async fn open_write(&self) -> io::Result<WriteableBlob> {
        Ok(match self {
            Self::Mem(mem) => WriteableBlob::MemMut(mem.clone()),
            Self::File(file) => WriteableBlob::File(file.open_write().await?),
        })
    }
}

#[derive(Debug, Clone)]
struct FileHandle(Arc<PathBuf>);

impl AsRef<Path> for FileHandle {
    fn as_ref(&self) -> &Path {
        self.0.as_ref()
    }
}

impl FileHandle {
    fn new(path: PathBuf) -> Self {
        Self(Arc::new(path))
    }

    async fn open_read(&self) -> io::Result<File> {
        let path = self.0.clone();
        File::create(move || std::fs::OpenOptions::new().read(true).open(path.as_ref())).await
    }

    async fn open_write(&self) -> io::Result<File> {
        let path = self.0.clone();
        File::create(move || {
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(path.as_ref())
        })
        .await
    }
}

/// The [PartialMapEntry] implementation for [Store].
#[derive(Debug, Clone)]
pub struct PartialEntry {
    hash: Hash,
    size: u64,
    data: MemOrFileHandle,
    outboard: Option<FileHandle>,
}

impl Map for Store {
    type Entry = Entry;
    type Outboard = PreOrderOutboard<WriteableBlob>;
    type DataReader = WriteableBlob;

    fn get(&self, hash: &Hash) -> io::Result<Option<Self::Entry>> {
        self.get_impl(hash)
    }
}

impl PartialMap for Store {
    type OutboardMut = PreOrderOutboard<WriteableBlob>;

    type DataWriter = WriteableBlob;

    type PartialEntry = PartialEntry;

    fn entry_status(&self, hash: &Hash) -> io::Result<EntryStatus> {
        self.entry_status_impl(hash)
    }

    fn get_possibly_partial(&self, hash: &Hash) -> io::Result<PossiblyPartialEntry<Self>> {
        self.get_possibly_partial_impl(hash)
    }

    fn get_or_create_partial(&self, hash: Hash, size: u64) -> io::Result<Self::PartialEntry> {
        self.get_or_create_partial_impl(hash, size)
    }

    fn insert_complete(&self, entry: Self::PartialEntry) -> BoxIoFut<()> {
        let this = self.clone();
        asyncify(move || this.insert_complete_impl(entry)).boxed()
    }
}

impl ReadableStore for Store {
    fn blobs(&self) -> io::Result<DbIter<Hash>> {
        let read_tx = self.0.db.begin_read().err_to_io()?;
        // TODO: avoid allocation
        let items: Vec<_> = {
            let full_table = read_tx.open_table(COMPLETE_TABLE).err_to_io()?;
            let iter = full_table.iter().err_to_io()?;
            iter.map(|r| r.map(|(k, _)| k.value()).err_to_io())
                .collect()
        };

        Ok(Box::new(items.into_iter()))
    }

    fn temp_tags(&self) -> Box<dyn Iterator<Item = HashAndFormat> + Send + Sync + 'static> {
        let inner = self.0.state.read().unwrap();
        let items = inner.temp.keys();
        Box::new(items)
    }

    fn tags(&self) -> io::Result<DbIter<(Tag, HashAndFormat)>> {
        let inner = self.0.db.begin_read().err_to_io()?;
        let tags_table = inner.open_table(TAGS_TABLE).err_to_io()?;
        let items = tags_table
            .iter()
            .err_to_io()?
            .map(|item| item.map(|(k, v)| (k.value(), v.value())).err_to_io())
            .collect::<Vec<_>>();
        Ok(Box::new(items.into_iter()))
    }

    fn validate(&self, _tx: mpsc::Sender<ValidateProgress>) -> BoxFuture<'_, anyhow::Result<()>> {
        unimplemented!()
    }

    fn partial_blobs(&self) -> io::Result<DbIter<Hash>> {
        let read_tx = self.0.db.begin_read().err_to_io()?;

        // TODO: avoid allocation
        let mut items: Vec<_> = {
            let partial_table = read_tx.open_table(PARTIAL_TABLE).err_to_io()?;
            let iter = partial_table.iter().err_to_io()?;
            iter.map(|r| r.map(|(k, _)| k.value()).err_to_io())
                .collect()
        };
        for item in self.0.state.read().unwrap().partial.keys() {
            items.push(Ok(*item));
        }
        Ok(Box::new(items.into_iter()))
    }

    fn export(
        &self,
        hash: Hash,
        target: PathBuf,
        mode: ExportMode,
        progress: impl Fn(u64) -> io::Result<()> + Send + Sync + 'static,
    ) -> BoxIoFut<()> {
        let this = self.clone();
        asyncify(move || this.export_impl(hash, target, mode, progress)).boxed()
    }
}

impl super::Store for Store {
    fn import_file(
        &self,
        path: PathBuf,
        mode: ImportMode,
        format: BlobFormat,
        progress: impl ProgressSender<Msg = ImportProgress> + IdGenerator,
    ) -> BoxIoFut<(TempTag, u64)> {
        let this = self.clone();
        asyncify(move || this.import_file_impl(path, mode, format, progress)).boxed()
    }

    fn import_bytes(&self, data: Bytes, format: BlobFormat) -> BoxIoFut<TempTag> {
        let this = self.clone();
        asyncify(move || this.import_bytes_impl(data, format)).boxed()
    }

    fn import_stream(
        &self,
        mut data: impl Stream<Item = io::Result<Bytes>> + Unpin + Send + 'static,
        format: BlobFormat,
        progress: impl ProgressSender<Msg = ImportProgress> + IdGenerator,
    ) -> BoxIoFut<(TempTag, u64)> {
        let this = self.clone();
        async move {
            let id = progress.new_id();
            // write to a temp file
            let temp_data_path = this.temp_path();
            let name = temp_data_path
                .file_name()
                .expect("just created")
                .to_string_lossy()
                .to_string();
            progress.send(ImportProgress::Found { id, name }).await?;
            let mut writer = tokio::fs::File::create(&temp_data_path).await?;
            let mut offset = 0;
            while let Some(chunk) = data.next().await {
                let chunk = chunk?;
                writer.write_all(&chunk).await?;
                offset += chunk.len() as u64;
                progress.try_send(ImportProgress::CopyProgress { id, offset })?;
            }
            writer.flush().await?;
            drop(writer);
            let file = ImportData::TempFile(temp_data_path);
            asyncify(move || this.finalize_import_impl(file, format, id, progress)).await
        }
        .boxed()
    }

    fn create_tag(&self, value: HashAndFormat) -> BoxIoFut<Tag> {
        let this = self.clone();
        asyncify(move || this.create_tag_impl(value)).boxed()
    }

    fn set_tag(&self, name: Tag, value: Option<HashAndFormat>) -> BoxIoFut<()> {
        let this = self.clone();
        asyncify(move || this.set_tag_impl(name, value)).boxed()
    }

    fn temp_tag(&self, tag: HashAndFormat) -> TempTag {
        TempTag::new(tag, Some(self.0.clone()))
    }

    fn clear_live(&self) {
        let mut state = self.0.state.write().unwrap();
        state.live.clear();
    }

    fn add_live(&self, elements: impl IntoIterator<Item = Hash>) {
        let mut state = self.0.state.write().unwrap();
        state.live.extend(elements);
    }

    fn is_live(&self, hash: &Hash) -> bool {
        let state = self.0.state.read().unwrap();
        // a blob is live if it is either in the live set, or it is temp tagged
        state.live.contains(hash) || state.temp.contains(hash)
    }

    fn delete(&self, hashes: Vec<Hash>) -> BoxIoFut<()> {
        tracing::debug!("delete: {:?}", hashes);
        let this = self.clone();
        asyncify(move || this.delete_impl(hashes)).boxed()
    }
}

impl LivenessTracker for Inner {
    fn on_clone(&self, inner: &HashAndFormat) {
        tracing::trace!("temp tagging: {:?}", inner);
        let mut state = self.state.write().unwrap();
        state.temp.inc(inner);
    }

    fn on_drop(&self, inner: &HashAndFormat) {
        tracing::trace!("temp tag drop: {:?}", inner);
        let mut state = self.state.write().unwrap();
        state.temp.dec(inner)
    }
}

/// Data to be imported
enum ImportData {
    TempFile(PathBuf),
    External(PathBuf),
}

impl ImportData {
    fn path(&self) -> &Path {
        match self {
            Self::TempFile(path) => path.as_path(),
            Self::External(path) => path.as_path(),
        }
    }
}

impl Store {
    fn temp_path(&self) -> PathBuf {
        self.0.options.partial_path.join(temp_name())
    }

    fn import_file_impl(
        self,
        path: PathBuf,
        mode: ImportMode,
        format: BlobFormat,
        progress: impl ProgressSender<Msg = ImportProgress> + IdGenerator,
    ) -> io::Result<(TempTag, u64)> {
        if !path.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "path must be absolute",
            ));
        }
        if !path.is_file() && !path.is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "path is not a file or symlink",
            ));
        }
        let id = progress.new_id();
        progress.blocking_send(ImportProgress::Found {
            id,
            name: path.to_string_lossy().to_string(),
        })?;
        let file = match mode {
            ImportMode::TryReference => ImportData::External(path),
            ImportMode::Copy => {
                let temp_path = self.temp_path();
                // copy the data, since it is not stable
                progress.try_send(ImportProgress::CopyProgress { id, offset: 0 })?;
                if reflink_copy::reflink_or_copy(&path, &temp_path)?.is_none() {
                    tracing::debug!("reflinked {} to {}", path.display(), temp_path.display());
                } else {
                    tracing::debug!("copied {} to {}", path.display(), temp_path.display());
                }
                ImportData::TempFile(temp_path)
            }
        };
        let (tag, size) = self.finalize_import_impl(file, format, id, progress)?;
        Ok((tag, size))
    }

    fn import_bytes_impl(&self, data: Bytes, format: BlobFormat) -> io::Result<TempTag> {
        let temp_data_path = self.temp_path();
        std::fs::write(&temp_data_path, &data)?;
        let id = 0;
        let file = ImportData::TempFile(temp_data_path);
        let progress = IgnoreProgressSender::default();
        let (tag, _size) = self.finalize_import_impl(file, format, id, progress)?;
        Ok(tag)
    }

    fn finalize_import_impl(
        &self,
        file: ImportData,
        format: BlobFormat,
        id: u64,
        progress: impl ProgressSender<Msg = ImportProgress>,
    ) -> io::Result<(TempTag, u64)> {
        let size = file.path().metadata()?.len();
        progress.blocking_send(ImportProgress::Size { id, size })?;
        let progress2 = progress.clone();
        let (hash, outboard) = compute_outboard(file.path(), size, move |offset| {
            Ok(progress2.try_send(ImportProgress::OutboardProgress { id, offset })?)
        })?;
        progress.blocking_send(ImportProgress::OutboardDone { id, hash })?;
        use super::Store;
        // from here on, everything related to the hash is protected by the temp tag
        let tag = self.temp_tag(HashAndFormat { hash, format });
        let hash = *tag.hash();
        let outboard = if let Some(outboard) = outboard {
            Some(
                if outboard.len() <= self.0.options.outboard_inline_threshold as usize {
                    MemOrFile::Mem(outboard)
                } else {
                    let uuid = new_uuid();
                    // we write the outboard to a temp file first, since while it is being written it is not complete.
                    // it is protected from deletion by the temp tag.
                    let temp_outboard_path = self.0.options.partial_outboard_path(hash, &uuid);
                    std::fs::write(&temp_outboard_path, outboard)?;
                    MemOrFile::File(temp_outboard_path)
                },
            )
        } else {
            None
        };
        // load the data file into memory if it is small enough to not need an outboard
        //
        // todo: compute outboard from memory if the data is small enough
        let data = if outboard.is_none() {
            Some(match &file {
                ImportData::External(path) => std::fs::read(path)?,
                ImportData::TempFile(path) => std::fs::read(path)?,
            })
        } else {
            None
        };
        // before here we did not touch the complete files at all.
        // all writes here are protected by the temp tag
        let complete_io_guard = self.0.complete_io_mutex.lock().unwrap();
        // move the data file into place, or create a reference to it
        let new = match file {
            ImportData::External(path) => CompleteEntry::new_external(size, path),
            ImportData::TempFile(temp_data_path) => {
                let data_path = self.owned_data_path(&hash);
                std::fs::rename(temp_data_path, data_path)?;
                CompleteEntry::new_default(size)
            }
        };
        // move the outboard file into place if we have one
        if let Some(MemOrFile::File(temp_outboard_path)) = &outboard {
            let outboard_path = self.owned_outboard_path(&hash);
            std::fs::rename(temp_outboard_path, outboard_path)?;
        }
        let size = new.size;

        let write_tx = self.0.db.begin_write().err_to_io()?;
        {
            let mut full_table = write_tx.open_table(COMPLETE_TABLE).err_to_io()?;
            let mut entry = match full_table.get(&hash).err_to_io()? {
                Some(e) => e.value(),
                None => CompleteEntry::default(),
            };
            entry.union_with(new)?;
            full_table.insert(hash, &entry).err_to_io()?;
            if let Some(data) = data {
                let mut blobs_table = write_tx.open_table(BLOBS_TABLE).err_to_io()?;
                blobs_table.insert(hash, data.as_slice()).err_to_io()?;
            }
            if let Some(MemOrFile::Mem(outboard)) = outboard {
                let mut outboards_table: redb::Table<'_, '_, Hash, &[u8]> =
                    write_tx.open_table(OUTBOARDS_TABLE).err_to_io()?;
                outboards_table
                    .insert(hash, outboard.as_slice())
                    .err_to_io()?;
            }
        }
        write_tx.commit().err_to_io()?;

        drop(complete_io_guard);
        Ok((tag, size))
    }

    fn set_tag_impl(&self, name: Tag, value: Option<HashAndFormat>) -> io::Result<()> {
        tracing::debug!("set_tag {} {:?}", name, value);
        let txn = self.0.db.begin_write().err_to_io()?;
        {
            let mut tags = txn.open_table(TAGS_TABLE).err_to_io()?;
            if let Some(target) = value {
                tags.insert(name, target)
            } else {
                tags.remove(name)
            }
            .err_to_io()?;
        }
        txn.commit().err_to_io()?;
        Ok(())
    }

    fn create_tag_impl(&self, value: HashAndFormat) -> io::Result<Tag> {
        tracing::debug!("create_tag {:?}", value);
        let txn = self.0.db.begin_write().err_to_io()?;
        let tag = {
            let mut tags = txn.open_table(TAGS_TABLE).err_to_io()?;
            let tag = Tag::auto(SystemTime::now(), |t| {
                tags.get(Tag(Bytes::copy_from_slice(t)))
                    .map(|x| x.is_some())
            })
            .err_to_io()?;
            tags.insert(&tag, value).err_to_io()?;
            tag
        };
        txn.commit().err_to_io()?;
        Ok(tag)
    }

    fn delete_impl(&self, hashes: Vec<Hash>) -> io::Result<()> {
        let mut data = Vec::new();
        let mut outboard = Vec::new();
        let mut partial_data = Vec::new();
        let mut partial_outboard = Vec::new();
        let complete_io_guard = self.0.complete_io_mutex.lock().unwrap();

        let write_tx = self.0.db.begin_write().err_to_io()?;
        {
            let mut full_table = write_tx.open_table(COMPLETE_TABLE).err_to_io()?;
            let mut partial_table = write_tx.open_table(PARTIAL_TABLE).err_to_io()?;
            let mut blobs_table = write_tx.open_table(BLOBS_TABLE).err_to_io()?;
            for hash in hashes.iter().copied() {
                if let Some(entry) = full_table.remove(hash).err_to_io()? {
                    let entry = entry.value();
                    if entry.owned_data {
                        data.push(self.owned_data_path(&hash));
                    }
                    if needs_outboard(entry.size) {
                        outboard.push(self.owned_outboard_path(&hash));
                    }
                }
                let e = partial_table.remove(hash).err_to_io()?;
                if let Some(partial) = e {
                    let partial = partial.value();
                    partial_data.push(self.0.options.partial_data_path(hash, &partial.uuid));
                    if needs_outboard(partial.size) {
                        partial_outboard
                            .push(self.0.options.partial_outboard_path(hash, &partial.uuid));
                    }
                }
                blobs_table.remove(hash).err_to_io()?;
            }
        }
        write_tx.commit().err_to_io()?;

        for data in data {
            tracing::debug!("deleting data {}", data.display());
            if let Err(cause) = std::fs::remove_file(data) {
                tracing::warn!("failed to delete data file: {}", cause);
            }
        }
        for outboard in outboard {
            tracing::debug!("deleting outboard {}", outboard.display());
            if let Err(cause) = std::fs::remove_file(outboard) {
                tracing::warn!("failed to delete outboard file: {}", cause);
            }
        }
        drop(complete_io_guard);
        // deleting the partial data and outboard files can happen at any time.
        // there is no race condition since these are unique names.
        for partial_data in partial_data {
            if let Err(cause) = std::fs::remove_file(partial_data) {
                tracing::warn!("failed to delete partial data file: {}", cause);
            }
        }
        for partial_outboard in partial_outboard {
            if let Err(cause) = std::fs::remove_file(partial_outboard) {
                tracing::warn!("failed to delete partial outboard file: {}", cause);
            }
        }
        Ok(())
    }

    fn get_or_create_partial_impl(&self, hash: Hash, size: u64) -> io::Result<PartialEntry> {
        let mut state = self.0.state.write().unwrap();
        // this protects the entry from being deleted until the next mark phase
        //
        // example: a collection containing this hash is temp tagged, but
        // we did not have the collection at the time of the mark phase.
        //
        // now we get the collection and it's child between the mark and the sweep
        // phase. the child is not in the live set and will be deleted.
        //
        // this prevents this from happening until the live set is cleared at the
        // beginning of the next mark phase, at which point this hash is normally
        // reachable.
        tracing::debug!("protecting partial hash {}", hash);
        state.live.insert(hash);

        Ok(if !needs_outboard(size) {
            // size is smaller than a block, so we keep it transient in memory.
            //
            // after a crash it will be gone, but that is ok since it is small.
            let file = state
                .partial
                .entry(hash)
                .or_insert_with(|| TransientPartialEntryData::new(size))
                .data
                .clone();
            PartialEntry {
                hash,
                size,
                data: MemOrFileHandle::Mem(file),
                outboard: None,
            }
        } else {
            // size is larger than a block, so both data and outboard need to be stored in a temp file.
            // they will be written to incrementally, and we want to retain partial data after a crash.
            let write_tx = self.0.db.begin_write().err_to_io()?;
            let entry = {
                let mut partial_table = write_tx.open_table(PARTIAL_TABLE).err_to_io()?;
                // we need to do this in two steps, since during the match the table is borrowed immutably
                let (entry, needs_insert) = match partial_table.get(hash).err_to_io()? {
                    Some(entry) => (entry.value(), false),
                    None => (PartialEntryData::new(size, new_uuid()), true),
                };

                if needs_insert {
                    partial_table.insert(hash, &entry).err_to_io()?;
                }
                entry
            };
            write_tx.commit().err_to_io()?;

            let data_path = self.0.options.partial_data_path(hash, &entry.uuid);
            let outboard_path = Some(self.0.options.partial_outboard_path(hash, &entry.uuid));
            PartialEntry {
                hash,
                size: entry.size,
                data: MemOrFileHandle::File(FileHandle::new(data_path)),
                outboard: outboard_path.map(FileHandle::new),
            }
        })
    }

    fn insert_complete_impl(&self, entry: PartialEntry) -> io::Result<()> {
        let hash = entry.hash;
        let size = entry.size;
        match entry.data {
            MemOrFileHandle::Mem(data) => {
                // for a short time we will have neither partial nor complete
                let mut state = self.0.state.write().unwrap();
                state.partial.remove(&entry.hash);
                drop(state);
                let write_tx = self.0.db.begin_write().err_to_io()?;
                {
                    let mut complete_table = write_tx.open_table(COMPLETE_TABLE).err_to_io()?;
                    let mut blobs_table = write_tx.open_table(BLOBS_TABLE).err_to_io()?;
                    let mut entry = match complete_table.get(hash).err_to_io()? {
                        Some(entry) => entry.value(),
                        None => CompleteEntry::default(),
                    };
                    entry.union_with(CompleteEntry::new_default(size))?;
                    complete_table.insert(hash, entry).err_to_io()?;
                    blobs_table
                        .insert(hash, data.freeze().as_ref())
                        .err_to_io()?;
                }
                write_tx.commit().err_to_io()?;
            }
            MemOrFileHandle::File(temp_data_path) => {
                // for a short time we will have neither partial nor complete
                let data_path = self.0.options.owned_data_path(&hash);
                let temp_outboard_path = entry.outboard;
                let complete_io_guard = self.0.complete_io_mutex.lock().unwrap();
                let write_tx = self.0.db.begin_write().err_to_io()?;
                {
                    let mut partial_table = write_tx.open_table(PARTIAL_TABLE).err_to_io()?;
                    partial_table.remove(hash).err_to_io()?;
                }
                write_tx.commit().err_to_io()?;

                std::fs::rename(temp_data_path, data_path)?;
                let inline_outboard = if let Some(temp_outboard_path) = temp_outboard_path {
                    if outboard_size(size, IROH_BLOCK_SIZE)
                        <= self.0.options.outboard_inline_threshold
                    {
                        let outboard = std::fs::read(&temp_outboard_path)?;
                        std::fs::remove_file(temp_outboard_path)?;
                        Some(outboard)
                    } else {
                        let outboard_path = self.0.options.owned_outboard_path(&hash);
                        std::fs::rename(temp_outboard_path, outboard_path)?;
                        None
                    }
                } else {
                    None
                };
                let write_tx = self.0.db.begin_write().err_to_io()?;
                {
                    let mut complete_table = write_tx.open_table(COMPLETE_TABLE).err_to_io()?;
                    let mut entry = match complete_table.get(hash).err_to_io()? {
                        Some(entry) => entry.value(),
                        None => CompleteEntry::default(),
                    };
                    entry.union_with(CompleteEntry::new_default(size))?;
                    complete_table.insert(hash, entry).err_to_io()?;
                    if let Some(outboard) = inline_outboard {
                        let mut outboards_table =
                            write_tx.open_table(OUTBOARDS_TABLE).err_to_io()?;
                        outboards_table
                            .insert(hash, outboard.as_slice())
                            .err_to_io()?;
                    }
                }
                write_tx.commit().err_to_io()?;
                drop(complete_io_guard);
            }
        }
        Ok(())
    }

    fn export_impl(
        &self,
        hash: Hash,
        target: PathBuf,
        mode: ExportMode,
        progress: impl Fn(u64) -> io::Result<()> + Send + Sync + 'static,
    ) -> io::Result<()> {
        tracing::trace!("exporting {} to {} ({:?})", hash, target.display(), mode);

        if !target.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "target path must be absolute",
            ));
        }
        let parent = target.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "target path has no parent directory",
            )
        })?;
        // create the directory in which the target file is
        std::fs::create_dir_all(parent)?;
        let (source, size, owned) = {
            let read_tx = self.0.db.begin_read().err_to_io()?;
            let blobs_table = read_tx.open_table(BLOBS_TABLE).err_to_io()?;
            if let Some(data) = blobs_table.get(hash).err_to_io()? {
                std::fs::write(target, data.value())?;
                return Ok(());
            }
            let full_table = read_tx.open_table(COMPLETE_TABLE).err_to_io()?;
            let Some(entry) = full_table.get(hash).err_to_io()? else {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "hash not found in database",
                ));
            };
            let entry = entry.value();
            let source = if entry.owned_data {
                self.owned_data_path(&hash)
            } else {
                entry
                    .external
                    .iter()
                    .next()
                    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no valid path found"))?
                    .clone()
            };
            (source, entry.size, entry.owned_data)
        };
        // copy all the things
        let stable = mode == ExportMode::TryReference;
        if size >= self.0.options.move_threshold && stable && owned {
            tracing::debug!("moving {} to {}", source.display(), target.display());
            if let Err(e) = std::fs::rename(source, &target) {
                tracing::error!("rename failed: {}", e);
                return Err(e)?;
            }

            let write_tx = self.0.db.begin_write().err_to_io()?;
            {
                let mut full_table = write_tx.open_table(COMPLETE_TABLE).err_to_io()?;
                let Some(e) = full_table.get(hash).err_to_io()? else {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        "hash not found in database",
                    ));
                };

                let mut entry = e.value();
                drop(e);
                entry.external.insert(target);
                full_table.insert(hash, entry).err_to_io()?;
            }
            write_tx.commit().err_to_io()?;
        } else {
            tracing::debug!("copying {} to {}", source.display(), target.display());
            progress(0)?;
            // todo: progress
            if reflink_copy::reflink_or_copy(&source, &target)?.is_none() {
                tracing::debug!("reflinked {} to {}", source.display(), target.display());
            } else {
                tracing::debug!("copied {} to {}", source.display(), target.display());
            }
            progress(size)?;

            if mode == ExportMode::TryReference {
                let write_tx = self.0.db.begin_write().err_to_io()?;
                {
                    let mut full_table = write_tx.open_table(COMPLETE_TABLE).err_to_io()?;
                    let Some(e) = full_table.get(hash).err_to_io()? else {
                        return Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            "hash not found in database",
                        ));
                    };

                    let mut entry = e.value();
                    drop(e);
                    entry.external.insert(target);
                    full_table.insert(hash, entry).err_to_io()?;
                }
                write_tx.commit().err_to_io()?;
            }
        }

        Ok(())
    }

    /// Path to the directory where complete files and outboard files are stored.
    pub(crate) fn complete_path(root: &Path) -> PathBuf {
        root.join("complete")
    }

    /// Path to the directory where partial files and outboard are stored.
    pub(crate) fn partial_path(root: &Path) -> PathBuf {
        root.join("partial")
    }

    /// Path to the directory where metadata is stored.
    pub(crate) fn meta_path(root: &Path) -> PathBuf {
        root.join("meta")
    }

    /// Path to the redb file where is stored.
    pub(crate) fn db_path(root: &Path) -> PathBuf {
        Self::meta_path(root).join("db.v1")
    }

    /// scan a directory for data
    pub(crate) fn load_impl(path: &Path) -> anyhow::Result<Self> {
        tracing::info!("loading database from {}", path.display(),);
        let complete_path = Self::complete_path(path);
        let partial_path = Self::partial_path(path);
        let meta_path = Self::meta_path(path);
        let db_path = Self::db_path(path);
        let options = Options {
            complete_path,
            partial_path,
            meta_path,
            move_threshold: 1024 * 128,
            outboard_inline_threshold: 1024 * 4 + 8,
        };
        let needs_v1_v2_migration = !db_path.exists()
            && (options.complete_path.exists()
                || options.partial_path.exists()
                || options.meta_path.exists());

        std::fs::create_dir_all(&options.complete_path)?;
        std::fs::create_dir_all(&options.partial_path)?;
        std::fs::create_dir_all(&options.meta_path)?;

        if needs_v1_v2_migration {
            // create the db in a temp file, then delete files that are no longer needed
            // and move it into place.
            let temp_path = Self::meta_path(path).join("db.v1.tmp");
            let db = Database::create(&temp_path)?;
            let write_tx = db.begin_write()?;
            let to_delete = Self::init_meta_from_files(&options, &write_tx)?;
            write_tx.commit()?;
            drop(db);
            for path in to_delete {
                std::fs::remove_file(path)?;
            }
            std::fs::rename(&temp_path, &db_path)?;
        }
        let db = Database::create(db_path)?;
        // create tables if they don't exist
        let write_tx = db.begin_write()?;
        {
            let _table = write_tx.open_table(PARTIAL_TABLE)?;
            let _table = write_tx.open_table(COMPLETE_TABLE)?;
            let _table = write_tx.open_table(TAGS_TABLE)?;
            let _table = write_tx.open_table(BLOBS_TABLE)?;
            let _table = write_tx.open_table(OUTBOARDS_TABLE)?;
            let mut meta_table = write_tx.open_table(META_TABLE)?;
            if let Some(version) = Self::db_version(&meta_table)? {
                anyhow::ensure!(version == 2, "unsupported database version: {}", version);
            } else {
                Self::set_db_version(&mut meta_table, 2)?;
            }
        }
        write_tx.commit()?;

        let res = Self(Arc::new(Inner {
            state: RwLock::new(State {
                live: Default::default(),
                temp: Default::default(),
                partial: Default::default(),
            }),
            options,
            complete_io_mutex: Mutex::new(()),
            db,
        }));

        Ok(res)
    }

    fn set_db_version(
        table: &mut redb::Table<&'static str, &'static [u8]>,
        value: u64,
    ) -> io::Result<()> {
        table
            .insert(VERSION_KEY, value.to_be_bytes().as_slice())
            .err_to_io()?;
        Ok(())
    }

    fn db_version(
        table: &impl redb::ReadableTable<&'static str, &'static [u8]>,
    ) -> io::Result<Option<u64>> {
        Ok(if let Some(version) = table.get(VERSION_KEY).err_to_io()? {
            let Ok(value) = version.value().try_into() else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected version size",
                ));
            };
            Some(u64::from_be_bytes(value))
        } else {
            None
        })
    }

    /// Scan the data directories for data files.
    ///
    /// The type of each file can be inferred from its name. So the result of this
    /// function represents the actual content of the data directories, no matter
    /// what is in the database.
    #[allow(clippy::type_complexity)]
    fn scan_data_files(
        options: &Options,
    ) -> anyhow::Result<(
        BTreeMap<Hash, CompleteEntry>,
        BTreeMap<Hash, PartialEntryData>,
        Vec<PathBuf>,
    )> {
        let complete_path = &options.complete_path;
        let partial_path = &options.partial_path;

        let mut partial_index =
            BTreeMap::<Hash, BTreeMap<[u8; 16], (Option<PathBuf>, Option<PathBuf>)>>::new();
        let mut full_index =
            BTreeMap::<Hash, (Option<PathBuf>, Option<PathBuf>, Option<PathBuf>)>::new();
        for entry in std::fs::read_dir(partial_path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                let Some(name) = path.file_name() else {
                    tracing::warn!("skipping unexpected partial file: {:?}", path);
                    continue;
                };
                let Some(name) = name.to_str() else {
                    tracing::warn!("skipping unexpected partial file: {:?}", path);
                    continue;
                };
                if let Ok(purpose) = FileName::from_str(name) {
                    match purpose {
                        FileName::PartialData(hash, uuid) => {
                            let m = partial_index.entry(hash).or_default();
                            let (data, _) = m.entry(uuid).or_default();
                            *data = Some(path);
                        }
                        FileName::PartialOutboard(hash, uuid) => {
                            let m = partial_index.entry(hash).or_default();
                            let (_, outboard) = m.entry(uuid).or_default();
                            *outboard = Some(path);
                        }
                        _ => {
                            // silently ignore other files, there could be a valid reason for them
                        }
                    }
                }
            }
        }

        for entry in std::fs::read_dir(complete_path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                let Some(name) = path.file_name() else {
                    tracing::warn!("skipping unexpected complete file: {:?}", path);
                    continue;
                };
                let Some(name) = name.to_str() else {
                    tracing::warn!("skipping unexpected complete file: {:?}", path);
                    continue;
                };
                if let Ok(purpose) = FileName::from_str(name) {
                    match purpose {
                        FileName::Data(hash) => {
                            let (data, _, _) = full_index.entry(hash).or_default();
                            *data = Some(path);
                        }
                        FileName::Outboard(hash) => {
                            let (_, outboard, _) = full_index.entry(hash).or_default();
                            *outboard = Some(path);
                        }
                        FileName::Paths(hash) => {
                            let (_, _, paths) = full_index.entry(hash).or_default();
                            *paths = Some(path);
                        }
                        _ => {
                            // silently ignore other files, there could be a valid reason for them
                        }
                    }
                }
            }
        }
        // figure out what we have completely
        let mut complete = BTreeMap::new();
        let mut path_files = Vec::new();
        for (hash, (data_path, outboard_path, paths_path)) in full_index {
            let external: BTreeSet<PathBuf> = if let Some(paths_path) = paths_path {
                let paths = std::fs::read(&paths_path)?;
                path_files.push(paths_path);
                postcard::from_bytes(&paths)?
            } else {
                Default::default()
            };
            let owned_data = data_path.is_some();
            let size = if let Some(data_path) = &data_path {
                let Ok(meta) = std::fs::metadata(data_path) else {
                    tracing::warn!(
                        "unable to open owned data file {}. removing {}",
                        data_path.display(),
                        hex::encode(hash)
                    );
                    continue;
                };
                meta.len()
            } else if let Some(external) = external.iter().next() {
                let Ok(meta) = std::fs::metadata(external) else {
                    tracing::warn!(
                        "unable to open external data file {}. removing {}",
                        external.display(),
                        hex::encode(hash)
                    );
                    continue;
                };
                meta.len()
            } else {
                tracing::error!(
                    "neither internal nor external file exists. removing {}",
                    hex::encode(hash)
                );
                continue;
            };
            if needs_outboard(size) {
                if let Some(outboard_path) = outboard_path {
                    anyhow::ensure!(
                        outboard_path.exists(),
                        "missing outboard file for {}",
                        hex::encode(hash)
                    );
                } else {
                    tracing::error!("missing outboard file for {}", hex::encode(hash));
                    // we could delete the data file here
                    continue;
                }
            }
            complete.insert(
                hash,
                CompleteEntry {
                    owned_data,
                    external,
                    size,
                },
            );
        }
        // retain only entries for which we have both outboard and data
        partial_index.retain(|hash, entries| {
            entries.retain(|uuid, (data, outboard)| match (data, outboard) {
                (Some(_), Some(_)) => true,
                (Some(data), None) => {
                    tracing::warn!(
                        "missing partial outboard file for {} {}",
                        hex::encode(hash),
                        hex::encode(uuid)
                    );
                    std::fs::remove_file(data).ok();
                    false
                }
                (None, Some(outboard)) => {
                    tracing::warn!(
                        "missing partial data file for {} {}",
                        hex::encode(hash),
                        hex::encode(uuid)
                    );
                    std::fs::remove_file(outboard).ok();
                    false
                }
                _ => false,
            });
            !entries.is_empty()
        });
        let mut partial = BTreeMap::new();
        for (hash, entries) in partial_index {
            let best = if !complete.contains_key(&hash) {
                entries
                    .iter()
                    .filter_map(|(uuid, (data_path, outboard_path))| {
                        let data_path = data_path.as_ref()?;
                        let outboard_path = outboard_path.as_ref()?;
                        let Ok(data_meta) = std::fs::metadata(data_path) else {
                            tracing::warn!(
                                "unable to open partial data file {}",
                                data_path.display()
                            );
                            return None;
                        };
                        let Ok(outboard_file) = std::fs::File::open(outboard_path) else {
                            tracing::warn!(
                                "unable to open partial outboard file {}",
                                outboard_path.display()
                            );
                            return None;
                        };
                        let mut expected_size = [0u8; 8];
                        let Ok(_) = outboard_file.read_at(0, &mut expected_size) else {
                            tracing::warn!(
                                "partial outboard file is missing length {}",
                                outboard_path.display()
                            );
                            return None;
                        };
                        let current_size = data_meta.len();
                        let expected_size = u64::from_le_bytes(expected_size);
                        Some((current_size, expected_size, uuid))
                    })
                    .max_by_key(|x| x.0)
            } else {
                None
            };
            if let Some((current_size, expected_size, uuid)) = best {
                if current_size > 0 {
                    partial.insert(
                        hash,
                        PartialEntryData {
                            size: expected_size,
                            uuid: *uuid,
                        },
                    );
                }
            }
            // remove all other entries
            let keep = partial.get(&hash).map(|x| x.uuid);
            for (uuid, (data_path, outboard_path)) in entries {
                if Some(uuid) != keep {
                    if let Some(data_path) = data_path {
                        tracing::debug!("removing partial data file {}", data_path.display());
                        std::fs::remove_file(data_path)?;
                    }
                    if let Some(outboard_path) = outboard_path {
                        tracing::debug!(
                            "removing partial outboard file {}",
                            outboard_path.display()
                        );
                        std::fs::remove_file(outboard_path)?;
                    }
                }
            }
        }
        for hash in complete.keys() {
            tracing::debug!("complete {}", hash);
            partial.remove(hash);
        }
        for hash in partial.keys() {
            tracing::info!("partial {}", hash);
        }
        Ok((complete, partial, path_files))
    }

    /// scan a directory for data and replace the database content with the ground truth
    /// from disk.
    pub fn sync_meta_from_files(&self) -> anyhow::Result<()> {
        let (mut complete, partial, _path_files) = Self::scan_data_files(&self.0.options)?;

        let txn = self.0.db.begin_write()?;
        {
            let mut complete_table = txn.open_table(COMPLETE_TABLE)?;
            let mut partial_table = txn.open_table(PARTIAL_TABLE)?;
            // get the external paths from the database before nuking it
            for item in complete_table.iter()? {
                let (k, v) = item?;
                let mut v = v.value();
                if !v.external.is_empty() {
                    v.owned_data = false;
                    let key = k.value();
                    let entry = complete.entry(key).or_default();
                    entry.union_with(v)?;
                }
            }
            complete_table.drain::<Hash>(..)?;
            partial_table.drain::<Hash>(..)?;
            for (hash, entry) in complete {
                complete_table.insert(hash, entry)?;
            }
            for (hash, entry) in partial {
                partial_table.insert(hash, entry)?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Init the database from the files on disk, including tags.
    fn init_meta_from_files(
        options: &Options,
        txn: &WriteTransaction,
    ) -> anyhow::Result<Vec<PathBuf>> {
        let meta_path = &options.meta_path;
        let tags_path = meta_path.join("tags.meta");
        let mut tags = BTreeMap::<Tag, HashAndFormat>::new();
        if tags_path.exists() {
            let data = std::fs::read(&tags_path)?;
            tags = postcard::from_bytes(&data)?;
            tracing::debug!("loaded tags. {} entries", tags.len());
        };
        let (complete, partial, path_files) = Self::scan_data_files(options)?;
        let mut to_delete = path_files;
        let mut complete_table = txn.open_table(COMPLETE_TABLE)?;
        let mut partial_table = txn.open_table(PARTIAL_TABLE)?;
        let mut tags_table = txn.open_table(TAGS_TABLE)?;
        let mut meta_table = txn.open_table(META_TABLE)?;
        Self::set_db_version(&mut meta_table, 2)?;
        complete_table.drain::<Hash>(..)?;
        partial_table.drain::<Hash>(..)?;
        for (hash, entry) in complete {
            complete_table.insert(hash, entry)?;
        }
        for (hash, entry) in partial {
            partial_table.insert(hash, entry)?;
        }
        for (tag, target) in tags {
            tags_table.insert(tag, target)?;
        }

        // remove tags file and all partial files, since they are now tracked by the database
        if tags_path.exists() {
            to_delete.push(tags_path);
        }

        Ok(to_delete)
    }

    /// Blocking load a database from disk.
    pub fn load_blocking(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db = Self::load_impl(path.as_ref())?;
        Ok(db)
    }

    /// Load a database from disk.
    pub async fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let db = tokio::task::spawn_blocking(move || Self::load_impl(&path)).await??;
        Ok(db)
    }

    fn owned_data_path(&self, hash: &Hash) -> PathBuf {
        self.0.options.owned_data_path(hash)
    }

    fn owned_outboard_path(&self, hash: &Hash) -> PathBuf {
        self.0.options.owned_outboard_path(hash)
    }

    fn entry_status_impl(&self, hash: &Hash) -> io::Result<EntryStatus> {
        let state = self.0.state.read().unwrap();
        if state.partial.contains_key(hash) {
            return Ok(EntryStatus::Partial);
        }
        drop(state);

        let read_tx = self.0.db.begin_read().err_to_io()?;
        {
            let full_table = read_tx.open_table(COMPLETE_TABLE).err_to_io()?;
            let record = full_table.get(hash).err_to_io()?;
            if record.is_some() {
                return Ok(EntryStatus::Complete);
            }
        }

        {
            let partial_table = read_tx.open_table(PARTIAL_TABLE).err_to_io()?;
            let record = partial_table.get(hash).err_to_io()?;
            if record.is_some() {
                return Ok(EntryStatus::Partial);
            }
        }

        Ok(EntryStatus::NotFound)
    }

    fn get_impl(&self, hash: &Hash) -> std::result::Result<Option<Entry>, io::Error> {
        let state = self.0.state.read().unwrap();
        if let Some(entry) = state.partial.get(hash) {
            let size = entry.size;
            return Ok(Some(Entry {
                hash: *hash,
                is_complete: false,
                entry: EntryData {
                    data: MemOrFile::Mem(entry.data.snapshot()),
                    outboard: MemOrFile::Mem(Bytes::from(size.to_le_bytes().to_vec())),
                },
            }));
        }
        drop(state);

        let read_tx = self.0.db.begin_read().err_to_io()?;
        {
            let full_table = read_tx.open_table(COMPLETE_TABLE).err_to_io()?;
            let blobs_table = read_tx.open_table(BLOBS_TABLE).err_to_io()?;
            let outboards_table = read_tx.open_table(OUTBOARDS_TABLE).err_to_io()?;
            let entry = full_table.get(hash).err_to_io()?;
            if let Some(entry) = entry {
                let entry = entry.value();
                return Ok(Some(self.get_complete_entry(
                    hash,
                    &entry,
                    &self.0.options,
                    &blobs_table,
                    &outboards_table,
                )?));
            }
        }

        {
            let partial_table = read_tx.open_table(PARTIAL_TABLE).err_to_io()?;
            let e = partial_table.get(hash).err_to_io()?;
            if let Some(entry) = e {
                let entry = entry.value();
                let data_path = self.0.options.partial_data_path(*hash, &entry.uuid);
                let outboard_path = self.0.options.partial_outboard_path(*hash, &entry.uuid);
                return Ok(Some(Entry {
                    hash: *hash,
                    is_complete: false,
                    entry: EntryData {
                        data: MemOrFile::File((data_path, entry.size)),
                        outboard: MemOrFile::File(outboard_path),
                    },
                }));
            }
        }

        tracing::trace!("got none {}", hash);
        Ok(None)
    }

    fn get_possibly_partial_impl(&self, hash: &Hash) -> io::Result<PossiblyPartialEntry<Self>> {
        let state = self.0.state.read().unwrap();
        if let Some(entry) = state.partial.get(hash) {
            return Ok(PossiblyPartialEntry::Partial(PartialEntry {
                hash: *hash,
                size: entry.size,
                data: MemOrFileHandle::Mem(entry.data.clone()),
                outboard: None,
            }));
        }
        drop(state);

        let read_tx = self.0.db.begin_read().err_to_io()?;
        {
            let partial_table = read_tx.open_table(PARTIAL_TABLE).err_to_io()?;
            let e = partial_table.get(hash).err_to_io()?;
            if let Some(entry) = e {
                let entry = entry.value();
                let needs_outboard = needs_outboard(entry.size);
                return Ok(PossiblyPartialEntry::Partial(PartialEntry {
                    hash: *hash,
                    size: entry.size,
                    data: MemOrFileHandle::File(FileHandle::new(
                        self.0.options.partial_data_path(*hash, &entry.uuid),
                    )),
                    outboard: if needs_outboard {
                        Some(FileHandle::new(
                            self.0.options.partial_outboard_path(*hash, &entry.uuid),
                        ))
                    } else {
                        None
                    },
                }));
            }
        }
        {
            let full_table = read_tx.open_table(COMPLETE_TABLE).err_to_io()?;
            let blobs_table = read_tx.open_table(BLOBS_TABLE).err_to_io()?;
            let outboards_table = read_tx.open_table(OUTBOARDS_TABLE).err_to_io()?;
            let e = full_table.get(hash).err_to_io()?;
            if let Some(entry) = e {
                let entry = entry.value();
                return Ok(self
                    .get_complete_entry(
                        hash,
                        &entry,
                        &self.0.options,
                        &blobs_table,
                        &outboards_table,
                    )
                    .map(PossiblyPartialEntry::Complete)
                    .unwrap_or(PossiblyPartialEntry::NotFound));
            }
        }
        Ok(PossiblyPartialEntry::NotFound)
    }

    /// Get a complete entry from the database.
    fn get_complete_entry(
        &self,
        hash: &Hash,
        entry: &CompleteEntry,
        options: &Options,
        blobs_table: &impl redb::ReadableTable<Hash, &'static [u8]>,
        outboards_table: &impl redb::ReadableTable<Hash, &'static [u8]>,
    ) -> io::Result<Entry> {
        let size = entry.size;
        tracing::trace!("got complete: {} {}", hash, entry.size);
        let outboard = if needs_outboard(size) {
            if let Some(outboard) = outboards_table.get(hash).err_to_io()? {
                MemOrFile::Mem(Bytes::copy_from_slice(outboard.value()))
            } else {
                MemOrFile::File(self.owned_outboard_path(hash))
            }
        } else {
            MemOrFile::Mem(Bytes::from(size.to_le_bytes().to_vec()))
        };
        let inline_data = blobs_table
            .get(hash)
            .err_to_io()?
            .map(|x| Bytes::copy_from_slice(x.value()));
        let entry = EntryData {
            data: if let Some(inline_data) = inline_data {
                MemOrFile::Mem(inline_data)
            } else {
                // get the data path
                let path = if entry.owned_data {
                    // use the path for the data in the default location
                    options.owned_data_path(hash)
                } else {
                    // use the first external path. if we don't have any
                    // we don't have a valid entry
                    entry
                        .external_path()
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::NotFound, "no valid path found for entry")
                        })?
                        .clone()
                };
                MemOrFile::File((path, entry.size))
            },
            outboard,
        };
        Ok(Entry {
            hash: *hash,
            entry,
            is_complete: true,
        })
    }
}

/// Synchronously compute the outboard of a file, and return hash and outboard.
///
/// It is assumed that the file is not modified while this is running.
///
/// If it is modified while or after this is running, the outboard will be
/// invalid, so any attempt to compute a slice from it will fail.
///
/// If the size of the file is changed while this is running, an error will be
/// returned.
fn compute_outboard(
    path: &Path,
    size: u64,
    progress: impl Fn(u64) -> io::Result<()> + Send + Sync + 'static,
) -> io::Result<(Hash, Option<Vec<u8>>)> {
    let span = trace_span!("outboard.compute", path = %path.display());
    let _guard = span.enter();
    let file = std::fs::File::open(path)?;
    // compute outboard size so we can pre-allocate the buffer.
    let outboard_size = usize::try_from(bao_tree::io::outboard_size(size, IROH_BLOCK_SIZE))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "size too large"))?;
    let mut outboard = Vec::with_capacity(outboard_size);

    // wrap the reader in a progress reader, so we can report progress.
    let reader = ProgressReader2::new(file, progress);
    // wrap the reader in a buffered reader, so we read in large chunks
    // this reduces the number of io ops and also the number of progress reports
    let mut reader = BufReader::with_capacity(1024 * 1024, reader);

    let hash =
        bao_tree::io::sync::outboard_post_order(&mut reader, size, IROH_BLOCK_SIZE, &mut outboard)?;
    let ob = PostOrderMemOutboard::load(hash, &outboard, IROH_BLOCK_SIZE)?.flip();
    tracing::trace!(%hash, "done");
    let ob = ob.into_inner_with_prefix();
    let ob = if ob.len() > 8 { Some(ob) } else { None };
    Ok((hash.into(), ob))
}

pub(crate) struct ProgressReader2<R, F: Fn(u64) -> io::Result<()>> {
    inner: R,
    offset: u64,
    cb: F,
}

impl<R: io::Read, F: Fn(u64) -> io::Result<()>> ProgressReader2<R, F> {
    #[allow(dead_code)]
    pub fn new(inner: R, cb: F) -> Self {
        Self {
            inner,
            offset: 0,
            cb,
        }
    }
}

impl<R: io::Read, F: Fn(u64) -> io::Result<()>> io::Read for ProgressReader2<R, F> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.offset += read as u64;
        (self.cb)(self.offset)?;
        Ok(read)
    }
}

/// A file name that indicates the purpose of the file.
#[derive(Clone, PartialEq, Eq)]
pub enum FileName {
    /// Incomplete data for the hash, with an unique id
    PartialData(Hash, [u8; 16]),
    /// File is storing data for the hash
    Data(Hash),
    /// File is storing a partial outboard
    PartialOutboard(Hash, [u8; 16]),
    /// File is storing an outboard
    ///
    /// We can have multiple files with the same outboard, in case the outboard
    /// does not contain hashes. But we don't store those outboards.
    Outboard(Hash),
    /// External paths for the hash, only used in outdated v1 format
    Paths(Hash),
    /// File is going to be used to store metadata
    Meta(Vec<u8>),
}

impl FileName {
    /// Get the file purpose from a path, handling weird cases
    pub fn from_path(path: impl AsRef<Path>) -> std::result::Result<Self, &'static str> {
        let path = path.as_ref();
        let name = path.file_name().ok_or("no file name")?;
        let name = name.to_str().ok_or("invalid file name")?;
        let purpose = Self::from_str(name).map_err(|_| "invalid file name")?;
        Ok(purpose)
    }
}

/// The extension for outboard files. We use obao4 to indicate that this is an outboard
/// in the standard pre order format (obao like in the bao crate), but with a chunk group
/// size of 4, unlike the bao crate which uses 0.
const OUTBOARD_EXT: &str = "obao4";

impl fmt::Display for FileName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PartialData(hash, uuid) => {
                write!(f, "{}-{}.data", hex::encode(hash), hex::encode(uuid))
            }
            Self::PartialOutboard(hash, uuid) => {
                write!(
                    f,
                    "{}-{}.{}",
                    hex::encode(hash),
                    hex::encode(uuid),
                    OUTBOARD_EXT
                )
            }
            Self::Paths(hash) => {
                write!(f, "{}.paths", hex::encode(hash))
            }
            Self::Data(hash) => write!(f, "{}.data", hex::encode(hash)),
            Self::Outboard(hash) => write!(f, "{}.{}", hex::encode(hash), OUTBOARD_EXT),
            Self::Meta(name) => write!(f, "{}.meta", hex::encode(name)),
        }
    }
}

impl FromStr for FileName {
    type Err = ();

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        // split into base and extension
        let Some((base, ext)) = s.rsplit_once('.') else {
            return Err(());
        };
        // strip optional leading dot
        let base = base.strip_prefix('.').unwrap_or(base);
        let mut hash = [0u8; 32];
        if let Some((base, uuid_text)) = base.split_once('-') {
            let mut uuid = [0u8; 16];
            hex::decode_to_slice(uuid_text, &mut uuid).map_err(|_| ())?;
            if ext == "data" {
                hex::decode_to_slice(base, &mut hash).map_err(|_| ())?;
                Ok(Self::PartialData(hash.into(), uuid))
            } else if ext == OUTBOARD_EXT {
                hex::decode_to_slice(base, &mut hash).map_err(|_| ())?;
                Ok(Self::PartialOutboard(hash.into(), uuid))
            } else {
                Err(())
            }
        } else if ext == "meta" {
            let data = hex::decode(base).map_err(|_| ())?;
            Ok(Self::Meta(data))
        } else {
            hex::decode_to_slice(base, &mut hash).map_err(|_| ())?;
            if ext == "data" {
                Ok(Self::Data(hash.into()))
            } else if ext == OUTBOARD_EXT {
                Ok(Self::Outboard(hash.into()))
            } else if ext == "paths" {
                Ok(Self::Paths(hash.into()))
            } else {
                Err(())
            }
        }
    }
}

struct DD<T: fmt::Display>(T);

impl<T: fmt::Display> fmt::Debug for DD<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl fmt::Debug for FileName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PartialData(hash, guid) => f
                .debug_tuple("PartialData")
                .field(&DD(hash))
                .field(&DD(hex::encode(guid)))
                .finish(),
            Self::Data(hash) => f.debug_tuple("Data").field(&DD(hash)).finish(),
            Self::PartialOutboard(hash, guid) => f
                .debug_tuple("PartialOutboard")
                .field(&DD(hash))
                .field(&DD(hex::encode(guid)))
                .finish(),
            Self::Outboard(hash) => f.debug_tuple("Outboard").field(&DD(hash)).finish(),
            Self::Meta(arg0) => f.debug_tuple("Meta").field(&DD(hex::encode(arg0))).finish(),
            Self::Paths(arg0) => f
                .debug_tuple("Paths")
                .field(&DD(hex::encode(arg0)))
                .finish(),
        }
    }
}

impl FileName {
    /// true if the purpose is for a temporary file
    pub fn temporary(&self) -> bool {
        match self {
            FileName::PartialData(_, _) => true,
            FileName::Data(_) => false,
            FileName::PartialOutboard(_, _) => true,
            FileName::Outboard(_) => false,
            FileName::Meta(_) => false,
            FileName::Paths(_) => false,
        }
    }
}

fn to_io_err(e: impl Into<redb::Error>) -> io::Error {
    let e = e.into();
    match e {
        redb::Error::Io(e) => e,
        e => io::Error::new(io::ErrorKind::Other, e),
    }
}

trait RedbResultExt<T> {
    fn err_to_io(self) -> io::Result<T>;
}

impl<E: Into<redb::Error>, T> RedbResultExt<T> for std::result::Result<T, E> {
    fn err_to_io(self) -> io::Result<T> {
        self.map_err(to_io_err)
    }
}

fn asyncify<F, T>(f: F) -> impl Future<Output = io::Result<T>> + 'static
where
    F: FnOnce() -> io::Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f).map(flatten_to_io)
}

#[cfg(test)]
mod tests {
    use crate::store::Store as StoreTrait;

    use super::*;
    use proptest::prelude::*;
    use testdir::testdir;

    #[tokio::test]
    async fn small_file_stress() {
        let dir = testdir!();
        {
            let db = Store::load(dir).await.unwrap();
            let mut tags = Vec::new();
            for i in 0..100000 {
                let data: Bytes = i.to_string().into();
                let tag = db.import_bytes(data, BlobFormat::Raw).await.unwrap();
                println!("tag: {}", i);
                tags.push(tag);
            }
        }
    }

    #[test]
    fn test_basics() -> anyhow::Result<()> {
        let dir = testdir!();
        {
            let store = Store::load_impl(&dir)?;
            let data: Bytes = "hello".into();
            let _tag = store.import_bytes_impl(data, BlobFormat::Raw)?;

            let blobs: Vec<_> = store.blobs()?.collect::<io::Result<Vec<_>>>()?;
            assert_eq!(blobs.len(), 1);
            let partial_blobs: Vec<_> = store.partial_blobs()?.collect::<io::Result<Vec<_>>>()?;
            assert_eq!(partial_blobs.len(), 0);
        }

        {
            let store = Store::load_impl(&dir)?;
            let blobs: Vec<_> = store.blobs()?.collect::<io::Result<Vec<_>>>()?;
            assert_eq!(blobs.len(), 1);
            let partial_blobs: Vec<_> = store.partial_blobs()?.collect::<io::Result<Vec<_>>>()?;
            assert_eq!(partial_blobs.len(), 0);
        }
        Ok(())
    }

    fn arb_hash() -> impl Strategy<Value = Hash> {
        any::<[u8; 32]>().prop_map(|x| x.into())
    }

    fn arb_filename() -> impl Strategy<Value = FileName> {
        prop_oneof![
            arb_hash().prop_map(FileName::Data),
            arb_hash().prop_map(FileName::Outboard),
            arb_hash().prop_map(FileName::Paths),
            (arb_hash(), any::<[u8; 16]>())
                .prop_map(|(hash, uuid)| FileName::PartialData(hash, uuid)),
            (arb_hash(), any::<[u8; 16]>())
                .prop_map(|(hash, uuid)| FileName::PartialOutboard(hash, uuid)),
            any::<Vec<u8>>().prop_map(FileName::Meta),
        ]
    }

    #[test]
    fn filename_parse_error() {
        assert!(FileName::from_str("foo").is_err());
        assert!(FileName::from_str("1234.data").is_err());
        assert!(FileName::from_str("1234ABDC.outboard").is_err());
        assert!(FileName::from_str("1234-1234.data").is_err());
        assert!(FileName::from_str("1234ABDC-1234.outboard").is_err());
    }

    proptest! {
        #[test]
        fn filename_roundtrip(name in arb_filename()) {
            let s = name.to_string();
            let name2 = super::FileName::from_str(&s).unwrap();
            prop_assert_eq!(name, name2);
        }
    }
}
