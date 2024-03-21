// Table Definitions

use bytes::Bytes;
use redb::{
    MultimapTable, MultimapTableDefinition, ReadOnlyMultimapTable, ReadOnlyTable, ReadTransaction,
    ReadableMultimapTable, ReadableTable, Table, TableDefinition, WriteTransaction,
};

use crate::PeerIdBytes;

/// Table: Authors
/// Key:   `[u8; 32]` # AuthorId
/// Value: `[u8; 32]` # Author
pub const AUTHORS_TABLE: TableDefinition<&[u8; 32], &[u8; 32]> = TableDefinition::new("authors-1");

/// Table: Namespaces v1 (replaced by Namespaces v2 in migration )
/// Key:   `[u8; 32]` # NamespaceId
/// Value: `[u8; 32]` # NamespaceSecret
pub const NAMESPACES_TABLE_V1: TableDefinition<&[u8; 32], &[u8; 32]> =
    TableDefinition::new("namespaces-1");

/// Table: Namespaces v2
/// Key:   `[u8; 32]`       # NamespaceId
/// Value: `(u8, [u8; 32])` # (CapabilityKind, Capability)
pub const NAMESPACES_TABLE: TableDefinition<&[u8; 32], (u8, &[u8; 32])> =
    TableDefinition::new("namespaces-2");

/// Table: Records
/// Key:   `([u8; 32], [u8; 32], &[u8])`
///      # (NamespaceId, AuthorId, Key)
/// Value: `(u64, [u8; 32], [u8; 32], u64, [u8; 32])`
///      # (timestamp, signature_namespace, signature_author, len, hash)
pub const RECORDS_TABLE: TableDefinition<RecordsId, RecordsValue> =
    TableDefinition::new("records-1");
pub type RecordsId<'a> = (&'a [u8; 32], &'a [u8; 32], &'a [u8]);
pub type RecordsIdOwned = ([u8; 32], [u8; 32], Bytes);
pub type RecordsValue<'a> = (u64, &'a [u8; 64], &'a [u8; 64], u64, &'a [u8; 32]);
pub type RecordsTable<'a> = ReadOnlyTable<'a, RecordsId<'static>, RecordsValue<'static>>;

/// Table: Latest per author
/// Key:   `([u8; 32], [u8; 32])`    # (NamespaceId, AuthorId)
/// Value: `(u64, Vec<u8>)`          # (Timestamp, Key)
pub const LATEST_PER_AUTHOR_TABLE: TableDefinition<LatestPerAuthorKey, LatestPerAuthorValue> =
    TableDefinition::new("latest-by-author-1");
pub type LatestPerAuthorKey<'a> = (&'a [u8; 32], &'a [u8; 32]);
pub type LatestPerAuthorValue<'a> = (u64, &'a [u8]);

/// Table: Records by key
/// Key:   `([u8; 32], Vec<u8>, [u8; 32]])` # (NamespaceId, Key, AuthorId)
/// Value: `()`
pub const RECORDS_BY_KEY_TABLE: TableDefinition<RecordsByKeyId, ()> =
    TableDefinition::new("records-by-key-1");
pub type RecordsByKeyId<'a> = (&'a [u8; 32], &'a [u8], &'a [u8; 32]);
pub type RecordsByKeyIdOwned = ([u8; 32], Bytes, [u8; 32]);

/// Table: Peers per document.
/// Key:   `[u8; 32]`        # NamespaceId
/// Value: `(u64, [u8; 32])` # ([`Nanos`], &[`PeerIdBytes`]) representing the last time a peer was used.
pub const NAMESPACE_PEERS_TABLE: MultimapTableDefinition<&[u8; 32], (Nanos, &PeerIdBytes)> =
    MultimapTableDefinition::new("sync-peers-1");
/// Number of seconds elapsed since [`std::time::SystemTime::UNIX_EPOCH`]. Used to register the
/// last time a peer was useful in a document.
// NOTE: resolution is nanoseconds, stored as a u64 since this covers ~500years from unix epoch,
// which should be more than enough
type Nanos = u64;

/// Table: Download policy
/// Key:   `[u8; 32]`        # NamespaceId
/// Value: `Vec<u8>`         # Postcard encoded download policy
pub const DOWNLOAD_POLICY_TABLE: TableDefinition<&[u8; 32], &[u8]> =
    TableDefinition::new("download-policy-1");

trait ReadableTables<'db> {
    fn records(&self) -> &impl ReadableTable<RecordsId<'static>, RecordsValue<'static>>;
    fn records_by_key(&self) -> &impl ReadableTable<RecordsByKeyId<'static>, ()>;
    fn namespaces(&self) -> &impl ReadableTable<&'static [u8; 32], (u8, &'static [u8; 32])>;
    fn latest_per_author(
        &self,
    ) -> &impl ReadableTable<LatestPerAuthorKey<'static>, LatestPerAuthorValue<'static>>;
    fn namespace_peers(
        &self,
    ) -> &impl ReadableMultimapTable<&'static [u8; 32], (Nanos, &'static PeerIdBytes)>;
    fn download_policy(&self) -> &impl ReadableTable<&'static [u8; 32], &'static [u8]>;
    fn authors(&self) -> &impl ReadableTable<&'static [u8; 32], &'static [u8; 32]>;
}

pub struct Tables<'db, 'tx> {
    pub records: Table<'db, 'tx, RecordsId<'static>, RecordsValue<'static>>,
    pub records_by_key: Table<'db, 'tx, RecordsByKeyId<'static>, ()>,
    pub namespaces: Table<'db, 'tx, &'static [u8; 32], (u8, &'static [u8; 32])>,
    pub latest_per_author:
        Table<'db, 'tx, LatestPerAuthorKey<'static>, LatestPerAuthorValue<'static>>,
    pub namespace_peers: MultimapTable<'db, 'tx, &'static [u8; 32], (Nanos, &'static PeerIdBytes)>,
    pub download_policy: Table<'db, 'tx, &'static [u8; 32], &'static [u8]>,
    pub authors: Table<'db, 'tx, &'static [u8; 32], &'static [u8; 32]>,
}

impl<'db, 'tx> Tables<'db, 'tx> {
    pub fn new(tx: &'tx WriteTransaction<'db>) -> Result<Self, redb::TableError> {
        let records = tx.open_table(RECORDS_TABLE)?;
        let records_by_key = tx.open_table(RECORDS_BY_KEY_TABLE)?;
        let namespaces = tx.open_table(NAMESPACES_TABLE)?;
        let latest_per_author = tx.open_table(LATEST_PER_AUTHOR_TABLE)?;
        let namespace_peers = tx.open_multimap_table(NAMESPACE_PEERS_TABLE)?;
        let download_policy = tx.open_table(DOWNLOAD_POLICY_TABLE)?;
        let authors = tx.open_table(AUTHORS_TABLE)?;
        Ok(Self {
            records,
            records_by_key,
            namespaces,
            latest_per_author,
            namespace_peers,
            download_policy,
            authors,
        })
    }
}

impl<'db, 'tx> ReadableTables<'db> for Tables<'db, 'tx> {
    fn records(&self) -> &impl ReadableTable<RecordsId<'static>, RecordsValue<'static>> {
        &self.records
    }

    fn records_by_key(&self) -> &impl ReadableTable<RecordsByKeyId<'static>, ()> {
        &self.records_by_key
    }

    fn namespaces(&self) -> &impl ReadableTable<&'static [u8; 32], (u8, &'static [u8; 32])> {
        &self.namespaces
    }

    fn latest_per_author(
        &self,
    ) -> &impl ReadableTable<LatestPerAuthorKey<'static>, LatestPerAuthorValue<'static>> {
        &self.latest_per_author
    }

    fn namespace_peers(
        &self,
    ) -> &impl ReadableMultimapTable<&'static [u8; 32], (Nanos, &'static PeerIdBytes)> {
        &self.namespace_peers
    }

    fn download_policy(&self) -> &impl ReadableTable<&'static [u8; 32], &'static [u8]> {
        &self.download_policy
    }

    fn authors(&self) -> &impl ReadableTable<&'static [u8; 32], &'static [u8; 32]> {
        &self.authors
    }
}

pub struct ReadOnlyTables<'db> {
    pub records: ReadOnlyTable<'db, RecordsId<'static>, RecordsValue<'static>>,
    pub records_by_key: ReadOnlyTable<'db, RecordsByKeyId<'static>, ()>,
    pub namespaces: ReadOnlyTable<'db, &'static [u8; 32], (u8, &'static [u8; 32])>,
    pub latest_per_author:
        ReadOnlyTable<'db, LatestPerAuthorKey<'static>, LatestPerAuthorValue<'static>>,
    pub namespace_peers:
        ReadOnlyMultimapTable<'db, &'static [u8; 32], (Nanos, &'static PeerIdBytes)>,
    pub download_policy: ReadOnlyTable<'db, &'static [u8; 32], &'static [u8]>,
    pub authors: ReadOnlyTable<'db, &'static [u8; 32], &'static [u8; 32]>,
}

impl<'db> ReadOnlyTables<'db> {
    pub fn new(db: &'db ReadTransaction<'db>) -> Result<Self, redb::TableError> {
        let records = db.open_table(RECORDS_TABLE)?;
        let records_by_key = db.open_table(RECORDS_BY_KEY_TABLE)?;
        let namespaces = db.open_table(NAMESPACES_TABLE)?;
        let latest_per_author = db.open_table(LATEST_PER_AUTHOR_TABLE)?;
        let namespace_peers = db.open_multimap_table(NAMESPACE_PEERS_TABLE)?;
        let download_policy = db.open_table(DOWNLOAD_POLICY_TABLE)?;
        let authors = db.open_table(AUTHORS_TABLE)?;
        Ok(Self {
            records,
            records_by_key,
            namespaces,
            latest_per_author,
            namespace_peers,
            download_policy,
            authors,
        })
    }
}

impl<'db> ReadableTables<'db> for ReadOnlyTables<'db> {
    fn records(&self) -> &impl ReadableTable<RecordsId<'static>, RecordsValue<'static>> {
        &self.records
    }

    fn records_by_key(&self) -> &impl ReadableTable<RecordsByKeyId<'static>, ()> {
        &self.records_by_key
    }

    fn namespaces(&self) -> &impl ReadableTable<&'static [u8; 32], (u8, &'static [u8; 32])> {
        &self.namespaces
    }

    fn latest_per_author(
        &self,
    ) -> &impl ReadableTable<LatestPerAuthorKey<'static>, LatestPerAuthorValue<'static>> {
        &self.latest_per_author
    }

    fn namespace_peers(
        &self,
    ) -> &impl ReadableMultimapTable<&'static [u8; 32], (Nanos, &'static PeerIdBytes)> {
        &self.namespace_peers
    }

    fn download_policy(&self) -> &impl ReadableTable<&'static [u8; 32], &'static [u8]> {
        &self.download_policy
    }

    fn authors(&self) -> &impl ReadableTable<&'static [u8; 32], &'static [u8; 32]> {
        &self.authors
    }
}
