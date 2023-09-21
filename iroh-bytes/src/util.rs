//! Utility functions and types.
use anyhow::Result;
use bao_tree::blake3;
use postcard::experimental::max_size::MaxSize;
use serde::{
    de::{self, SeqAccess},
    ser::SerializeTuple,
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::{fmt, result, str::FromStr};
use thiserror::Error;
pub mod io;
pub mod progress;
pub mod runtime;

/// A format identifier
///
/// Should we make this an u64 and use ?
///
/// That table is so weird. There is so much unrelated stuff in there, so the smallest value we would be
/// able to use for iroh collections would be 2 bytes varint encoded or something...
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BlobFormat(u64);

impl BlobFormat {
    /// Raw format
    ///
    /// raw binary in the multiformat table
    pub const RAW: Self = Self(0x55);

    /// Collection format
    ///
    /// seems to be one of the smallest values that are free
    /// if we were to change a collection to be just a sequence of hashes,
    /// we could just use the blake3 value (0x1e) here
    pub const COLLECTION: Self = Self(0x73);

    /// true if this is a raw blob
    pub const fn is_raw(&self) -> bool {
        self.0 == Self::RAW.0
    }

    /// true if this is an iroh collection
    pub const fn is_collection(&self) -> bool {
        self.0 == Self::COLLECTION.0
    }
}

impl From<BlobFormat> for u64 {
    fn from(value: BlobFormat) -> Self {
        value.0
    }
}

impl fmt::Debug for BlobFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self == &Self::RAW {
            f.write_str("Raw")
        } else if self == &Self::COLLECTION {
            f.write_str("Collection")
        } else {
            f.debug_tuple("Other").field(&self.0).finish()
        }
    }
}

/// A hash and format pair
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Cid(pub Hash, pub BlobFormat);

impl Cid {
    /// Convert to cid bytes
    pub fn to_cid_bytes(&self) -> Vec<u8> {
        let helper = CidHelper::from(*self);
        postcard::to_stdvec(&helper).unwrap()
    }

    /// Convert from cid bytes
    pub fn from_cid_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        let helper = postcard::from_bytes::<CidHelper>(bytes)?;
        helper.try_into()
    }
}

/// Helper struct for serializing and deserializing to multiformat cids.
///
/// Serializing this using the postcard format will produce a multiformat cid.
/// Unsigned integers in postcard are varint encoded using the same scheme as
/// multiformat, and the data, due to being fixed size, won't have a length
/// prefix.
#[derive(Serialize, Deserialize)]
struct CidHelper {
    version: u64,
    codec: u64,
    hash: u64,
    size: u64,
    data: [u8; 32],
}

impl fmt::Display for CidHelper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = postcard::to_stdvec(&self).unwrap();
        let mut res = String::with_capacity(40);
        res.push('b');
        data_encoding::BASE32_NOPAD.encode_append(&bytes, &mut res);
        write!(f, "{}", res.to_ascii_lowercase())
    }
}

impl From<Cid> for CidHelper {
    fn from(value: Cid) -> Self {
        Self {
            version: 1,            // cid version 1
            codec: value.1.into(), // the only thing not hardcoded
            hash: 0x1e,            // blake3
            size: 32,              // the hash size, must be 32
            data: *value.0.as_bytes(),
        }
    }
}

impl TryFrom<CidHelper> for Cid {
    type Error = anyhow::Error;

    fn try_from(value: CidHelper) -> Result<Self, Self::Error> {
        anyhow::ensure!(value.version == 1, "invalid cid version");
        anyhow::ensure!(value.hash == 0x1e, "invalid hash");
        anyhow::ensure!(value.size == 32, "invalid hash size");
        Ok(Self(Hash::from(value.data), BlobFormat(value.codec)))
    }
}

/// Hash type used throught.
#[derive(PartialEq, Eq, Copy, Clone, Hash)]
pub struct Hash(blake3::Hash);

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Hash").field(&DD(self.to_hex())).finish()
    }
}

struct DD<T: fmt::Display>(T);

impl<T: fmt::Display> fmt::Debug for DD<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl Hash {
    /// Calculate the hash of the provide bytes.
    pub fn new(buf: impl AsRef<[u8]>) -> Self {
        let val = blake3::hash(buf.as_ref());
        Hash(val)
    }

    /// Bytes of the hash.
    pub fn as_bytes(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }

    /// Get the cid as bytes.
    pub fn as_cid_bytes(&self) -> [u8; 36] {
        let hash = self.0.as_bytes();
        let mut res = [0u8; 36];
        res[0..4].copy_from_slice(&CID_PREFIX);
        res[4..36].copy_from_slice(hash);
        res
    }

    /// Try to create a blake3 cid from cid bytes.
    ///
    /// This will only work if the prefix is the following:
    /// - version 1
    /// - raw codec
    /// - blake3 hash function
    /// - 32 byte hash size
    pub fn from_cid_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(
            bytes.len() == 36,
            "invalid cid length, expected 36, got {}",
            bytes.len()
        );
        anyhow::ensure!(bytes[0..4] == CID_PREFIX, "invalid cid prefix");
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&bytes[4..36]);
        Ok(Self::from(hash))
    }

    /// Convert the hash to a hex string.
    pub fn to_hex(&self) -> String {
        self.0.to_hex().to_string()
    }
}

impl AsRef<[u8]> for Hash {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl From<Hash> for blake3::Hash {
    fn from(value: Hash) -> Self {
        value.0
    }
}

impl From<blake3::Hash> for Hash {
    fn from(value: blake3::Hash) -> Self {
        Hash(value)
    }
}

impl From<[u8; 32]> for Hash {
    fn from(value: [u8; 32]) -> Self {
        Hash(blake3::Hash::from(value))
    }
}

impl From<Hash> for [u8; 32] {
    fn from(value: Hash) -> Self {
        *value.as_bytes()
    }
}

impl From<&[u8; 32]> for Hash {
    fn from(value: &[u8; 32]) -> Self {
        Hash(blake3::Hash::from(*value))
    }
}

impl PartialOrd for Hash {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.0.as_bytes().cmp(other.0.as_bytes()))
    }
}

impl Ord for Hash {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.as_bytes().cmp(other.0.as_bytes())
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // result will be 58 bytes plus prefix
        let mut res = [b'b'; 59];
        // write the encoded bytes
        data_encoding::BASE32_NOPAD.encode_mut(&self.as_cid_bytes(), &mut res[1..]);
        // convert to string, this is guaranteed to succeed
        let t = std::str::from_utf8_mut(res.as_mut()).unwrap();
        // hack since data_encoding doesn't have BASE32LOWER_NOPAD as a const
        t.make_ascii_lowercase();
        // write the str, no allocations
        f.write_str(t)
    }
}

impl FromStr for Hash {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let sb = s.as_bytes();
        if sb.len() == 59 && sb[0] == b'b' {
            // this is a base32 encoded cid, we can decode it directly
            let mut t = [0u8; 58];
            t.copy_from_slice(&sb[1..]);
            // hack since data_encoding doesn't have BASE32LOWER_NOPAD as a const
            std::str::from_utf8_mut(t.as_mut())
                .unwrap()
                .make_ascii_uppercase();
            // decode the bytes
            let mut res = [0u8; 36];
            data_encoding::BASE32_NOPAD
                .decode_mut(&t, &mut res)
                .map_err(|_e| anyhow::anyhow!("invalid base32"))?;
            // convert to cid, this will check the prefix
            Self::from_cid_bytes(&res)
        } else {
            // if we want to support all the weird multibase prefixes, we have no choice
            // but to use the multibase crate
            let (_base, bytes) = multibase::decode(s)?;
            Self::from_cid_bytes(bytes.as_ref())
        }
    }
}

impl Serialize for Hash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Fixed-length structures, including arrays, are supported in Serde as tuples
        // See: https://serde.rs/impl-serialize.html#serializing-a-tuple
        let mut s = serializer.serialize_tuple(32)?;
        for item in self.0.as_bytes() {
            s.serialize_element(item)?;
        }
        s.end()
    }
}

impl<'de> Deserialize<'de> for Hash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_tuple(32, HashVisitor)
    }
}

struct HashVisitor;

impl<'de> de::Visitor<'de> for HashVisitor {
    type Value = Hash;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "an array of 32 bytes containing hash data")
    }

    /// Process a sequence into an array
    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut arr = [0u8; 32];
        let mut i = 0;
        while let Some(val) = seq.next_element()? {
            arr[i] = val;
            i += 1;
            if i > 32 {
                return Err(de::Error::invalid_length(i, &self));
            }
        }

        Ok(Hash::from(arr))
    }
}

impl MaxSize for Hash {
    const POSTCARD_MAX_SIZE: usize = 32;
}

const CID_PREFIX: [u8; 4] = [
    0x01, // version
    0x55, // raw codec
    0x1e, // hash function, blake3
    0x20, // hash size, 32 bytes
];

/// A serializable error type for use in RPC responses.
#[derive(Serialize, Deserialize, Debug, Error)]
pub struct RpcError(serde_error::Error);

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl From<anyhow::Error> for RpcError {
    fn from(e: anyhow::Error) -> Self {
        RpcError(serde_error::Error::new(&*e))
    }
}

impl From<std::io::Error> for RpcError {
    fn from(e: std::io::Error) -> Self {
        RpcError(serde_error::Error::new(&e))
    }
}

/// A serializable result type for use in RPC responses.
#[allow(dead_code)]
pub type RpcResult<T> = result::Result<T, RpcError>;

/// A non-sendable marker type
#[derive(Debug)]
pub(crate) struct NonSend {
    _marker: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl NonSend {
    /// Create a new non-sendable marker.
    #[allow(dead_code)]
    pub const fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use iroh_test::{assert_eq_hex, hexdump::parse_hexdump};

    use super::*;

    use serde_test::{assert_tokens, Token};

    #[test]
    fn test_hash() {
        let data = b"hello world";
        let hash = Hash::new(data);

        let encoded = hash.to_string();
        assert_eq!(encoded.parse::<Hash>().unwrap(), hash);
    }

    #[test]
    fn hash_wire_format() {
        let hash = Hash::from([0xab; 32]);
        let serialized = postcard::to_stdvec(&hash).unwrap();
        let expected = parse_hexdump(r"
            ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab # hash
        ").unwrap();
        assert_eq_hex!(serialized, expected);
    }

    #[test]
    fn hash_multiformat() {
        let hash = Hash::from([0xab; 32]);
        let serialized = hash.as_cid_bytes();
        let expected = parse_hexdump(r"
            01 # v1
            55 # raw
            1e # blake3
            20 # 32 bytes
            ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab # hash
        ").unwrap();
        assert_eq_hex!(serialized, expected);
    }

    #[test]
    fn cid_multiformat() {
        let hash = Hash::from([0xab; 32]);
        let cid = Cid(hash, BlobFormat::RAW);
        let serialized = cid.to_cid_bytes();
        let expected = parse_hexdump(r"
            01 # v1
            55 # raw
            1e # blake3
            20 # 32 bytes
            ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab # hash
        ").unwrap();
        assert_eq_hex!(serialized, expected);
        assert_eq!(CidHelper::from(cid).to_string(), hash.to_string());

        let cid = Cid(hash, BlobFormat(0x71)); // dag-cbor
        let serialized = cid.to_cid_bytes();
        let expected = parse_hexdump(r"
            01 # v1
            71 # dag-cbor
            1e # blake3
            20 # 32 bytes
            ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab # hash
        ").unwrap();
        assert_eq_hex!(serialized, expected);
        assert_eq!(
            CidHelper::from(cid).to_string(),
            "bafyr4iflvov2xk5lvov2xk5lvov2xk5lvov2xk5lvov2xk5lvov2xk5lvm"
        );

        let cid = Cid(hash, BlobFormat(0x90)); // eth-block
        let serialized = cid.to_cid_bytes();
        let expected = parse_hexdump(r"
            01 # v1
            90 01 # eth-block
            1e # blake3
            20 # 32 bytes
            ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab # hash
        ").unwrap();
        assert_eq_hex!(serialized, expected);
        assert_eq!(
            CidHelper::from(cid).to_string(),
            "bagiachravov2xk5lvov2xk5lvov2xk5lvov2xk5lvov2xk5lvov2xk5lvovq"
        );
    }

    #[test]
    fn test_hash_serde() {
        let hash = Hash::new("hello");

        // Hashes are serialized as 32 tuples
        let mut tokens = Vec::new();
        tokens.push(Token::Tuple { len: 32 });
        for byte in hash.as_bytes() {
            tokens.push(Token::U8(*byte));
        }
        tokens.push(Token::TupleEnd);
        assert_eq!(tokens.len(), 34);

        assert_tokens(&hash, &tokens);
    }

    #[test]
    fn test_hash_postcard() {
        let hash = Hash::new("hello");
        let ser = postcard::to_stdvec(&hash).unwrap();
        let de = postcard::from_bytes(&ser).unwrap();
        assert_eq!(hash, de);

        assert_eq!(ser.len(), 32);
    }
}
