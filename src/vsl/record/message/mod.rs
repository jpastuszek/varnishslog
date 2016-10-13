pub mod parser;

pub type TimeStamp = f64;
pub type Duration = f64;
pub type ByteCount = u64;
pub type BitCount = u64;
pub type FetchMode = u32;
pub type Status = u32;
pub type Port = u16;
pub type FileDescriptor = isize;

#[derive(Debug, Clone, PartialEq)]
pub enum AclResult {
    Match,
    NoMatch,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CompressionOperation {
    Gzip,
    Gunzip,
    GunzipTest,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CompressionDirection {
    Fetch,
    Deliver,
}
