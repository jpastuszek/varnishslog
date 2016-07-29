#[macro_use]
extern crate nom;
#[macro_use]
extern crate log;
extern crate stderrlog;

use std::fmt::{self, Debug};
use std::io::{self, stdin};
use std::ffi::{CStr, FromBytesWithNulError};
use nom::{le_u32};

#[macro_use]
mod stream_buf;
use stream_buf::{StreamBuf, ReadStreamBuf, FillApplyError};

/*
 * Shared memory log format
 *
 * The log member points to an array of 32bit unsigned integers containing
 * log records.
 *
 * Each logrecord consist of:
 *    [n]               = ((type & 0xff) << 24) | (length & 0xffff)
 *    [n + 1]           = ((marker & 0x03) << 30) | (identifier & 0x3fffffff)
 *    [n + 2] ... [m]   = content (NUL-terminated)
 *
 * Logrecords are NUL-terminated so that string functions can be run
 * directly on the shmlog data.
 *
 * Notice that the constants in these macros cannot be changed without
 * changing corresponding magic numbers in varnishd/cache/cache_shmlog.c
 */

const VSL_LENOFFSET: u32 = 24;
const VSL_LENMASK: u32 = 0xffff;
const VSL_MARKERMASK: u32 = 0x03;
//const VSL_CLIENTMARKER: u32 = 1 << 30;
//const VSL_BACKENDMARKER: u32 = 1 << 31;
const VSL_IDENTOFFSET: u32 = 30;
const VSL_IDENTMASK: u32 = !(3 << VSL_IDENTOFFSET);

// https://github.com/varnishcache/varnish-cache/blob/master/include/vapi/vsl_int.h
// https://github.com/varnishcache/varnish-cache/blob/master/include/tbl/vsl_tags.h
// https://github.com/varnishcache/varnish-cache/blob/master/include/tbl/vsl_tags_http.h
/* TODO: generate with build.rs from the header files
enum VslTag {
    SLT__Bogus = 0,
    SLT__Reserved = 254,
    SLT__Batch = 255
}
*/

named!(binary_vsl_tag<&[u8], &[u8]>, tag!(b"VSL\0"));

#[derive(Debug)]
struct VslRecordHeader {
    pub tag: u8,
    pub len: u16,
    pub marker: u8,
    pub ident: u32,
}

fn vsl_record_header<'b>(input: &'b[u8]) -> nom::IResult<&'b[u8], VslRecordHeader, u32> {
    chain!(
        input, r1: le_u32 ~ r2: le_u32,
        || {
            VslRecordHeader {
                tag: (r1 >> VSL_LENOFFSET) as u8,
                len: (r1 & VSL_LENMASK) as u16,
                marker: (r2 & VSL_MARKERMASK >> VSL_IDENTOFFSET) as u8,
                ident: r2 & VSL_IDENTMASK,
            }
        })
}

struct VslRecord<'b> {
    pub tag: u8,
    pub marker: u8,
    pub ident: u32,
    pub data: &'b[u8],
}

impl<'b> VslRecord<'b> {
    fn body(&'b self) -> Result<&'b CStr, FromBytesWithNulError> {
        CStr::from_bytes_with_nul(self.data)
    }
}

impl<'b> Debug for VslRecord<'b> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.debug_struct("VSL Record")
            .field("tag", &self.tag)
            .field("marker", &self.marker)
            .field("ident", &self.ident)
            .field("body", &self.body())
            .finish()
    }
}

fn vsl_record<'b>(input: &'b[u8]) -> nom::IResult<&'b[u8], VslRecord<'b>, u32> {
    chain!(
        input,
        header: vsl_record_header ~ data: take!(header.len) ~ take!((4 - header.len % 4) % 4),
        || {
            VslRecord { tag: header.tag, marker: header.marker, ident: header.ident, data: data }
        })
}

/*
fn binary_vsl_records<'b>(input: &'b[u8]) -> nom::IResult<&'b[u8], Vec<VslRecord<'b>>, u32> {
    chain!(
        input, binary_vsl_tag ~ records: many1!(vsl_record),
        || { records })
}
*/

fn main() {
    stderrlog::new()
        .module(module_path!())
        .quiet(false)
        .verbosity(4)
        .init()
        .unwrap();

    let stdin = stdin();
    let stdin = stdin.lock();
    // for testing
    //let mut rfb = ReadStreamBuf::with_capacity(stdin, 123);
    let mut rfb = ReadStreamBuf::new(stdin);

    while let None = rfb.fill_apply(binary_vsl_tag).expect("binary stream") {}
    rfb.recycle(); // TODO: VSL should benefit from alignment - bench test it

    loop {
        let record = match rfb.fill_apply(vsl_record) {
            Err(FillApplyError::Io(err)) => {
                if err.kind() == io::ErrorKind::UnexpectedEof {
                    info!("Reached end of stream; exiting");
                    return
                }
                error!("Got IO Error while reading stream: {}", err);
                panic!("Boom!")
            },
            Err(FillApplyError::Parser(err)) => {
                error!("Failed to parse VSL record: {}", err);
                panic!("Boom!")
            },
            Ok(None) => continue,
            Ok(Some(record)) => record
        };
        println!("{:?}", record);
    }
}
