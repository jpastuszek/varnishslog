#[macro_use]
extern crate nom;
#[macro_use]
extern crate log;
extern crate stderrlog;
#[macro_use]
extern crate clap;
use clap::{Arg, App};

use std::fmt::{self, Debug};
use std::io::{self, stdin};
use std::str::Utf8Error;
use nom::{le_u32};

#[macro_use]
mod stream_buf;
use stream_buf::{StreamBuf, ReadStreamBuf, FillError, FillApplyError};

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
    fn body(&'b self) -> Result<&'b str, Utf8Error> {
        std::str::from_utf8(self.data)
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

fn vsl_record_v3<'b>(input: &'b[u8]) -> nom::IResult<&'b[u8], VslRecord<'b>, u32> {
    chain!(
        input,
        header: vsl_record_header ~ data: take!(header.len) ~ take!((4 - header.len % 4) % 4),
        || {
            VslRecord { tag: header.tag, marker: header.marker, ident: header.ident, data: data }
        })
}

fn vsl_record_v4<'b>(input: &'b[u8]) -> nom::IResult<&'b[u8], VslRecord<'b>, u32> {
    chain!(
        input,
        header: vsl_record_header ~ data: take!(header.len - 1) ~ take!(1) ~ take!((4 - header.len % 4) % 4),
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
    let arguments = App::new("Varnish VSL log to syslog logger")
        .version(crate_version!())
        .author(crate_authors!())
        .about("Reads binary VSL log entreis, correlates them togeter and emits JSON log entry to syslog")
        .arg(Arg::with_name("v3")
             .long("varnish-v3")
             .short("3")
             .help("Parse Varnish v3 binary log"))
        .arg(Arg::with_name("quiet")
             .long("quiet")
             .short("q")
             .help("Don't log anything"))
        .arg(Arg::with_name("verbose")
             .long("verbose")
             .short("v")
             .multiple(true)
             .help("Sets the level of verbosity; e.g. -vv for INFO level, -vvvv for TRACE level"))
        .get_matches();

    stderrlog::new()
        .module(module_path!())
        .quiet(arguments.is_present("quiet"))
        .verbosity(arguments.occurrences_of("verbose") as usize)
        .init()
        .unwrap();

    let stdin = stdin();
    let stdin = stdin.lock();
    // for testing
    //let mut rfb = ReadStreamBuf::with_capacity(stdin, 123);
    let mut rfb = ReadStreamBuf::new(stdin);

    let vsl_record: fn(&[u8]) -> nom::IResult<&[u8], VslRecord>;

    if ! arguments.is_present("v3") {
        loop {
            match rfb.fill_apply(binary_vsl_tag) {
                Err(FillApplyError::Parser(_)) => {
                    error!("Input is not Varnish v4 VSL binary format");
                    panic!("Bad input format")
                }
                Err(err) => {
                    error!("Error while reading VSL tag: {}", err);
                    panic!("VSL tag error")
                }
                Ok(None) => continue,
                Ok(Some(_)) => break,
            }
        }
        vsl_record = vsl_record_v4;
    } else {
        vsl_record = vsl_record_v3;
    }

    rfb.recycle(); // TODO: VSL should benefit from alignment - bench test it

    loop {
        let record = match rfb.fill_apply(vsl_record) {
            Err(FillApplyError::FillError(FillError::Io(err))) => {
                if err.kind() == io::ErrorKind::UnexpectedEof {
                    info!("Reached end of stream; exiting");
                    return
                }
                error!("Got IO Error while reading stream: {}", err);
                panic!("Stream IO error")
            },
            Err(FillApplyError::FillError(err)) => {
                error!("Failed to fill parsing buffer: {}", err);
                panic!("Fill error")
            }
            Err(FillApplyError::Parser(err)) => {
                error!("Failed to parse VSL record: {}", err);
                panic!("Parser error")
            },
            Ok(None) => continue,
            Ok(Some(record)) => record,
        };
        println!("{:?}", record);
    }
}
