use std::fmt::{self, Debug, Display};
use std::str::{from_utf8, Utf8Error};
use std::mem;

use nom::{self, le_u32, IResult};
use quick_error::ResultExt;

mod tag_e;
pub use self::tag_e::VSL_tag_e as VslRecordTag;

mod message_parsers;
pub use self::message_parsers::*;

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

pub type VslIdent = u32;

named!(pub binary_vsl_tag<&[u8], &[u8]>, tag!(b"VSL\0"));

#[derive(Debug)]
struct VslRecordHeader {
    tag: u8,
    len: u16,
    marker: u8,
    ident: VslIdent,
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

pub struct VslRecord<'b> {
    pub tag: VslRecordTag,
    pub marker: u8,
    pub ident: VslIdent,
    pub data: &'b[u8],
}

quick_error! {
    #[derive(Debug)]
    pub enum VslRecordParseError {
        Nom(nom_err: String, tag: VslRecordTag, message: String) {
            context(record: (VslRecordTag, String), err: nom::Err<&'a [u8]>) -> (format!("{}", err), record.0, record.1)
            display("Nom parser failed on VSL record: {}; tag: {:?} message: {:?}", nom_err, tag, message)
        }
    }
}

trait IResultExt<O, E> {
    fn into_result(self) -> Result<O, E>;
}

impl<I, O, E> IResultExt<O, nom::Err<I, E>> for IResult<I, O, E> {
    fn into_result(self) -> Result<O, nom::Err<I, E>> {
        match self {
            IResult::Done(_, o) => Ok(o),
            IResult::Error(err) => Err(err),
            IResult::Incomplete(_) => panic!("assuming that parser is wrapped around complete!()"),
        }
    }
}

impl<'b> VslRecord<'b> {
    //TODO: return MaybeString so can be used in logging
    pub fn message(&'b self) -> Result<&'b str, Utf8Error> {
        from_utf8(self.data)
    }

    //TODO: work with bytes; rename to parse_data?
    pub fn parsed_message<T, P>(&'b self, parser: P) -> Result<T, VslRecordParseError> where
        P: Fn(&'b [u8]) -> nom::IResult<&'b [u8], T> {
        Ok(try!(complete!(self.data, parser).into_result().context((self.tag, self.message().unwrap().to_string()))))
    }

    #[cfg(test)]
    pub fn from_str<'s>(tag: VslRecordTag, ident: VslIdent, message: &'s str) -> VslRecord<'s> {
        VslRecord {
            tag: tag,
            marker: 0,
            ident: ident,
            data: message.as_ref()
        }
    }
}

impl<'b> Debug for VslRecord<'b> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.debug_struct("VSL Record")
            .field("tag", &self.tag)
            .field("marker", &self.marker)
            .field("ident", &self.ident)
            .field("message", &self.message())
            .finish()
    }
}

impl<'b> Display for VslRecord<'b> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let tag = format!("{:?}", self.tag);
        write!(f, "{:5} {:18} {}", self.ident, tag, self.message().unwrap_or("<non valid UTF-8>"))
    }
}

fn to_vsl_record_tag(num: u8) -> VslRecordTag {
    // Tend to work even for missing tags as they end up as SLT__Bogus (0)
    unsafe { mem::transmute(num as u32) }
}

pub fn vsl_record_v3<'b>(input: &'b[u8]) -> nom::IResult<&'b[u8], VslRecord<'b>, u32> {
    chain!(
        input,
        header: vsl_record_header ~ data: take!(header.len) ~ take!((4 - header.len % 4) % 4),
        || {
            VslRecord {
                tag: to_vsl_record_tag(header.tag),
                marker: header.marker,
                ident: header.ident,
                data: data
            }
        })
}

pub fn vsl_record_v4<'b>(input: &'b[u8]) -> nom::IResult<&'b[u8], VslRecord<'b>, u32> {
    chain!(
        input,
        header: vsl_record_header ~ data: take!(header.len - 1) ~ take!(1) ~ take!((4 - header.len % 4) % 4),
        || {
            VslRecord {
                tag: to_vsl_record_tag(header.tag),
                marker: header.marker,
                ident: header.ident,
                data: data
            }
        })
}

/*
fn binary_vsl_records<'b>(input: &'b[u8]) -> nom::IResult<&'b[u8], Vec<VslRecord<'b>>, u32> {
    chain!(
        input, binary_vsl_tag ~ records: many1!(vsl_record),
        || { records })
}
*/

