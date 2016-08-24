use std::fmt::{self, Debug, Display};
use std::mem;

use nom::{self, le_u32};
use quick_error::ResultExt;

mod tag_e;
pub use self::tag_e::VSL_tag_e as VslRecordTag;

mod message_parsers;
pub use self::message_parsers::*;

mod maybe_string;
pub use self::maybe_string::{MaybeStr, MaybeString};

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
const VSL_IDENTOFFSET: u8 = 30;
const VSL_IDENTMASK: u32 = !(0b0000_0011 << VSL_IDENTOFFSET);

/*
* VSL_CLIENT(ptr)
*   Non-zero if this is a client transaction
*
* VSL_BACKEND(ptr)
*   Non-zero if this is a backend transaction
*/

bitflags! {
    pub flags Marker: u8 {
        const VSL_CLIENTMARKER  = 0b0000_0001,
        const VSL_BACKENDMARKER = 0b0000_0010,
    }
}

impl Display for Marker {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "[{}{}]",
               if self.contains(VSL_CLIENTMARKER) { "C" } else { " " },
               if self.contains(VSL_BACKENDMARKER) { "B" } else { " " })
    }
}

pub type VslIdent = u32;

named!(pub binary_vsl_tag<&[u8], Option<&[u8]> >, opt!(complete!(tag!(b"VSL\0"))));

#[derive(Debug)]
struct VslRecordHeader {
    tag: u8,
    len: u16,
    marker: Marker,
    ident: VslIdent,
}

fn vsl_record_header<'b>(input: &'b[u8]) -> nom::IResult<&'b[u8], VslRecordHeader, u32> {
    chain!(
        input, r1: le_u32 ~ r2: le_u32,
        || {
            VslRecordHeader {
                tag: (r1 >> VSL_LENOFFSET) as u8,
                len: (r1 & VSL_LENMASK) as u16,
                marker: Marker::from_bits_truncate(((r2 & !VSL_IDENTMASK) >> VSL_IDENTOFFSET) as u8),
                ident: r2 & VSL_IDENTMASK,
            }
        })
}

pub struct VslRecord<'b> {
    pub tag: VslRecordTag,
    pub marker: Marker,
    pub ident: VslIdent,
    pub data: &'b[u8],
}

quick_error! {
    #[derive(Debug)]
    pub enum VslRecordParseError {
        Nom(nom_err: String, tag: VslRecordTag, record: String) {
            context(record: &'a VslRecord<'a>, err: nom::Err<&'a [u8]>) -> (format!("{}", err), record.tag, format!("{}", record))
            display("Nom parser failed on {}: {}", record, nom_err)
        }
    }
}

impl<'b> VslRecord<'b> {
    pub fn parse_data<T, P>(&'b self, parser: P) -> Result<T, VslRecordParseError> where
    P: Fn(&'b [u8]) -> nom::IResult<&'b [u8], T> {
        // Note: need type annotaion for the u32 error type as the output IResult has no Error
        // variant that would help to infer it
        let result: nom::IResult<_, Result<T, _>, u32> = opt_res!(self.data, complete!(parser));
        // unwrap here is safe as complete! eliminates Incomplete variant and opt_res! remaining Error variant
        result.unwrap().1.context(self).map_err(|err| From::from(err))
    }

    pub fn is_client(&self) -> bool {
        self.marker.contains(VSL_CLIENTMARKER)
    }

    pub fn is_backend(&self) -> bool {
        self.marker.contains(VSL_BACKENDMARKER)
    }

    #[cfg(test)]
    pub fn from_str<'s>(tag: VslRecordTag, ident: VslIdent, message: &'s str) -> VslRecord<'s> {
        VslRecord {
            tag: tag,
            marker: VSL_CLIENTMARKER,
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
            .field("data", &MaybeStr::from_bytes(&self.data))
            .finish()
    }
}

impl<'b> Display for VslRecord<'b> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let tag = format!("{:?}", self.tag);

        if f.alternate() {
            write!(f, "{} {:5} {:18} {}", self.marker, self.ident, tag, MaybeStr::from_bytes(self.data))
        } else {
            write!(f, "VSL record (marker: {} ident: {} tag: {} data: {:?})", self.marker, self.ident, tag, MaybeStr::from_bytes(self.data))
        }
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

