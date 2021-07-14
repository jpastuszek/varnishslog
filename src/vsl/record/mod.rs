// Generated with ./mk_vsl_tag from Varnish headers: include/tbl/vsl_tags.h include/tbl/vsl_tags_http.h include/vsl_int.h
// https://github.com/varnishcache/varnish-cache/blob/master/include/vapi/vsl_int.h
// https://github.com/varnishcache/varnish-cache/blob/master/include/tbl/vsl_tags.h
// https://github.com/varnishcache/varnish-cache/blob/master/include/tbl/vsl_tags_http.h
mod tag_e;
pub mod message;
pub mod parser;

use std::fmt::{self, Debug, Display};
use quick_error::ResultExt;
use nom;
use quick_error::quick_error;
use bitflags::bitflags;

use crate::maybe_string::MaybeStr;
pub use self::tag_e::VSL_tag_e as VslRecordTag;

bitflags! {
    pub struct Marker: u8 {
        const VSL_CLIENTMARKER  = 0b0000_0001;
        const VSL_BACKENDMARKER = 0b0000_0010;
    }
}

impl Display for Marker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "[{}{}]",
               if self.contains(Marker::VSL_CLIENTMARKER) { "C" } else { " " },
               if self.contains(Marker::VSL_BACKENDMARKER) { "B" } else { " " })
    }
}

pub type VslIdent = u32;

#[derive(Debug)]
struct VslRecordHeader {
    tag: u8,
    len: u16,
    marker: Marker,
    ident: VslIdent,
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
        result.unwrap().1.context(self).map_err(From::from)
    }

    pub fn is_client(&self) -> bool {
        self.marker.contains(Marker::VSL_CLIENTMARKER)
    }

    pub fn is_backend(&self) -> bool {
        self.marker.contains(Marker::VSL_BACKENDMARKER)
    }
}

impl<'b> Debug for VslRecord<'b> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_struct("VSL Record")
            .field("tag", &self.tag)
            .field("marker", &self.marker)
            .field("ident", &self.ident)
            .field("data", &MaybeStr::from_bytes(self.data))
            .finish()
    }
}

impl<'b> Display for VslRecord<'b> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let tag = format!("{:?}", self.tag);

        if f.alternate() {
            write!(f, "{} {:5} {:18} {}", self.marker, self.ident, tag, MaybeStr::from_bytes(self.data))
        } else {
            write!(f, "VSL record (marker: {} ident: {} tag: {} data: {:?})", self.marker, self.ident, tag, MaybeStr::from_bytes(self.data))
        }
    }
}
