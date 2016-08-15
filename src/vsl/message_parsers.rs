use std::str::{FromStr, from_utf8};
use std::fmt::{self, Debug, Display};

use nom::{rest, space, eof};

use super::VslIdent;

/// Parsers for the message body of the VSL records
///
/// This should not allocate memory but do primitive conversion when applicable
/// To keep this simple they will be returning tuples.
/// Format and comments are form include/tbl/vsl_tags.h and include/tbl/vsl_tags_http.h.
///
/// Parsing:
/// * numeric types are parsed out - this is done with 0 allocation by mapping bytes as string and convertion
/// * strings
///   * symbol - this are comming from Varnish code (strings or numbers/IP) so they will be mapped to &str or
///   parsing will fail; symbols can be trusted to be UTF-8 compatible (ASCII)
///   * maybe_str - foreign string: URL, headers, methods, etc. - we cannot trust them to be UTF-8 so they
///   will be provided as bytes to the caller - this is to avoid mem alloc of lossless convertion and let the
///   client do the checking, logging and converstion etc

#[derive(PartialEq)]
pub struct MaybeStr([u8]);

impl MaybeStr {
    pub fn from_bytes(bytes: &[u8]) -> &MaybeStr {
        unsafe { &*((bytes as *const [u8]) as *const MaybeStr)}
    }

    fn as_bytes(&self) -> &[u8] {
        unsafe { &*((self as *const MaybeStr) as *const [u8])}
    }

    pub fn to_lossy_string(&self) -> String {
        String::from_utf8_lossy(self.as_bytes()).into_owned()
    }

    pub fn to_maybe_string(&self) -> MaybeString {
        MaybeString(self.as_bytes().to_owned())
    }
}

impl Debug for MaybeStr {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        if let Ok(string) = from_utf8(self.as_bytes()) {
            write!(f, "{:?}", string)
        } else {
            write!(f, "{:?}<non-UTF-8 data: {:?}>", String::from_utf8_lossy(&self.as_bytes()), &self.as_bytes())
        }
    }
}

impl Display for MaybeStr {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", String::from_utf8_lossy(&self.as_bytes()))
    }
}

//TODO: Debug, Display
#[derive(PartialEq, Debug)]
pub struct MaybeString(pub Vec<u8>);

impl MaybeString {
    pub fn to_lossy_string(self) -> String {
        match String::from_utf8(self.0) {
            Ok(string) => string,
            Err(err) => {
                String::from_utf8_lossy(err.into_bytes().as_slice()).into_owned()
            }
        }
    }

    pub fn as_maybe_str(&self) -> &MaybeStr {
        MaybeStr::from_bytes(self.0.as_slice())
    }
}

pub type TimeStamp = f64;
pub type Duration = f64;
pub type ByteCount = u64;
pub type FetchMode = u32;
pub type Status = u32;
pub type Port = u16;
pub type FileDescriptor = isize;

/// Wrap result in MaybeStr type
macro_rules! maybe_str {
    ($i:expr, $submac:ident!( $($args:tt)* )) => {
        map!($i, $submac!($($args)*), MaybeStr::from_bytes)
    };
    ($i:expr, $f:expr) => {
        map!($i, call!($f), MaybeStr::from_bytes)
    };
}

//TODO: benchmark symbol unsafe conversion
named!(token<&[u8], &[u8]>, terminated!(is_not!(b" "), alt_complete!(space | eof)));
named!(label<&[u8], &str>, map_res!(terminated!(take_until!(b": "), tag!(b": ")), from_utf8));
named!(symbol<&[u8], &str>, map_res!(token, from_utf8));

named!(header_name<&[u8], &MaybeStr>, maybe_str!(
        terminated!(take_until!(b":"), tag!(b":"))));
named!(header_value<&[u8], Option<&MaybeStr> >,
        delimited!(opt!(space), opt!(maybe_str!(rest)), eof));

macro_rules! named_parsed_symbol {
    ($name:ident<$parse:ty>) => {
        named!($name<&[u8], $parse>, map_res!(symbol, FromStr::from_str));
    }
}

named_parsed_symbol!(vsl_ident<VslIdent>);
named_parsed_symbol!(byte_count<ByteCount>);
named_parsed_symbol!(fech_mode<FetchMode>);
named_parsed_symbol!(status<Status>);
named_parsed_symbol!(time_stamp<TimeStamp>);
named_parsed_symbol!(duration<Duration>);
named_parsed_symbol!(port<Port>);
named_parsed_symbol!(file_descriptor<FileDescriptor>);

// VSL record message parsers by tag

named!(pub slt_begin<&[u8], (&str, VslIdent, &str)>, tuple!(
        symbol,     // Type (b"sess", "req" or "bereq")
        vsl_ident,  // Parent vxid
        symbol));   // Reason

named!(pub slt_timestamp<&[u8], (&str, TimeStamp, Duration, Duration)>, tuple!(
        label,          // Event label
        time_stamp,     // Absolute time of event
        duration,       // Time since start of work unit
        duration));     // Time since last timestamp

named!(pub slt_reqacc<&[u8], (ByteCount, ByteCount, ByteCount, ByteCount, ByteCount, ByteCount) >, tuple!(
        byte_count,     // Header bytes received
        byte_count,     // Body bytes received
        byte_count,     // Total bytes received
        byte_count,     // Header bytes transmitted
        byte_count,     // Body bytes transmitted
        byte_count));   // Total bytes transmitted

named!(pub slt_method<&[u8], &MaybeStr>, maybe_str!(
        rest));

named!(pub slt_url<&[u8], &MaybeStr>, maybe_str!(
        rest));

named!(pub slt_protocol<&[u8], &MaybeStr>, maybe_str!(
        rest));

named!(pub slt_status<&[u8], Status>, call!(
        status));

named!(pub slt_reason<&[u8], &MaybeStr>, maybe_str!(
        rest));

named!(pub slt_header<&[u8], (&MaybeStr, Option<&MaybeStr>)>, tuple!(
        header_name,
        header_value));

named!(pub slt_sess_open<&[u8], ((&str, Port), &str, Option<(&str, Port)>, TimeStamp, FileDescriptor)>, tuple!(
        // Remote IPv4/6 address
        // Remote TCP port
        tuple!(symbol, port),
        symbol,                  // Listen socket (-a argument)
        // Local IPv4/6 address ('-' if !$log_local_addr)
        // Local TCP port ('-' if !$log_local_addr)
        chain!(
            some: map!(peek!(tuple!(token, token)), |(ip, port)| { ip != b"-" && port != b"-" }) ~
            addr: cond!(some, tuple!(symbol, port)),
            || { addr }),
        time_stamp,             // Time stamp (undocumented)
        file_descriptor));      // File descriptor number

named!(pub slt_link<&[u8], (&str, VslIdent, &str)>, tuple!(
        symbol,     // Child type ("req" or "bereq")
        vsl_ident,  // Child vxid
        symbol));   // Reason

named!(pub slt_sess_close<&[u8], (&str, Duration)>, tuple!(
        symbol,         // Why the connection closed
        duration));     // How long the session was open

named!(pub slt_call<&[u8], &str>, call!(
        symbol));   // VCL method name

named!(pub slt_fetch_body<&[u8], (FetchMode, &str, bool)>, tuple!(
        fech_mode,  // Body fetch mode
        symbol,     // Text description of body fetch mode
        // 'stream' or '-'
        terminated!(map!(
                alt_complete!(tag!(b"stream") | tag!(b"-")),
                |s| s == b"stream"),
            eof)));

named!(pub slt_log<&[u8], &MaybeStr>, maybe_str!(
        rest));
