use std::str::{FromStr, from_utf8};

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
///   * token - foreign maybe string: URL, headers, methods, etc. - we cannot trust them to be UTF-8 so they
///   will be provided as bytes to the caller - this is to avoid mem alloc of lossless convertion and let the
///   client do the checking, logging and converstion etc

pub type TimeStamp = f64;
pub type Duration = f64;
pub type ByteCount = u64;
pub type FetchMode = u32;
pub type Status = u32;
pub type Port = u16;
pub type FileDescriptor = isize;

//TODO: benchmark symbol unsafe conversion
named!(token<&[u8], &[u8]>, terminated!(is_not!(b" "), alt_complete!(space | eof)));
named!(label<&[u8], &str>, map_res!(terminated!(take_until!(b": "), tag!(b": ")), from_utf8));
named!(symbol<&[u8], &str>, map_res!(token, from_utf8));
named!(header_name<&[u8], &[u8]>, terminated!(take_until!(b":"), tag!(b":")));
named!(header_value<&[u8], Option<&[u8]> >, delimited!(opt!(space), opt!(rest), eof));

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

named!(pub slt_method<&[u8], &[u8]>, call!(
        rest));

named!(pub slt_url<&[u8], &[u8]>, call!(
        rest));

named!(pub slt_protocol<&[u8], &[u8]>, call!(
        rest));

named!(pub slt_status<&[u8], Status>, call!(
        status));

named!(pub slt_reason<&[u8], &[u8]>, call!(
        rest));

named!(pub slt_header<&[u8], (&[u8], Option<&[u8]>)>, tuple!(
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

named!(pub slt_log<&[u8], &[u8]>, call!(
        rest));
