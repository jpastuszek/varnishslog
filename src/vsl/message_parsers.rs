use super::VslIdent;
use nom::{self, IResult};

pub trait IResultExt<O, E> {
    fn into_result(self) -> Result<O, E>;
}

//TODO: Move this to VslRecord?
impl<I, O, E> IResultExt<O, nom::Err<I, E>> for IResult<I, O, E> {
    fn into_result(self) -> Result<O, nom::Err<I, E>> {
        match self {
            IResult::Done(_, o) => Ok(o),
            IResult::Error(err) => Err(err),
            IResult::Incomplete(_) => panic!("got Incomplete IResult!"),
        }
    }
}

use nom::{rest_s, space, eof};
use std::str::FromStr;

/// Parsers for the message body of the VSL records
///
/// This should not allocate memory but do primitive conversion when applicable
/// To keep this simple they will be returning tuples.
/// Format and comments are form include/tbl/vsl_tags.h and include/tbl/vsl_tags_http.h.
///

pub type TimeStamp = f64;
pub type Duration = f64;
pub type Bytes = u64;
pub type FetchMode = u32;
pub type Status = u32;
pub type Port = u16;
pub type FileDescriptor = isize;

named!(label<&str, &str>, terminated!(take_until_s!(": "), tag_s!(": ")));
named!(token<&str, &str>, terminated!(is_not_s!(" "), alt_complete!(space | eof)));

//TODO: needs macro
named!(vsl_ident<&str, VslIdent>, map_res!(token, FromStr::from_str));
named!(bytes<&str, Bytes>, map_res!(token, FromStr::from_str));
named!(fech_mode<&str, FetchMode>, map_res!(token, FromStr::from_str));
named!(status<&str, Status>, map_res!(token, FromStr::from_str));
named!(time_stamp<&str, TimeStamp>, map_res!(token, FromStr::from_str));
named!(duration<&str, Duration>, map_res!(token, FromStr::from_str));
named!(port<&str, Port>, map_res!(token, FromStr::from_str));
named!(file_descriptor<&str, FileDescriptor>, map_res!(token, FromStr::from_str));

named!(pub slt_begin<&str, (&str, VslIdent, &str)>, complete!(tuple!(
        token,       // Type ("sess", "req" or "bereq")
        vsl_ident,   // Parent vxid
        token)));    // Reason

named!(pub slt_timestamp<&str, (&str, TimeStamp, Duration, Duration)>, complete!(tuple!(
        label,          // Event label
        time_stamp,     // Absolute time of event
        duration,       // Time since start of work unit
        duration)));    // Time since last timestamp

named!(pub slt_reqacc<&str, (Bytes, Bytes, Bytes, Bytes, Bytes, Bytes) >, complete!(tuple!(
        bytes,            // Header bytes received
        bytes,            // Body bytes received
        bytes,            // Total bytes received
        bytes,            // Header bytes transmitted
        bytes,            // Body bytes transmitted
        bytes)));     // Total bytes transmitted

named!(pub slt_method<&str, &str>, complete!(rest_s));
named!(pub slt_url<&str, &str>, complete!(rest_s));
named!(pub slt_protocol<&str, &str>, complete!(rest_s));
named!(pub slt_status<&str, Status>, complete!(status));
named!(pub slt_reason<&str, &str>, complete!(rest_s));

named!(pub header_name<&str, &str>, terminated!(take_until_s!(":"), tag_s!(":")));
pub fn header_value<'a>(input: &'a str) -> nom::IResult<&'a str, Option<&'a str>> {
    delimited!(input, opt!(space), opt!(rest_s), eof)
}
pub fn slt_header<'a>(input: &'a str) -> nom::IResult<&'a str, (&'a str, Option<&'a str>)> {
    complete!(input, tuple!(
        header_name,
        header_value))
}

named!(pub slt_sess_open<&str, ((&str, Port), &str, Option<(&str, Port)>, TimeStamp, FileDescriptor)>, complete!(tuple!(
        // Remote IPv4/6 address
        // Remote TCP port
        tuple!(token, port),
        // Listen socket (-a argument)
        token,
        // Local IPv4/6 address ('-' if !$log_local_addr)
        // Local TCP port ('-' if !$log_local_addr)
        chain!(
            some: map!(peek!(tuple!(token, token)), |pair| { pair != ("-", "-") }) ~
            addr: cond!(some, tuple!(token, port)),
            || { addr }),
        // Time stamp (undocumented)
        time_stamp,
        // File descriptor number
        file_descriptor)));

named!(pub slt_link<&str, (&str, VslIdent, &str)>, complete!(tuple!(
        token,      // Child type ("req" or "bereq")
        vsl_ident,  // Child vxid
        token)));   // Reason

named!(pub slt_sess_close<&str, (&str, Duration)>, complete!(tuple!(
        token,          // Why the connection closed
        duration)));    // How long the session was open

named!(pub stl_call<&str, &str>, complete!(rest_s));      // VCL method name

named!(pub stl_fetch_body<&str, (FetchMode, &str, bool)>, complete!(tuple!(
        fech_mode,  // Body fetch mode
        token,        // Text description of body fetch mode
        terminated!(map!(
                alt_complete!(tag_s!("stream") | tag_s!("-")),
                |s| s == "stream"), // 'stream' or '-'
            eof))));

