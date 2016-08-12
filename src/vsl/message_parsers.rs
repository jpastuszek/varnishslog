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

pub type TimeStamp = f64;
pub type Duration = f64;
pub type Bytes = u64;
pub type FetchMode = u32;
pub type Address = (String, u16);

/// Parsers for the message body of the VSL records
///
/// This should not allocate memory but do primitive conversion when applicable
/// To keep this simple they will be returning tuples.
/// Format and comments are form include/tbl/vsl_tags.h and include/tbl/vsl_tags_http.h.
///

named!(label<&str, &str>, terminated!(take_until_s!(": "), tag_s!(": ")));

named!(str_space<&str, &str>, terminated!(is_not_s!(" "), space));
named!(str_space_eof<&str, &str>, terminated!(is_not_s!(" "), eof));

named!(bytes_space<&str, Bytes>, map_res!(str_space, FromStr::from_str));
named!(bytes_space_eof<&str, Bytes>, map_res!(str_space_eof, FromStr::from_str));
named!(vsl_ident_space<&str, VslIdent>, map_res!(str_space, FromStr::from_str));
named!(fech_mode_space<&str, FetchMode>, map_res!(str_space, FromStr::from_str));

named!(pub slt_begin<&str, (&str, VslIdent, &str)>, complete!(tuple!(
        str_space,           // Type ("sess", "req" or "bereq")
        vsl_ident_space,     // Parent vxid
        str_space_eof)));    // Reason

named!(pub slt_timestamp<&str, (&str, &str, &str, &str)>, complete!(tuple!(
        label,                      // Event label
        str_space,           // Absolute time of event
        str_space,           // Time since start of work unit
        str_space_eof)));    // Time since last timestamp

named!(pub slt_reqacc<&str, (Bytes, Bytes, Bytes, Bytes, Bytes, Bytes) >, complete!(tuple!(
        bytes_space,            // Header bytes received
        bytes_space,            // Body bytes received
        bytes_space,            // Total bytes received
        bytes_space,            // Header bytes transmitted
        bytes_space,            // Body bytes transmitted
        bytes_space_eof)));     // Total bytes transmitted

named!(pub slt_method<&str, &str>, complete!(rest_s));
named!(pub slt_url<&str, &str>, complete!(rest_s));
named!(pub slt_protocol<&str, &str>, complete!(rest_s));
named!(pub slt_status<&str, &str>, complete!(rest_s));
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

named!(pub slt_session<&str, (&str, &str, &str, &str, &str, &str, &str)>, complete!(tuple!(
        str_space,           // Remote IPv4/6 address
        str_space,           // Remote TCP port
        str_space,           // Listen socket (-a argument)
        str_space,           // Local IPv4/6 address ('-' if !$log_local_addr)
        str_space,           // Local TCP port ('-' if !$log_local_addr)
        str_space,           // Time stamp (undocumented)
        str_space_eof)));    // File descriptor number

named!(pub slt_link<&str, (&str, &str, &str)>, complete!(tuple!(
        str_space,           // Child type ("req" or "bereq")
        str_space,           // Child vxid
        str_space_eof)));    // Reason

named!(pub slt_sess_close<&str, (&str, &str)>, complete!(tuple!(
        str_space,           // Why the connection closed
        str_space_eof)));    // How long the session was open

named!(pub stl_call<&str, &str>, complete!(rest_s));      // VCL method name

named!(pub stl_fetch_body<&str, (FetchMode, &str, bool)>, complete!(tuple!(
        fech_mode_space,  // Body fetch mode
        str_space,        // Text description of body fetch mode
        terminated!(map!(
                alt_complete!(tag_s!("stream") | tag_s!("-")),
                |s| s == "stream"), // 'stream' or '-'
            eof))));

