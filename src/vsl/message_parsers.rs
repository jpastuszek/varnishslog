use std::str::FromStr;

use nom::{rest_s, space, eof};

use super::VslIdent;

/// Parsers for the message body of the VSL records
///
/// This should not allocate memory but do primitive conversion when applicable
/// To keep this simple they will be returning tuples.
/// Format and comments are form include/tbl/vsl_tags.h and include/tbl/vsl_tags_http.h.
///

//TODO: work with bytes!

pub type TimeStamp = f64;
pub type Duration = f64;
pub type ByteCount = u64;
pub type FetchMode = u32;
pub type Status = u32;
pub type Port = u16;
pub type FileDescriptor = isize;

named!(label<&str, &str>, terminated!(take_until_s!(": "), tag_s!(": ")));
named!(token<&str, &str>, terminated!(is_not_s!(" "), alt_complete!(space | eof)));
named!(header_name<&str, &str>, terminated!(take_until_s!(":"), tag_s!(":")));
named!(header_value<&str, Option<&str> >, delimited!(opt!(space), opt!(rest_s), eof));

macro_rules! named_parsed_token {
    ($name:ident<$parse:ty>) => {
        named!($name<&str, $parse>, map_res!(token, FromStr::from_str));
    }
}

named_parsed_token!(vsl_ident<VslIdent>);
named_parsed_token!(byte_count<ByteCount>);
named_parsed_token!(fech_mode<FetchMode>);
named_parsed_token!(status<Status>);
named_parsed_token!(time_stamp<TimeStamp>);
named_parsed_token!(duration<Duration>);
named_parsed_token!(port<Port>);
named_parsed_token!(file_descriptor<FileDescriptor>);

// VSL record message parsers by tag

named!(pub slt_begin<&str, (&str, VslIdent, &str)>, tuple!(
        token,       // Type ("sess", "req" or "bereq")
        vsl_ident,   // Parent vxid
        token));     // Reason

named!(pub slt_timestamp<&str, (&str, TimeStamp, Duration, Duration)>, tuple!(
        label,          // Event label
        time_stamp,     // Absolute time of event
        duration,       // Time since start of work unit
        duration));     // Time since last timestamp

named!(pub slt_reqacc<&str, (ByteCount, ByteCount, ByteCount, ByteCount, ByteCount, ByteCount) >, tuple!(
        byte_count,     // Header bytes received
        byte_count,     // Body bytes received
        byte_count,     // Total bytes received
        byte_count,     // Header bytes transmitted
        byte_count,     // Body bytes transmitted
        byte_count));   // Total bytes transmitted

named!(pub slt_method<&str, &str>, call!(
        rest_s));

named!(pub slt_url<&str, &str>, call!(
        rest_s));

named!(pub slt_protocol<&str, &str>, call!(
        rest_s));

named!(pub slt_status<&str, Status>, call!(
        status));

named!(pub slt_reason<&str, &str>, call!(
        rest_s));

named!(pub slt_header<&str, (&str, Option<&str>)>, tuple!(
        header_name,
        header_value));

named!(pub slt_sess_open<&str, ((&str, Port), &str, Option<(&str, Port)>, TimeStamp, FileDescriptor)>, tuple!(
        // Remote IPv4/6 address
        // Remote TCP port
        tuple!(token, port),
        token,                  // Listen socket (-a argument)
        // Local IPv4/6 address ('-' if !$log_local_addr)
        // Local TCP port ('-' if !$log_local_addr)
        chain!(
            some: map!(peek!(tuple!(token, token)), |pair| { pair != ("-", "-") }) ~
            addr: cond!(some, tuple!(token, port)),
            || { addr }),
        time_stamp,             // Time stamp (undocumented)
        file_descriptor));      // File descriptor number

named!(pub slt_link<&str, (&str, VslIdent, &str)>, tuple!(
        token,      // Child type ("req" or "bereq")
        vsl_ident,  // Child vxid
        token));    // Reason

named!(pub slt_sess_close<&str, (&str, Duration)>, tuple!(
        token,          // Why the connection closed
        duration));     // How long the session was open

named!(pub slt_call<&str, &str>, call!(
        rest_s));   // VCL method name

named!(pub slt_fetch_body<&str, (FetchMode, &str, bool)>, tuple!(
        fech_mode,  // Body fetch mode
        token,      // Text description of body fetch mode
        // 'stream' or '-'
        terminated!(map!(
                alt_complete!(tag_s!("stream") | tag_s!("-")),
                |s| s == "stream"),
            eof)));

named!(pub slt_log<&str, &str>, call!(
        rest_s));
