#[cfg(test)]
#[macro_use]
mod test_helpers {
    use vsl::{VslRecord, VslRecordTag, VslIdent};
    use std::sync::{Once, ONCE_INIT};

    pub use vsl::VslRecordTag::*;

    pub fn vsl(tag: VslRecordTag, ident: VslIdent, message: &str) -> VslRecord {
        VslRecord::from_str(tag, ident, message)
    }

    static LOGGER: Once = ONCE_INIT;

    pub fn log() {
        use env_logger;

        LOGGER.call_once(|| {
            env_logger::init().unwrap();
        });
    }

    macro_rules! assert_none {
        ($x:expr) => {{
            let opt: Option<_> = $x;
            assert!(opt.is_none(), "expected `{}` to be None", stringify!($x));
        }};
    }

    macro_rules! assert_some {
        ($x:expr) => {{
            let opt: Option<_> = $x;
            assert!(opt.is_some(), "expected `{}` to be Some", stringify!($x));
            opt.unwrap()
        }};
    }

    macro_rules! apply {
        ($state:ident, $ident:expr, $tag:ident, $message:expr) => {{
            let opt: Option<_> = $state.apply(&vsl($tag, $ident, $message));
            assert!(opt.is_none(), "expected apply to return None after applying: `{}, {:?}, {};`", $ident, $tag, $message);
        }};
    }

    macro_rules! apply_all {
        ($state:ident, $($t_ident:expr, $t_tag:ident, $t_message:expr;)+) => {{
            $(apply!($state, $t_ident, $t_tag, $t_message);)*
        }};
    }

    macro_rules! apply_final {
        ($state:ident, $ident:expr, $tag:ident, $message:expr) => {
            assert_some!($state.apply(&vsl($tag, $ident, $message)))
        };
    }
}

mod session_state;
mod record_state;


pub use self::record_state::*;
pub use self::session_state::SessionState;

include!(concat!(env!("OUT_DIR"), "/serde_types.rs"));

use std::io::Error as IoError;

use serde_json::error::Error as JsonError;
use serde_json::ser::to_writer as write_json;
use serde_json::ser::to_writer_pretty as write_json_pretty;
use std::io::Write;

quick_error! {
    #[derive(Debug)]
    pub enum OutputError {
        JsonSerialization(err: JsonError) {
            display("Failed to serialize to JSON: {}", err)
            from()
        }
        Io(err: IoError) {
            display("Failed to write to output: {}", err)
            from()
        }
    }
}

pub enum Format {
    Json,
    JsonPretty,
}

trait AsSer<'a> {
    type Out;

    fn as_ser(&'a self) -> Self::Out;
}

impl<'a> AsSer<'a> for Address {
    type Out = (&'a str, u16);
    fn as_ser(&'a self) -> Self::Out {
        (self.0.as_str(), self.1)
    }
}

impl<'a> AsSer<'a> for Handling {
    type Out = &'a str;
    fn as_ser(&self) -> Self::Out {
        match self {
            &Handling::Hit(_) => "hit",
            &Handling::Miss => "miss",
            &Handling::Pass => "pass",
            &Handling::HitPass(_) => "hit_for_pass",
            &Handling::Synth => "synth",
            &Handling::Pipe => "pipe",
        }
    }
}

impl<'a> AsSer<'a> for HttpRequest {
    type Out = HttpRequestLogEntry<'a>;
    fn as_ser(&'a self) -> Self::Out {
        HttpRequestLogEntry {
            protocol: self.protocol.as_str(),
            method: self.method.as_str(),
            url: self.url.as_str(),
            headers: self.headers.as_slice(),
        }
    }
}

impl<'a> AsSer<'a> for HttpResponse {
    type Out = HttpResponseLogEntry<'a>;
    fn as_ser(&'a self) -> Self::Out {
        HttpResponseLogEntry {
            status: self.status,
            reason: self.reason.as_str(),
            protocol: self.protocol.as_str(),
            headers: self.headers.as_slice(),
        }
    }
}

impl<'a> AsSer<'a> for Vec<LogEntry> {
    type Out = LogBook<'a>;
    fn as_ser(&'a self) -> Self::Out {
        LogBook {
            entries: self.as_slice(),
        }
    }
}

pub trait AccessLog {
    fn client_access_logs<W>(&self, format: &Format, out: &mut W) -> Result<(), OutputError> where W: Write;
}

impl AccessLog for SessionRecord {
    fn client_access_logs<W>(&self, format: &Format, out: &mut W) -> Result<(), OutputError> where W: Write {
        fn write<W>(format: &Format, out: &mut W, log_entry: &ClientAccessLogEntry) -> Result<(), OutputError> where W: Write {
            match format {
                &Format::Json | &Format::JsonPretty => {
                    let write = match format {
                        &Format::Json => write_json,
                        &Format::JsonPretty => write_json_pretty,
                    };

                    try!(write(out, &Entry {
                        record_type: "client_access",
                        record: &log_entry,
                    }));

                    try!(writeln!(out, ""));
                }
            }
            Ok(())
        }

        for record_link in self.client_records.iter() {
            if let Some(record) = record_link.get_resolved() {
                match record.transaction {
                    ClientAccessTransaction::Full {
                        ref esi_records,
                        ref request,
                        ref response,
                        process,
                        fetch,
                        ttfb,
                        serve,
                        ref accounting,
                        ..
                    } => try!(write(format, out, &ClientAccessLogEntry {
                        remote_address: self.remote.as_ser(),
                        session_timestamp: self.open,
                        start_timestamp: record.start,
                        end_timestamp: record.end,
                        handing: record.handling.as_ser(),
                        request: request.as_ser(),
                        response: response.as_ser(),
                        process: process,
                        fetch: fetch,
                        ttfb: ttfb,
                        serve: serve,
                        recv_header_bytes: accounting.recv_header,
                        recv_body_bytes: accounting.recv_body,
                        recv_total_bytes: accounting.recv_total,
                        sent_header_bytes: accounting.sent_header,
                        sent_body_bytes: accounting.sent_body,
                        sent_total_bytes: accounting.sent_total,
                        esi_count: esi_records.len(),
                        log: record.log.as_ser(),
                    })),
                    ClientAccessTransaction::Restarted { .. } => continue,
                    ClientAccessTransaction::Piped { .. } => continue,
                }
            } else {
                warn!("Found unresolved link: {:?}", record_link);
                continue
            }
        }
        Ok(())
    }
}
