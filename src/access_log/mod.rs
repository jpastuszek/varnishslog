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

impl<'a> AsSer<'a> for BackendConnection {
    type Out = BackendConnectionLogEntry<'a>;
    fn as_ser(&'a self) -> Self::Out {
        BackendConnectionLogEntry {
            fd: self.fd,
            name: self.name.as_str(),
            remote_address: self.remote.as_ser(),
            local_address: self.local.as_ser(),
        }
    }
}

pub trait AccessLog {
    fn access_log<W>(&self, format: &Format, out: &mut W) -> Result<(), OutputError> where W: Write;
}

impl AccessLog for SessionRecord {
    fn access_log<W>(&self, format: &Format, out: &mut W) -> Result<(), OutputError> where W: Write {
        fn write<W, E>(format: &Format, out: &mut W, log_entry: &E) -> Result<(), OutputError> where W: Write, E: EntryType {
            match format {
                &Format::Json | &Format::JsonPretty => {
                    let write = match format {
                        &Format::Json => write_json,
                        &Format::JsonPretty => write_json_pretty,
                    };

                    try!(write(out, &Entry {
                        record_type: E::type_name(),
                        record: &log_entry,
                    }));

                    try!(writeln!(out, ""));
                }
            }
            Ok(())
        }

        fn find_final(record: &ClientAccessRecord, restart_count: usize) -> Option<(&ClientAccessRecord, usize)> {
            match record.transaction {
                ClientAccessTransaction::Full { .. } | ClientAccessTransaction::Piped { .. } => Some((record, restart_count)),
                ClientAccessTransaction::Restarted { ref restart_record, .. } => {
                    if let Some(record) = restart_record.get_resolved() {
                        find_final(record, restart_count + 1)
                    } else {
                        warn!("Found unresolved link {:?} in: {:?}", restart_record, record);
                        None
                    }
                },
            }
        }

        fn log_linked_client_access_record<W>(
            format: &Format,
            out: &mut W,
            session_record: &SessionRecord,
            record_link: &Link<ClientAccessRecord>,
            request_type: &'static str) -> Result<(), OutputError> where W: Write {
            if let Some(record) = record_link.get_resolved() {
                if let Some((final_record, restart_count)) = find_final(record, 0) {
                    // Note: we skip all the intermediate restart records
                    match (&record.transaction, &final_record.transaction) {
                        (&ClientAccessTransaction::Restarted {
                            ref request,
                            process,
                            ..
                        }, &ClientAccessTransaction::Full {
                            ref esi_records,
                            ref response,
                            fetch,
                            ttfb,
                            serve,
                            ref accounting,
                            ..
                        }) => try!(write(format, out, &ClientAccessLogEntry {
                            request_type: request_type,
                            remote_address: session_record.remote.as_ser(),
                            session_timestamp: session_record.open,
                            start_timestamp: record.start,
                            end_timestamp: final_record.end,
                            handing: final_record.handling.as_ser(),
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
                            restart_count: restart_count,
                            restart_log: Some(record.log.as_ser()),
                            log: final_record.log.as_ser(),
                        })),
                        (_, &ClientAccessTransaction::Full {
                            ref esi_records,
                            ref request,
                            ref response,
                            process,
                            fetch,
                            ttfb,
                            serve,
                            ref accounting,
                            ..
                        }) => try!(write(format, out, &ClientAccessLogEntry {
                            request_type: request_type,
                            remote_address: session_record.remote.as_ser(),
                            session_timestamp: session_record.open,
                            start_timestamp: final_record.start,
                            end_timestamp: final_record.end,
                            handing: final_record.handling.as_ser(),
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
                            restart_count: restart_count,
                            restart_log: None,
                            log: final_record.log.as_ser(),
                        })),
                        (_, &ClientAccessTransaction::Piped {
                            ref request,
                            ref backend_record,
                            process,
                            ttfb,
                            ..
                        }) => {
                            if let Some(backend_record) = backend_record.get_resolved() {
                                if let BackendAccessTransaction::Piped {
                                    request: ref backend_request,
                                    ref backend_connection,
                                    ..
                                } = backend_record.transaction {
                                    try!(write(format, out, &PipeSessionLogEntry {
                                        remote_address: session_record.remote.as_ser(),
                                        session_timestamp: session_record.open,
                                        start_timestamp: final_record.start,
                                        end_timestamp: final_record.end,
                                        handing: final_record.handling.as_ser(),
                                        request: request.as_ser(),
                                        backend_request: backend_request.as_ser(),
                                        process: process,
                                        ttfb: ttfb,
                                        log: final_record.log.as_ser(),
                                        backend_connection: backend_connection.as_ser(),
                                    }))
                                } else {
                                    warn!("Expected Piped ClientAccessRecord to link Piped BackendAccessTransaction; link {:?} in: {:?}",
                                          backend_record, final_record);
                                }
                            } else {
                                warn!("Found unresolved link {:?} in: {:?}", backend_record, final_record);
                            }
                        }
                        (_, &ClientAccessTransaction::Restarted { .. }) => panic!("got ClientAccessTransaction::Restarted as final final_record")
                    }

                    match &final_record.transaction {
                        &ClientAccessTransaction::Full {
                            ref esi_records,
                            ..
                        } => for esi_record_link in esi_records {
                            try!(log_linked_client_access_record(format, out, session_record, esi_record_link, "ESI"))
                        },
                        &ClientAccessTransaction::Restarted { .. } | &ClientAccessTransaction::Piped { .. } => (),
                    }
                } else {
                    warn!("Failed to find final record for: {:?}", record);
                }
            } else {
                warn!("Found unresolved link {:?} in: {:?}", record_link, session_record);
            }
            Ok(())
        }

        for record_link in self.client_records.iter() {
            try!(log_linked_client_access_record(format, out, self, record_link, "external"))
        }
        Ok(())
    }
}
