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

pub use std::io::Error as IoError;

pub use serde_json::error::Error as JsonError;
use serde_json::ser::to_writer as write_json;
use serde_json::ser::to_writer_pretty as write_json_pretty;
use std::io::Write;

use chrono::NaiveDateTime;
use linked_hash_map::LinkedHashMap;
use boolinator::Boolinator;

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
    NcsaJson,
}

trait AsSer<'a> {
    type Out;

    fn as_ser(&'a self) -> Self::Out;
}

impl<'a> AsSer<'a> for Address {
    type Out = AddressLogEntry<'a>;
    fn as_ser(&'a self) -> Self::Out {
        AddressLogEntry {
            ip: self.0.as_str(),
            port: self.1,
        }
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

impl<'a> AsSer<'a> for Vec<(String, String)> {
    type Out = &'a [(String, String)];
    fn as_ser(&'a self) -> Self::Out {
        self.as_slice()
    }
}

impl<'a> AsSer<'a> for HttpRequest {
    type Out = HttpRequestLogEntry<'a>;
    fn as_ser(&'a self) -> Self::Out {
        HttpRequestLogEntry {
            protocol: self.protocol.as_str(),
            method: self.method.as_str(),
            url: self.url.as_str(),
            headers: self.headers.as_ser(),
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
            headers: self.headers.as_ser(),
        }
    }
}

impl<'a> AsSer<'a> for Vec<LogEntry> {
    type Out = LogBook<'a>;
    fn as_ser(&'a self) -> Self::Out {
        LogBook(self)
    }
}

impl<'a> AsSer<'a> for LinkedHashMap<String, Vec<String>> {
    type Out = Index<'a>;
    fn as_ser(&'a self) -> Self::Out {
        Index(self)
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

impl<'a> AsSer<'a> for CacheObject {
    type Out = CacheObjectLogEntry<'a>;
    fn as_ser(&'a self) -> Self::Out {
        CacheObjectLogEntry {
            storage_type: self.storage_type.as_str(),
            storage_name: self.storage_name.as_str(),
            ttl_duration: self.ttl,
            grace_duration: self.grace,
            keep_duration: self.keep,
            since_timestamp: self.since,
            origin_timestamp: self.origin,
            fetch_mode: self.fetch_mode.as_str(),
            fetch_streamed: self.fetch_streamed,
            response: self.response.as_ser(),
        }
    }
}

pub fn log_session_record<W>(session_record: &SessionRecord, format: &Format, out: &mut W, make_indices: bool) -> Result<(), OutputError> where W: Write {
    fn write<W, E>(format: &Format, out: &mut W, log_entry: &E) -> Result<(), OutputError> where W: Write, E: EntryType {
        let write_entry = match format {
            &Format::Json => write_json,
            &Format::JsonPretty => write_json_pretty,
            &Format::NcsaJson => write_json,
        };
        match format {
            &Format::Json | &Format::JsonPretty => {
                try!(write_entry(out, &log_entry));

                try!(writeln!(out, ""));
            }
            &Format::NcsaJson => {
                // 192.168.1.115 - - [25/Aug/2016:11:56:55 +0000] "GET http://staging.eod.whatclinic.net/ HTTP/1.1" 503 1366
                let date_time = NaiveDateTime::from_timestamp(log_entry.timestamp() as i64, 0);

                //TODO: bench
                /*
                fn escape(s: &str) -> String {
                    s.split('"').collect::<Vec<_>>().join("\\\"")
                }
                */
                fn write_escaped<W>(out: &mut W, s: &str) -> Result<(), IoError> where W: Write {
                    let mut iter = s.split('"').peekable();
                    loop {
                        match (iter.next(), iter.peek()) {
                            (Some(i), Some(_)) => try!(write!(out, "{}\\\"", i)),
                            (Some(i), None) => {
                                try!(write!(out, "{}", i));
                                break
                            }
                            _ => unreachable!()
                        }
                    }
                    Ok(())
                }

                try!(write!(out, "{} {} - [{}] \"",
                            log_entry.remote_ip(),
                            log_entry.type_name(),
                            date_time.format("%d/%b/%Y:%H:%M:%S +0000")));

                try!(write_escaped(out, log_entry.request_method()));
                try!(write!(out, " "));
                try!(write_escaped(out, log_entry.request_url()));
                try!(write!(out, " "));
                try!(write_escaped(out, log_entry.request_protocol()));

                try!(write!(out, "\" {} {} ",
                            log_entry.response_status().unwrap_or(0),
                            log_entry.response_bytes().unwrap_or(0)));

                try!(write_entry(out, &log_entry));

                try!(writeln!(out, ""));
            }
        }
        Ok(())
    }

    fn follow_restarts(record: &ClientAccessRecord, restart_count: usize) -> Option<(&ClientAccessRecord, usize)> {
        match record.transaction {
            ClientAccessTransaction::Full { .. } |
            ClientAccessTransaction::Piped { .. } => Some((record, restart_count)),
            ClientAccessTransaction::Restarted { ref restart_record, .. } => {
                if let Some(record) = restart_record.get_resolved() {
                    follow_restarts(record, restart_count + 1)
                } else {
                    warn!("Found unresolved link {:?} in: {:?}", restart_record, record);
                    None
                }
            },
        }
    }

    fn make_header_index(headers: &[(String, String)]) -> LinkedHashMap<String, Vec<String>> {
        fn title_case(s: &str) -> String {
            let mut c = s.chars();
            match c.next() {
                None => String::new(),
                Some(f) => f.to_uppercase().chain(c.flat_map(|t| t.to_lowercase())).collect(),
            }
        }

        fn normalize_header_name(name: &str) -> String {
            //TODO: benchmark with itertools join
            name.split('-').map(|part| title_case(part)).collect::<Vec<_>>().join("-")
        }

        headers.iter().fold(LinkedHashMap::new(), |mut index, &(ref name, ref value)| {
            let name = normalize_header_name(name);

            // Note: this will put the header at the end of the index
            let mut values = index.remove(&name).unwrap_or(Vec::new());
            values.push(value.to_owned());
            index.insert(name, values);

            index
        })
    }

    fn make_log_vars_index(logs: &[LogEntry]) -> LinkedHashMap<String, Vec<String>> {
        let mut index = LinkedHashMap::new();

        for log_entry in logs {
            if let &LogEntry::Vcl(ref message) = log_entry {
                let mut s = message.splitn(2, ": ");
                if let Some(name) = s.next() {
                    if name.contains(' ') {
                        continue
                    }
                    if let Some(value) = s.next() {
                        let mut values = index.remove(name).unwrap_or(Vec::new());
                        values.push(value.to_owned());
                        index.insert(name.to_owned(), values);
                    }
                }
            }
        }
        index
    }

    fn with_linked_backend_access_record<F, R>(
        session_record: &SessionRecord,
        client_record: &ClientAccessRecord,
        record: &Option<Link<BackendAccessRecord>>,
        retry: usize,
        make_indices: bool,
        block: F) -> R where F: FnOnce(Option<&BackendAccessLogEntry>) -> R {
        if let &Some(ref record_link) = record {
            if let Some(ref record) = record_link.get_resolved() {
                match record.transaction {
                    BackendAccessTransaction::Full {
                        ref request,
                        ref response,
                        ref backend_connection,
                        ref cache_object,
                        send,
                        wait,
                        ttfb,
                        fetch,
                        ref accounting,
                        ..
                    } => {
                        let request_header_index = make_indices.as_some_from(|| make_header_index(request.headers.as_slice()));
                        let response_header_index = make_indices.as_some_from(|| make_header_index(response.headers.as_slice()));
                        let log_vars_index = make_indices.as_some_from(|| make_log_vars_index(record.log.as_slice()));
                        block(Some(&BackendAccessLogEntry {
                            vxid: client_record.ident,
                            remote_address: session_record.remote.as_ser(),
                            session_timestamp: session_record.open,
                            start_timestamp: record.start,
                            end_timestamp: record.end.unwrap_or(record.start),
                            handling: "fetch",
                            request: request.as_ser(),
                            response: Some(response.as_ser()),
                            send_duration: send,
                            wait_duration: Some(wait),
                            ttfb_duration: Some(ttfb),
                            fetch_duration: Some(fetch),
                            sent_header_bytes: Some(accounting.sent_header),
                            sent_body_bytes: Some(accounting.sent_body),
                            sent_total_bytes: Some(accounting.sent_total),
                            recv_header_bytes: Some(accounting.recv_header),
                            recv_body_bytes: Some(accounting.recv_body),
                            recv_total_bytes: Some(accounting.recv_total),
                            retry: retry,
                            backend_connection: Some(backend_connection.as_ser()),
                            cache_object: Some(cache_object.as_ser()),
                            log: record.log.as_ser(),
                            request_header_index: request_header_index.as_ref().map(|v| v.as_ser()),
                            response_header_index: response_header_index.as_ref().map(|v| v.as_ser()),
                            log_vars_index: log_vars_index.as_ref().map(|v| v.as_ser()),
                    }))},
                    BackendAccessTransaction::Failed { retry_record: ref record_link @ Some(_), .. } |
                    BackendAccessTransaction::Abandoned { retry_record: ref record_link @ Some(_), .. } =>
                        with_linked_backend_access_record(session_record, client_record, record_link, retry + 1, make_indices, block),
                    BackendAccessTransaction::Failed {
                        ref request,
                        synth,
                        ref accounting,
                        ..
                    } => {
                        let request_header_index = make_indices.as_some_from(|| make_header_index(request.headers.as_slice()));
                        let log_vars_index = make_indices.as_some_from(|| make_log_vars_index(record.log.as_slice()));
                        block(Some(&BackendAccessLogEntry {
                            vxid: client_record.ident,
                            remote_address: session_record.remote.as_ser(),
                            session_timestamp: session_record.open,
                            start_timestamp: record.start,
                            end_timestamp: record.end.unwrap_or(record.start),
                            handling: "fail",
                            request: request.as_ser(),
                            response: None,
                            send_duration: synth,
                            wait_duration: None,
                            ttfb_duration: None,
                            fetch_duration: None,
                            sent_header_bytes: Some(accounting.sent_header),
                            sent_body_bytes: Some(accounting.sent_body),
                            sent_total_bytes: Some(accounting.sent_total),
                            recv_header_bytes: Some(accounting.recv_header),
                            recv_body_bytes: Some(accounting.recv_body),
                            recv_total_bytes: Some(accounting.recv_total),
                            retry: retry,
                            backend_connection: None,
                            cache_object: None,
                            log: record.log.as_ser(),
                            request_header_index: request_header_index.as_ref().map(|v| v.as_ser()),
                            response_header_index: None,
                            log_vars_index: log_vars_index.as_ref().map(|v| v.as_ser()),
                        }))},
                    BackendAccessTransaction::Abandoned {
                        ref request,
                        ref response,
                        ref backend_connection,
                        ref retry_record,
                        send,
                        wait,
                        ttfb,
                        fetch,
                        ..
                    } => {
                        let request_header_index = make_indices.as_some_from(|| make_header_index(request.headers.as_slice()));
                        let response_header_index = make_indices.as_some_from(|| make_header_index(response.headers.as_slice()));
                        let log_vars_index = make_indices.as_some_from(|| make_log_vars_index(record.log.as_slice()));
                        block(Some(&BackendAccessLogEntry {
                            vxid: client_record.ident,
                            remote_address: session_record.remote.as_ser(),
                            session_timestamp: session_record.open,
                            start_timestamp: record.start,
                            end_timestamp: record.end.unwrap_or(record.start),
                            handling: if retry_record.is_some() { "retry" } else { "abandon" },
                            request: request.as_ser(),
                            response: Some(response.as_ser()),
                            send_duration: send,
                            wait_duration: Some(wait),
                            ttfb_duration: Some(ttfb),
                            fetch_duration: fetch,
                            recv_header_bytes: None,
                            recv_body_bytes: None,
                            recv_total_bytes: None,
                            sent_header_bytes: None,
                            sent_body_bytes: None,
                            sent_total_bytes: None,
                            retry: retry,
                            backend_connection: Some(backend_connection.as_ser()),
                            cache_object: None,
                            log: record.log.as_ser(),
                            request_header_index: request_header_index.as_ref().map(|v| v.as_ser()),
                            response_header_index: response_header_index.as_ref().map(|v| v.as_ser()),
                            log_vars_index: log_vars_index.as_ref().map(|v| v.as_ser()),
                        }))},
                    BackendAccessTransaction::Aborted { .. } |
                    BackendAccessTransaction::Piped { .. } => block(None),
                }
            } else {
                warn!("Found unresolved link {:?} in: {:?}", record_link, session_record);
                block(None)
            }
        } else {
            block(None)
        }
    }

    fn log_linked_client_access_record<W>(
        format: &Format,
        out: &mut W,
        session_record: &SessionRecord,
        record_link: &Link<ClientAccessRecord>,
        record_type: &'static str,
        make_indices: bool) -> Result<(), OutputError> where W: Write {
        if let Some(record) = record_link.get_resolved() {
            if let Some((final_record, restart_count)) = follow_restarts(record, 0) {
                // Note: we skip all the intermediate restart records
                match (&record.transaction, &final_record.transaction) {
                    (&ClientAccessTransaction::Restarted {
                        ref request,
                        process,
                        ..
                    }, &ClientAccessTransaction::Full {
                        ref esi_records,
                        ref response,
                        ref backend_record,
                        fetch,
                        ttfb,
                        serve,
                        ref accounting,
                        ..
                    }) => {
                        let request_header_index = make_indices.as_some_from(|| make_header_index(request.headers.as_slice()));
                        let response_header_index = make_indices.as_some_from(|| make_header_index(response.headers.as_slice()));
                        let log_vars_index = make_indices.as_some_from(|| make_log_vars_index(final_record.log.as_slice()));
                        try!(with_linked_backend_access_record(session_record, record, backend_record, 0, make_indices, |backend_access| {
                            write(format, out, &ClientAccessLogEntry {
                                record_type: record_type,
                                vxid: record.ident,
                                remote_address: session_record.remote.as_ser(),
                                session_timestamp: session_record.open,
                                start_timestamp: record.start,
                                end_timestamp: final_record.end,
                                handling: final_record.handling.as_ser(),
                                request: request.as_ser(),
                                response: response.as_ser(),
                                backend_access: backend_access,
                                process_duration: process,
                                fetch_duration: fetch,
                                ttfb_duration: ttfb,
                                serve_duration: serve,
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
                                request_header_index: request_header_index.as_ref().map(|v| v.as_ser()),
                                response_header_index: response_header_index.as_ref().map(|v| v.as_ser()),
                                log_vars_index: log_vars_index.as_ref().map(|v| v.as_ser()),
                            })}))},
                    (_, &ClientAccessTransaction::Full {
                        ref esi_records,
                        ref request,
                        ref response,
                        ref backend_record,
                        process,
                        fetch,
                        ttfb,
                        serve,
                        ref accounting,
                        ..
                    }) => {
                        let request_header_index = make_indices.as_some_from(|| make_header_index(request.headers.as_slice()));
                        let response_header_index = make_indices.as_some_from(|| make_header_index(response.headers.as_slice()));
                        let log_vars_index = make_indices.as_some_from(|| make_log_vars_index(final_record.log.as_slice()));
                        try!(with_linked_backend_access_record(session_record, record, backend_record, 0, make_indices, |backend_access| {
                            write(format, out, &ClientAccessLogEntry {
                                record_type: record_type,
                                vxid: record.ident,
                                remote_address: session_record.remote.as_ser(),
                                session_timestamp: session_record.open,
                                start_timestamp: final_record.start,
                                end_timestamp: final_record.end,
                                handling: final_record.handling.as_ser(),
                                request: request.as_ser(),
                                response: response.as_ser(),
                                backend_access: backend_access,
                                process_duration: process,
                                fetch_duration: fetch,
                                ttfb_duration: ttfb,
                                serve_duration: serve,
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
                                request_header_index: request_header_index.as_ref().map(|v| v.as_ser()),
                                response_header_index: response_header_index.as_ref().map(|v| v.as_ser()),
                                log_vars_index: log_vars_index.as_ref().map(|v| v.as_ser()),
                        })}))},
                    (_, &ClientAccessTransaction::Piped {
                        ref request,
                        ref backend_record,
                        process,
                        ttfb,
                        ref accounting,
                        ..
                    }) => {
                        if let Some(backend_record) = backend_record.get_resolved() {
                            if let BackendAccessTransaction::Piped {
                                request: ref backend_request,
                                ref backend_connection,
                                ..
                            } = backend_record.transaction {
                                let request_header_index = make_indices.as_some_from(|| make_header_index(request.headers.as_slice()));
                                let backend_request_header_index = make_indices.as_some_from(|| make_header_index(backend_request.headers.as_slice()));
                                let log_vars_index = make_indices.as_some_from(|| make_log_vars_index(final_record.log.as_slice()));
                                try!(write(format, out, &PipeSessionLogEntry {
                                    record_type: "pipe_session",
                                    vxid: record.ident,
                                    remote_address: session_record.remote.as_ser(),
                                    session_timestamp: session_record.open,
                                    start_timestamp: final_record.start,
                                    end_timestamp: final_record.end,
                                    request: request.as_ser(),
                                    backend_request: backend_request.as_ser(),
                                    process_duration: process,
                                    ttfb_duration: ttfb,
                                    recv_total_bytes: accounting.recv_total,
                                    sent_total_bytes: accounting.sent_total,
                                    log: final_record.log.as_ser(),
                                    backend_connection: backend_connection.as_ser(),
                                    request_header_index: request_header_index.as_ref().map(|v| v.as_ser()),
                                    backend_request_header_index: backend_request_header_index.as_ref().map(|v| v.as_ser()),
                                    log_vars_index: log_vars_index.as_ref().map(|v| v.as_ser()),
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
                        try!(log_linked_client_access_record(format, out, session_record, esi_record_link, "ESI_subrequest", make_indices))
                    },
                    _ => (),
                }
            } else {
                warn!("Failed to find final record for: {:?}", record);
            }
        } else {
            warn!("Found unresolved link {:?} in: {:?}", record_link, session_record);
        }
        Ok(())
    }

    for record_link in session_record.client_records.iter() {
        try!(log_linked_client_access_record(format, out, session_record, record_link, "client_request", make_indices))
    }
    Ok(())
}
