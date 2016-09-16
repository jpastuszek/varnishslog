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

pub use std::io::Error as IoError;

pub use serde_json::error::Error as JsonError;
use serde_json::ser::to_writer as write_json;
use serde_json::ser::to_writer_pretty as write_json_pretty;
use std::io::Write;

use chrono::NaiveDateTime;
use linked_hash_map::LinkedHashMap;
use boolinator::Boolinator;

mod ser {
    use super::LogEntry as VslLogEntry;
    use super::AclResult as VslAclResult;
    include!(concat!(env!("OUT_DIR"), "/serde_types.rs"));
}

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

trait AsSerIndexed<'a: 'i, 'i> {
    type Out;
    fn as_ser_indexed(&'a self, index: &'i LinkedHashMap<String, Vec<&'a str>>) -> Self::Out;
}

impl<'a> AsSer<'a> for Address {
    type Out = ser::Address<'a>;
    fn as_ser(&'a self) -> Self::Out {
        ser::Address {
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
    type Out = ser::HttpRequest<'a, 'a>;
    fn as_ser(&'a self) -> Self::Out {
        ser::HttpRequest {
            protocol: self.protocol.as_str(),
            method: self.method.as_str(),
            url: self.url.as_str(),
            headers: ser::Headers::Raw(self.headers.as_ser()),
        }
    }
}

impl<'a: 'i, 'i> AsSerIndexed<'a, 'i> for HttpRequest {
    type Out = ser::HttpRequest<'a, 'i>;
    fn as_ser_indexed(&'a self, index: &'i LinkedHashMap<String, Vec<&'a str>>) -> Self::Out {
        ser::HttpRequest {
            protocol: self.protocol.as_str(),
            method: self.method.as_str(),
            url: self.url.as_str(),
            headers: ser::Headers::Indexed(index.as_ser()),
        }
    }
}

impl<'a> AsSer<'a> for HttpResponse {
    type Out = ser::HttpResponse<'a, 'a>;
    fn as_ser(&'a self) -> Self::Out {
        ser::HttpResponse {
            status: self.status,
            reason: self.reason.as_str(),
            protocol: self.protocol.as_str(),
            headers: ser::Headers::Raw(self.headers.as_ser()),
        }
    }
}

impl<'a: 'i, 'i> AsSerIndexed<'a, 'i> for HttpResponse {
    type Out = ser::HttpResponse<'a, 'i>;
    fn as_ser_indexed(&'a self, index: &'i LinkedHashMap<String, Vec<&'a str>>) -> Self::Out {
        ser::HttpResponse {
            status: self.status,
            reason: self.reason.as_str(),
            protocol: self.protocol.as_str(),
            headers: ser::Headers::Indexed(index.as_ser()),
        }
    }
}

impl<'a> AsSer<'a> for Vec<LogEntry> {
    type Out = ser::Log<'a>;
    fn as_ser(&'a self) -> Self::Out {
        //TODO: map LogEntry when impl Iterator is stable
        ser::Log(self)
    }
}

impl<'a: 'i, 'i> AsSer<'i> for LinkedHashMap<String, Vec<&'a str>> {
    type Out = ser::Index<'a, 'i>;
    fn as_ser(&'i self) -> Self::Out {
        ser::Index(self)
    }
}

impl<'a: 'i, 'i> AsSer<'i> for LinkedHashMap<&'a str, &'a str> {
    type Out = ser::LogVarsIndex<'a, 'i>;
    fn as_ser(&'i self) -> Self::Out {
        ser::LogVarsIndex(self)
    }
}

impl<'a: 'i, 'i> AsSer<'i> for Vec<&'a str> {
    type Out = ser::LogMessages<'a, 'i>;
    fn as_ser(&'i self) -> Self::Out {
        self.as_slice()
    }
}

impl<'a> AsSer<'a> for BackendConnection {
    type Out = ser::BackendConnection<'a>;
    fn as_ser(&'a self) -> Self::Out {
        ser::BackendConnection {
            fd: self.fd,
            name: self.name.as_str(),
            remote_address: self.remote.as_ser(),
            local_address: self.local.as_ser(),
        }
    }
}

impl<'a> AsSer<'a> for CacheObject {
    type Out = ser::CacheObject<'a, 'a>;
    fn as_ser(&'a self) -> Self::Out {
        ser::CacheObject {
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

impl<'a: 'i, 'i> AsSerIndexed<'a, 'i> for CacheObject {
    type Out = ser::CacheObject<'a, 'i>;
    fn as_ser_indexed(&'a self, index: &'i LinkedHashMap<String, Vec<&'a str>>) -> Self::Out {
        ser::CacheObject {
            storage_type: self.storage_type.as_str(),
            storage_name: self.storage_name.as_str(),
            ttl_duration: self.ttl,
            grace_duration: self.grace,
            keep_duration: self.keep,
            since_timestamp: self.since,
            origin_timestamp: self.origin,
            fetch_mode: self.fetch_mode.as_str(),
            fetch_streamed: self.fetch_streamed,
            response: self.response.as_ser_indexed(index),
        }
    }
}

pub fn log_session_record<W>(session_record: &SessionRecord, format: &Format, out: &mut W,
                             index_log_vars: bool, index_headers: bool, index_headers_inplace: bool, no_log: bool)
    -> Result<(), OutputError> where W: Write {
    fn write<W, E>(format: &Format, out: &mut W, log_entry: &E) -> Result<(), OutputError> where W: Write, E: ser::EntryType {
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

    fn make_header_index(headers: &[(String, String)]) -> LinkedHashMap<String, Vec<&str>> {
        fn title_case(s: &str) -> String {
            let mut c = s.chars();
            match c.next() {
                None => String::new(),
                Some(f) => f.to_uppercase().chain(c.flat_map(|t| t.to_lowercase())).collect(),
            }
        }

        fn normalize_header_name(name: &str) -> String {
            //TODO: benchmark with itertools join
            //TODO: what about Cow?
            name.split('-').map(|part| title_case(part)).collect::<Vec<_>>().join("-")
        }

        headers.iter().fold(LinkedHashMap::new(), |mut index, &(ref name, ref value)| {
            let name = normalize_header_name(name);

            // Note: this will put the header at the end of the index
            let mut values = index.remove(&name).unwrap_or(Vec::new());
            values.push(value);
            index.insert(name, values);

            index
        })
    }

    struct LogIndex<'a> {
        vars: LinkedHashMap<&'a str, &'a str>,
        messages: Vec<&'a str>,
        acl_matched: Vec<&'a str>,
        acl_not_matched: Vec<&'a str>,
    }

    fn index_log<'a>(logs: &'a [LogEntry]) -> LogIndex<'a> {
        let mut vars = LinkedHashMap::new();
        let mut messages = Vec::new();
        let mut acl_matched = Vec::new();
        let mut acl_not_matched = Vec::new();

        for log_entry in logs {
            match log_entry {
                &LogEntry::Vcl(ref message) => {
                    let mut s = message.splitn(2, ": ").fuse();
                    if let (Some(name), Some(value)) = (s.next(), s.next()) {
                        if !name.contains(' ') {
                            vars.insert(name, value);
                            continue;
                        }
                    }
                    messages.push(message.as_str());
                }
                &LogEntry::Debug(ref message) => messages.push(message.as_str()),
                &LogEntry::Error(ref message) => messages.push(message.as_str()),
                &LogEntry::FetchError(ref message) => messages.push(message.as_str()),
                &LogEntry::Warning(ref message) => messages.push(message.as_str()),
                &LogEntry::Acl(ref result, ref name, _) => {
                    match result {
                        &AclResult::Match => acl_matched.push(name.as_str()),
                        &AclResult::NoMatch => acl_not_matched.push(name.as_str()),
                    }
                }
            }
        }

        LogIndex {
            vars: vars,
            messages: messages,
            acl_matched: acl_matched,
            acl_not_matched: acl_not_matched,
        }
    }

    struct FlatBackendAccessRecord<'a> {
        final_record: &'a BackendAccessRecord,
        handling: &'static str,
        request: &'a HttpRequest,
        response: Option<&'a HttpResponse>,
        send_duration: Duration,
        wait_duration: Option<Duration>,
        ttfb_duration: Option<Duration>,
        fetch_duration: Option<Duration>,
        accounting: Option<&'a Accounting>,
        retry: usize,
        backend_connection: Option<&'a BackendConnection>,
        cache_object: Option<&'a CacheObject>,
    }

    enum FlatClientAccessRecord<'a> {
        ClientAccess {
            record: &'a ClientAccessRecord,
            final_record: &'a ClientAccessRecord,
            request: &'a HttpRequest,
            response: &'a HttpResponse,
            backend_record: &'a Option<Link<BackendAccessRecord>>,
            process_duration: Option<Duration>,
            fetch_duration: Option<Duration>,
            ttfb_duration: Duration,
            serve_duration: Duration,
            accounting: &'a Accounting,
            esi_records: &'a Vec<Link<ClientAccessRecord>>,
            restart_count: usize,
            restart_log: Option<&'a Vec<LogEntry>>,
        },
        PipeSession {
            record: &'a ClientAccessRecord,
            final_record: &'a ClientAccessRecord,
            request: &'a HttpRequest,
            backend_request: &'a HttpRequest,
            process_duration: Option<Duration>,
            ttfb_duration: Duration,
            accounting: &'a PipeAccounting,
            backend_connection: &'a BackendConnection,
        }
    }

    fn flatten_linked_backend_log_record<F, R>(
        session_record: &SessionRecord,
        client_record: &ClientAccessRecord,
        maybe_record_link: &Option<Link<BackendAccessRecord>>,
        retry: usize,
        block: F) -> R where F: FnOnce(Option<&FlatBackendAccessRecord>) -> R {
        if let &Some(ref record_link) = maybe_record_link {
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
                    } => return block(Some(&FlatBackendAccessRecord {
                        final_record: record,
                        handling: "fetch",
                        request: request,
                        response: Some(response),
                        send_duration: send,
                        wait_duration: Some(wait),
                        ttfb_duration: Some(ttfb),
                        fetch_duration: Some(fetch),
                        accounting: Some(accounting),
                        retry: retry,
                        backend_connection: Some(backend_connection),
                        cache_object: Some(cache_object),
                    })),
                    BackendAccessTransaction::Failed { retry_record: ref record_link @ Some(_), .. } |
                    BackendAccessTransaction::Abandoned { retry_record: ref record_link @ Some(_), .. } =>
                        return flatten_linked_backend_log_record(session_record, client_record, record_link, retry + 1, block),
                    BackendAccessTransaction::Failed {
                        ref request,
                        synth,
                        ref accounting,
                        ..
                    } => return block(Some(&FlatBackendAccessRecord {
                        final_record: record,
                        handling: "fail",
                        request: request,
                        response: None,
                        send_duration: synth,
                        wait_duration: None,
                        ttfb_duration: None,
                        fetch_duration: None,
                        accounting: Some(accounting),
                        retry: retry,
                        backend_connection: None,
                        cache_object: None,
                    })),
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
                    } => return block(Some(&FlatBackendAccessRecord {
                        final_record: record,
                        handling: if retry_record.is_some() { "retry" } else { "abandon" },
                        request: request,
                        response: Some(response),
                        send_duration: send,
                        wait_duration: Some(wait),
                        ttfb_duration: Some(ttfb),
                        fetch_duration: fetch,
                        accounting: None,
                        retry: retry,
                        backend_connection: Some(backend_connection),
                        cache_object: None,
                    })),
                    BackendAccessTransaction::Aborted { .. } |
                    BackendAccessTransaction::Piped { .. } => return block(None),
                }
            } else {
                warn!("Found unresolved link {:?} in: {:?}", record_link, session_record);
            }
        }
        block(None)
    }

    fn flatten_linked_client_log_record<F, R>(
        session_record: &SessionRecord,
        record_link: &Link<ClientAccessRecord>,
        block: F) -> R where F: FnOnce(Option<&FlatClientAccessRecord>) -> R {
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
                    }) => return block(Some(&FlatClientAccessRecord::ClientAccess {
                        record: record,
                        final_record: final_record,
                        request: request,
                        response: response,
                        backend_record: backend_record,
                        process_duration: process,
                        fetch_duration: fetch,
                        ttfb_duration: ttfb,
                        serve_duration: serve,
                        accounting: accounting,
                        esi_records: esi_records,
                        restart_count: restart_count,
                        restart_log: Some(&record.log),
                    })),
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
                    }) => return block(Some(&FlatClientAccessRecord::ClientAccess {
                        record: record,
                        final_record: final_record,
                        request: request,
                        response: response,
                        backend_record: backend_record,
                        process_duration: process,
                        fetch_duration: fetch,
                        ttfb_duration: ttfb,
                        serve_duration: serve,
                        accounting: accounting,
                        esi_records: esi_records,
                        restart_count: restart_count,
                        restart_log: None,
                    })),
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

                                return block(Some(&FlatClientAccessRecord::PipeSession {
                                    record: record,
                                    final_record: final_record,
                                    request: request,
                                    backend_request: backend_request,
                                    process_duration: process,
                                    ttfb_duration: ttfb,
                                    accounting: accounting,
                                    backend_connection: backend_connection,
                                }))
                            } else {
                                warn!("Expected Piped ClientAccessRecord to link Piped BackendAccessTransaction; link {:?} in: {:?}",
                                      backend_record, final_record);
                            }
                        } else {
                            warn!("Found unresolved link {:?} in: {:?}", backend_record, final_record);
                        }
                    },
                    (_, &ClientAccessTransaction::Restarted { .. }) => panic!("got ClientAccessTransaction::Restarted as final final_record"),
                }
            } else {
                warn!("Failed to find final record for: {:?}", record);
            }
        } else {
            warn!("Found unresolved link {:?} in: {:?}", record_link, session_record);
        }
        block(None)
    }

    fn log_linked_client_access_record<W>(
        format: &Format,
        out: &mut W,
        session_record: &SessionRecord,
        record_link: &Link<ClientAccessRecord>,
        record_type: &'static str,
        index_log_vars: bool, index_headers: bool, index_headers_inplace: bool, no_log: bool
        ) -> Result<(), OutputError> where W: Write {
        flatten_linked_client_log_record(session_record, record_link, |client_log_record| {
            if let Some(client_log_record) = client_log_record {
                match client_log_record {
                    &FlatClientAccessRecord::ClientAccess {
                        record,
                        final_record,
                        request,
                        response,
                        backend_record,
                        process_duration,
                        fetch_duration,
                        ttfb_duration,
                        serve_duration,
                        accounting,
                        esi_records,
                        restart_count,
                        restart_log,
                    } => {
                        for esi_record_link in esi_records {
                            try!(log_linked_client_access_record(format, out, session_record, esi_record_link, "esi_subrequest",
                                                                 index_log_vars, index_headers, index_headers_inplace, no_log));
                        }

                        try!(flatten_linked_backend_log_record(session_record, record, backend_record, 0, |backend_log_record| {
                            // backend record
                            // Need to live up to write()
                            let mut log_index = None;

                            let mut request_header_index = None;
                            let mut response_header_index = None;
                            let mut cache_object_response_header_index = None;


                            let backend_access_log_entry = backend_log_record.map(|backend_log_record| {
                                let indexed_request;
                                let indexed_response;
                                let indexed_cache_object;

                                if index_log_vars {
                                    log_index = Some(index_log(backend_log_record.final_record.log.as_slice()));
                                }

                                if index_headers | index_headers_inplace {
                                    request_header_index = Some(make_header_index(backend_log_record.request.headers.as_slice()));
                                    response_header_index = backend_log_record.response.as_ref().map(|response| make_header_index(response.headers.as_slice()));
                                    cache_object_response_header_index = backend_log_record.cache_object.as_ref().map(|cache_object| make_header_index(cache_object.response.headers.as_slice()));
                                }

                                if index_headers_inplace {
                                    indexed_request = backend_log_record.request.as_ser_indexed(request_header_index.as_ref().unwrap());
                                    indexed_response = backend_log_record.response.map(|response| response.as_ser_indexed(response_header_index.as_ref().unwrap()));
                                    indexed_cache_object = backend_log_record.cache_object.map(|cache_object| cache_object.as_ser_indexed(cache_object_response_header_index.as_ref().unwrap()));
                                } else {
                                    indexed_request = backend_log_record.request.as_ser();
                                    indexed_response = backend_log_record.response.map(|response| response.as_ser());
                                    indexed_cache_object = backend_log_record.cache_object.map(|cache_object| cache_object.as_ser());
                                }

                                ser::BackendAccess {
                                    vxid: record.ident,
                                    remote_address: session_record.remote.as_ser(),
                                    session_timestamp: session_record.open,
                                    start_timestamp: backend_log_record.final_record.start,
                                    end_timestamp: backend_log_record.final_record.end.unwrap_or(backend_log_record.final_record.start),
                                    handling: backend_log_record.handling,
                                    request: indexed_request,
                                    response: indexed_response,
                                    send_duration: backend_log_record.send_duration,
                                    wait_duration: backend_log_record.wait_duration,
                                    ttfb_duration: backend_log_record.ttfb_duration,
                                    fetch_duration: backend_log_record.fetch_duration,
                                    sent_header_bytes: backend_log_record.accounting.map(|a| a.sent_header),
                                    sent_body_bytes: backend_log_record.accounting.map(|a| a.sent_body),
                                    sent_total_bytes: backend_log_record.accounting.map(|a| a.sent_total),
                                    recv_header_bytes: backend_log_record.accounting.map(|a| a.recv_header),
                                    recv_body_bytes: backend_log_record.accounting.map(|a| a.recv_body),
                                    recv_total_bytes: backend_log_record.accounting.map(|a| a.recv_total),
                                    retry: backend_log_record.retry,
                                    backend_connection: backend_log_record.backend_connection.map(|b| b.as_ser()),
                                    cache_object: indexed_cache_object,
                                    log: (!no_log).as_some_from(|| backend_log_record.final_record.log.as_ser()),
                                    request_header_index: index_headers.as_some_from(|| request_header_index.as_ref().unwrap().as_ser()),
                                    response_header_index: response_header_index.as_ref().and_then(|index| index_headers.as_some_from(|| index.as_ser())),
                                    cache_object_response_header_index: cache_object_response_header_index.as_ref().and_then(|index| index_headers.as_some_from(|| index.as_ser())),
                                    log_vars: log_index.as_ref().map(|v| v.vars.as_ser()),
                                    log_messages: log_index.as_ref().map(|v| v.messages.as_ser()),
                                    acl_matched: log_index.as_ref().map(|v| v.acl_matched.as_ser()),
                                    acl_not_matched: log_index.as_ref().map(|v| v.acl_not_matched.as_ser()),
                                }
                            });

                            // client record
                            let mut log_index = None;

                            let mut request_header_index = None;
                            let mut response_header_index = None;

                            let indexed_request;
                            let indexed_response;

                            if index_log_vars {
                                log_index = Some(index_log(final_record.log.as_slice()));
                            }

                            if index_headers | index_headers_inplace {
                                request_header_index = Some(make_header_index(request.headers.as_slice()));
                                response_header_index = Some(make_header_index(response.headers.as_slice()));
                            }

                            if index_headers_inplace {
                                indexed_request = request.as_ser_indexed(request_header_index.as_ref().unwrap());
                                indexed_response = response.as_ser_indexed(response_header_index.as_ref().unwrap());
                            } else {
                                indexed_request = request.as_ser();
                                indexed_response = response.as_ser();
                            }

                            let client_access = ser::ClientAccess {
                                record_type: record_type,
                                vxid: record.ident,
                                remote_address: session_record.remote.as_ser(),
                                session_timestamp: session_record.open,
                                start_timestamp: final_record.start,
                                end_timestamp: final_record.end,
                                handling: final_record.handling.as_ser(),
                                request: indexed_request,
                                response: indexed_response,
                                backend_access: backend_access_log_entry.as_ref(),
                                process_duration: process_duration,
                                fetch_duration: fetch_duration,
                                ttfb_duration: ttfb_duration,
                                serve_duration: serve_duration,
                                recv_header_bytes: accounting.recv_header,
                                recv_body_bytes: accounting.recv_body,
                                recv_total_bytes: accounting.recv_total,
                                sent_header_bytes: accounting.sent_header,
                                sent_body_bytes: accounting.sent_body,
                                sent_total_bytes: accounting.sent_total,
                                esi_count: esi_records.len(),
                                restart_count: restart_count,
                                restart_log: restart_log.and_then(|restart_log| (!no_log).as_some_from(|| restart_log.as_ser())),
                                log: (!no_log).as_some_from(|| final_record.log.as_ser()),
                                request_header_index: index_headers.as_some_from(|| request_header_index.as_ref().unwrap().as_ser()),
                                response_header_index: index_headers.as_some_from(|| response_header_index.as_ref().unwrap().as_ser()),
                                log_vars: log_index.as_ref().map(|v| v.vars.as_ser()),
                                log_messages: log_index.as_ref().map(|v| v.messages.as_ser()),
                                acl_matched: log_index.as_ref().map(|v| v.acl_matched.as_ser()),
                                acl_not_matched: log_index.as_ref().map(|v| v.acl_not_matched.as_ser()),
                            };
                            write(format, out, &client_access)
                        }));
                        Ok(())
                    },
                    &FlatClientAccessRecord::PipeSession {
                        record,
                        final_record,
                        request,
                        backend_request,
                        process_duration,
                        ttfb_duration,
                        accounting,
                        backend_connection,
                    } => {
                        let mut log_index = None;
                        let mut request_header_index = None;
                        let mut backend_request_header_index = None;

                        let indexed_request;
                        let indexed_backend_request;

                        if index_log_vars {
                            log_index = Some(index_log(final_record.log.as_slice()));
                        }

                        if index_headers || index_headers_inplace {
                            request_header_index = Some(make_header_index(request.headers.as_slice()));
                            backend_request_header_index = Some(make_header_index(backend_request.headers.as_slice()));
                        }

                        if index_headers_inplace {
                            indexed_request = request.as_ser_indexed(request_header_index.as_ref().unwrap());
                            indexed_backend_request = backend_request.as_ser_indexed(backend_request_header_index.as_ref().unwrap());
                        } else {
                            indexed_request = request.as_ser();
                            indexed_backend_request = backend_request.as_ser();
                        }

                        let pipe_session = ser::PipeSession {
                            record_type: "pipe_session",
                            vxid: record.ident,
                            remote_address: session_record.remote.as_ser(),
                            session_timestamp: session_record.open,
                            start_timestamp: final_record.start,
                            end_timestamp: final_record.end,
                            request: indexed_request,
                            backend_request: indexed_backend_request,
                            process_duration: process_duration,
                            ttfb_duration: ttfb_duration,
                            recv_total_bytes: accounting.recv_total,
                            sent_total_bytes: accounting.sent_total,
                            log: (!no_log).as_some_from(|| final_record.log.as_ser()),
                            backend_connection: backend_connection.as_ser(),
                            request_header_index: index_headers.as_some_from(|| request_header_index.as_ref().unwrap().as_ser()),
                            backend_request_header_index: index_headers.as_some_from(|| backend_request_header_index.as_ref().unwrap().as_ser()),
                            log_vars: log_index.as_ref().map(|v| v.vars.as_ser()),
                            log_messages: log_index.as_ref().map(|v| v.messages.as_ser()),
                            acl_matched: log_index.as_ref().map(|v| v.acl_matched.as_ser()),
                            acl_not_matched: log_index.as_ref().map(|v| v.acl_not_matched.as_ser()),
                        };
                        write(format, out, &pipe_session)
                    }
                }
            } else {
                warn!("No log entry found for linked client access record {:?} in: {:?}", record_link, session_record);
                Ok(())
            }
        })
    }

    for record_link in session_record.client_records.iter() {
        try!(log_linked_client_access_record(format, out, session_record, record_link, "client_request",
                                             index_log_vars, index_headers, index_headers_inplace, no_log))
    }
    Ok(())
}
