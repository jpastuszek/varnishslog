use std::io::Write;
use std::io::Error as IoError;
use std::fmt;
use std::fmt::Display;

pub use serde_json::error::Error as JsonError;
use serde_json::ser::to_writer as write_json;
use serde_json::ser::to_writer_pretty as write_json_pretty;

use chrono::NaiveDateTime;
use linked_hash_map::LinkedHashMap;
use boolinator::Boolinator;

use access_log::record::{
    Address,
    Handling,
    HttpRequest,
    HttpResponse,
    LogEntry,
    BackendConnection,
    CacheObject,
    ClientAccessRecord,
    ClientAccessTransaction,
    BackendAccessRecord,
    BackendAccessTransaction,
    Proxy,
    SessionInfo,
    AclResult,
    Duration,
    Link,
    Accounting,
    PipeAccounting,
    Compression,
    CompressionOperation,
};

mod ser {
    use access_log::record::LogEntry as VslLogEntry;
    use access_log::record::AclResult as VslAclResult;
    include!(concat!(env!("OUT_DIR"), "/serde_types.rs"));
}

pub struct Config {
    pub no_log_processing: bool,
    pub keep_raw_log: bool,
    pub no_header_indexing: bool,
    pub keep_raw_headers: bool,
}

pub enum Format {
    Json,
    JsonPretty,
    NcsaJson,
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
        match *self {
            Handling::Hit(_) => "hit",
            Handling::Miss => "miss",
            Handling::Pass => "pass",
            Handling::HitPass(_) => "hit_for_pass",
            Handling::HitMiss(_, _) => "hit_for_miss",
            Handling::Synth => "synth",
            Handling::Pipe => "pipe",
        }
    }
}

impl<'a> AsSer<'a> for Vec<(String, String)> {
    type Out = &'a [(String, String)];
    fn as_ser(&'a self) -> Self::Out {
        self.as_slice()
    }
}

impl<'a> AsSer<'a> for Proxy {
    type Out = ser::Proxy<'a>;
    fn as_ser(&'a self) -> Self::Out {
        ser::Proxy {
            version: self.version.as_str(),
            client_address: self.client.as_ser(),
            server_address: self.server.as_ser(),
        }
    }
}

impl<'a> AsSer<'a> for SessionInfo {
    type Out = ser::SessionInfo<'a>;
    fn as_ser(&'a self) -> Self::Out {
        ser::SessionInfo {
            vxid: self.ident,
            open_timestamp: self.open,
            local_address: self.local.as_ref().map(AsSer::as_ser),
            remote_address: self.remote.as_ser(),
            proxy: self.proxy.as_ref().map(AsSer::as_ser),
        }
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

impl<'a> AsSer<'a> for Compression {
    type Out = ser::Compression;
    fn as_ser(&self) -> Self::Out {
        ser::Compression {
            operation: match self.operation {
                CompressionOperation::Gzip => "Gzip",
                CompressionOperation::Gunzip => "Gunzip",
                CompressionOperation::GunzipTest => "Gunzip-test",
            },
            bytes_in: self.bytes_in,
            bytes_out: self.bytes_out,
        }
    }
}

impl<'a> AsSer<'a> for Vec<LogEntry> {
    type Out = ser::RawLog<'a>;
    fn as_ser(&'a self) -> Self::Out {
        //TODO: map LogEntry when impl Iterator is stable
        ser::RawLog(self)
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
            remote_address: self.remote.as_ref().map(|r| r.as_ser()),
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
            fetch_mode: self.fetch_mode.as_ref().map(|f| f.as_str()),
            fetch_streamed: self.fetch_streamed,
            response: self.response.as_ref().map(|f| f.as_ser()),
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
            fetch_mode: self.fetch_mode.as_ref().map(|f| f.as_str()),
            fetch_streamed: self.fetch_streamed,
            response: self.response.as_ref().map(|f| f.as_ser_indexed(index)),
        }
    }
}

struct NcsaEscaped<T: AsRef<str>>(T);

impl<T: AsRef<str>> Display for NcsaEscaped<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut iter = self.0.as_ref().split('"').peekable();
        loop {
            match (iter.next(), iter.peek()) {
                (Some(i), Some(_)) => try!(write!(f, "{}\\\"", i)),
                (Some(i), None) => {
                    try!(write!(f, "{}", i));
                    break
                }
                _ => unreachable!()
            }
        }
        Ok(())
    }
}

struct NcsaOption<T: Display>(Option<T>);

impl<T: Display> Display for NcsaOption<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(t) = self.0.as_ref() {
            write!(f, "{}", t)
        } else {
            write!(f, "-")
        }
    }
}

pub fn log_client_record<W>(client_record: &ClientAccessRecord, format: &Format, out: &mut W, config: &Config)
    -> Result<(), OutputError> where W: Write {
    fn write<W, E>(format: &Format, out: &mut W, log_entry: &E) -> Result<(), OutputError> where W: Write, E: ser::EntryType {
        let write_entry = match *format {
            Format::Json |
            Format::NcsaJson => write_json,
            Format::JsonPretty => write_json_pretty,
        };
        match *format {
            Format::Json |
            Format::JsonPretty => {
                try!(write_entry(out, &log_entry));

                try!(writeln!(out, ""));
            }
            Format::NcsaJson => {
                // 192.168.1.115 - - [25/Aug/2016:11:56:55 +0000] "GET http://staging.eod.whatclinic.net/ HTTP/1.1" 503 1366
                let date_time = NaiveDateTime::from_timestamp(log_entry.timestamp() as i64, 0);

                try!(write!(out, "{} {} - [{}]",
                            log_entry.remote_ip(),
                            log_entry.type_name(),
                            date_time.format("%d/%b/%Y:%H:%M:%S +0000")));

                if let (Some(method), Some(url), Some(protocol)) = (log_entry.request_method(), log_entry.request_url(), log_entry.request_protocol()) {
                    try!(write!(out, " \"{} {} {}\"", 
                        NcsaEscaped(method),
                        NcsaEscaped(url),
                        NcsaEscaped(protocol)));
                } else {
                    try!(write!(out, " -"));
                }

                try!(write!(out, " {} {} ",
                            NcsaOption(log_entry.response_status()),
                            NcsaOption(log_entry.response_bytes())));

                try!(write_entry(out, &log_entry));

                try!(writeln!(out, ""));
            }
        }
        Ok(())
    }

    fn follow_restarts(record: &ClientAccessRecord, restart_count: usize) -> Option<(&ClientAccessRecord, usize)> {
        match record.transaction {
            ClientAccessTransaction::Full { .. } |
            ClientAccessTransaction::Bad { .. } |
            ClientAccessTransaction::Piped { .. } => Some((record, restart_count)),
            ClientAccessTransaction::RestartedEarly { ref restart_record, .. } |
            ClientAccessTransaction::RestartedLate { ref restart_record, .. }  => {
                if let Some(record) = restart_record.get_resolved() {
                    follow_restarts(record, restart_count + 1)
                } else {
                    warn!("Found unresolved link {:?} in:\n{:#?}", restart_record, record);
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
            let mut values = index.remove(&name).unwrap_or_default();
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

    fn index_log(logs: &[LogEntry]) -> LogIndex {
        let mut vars = LinkedHashMap::new();
        let mut messages = Vec::new();
        let mut acl_matched = Vec::new();
        let mut acl_not_matched = Vec::new();

        for log_entry in logs {
            match *log_entry {
                LogEntry::Vcl(ref message) => {
                    let mut s = message.splitn(2, ": ").fuse();
                    if let (Some(name), Some(value)) = (s.next(), s.next()) {
                        if !name.contains(' ') {
                            vars.insert(name, value);
                            continue;
                        }
                    }
                    messages.push(message.as_str());
                }
                LogEntry::Acl(ref result, ref name, _) => {
                    match *result {
                        AclResult::Match => acl_matched.push(name.as_str()),
                        AclResult::NoMatch => acl_not_matched.push(name.as_str()),
                    }
                }
                LogEntry::Debug(ref message) |
                LogEntry::Error(ref message) |
                LogEntry::VclError(ref message) |
                LogEntry::FetchError(ref message) |
                LogEntry::Warning(ref message) => messages.push(message.as_str()),
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
        lru_nuked: u32,
    }

    enum FlatClientAccessRecord<'a> {
        ClientAccess {
            record: &'a ClientAccessRecord,
            final_record: &'a ClientAccessRecord,
            request: Option<&'a HttpRequest>,
            response: &'a HttpResponse,
            restarted_backend_record: Option<&'a Link<BackendAccessRecord>>,
            backend_record: Option<&'a Link<BackendAccessRecord>>,
            process_duration: Option<Duration>,
            fetch_duration: Option<Duration>,
            ttfb_duration: Duration,
            serve_duration: Duration,
            accounting: &'a Accounting,
            esi_records: Option<&'a Vec<Link<ClientAccessRecord>>>,
            restart_count: usize,
            restart_log: Option<&'a Vec<LogEntry>>,
        },
        PipeSession {
            record: &'a ClientAccessRecord,
            final_record: &'a ClientAccessRecord,
            request: &'a HttpRequest,
            backend_request: &'a HttpRequest,
            process_duration: Option<Duration>,
            ttfb_duration: Option<Duration>,
            accounting: &'a PipeAccounting,
            backend_connection: Option<&'a BackendConnection>,
        }
    }

    fn flatten_linked_backend_log_record<F, R>(
        client_record: &ClientAccessRecord,
        maybe_record_link: Option<&Link<BackendAccessRecord>>,
        retry: usize,
        block: F) -> R where F: FnOnce(Option<&FlatBackendAccessRecord>) -> R {
        if let Some(record_link) = maybe_record_link {
            if let Some(record) = record_link.get_resolved() {
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
                        lru_nuked: record.lru_nuked,
                    })),
                    BackendAccessTransaction::Failed { retry_record: Some(ref record_link), .. } |
                    BackendAccessTransaction::Abandoned { retry_record: Some(ref record_link), .. } =>
                        return flatten_linked_backend_log_record(client_record, Some(record_link), retry + 1, block),
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
                        lru_nuked: record.lru_nuked,
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
                        lru_nuked: record.lru_nuked,
                    })),
                    BackendAccessTransaction::Aborted { .. } |
                    BackendAccessTransaction::Piped { .. } => return block(None),
                }
            } else {
                warn!("Found unresolved link {:?} in:\n{:#?}", record_link, client_record);
            }
        }
        block(None)
    }

    fn flatten_client_log_record<F, R>(
        record: &ClientAccessRecord,
        block: F) -> R where F: FnOnce(Option<&FlatClientAccessRecord>) -> R {
        if let Some((final_record, restart_count)) = follow_restarts(record, 0) {
            // Note: we skip all the intermediate restart records
            match (&record.transaction, &final_record.transaction) {
                (&ClientAccessTransaction::RestartedEarly {
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
                    request: Some(request),
                    response: response,
                    restarted_backend_record: None,
                    backend_record: backend_record.as_ref(),
                    process_duration: process,
                    fetch_duration: fetch,
                    ttfb_duration: ttfb,
                    serve_duration: serve,
                    accounting: accounting,
                    esi_records: Some(esi_records),
                    restart_count: restart_count,
                    restart_log: Some(&record.log),
                })),
                (&ClientAccessTransaction::RestartedLate {
                    ref request,
                    backend_record: ref restarted_backend_record,
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
                    request: Some(request),
                    response: response,
                    restarted_backend_record: restarted_backend_record.as_ref(),
                    backend_record: backend_record.as_ref(),
                    process_duration: process,
                    fetch_duration: fetch,
                    ttfb_duration: ttfb,
                    serve_duration: serve,
                    accounting: accounting,
                    esi_records: Some(esi_records),
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
                    request: Some(request),
                    response: response,
                    backend_record: backend_record.as_ref(),
                    restarted_backend_record: None,
                    process_duration: process,
                    fetch_duration: fetch,
                    ttfb_duration: ttfb,
                    serve_duration: serve,
                    accounting: accounting,
                    esi_records: Some(esi_records),
                    restart_count: restart_count,
                    restart_log: None,
                })),
                (_, &ClientAccessTransaction::Bad {
                    ref request,
                    ref response,
                    ttfb,
                    serve,
                    ref accounting,
                    ..
                }) => return block(Some(&FlatClientAccessRecord::ClientAccess {
                    record: record,
                    final_record: final_record,
                    request: request.as_ref(),
                    response: response,
                    backend_record: None,
                    restarted_backend_record: None,
                    process_duration: None,
                    fetch_duration: None,
                    ttfb_duration: ttfb,
                    serve_duration: serve,
                    accounting: accounting,
                    esi_records: None,
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
                                backend_connection: backend_connection.as_ref(),
                            }))
                        } else {
                            warn!("Expected Piped ClientAccessRecord to link Piped BackendAccessTransaction; link {:?} in:\n{:#?}",
                                  backend_record, final_record);
                        }
                    } else {
                        warn!("Found unresolved link {:?} in:\n{:#?}", backend_record, final_record);
                    }
                },
                (_, &ClientAccessTransaction::RestartedEarly { .. }) => panic!("got ClientAccessTransaction::RestartedEarly as final final_record"),
                (_, &ClientAccessTransaction::RestartedLate { .. }) => panic!("got ClientAccessTransaction::RestartedLate as final final_record"),
            }
        } else {
            warn!("Failed to find final record for:\n{:#?}", record);
        }
        block(None)
    }

    fn log_client_access_record<W>(
        format: &Format,
        out: &mut W,
        record: &ClientAccessRecord,
        record_type: &'static str,
        config: &Config) -> Result<(), OutputError> where W: Write {
        flatten_client_log_record(record, |client_log_record| {
            if let Some(client_log_record) = client_log_record {
                match *client_log_record {
                    FlatClientAccessRecord::ClientAccess {
                        record,
                        final_record,
                        request,
                        response,
                        restarted_backend_record,
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
                        if let Some(esi_records) = esi_records {
                            for esi_record_link in esi_records {
                                if let Some(esi_record) = esi_record_link.get_resolved() {
                                    try!(log_client_access_record(format, out, esi_record, "esi_subrequest", config));
                                } else {
                                    warn!("Found unresolved ESI record link {:?} in:\n{:#?}", esi_record_link, record);
                                }
                            }
                        }

                        let ber = backend_record.or(restarted_backend_record);

                        try!(flatten_linked_backend_log_record(record, ber, 0, |backend_log_record| {
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

                                if !config.no_log_processing {
                                    log_index = Some(index_log(backend_log_record.final_record.log.as_slice()));
                                }

                                if !config.no_header_indexing {
                                    request_header_index = Some(make_header_index(backend_log_record.request.headers.as_slice()));
                                    response_header_index = backend_log_record.response.as_ref().map(|response| make_header_index(response.headers.as_slice()));
                                    cache_object_response_header_index = backend_log_record.cache_object.as_ref().map(|cache_object| cache_object.response.as_ref().map(|response| make_header_index(response.headers.as_slice())));
                                }

                                if config.keep_raw_headers | config.no_header_indexing {
                                    indexed_request = backend_log_record.request.as_ser();
                                    indexed_response = backend_log_record.response.map(|response| response.as_ser());
                                    indexed_cache_object = backend_log_record.cache_object.map(|cache_object| cache_object.as_ser());
                                } else {
                                    indexed_request = backend_log_record.request.as_ser_indexed(request_header_index.as_ref().unwrap());
                                    indexed_response = backend_log_record.response.map(|response| response.as_ser_indexed(response_header_index.as_ref().unwrap()));
                                    indexed_cache_object = backend_log_record.cache_object.and_then(|cache_object| cache_object_response_header_index.as_ref().and_then(|cache_object_response_header_index| cache_object_response_header_index.as_ref().map(|cache_object_response_header_index| cache_object.as_ser_indexed(cache_object_response_header_index))));
                                }

                                let log = ser::Log {
                                    raw_log: (config.no_log_processing | config.keep_raw_log).as_some_from(|| backend_log_record.final_record.log.as_ser()),
                                    vars: log_index.as_ref().map(|v| v.vars.as_ser()),
                                    messages: log_index.as_ref().map(|v| v.messages.as_ser()),
                                    acl_matched: log_index.as_ref().map(|v| v.acl_matched.as_ser()),
                                    acl_not_matched: log_index.as_ref().map(|v| v.acl_not_matched.as_ser()),
                                };

                                ser::BackendAccess {
                                    vxid: backend_log_record.final_record.ident,
                                    start_timestamp: backend_log_record.final_record.start,
                                    end_timestamp: backend_log_record.final_record.end,
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
                                    compression: backend_log_record.final_record.compression.as_ref().map(|c| c.as_ser()),
                                    log: log,
                                    request_header_index: (config.keep_raw_headers & !config.no_header_indexing).as_some_from(|| request_header_index.as_ref().unwrap().as_ser()),
                                    response_header_index: response_header_index.as_ref().and_then(|index| (config.keep_raw_headers & !config.no_header_indexing).as_some_from(|| index.as_ser())),
                                    cache_object_response_header_index: cache_object_response_header_index.as_ref().and_then(|index| (config.keep_raw_headers & !config.no_header_indexing).and_option_from(|| index.as_ref().map(|i| i.as_ser()))),
                                    lru_nuked: backend_log_record.lru_nuked,
                                }
                            });

                            // client record
                            let mut log_index = None;
                            let mut restart_log_index = None;

                            let mut request_header_index = None;
                            let mut response_header_index = None;

                            let indexed_request;
                            let indexed_response;

                            // TODO: can this be refactored somehow so that we don't need to unwarp
                            // and it is more clear? match?
                            if !config.no_log_processing {
                                log_index = Some(index_log(final_record.log.as_slice()));
                                restart_log_index = restart_log.map(|restart_log| index_log(restart_log.as_slice()));
                            }

                            if !config.no_header_indexing {
                                request_header_index = request.map(|request| make_header_index(request.headers.as_slice()));
                                response_header_index = Some(make_header_index(response.headers.as_slice()));
                            }

                            if config.keep_raw_headers | config.no_header_indexing {
                                indexed_request = request.map(|request| request.as_ser());
                                indexed_response = response.as_ser();
                            } else {
                                indexed_request = request.map(|request| request.as_ser_indexed(request_header_index.as_ref().unwrap()));
                                indexed_response = response.as_ser_indexed(response_header_index.as_ref().unwrap());
                            }

                            let restart_log = restart_log.map(|_| ser::Log {
                                raw_log: (config.no_log_processing | config.keep_raw_log).as_some_from(|| restart_log.unwrap().as_ser()),
                                vars: restart_log_index.as_ref().map(|v| v.vars.as_ser()),
                                messages: restart_log_index.as_ref().map(|v| v.messages.as_ser()),
                                acl_matched: restart_log_index.as_ref().map(|v| v.acl_matched.as_ser()),
                                acl_not_matched: restart_log_index.as_ref().map(|v| v.acl_not_matched.as_ser()),
                            });

                            let log = ser::Log {
                                raw_log: (config.no_log_processing | config.keep_raw_log).as_some_from(|| final_record.log.as_ser()),
                                vars: log_index.as_ref().map(|v| v.vars.as_ser()),
                                messages: log_index.as_ref().map(|v| v.messages.as_ser()),
                                acl_matched: log_index.as_ref().map(|v| v.acl_matched.as_ser()),
                                acl_not_matched: log_index.as_ref().map(|v| v.acl_not_matched.as_ser()),
                            };

                            let client_access = ser::ClientAccess {
                                record_type: record_type,
                                vxid: record.ident,
                                session: record.session.as_ref().map(AsSer::as_ser),
                                remote_address: record.remote.as_ser(),
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
                                esi_count: esi_records.map(|esi_records| esi_records.len()).unwrap_or(0),
                                compression: final_record.compression.as_ref().map(|c| c.as_ser()),
                                restart_count: restart_count,
                                restart_log: restart_log,
                                log: log,
                                request_header_index: (config.keep_raw_headers & !config.no_header_indexing).as_some_from(|| request_header_index.as_ref().unwrap().as_ser()),
                                response_header_index: (config.keep_raw_headers & !config.no_header_indexing).as_some_from(|| response_header_index.as_ref().unwrap().as_ser()),
                            };
                            write(format, out, &client_access)
                        }));
                        Ok(())
                    },
                    FlatClientAccessRecord::PipeSession {
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

                        if !config.no_log_processing {
                            log_index = Some(index_log(final_record.log.as_slice()));
                        }

                        if !config.no_header_indexing {
                            request_header_index = Some(make_header_index(request.headers.as_slice()));
                            backend_request_header_index = Some(make_header_index(backend_request.headers.as_slice()));
                        }

                        if config.keep_raw_headers | config.no_header_indexing {
                            indexed_request = request.as_ser();
                            indexed_backend_request = backend_request.as_ser();
                        } else {
                            indexed_request = request.as_ser_indexed(request_header_index.as_ref().unwrap());
                            indexed_backend_request = backend_request.as_ser_indexed(backend_request_header_index.as_ref().unwrap());
                        }

                        let log = ser::Log {
                            raw_log: (config.no_log_processing | config.keep_raw_log).as_some_from(|| final_record.log.as_ser()),
                            vars: log_index.as_ref().map(|v| v.vars.as_ser()),
                            messages: log_index.as_ref().map(|v| v.messages.as_ser()),
                            acl_matched: log_index.as_ref().map(|v| v.acl_matched.as_ser()),
                            acl_not_matched: log_index.as_ref().map(|v| v.acl_not_matched.as_ser()),
                        };

                        let pipe_session = ser::PipeSession {
                            record_type: "pipe_session",
                            vxid: record.ident,
                            remote_address: record.remote.as_ser(),
                            start_timestamp: final_record.start,
                            end_timestamp: final_record.end,
                            request: indexed_request,
                            backend_request: indexed_backend_request,
                            process_duration: process_duration,
                            ttfb_duration: ttfb_duration,
                            recv_total_bytes: accounting.recv_total,
                            sent_total_bytes: accounting.sent_total,
                            log: log,
                            backend_connection: backend_connection.map(|b| b.as_ser()),
                            request_header_index: (config.keep_raw_headers & !config.no_header_indexing).as_some_from(|| request_header_index.as_ref().unwrap().as_ser()),
                            backend_request_header_index: (config.keep_raw_headers & !config.no_header_indexing).as_some_from(|| backend_request_header_index.as_ref().unwrap().as_ser()),
                        };
                        write(format, out, &pipe_session)
                    }
                }
            } else {
                warn!("No log entry found for linked client access record:\n{:#?}", record);
                Ok(())
            }
        })
    }

    log_client_access_record(format, out, client_record, "client_request", config)
}

