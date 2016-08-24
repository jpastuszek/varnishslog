use serde::ser::Serialize;
use serde::ser::Serializer;

#[derive(Serialize, Debug)]
struct Entry<'a, S> where S: Serialize + 'a {
    record_type: &'a str,
    record: &'a S,
}

trait EntryType: Serialize {
    fn type_name() -> &'static str;
}

#[derive(Serialize, Debug)]
struct ClientAccessLogEntry<'a> {
    vxid: u32,
    request_type: &'a str,
    remote_address: (&'a str, u16),
    session_timestamp: f64,
    start_timestamp: f64,
    end_timestamp: f64,
    handing: &'a str,
    request: HttpRequestLogEntry<'a>,
    response: HttpResponseLogEntry<'a>,
    process_duration: Option<f64>,
    fetch_duration: Option<f64>,
    ttfb_duration: f64,
    serve_duration: f64,
    recv_header_bytes: u64,
    recv_body_bytes: u64,
    recv_total_bytes: u64,
    sent_header_bytes: u64,
    sent_body_bytes: u64,
    sent_total_bytes: u64,
    esi_count: usize,
    restart_count: usize,
    restart_log: Option<LogBook<'a>>,
    log: LogBook<'a>,
}

impl<'a> EntryType for ClientAccessLogEntry<'a> {
    fn type_name() -> &'static str {
        "client_access"
    }
}

#[derive(Serialize, Debug)]
struct BackendAccessLogEntry<'a> {
    vxid: u32,
    remote_address: (&'a str, u16),
    session_timestamp: f64,
    start_timestamp: f64,
    end_timestamp: f64,
    handing: &'a str,
    request: HttpRequestLogEntry<'a>,
    response: Option<HttpResponseLogEntry<'a>>,
    send_duration: f64,
    wait_duration: Option<f64>,
    ttfb_duration: Option<f64>,
    fetch_duration: Option<f64>,
    retry: usize,
    backend_connection: Option<BackendConnectionLogEntry<'a>>,
    cache_object: Option<CacheObjectLogEntry<'a>>,
    log: LogBook<'a>,
}

impl<'a> EntryType for BackendAccessLogEntry<'a> {
    fn type_name() -> &'static str {
        "backend_access"
    }
}

#[derive(Serialize, Debug)]
struct PipeSessionLogEntry<'a> {
    vxid: u32,
    remote_address: (&'a str, u16),
    session_timestamp: f64,
    start_timestamp: f64,
    end_timestamp: f64,
    handing: &'a str,
    backend_connection: BackendConnectionLogEntry<'a>,
    request: HttpRequestLogEntry<'a>,
    backend_request: HttpRequestLogEntry<'a>,
    process_duration: Option<f64>,
    ttfb_duration: f64,
    recv_total_bytes: u64,
    sent_total_bytes: u64,
    log: LogBook<'a>,
    //TODO: thre should be SessAct or something with session bytes?
}

impl<'a> EntryType for PipeSessionLogEntry<'a> {
    fn type_name() -> &'static str {
        "pipe_session"
    }
}

#[derive(Serialize, Debug)]
struct HttpRequestLogEntry<'a> {
    protocol: &'a str,
    method: &'a str,
    url: &'a str,
    headers: &'a [(String, String)],
}

#[derive(Serialize, Debug)]
struct HttpResponseLogEntry<'a> {
    status: u32,
    reason: &'a str,
    protocol: &'a str,
    headers: &'a [(String, String)],
}

#[derive(Debug)]
struct LogBook<'a> {
    entries: &'a [LogEntry],
}

#[derive(Serialize, Debug)]
struct LogBookEntry<'a> {
    entry_type: &'a str,
    message: &'a str,
    #[serde(skip_serializing_if="Option::is_none")]
    detail: Option<&'a str>,
}

impl<'a> Serialize for LogBook<'a> {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where S: Serializer {
        let mut state = try!(serializer.serialize_seq(Some(self.entries.len())));
        for log_entry in self.entries {
            let (entry_type, message, detail) = match log_entry {
                &LogEntry::Vcl(ref msg) => ("VCL", msg.as_str(), None),
                &LogEntry::Debug(ref msg) => ("Debug", msg.as_str(), None),
                &LogEntry::Error(ref msg) => ("Error", msg.as_str(), None),
                &LogEntry::FetchError(ref msg) => ("Fetch Error", msg.as_str(), None),
                &LogEntry::Warning(ref msg) => ("Warning", msg.as_str(), None),
                &LogEntry::Acl(ref result, ref name, ref addr) => match result.as_str() {
                    "MATCH" => ("ACL Match", name.as_str(), addr.as_ref().map(String::as_str)),
                    "NO_MATCH" => ("ACL No Match", name.as_str(), addr.as_ref().map(String::as_str)),
                    _ => ("ACL Other", result.as_str(), Some(name.as_str())),
                },
            };

            try!(serializer.serialize_seq_elt(&mut state, &LogBookEntry {
                entry_type: entry_type,
                message: message,
                detail: detail,
            }));
        }
        try!(serializer.serialize_seq_end(state));
        Ok(())
    }
}

#[derive(Serialize, Debug)]
struct CacheObjectLogEntry<'a> {
    storage_type: &'a str,
    storage_name: &'a str,
    ttl_duration: Option<f64>,
    grace_duration: Option<f64>,
    keep_duration: Option<f64>,
    since_timestamp: f64,
    origin_timestamp: f64,
    fetch_mode: &'a str,
    fetch_streamed: bool,
    response: HttpResponseLogEntry<'a>,
}

#[derive(Serialize, Debug)]
struct BackendConnectionLogEntry<'a> {
    fd: isize,
    name: &'a str,
    remote_address: (&'a str, u16),
    local_address: (&'a str, u16),
}
