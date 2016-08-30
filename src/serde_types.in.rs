use serde::ser::Serialize;
use serde::ser::Serializer;

trait EntryType: Serialize {
    fn type_name() -> &'static str;
    fn remote_ip(&self) -> &str;
    fn timestamp(&self) -> f64;
    fn request_method(&self) -> &str;
    fn request_url(&self) -> &str;
    fn request_protocol(&self) -> &str;
    fn response_status(&self) -> Option<u32>;
    fn response_bytes(&self) -> Option<u64>;
}

#[derive(Serialize, Debug)]
struct ClientAccessLogEntry<'a> {
    record_type: &'a str,
    vxid: u32,
    request_type: &'a str,
    remote_address: AddressLogEntry<'a>,
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
    #[serde(skip_serializing_if="Option::is_none")]
    request_header_index: Option<Index<'a>>,
    #[serde(skip_serializing_if="Option::is_none")]
    response_header_index: Option<Index<'a>>,
    #[serde(skip_serializing_if="Option::is_none")]
    log_vars_index: Option<Index<'a>>,
}

impl<'a> EntryType for ClientAccessLogEntry<'a> {
    fn type_name() -> &'static str {
        "client_access"
    }
    fn remote_ip(&self) -> &str {
        self.remote_address.ip
    }
    fn timestamp(&self) -> f64 {
        self.end_timestamp
    }
    fn request_method(&self) -> &str {
        self.request.method
    }
    fn request_url(&self) -> &str {
        self.request.url
    }
    fn request_protocol(&self) -> &str {
        self.request.protocol
    }
    fn response_status(&self) -> Option<u32> {
        Some(self.response.status)
    }
    fn response_bytes(&self) -> Option<u64> {
        Some(self.sent_total_bytes)
    }
}

#[derive(Serialize, Debug)]
struct BackendAccessLogEntry<'a> {
    record_type: &'a str,
    vxid: u32,
    remote_address: AddressLogEntry<'a>,
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
    sent_header_bytes: Option<u64>,
    sent_body_bytes: Option<u64>,
    sent_total_bytes: Option<u64>,
    recv_header_bytes: Option<u64>,
    recv_body_bytes: Option<u64>,
    recv_total_bytes: Option<u64>,
    retry: usize,
    backend_connection: Option<BackendConnectionLogEntry<'a>>,
    cache_object: Option<CacheObjectLogEntry<'a>>,
    log: LogBook<'a>,
    #[serde(skip_serializing_if="Option::is_none")]
    request_header_index: Option<Index<'a>>,
    #[serde(skip_serializing_if="Option::is_none")]
    response_header_index: Option<Index<'a>>,
    #[serde(skip_serializing_if="Option::is_none")]
    log_vars_index: Option<Index<'a>>,
}

impl<'a> EntryType for BackendAccessLogEntry<'a> {
    fn type_name() -> &'static str {
        "backend_access"
    }
    fn remote_ip(&self) -> &str {
        self.remote_address.ip
    }
    fn timestamp(&self) -> f64 {
        self.end_timestamp
    }
    fn request_method(&self) -> &str {
        self.request.method
    }
    fn request_url(&self) -> &str {
        self.request.url
    }
    fn request_protocol(&self) -> &str {
        self.request.protocol
    }
    fn response_status(&self) -> Option<u32> {
        self.response.as_ref().map(|r| r.status).or(Some(503)) // no response
    }
    fn response_bytes(&self) -> Option<u64> {
        self.recv_total_bytes
    }
}

#[derive(Serialize, Debug)]
struct PipeSessionLogEntry<'a> {
    record_type: &'a str,
    vxid: u32,
    remote_address: AddressLogEntry<'a>,
    session_timestamp: f64,
    start_timestamp: f64,
    end_timestamp: f64,
    backend_connection: BackendConnectionLogEntry<'a>,
    request: HttpRequestLogEntry<'a>,
    backend_request: HttpRequestLogEntry<'a>,
    process_duration: Option<f64>,
    ttfb_duration: f64,
    recv_total_bytes: u64,
    sent_total_bytes: u64,
    log: LogBook<'a>,
    #[serde(skip_serializing_if="Option::is_none")]
    request_header_index: Option<Index<'a>>,
    #[serde(skip_serializing_if="Option::is_none")]
    backend_request_header_index: Option<Index<'a>>,
    #[serde(skip_serializing_if="Option::is_none")]
    log_vars_index: Option<Index<'a>>,
}

impl<'a> EntryType for PipeSessionLogEntry<'a> {
    fn type_name() -> &'static str {
        "pipe_session"
    }
    fn remote_ip(&self) -> &str {
        self.remote_address.ip
    }
    fn timestamp(&self) -> f64 {
        self.end_timestamp
    }
    fn request_method(&self) -> &str {
        self.request.method
    }
    fn request_url(&self) -> &str {
        self.request.url
    }
    fn request_protocol(&self) -> &str {
        self.request.protocol
    }
    fn response_status(&self) -> Option<u32> {
        None
    }
    fn response_bytes(&self) -> Option<u64> {
        Some(self.sent_total_bytes)
    }
}

#[derive(Serialize, Debug)]
struct AddressLogEntry<'a> {
    ip: &'a str,
    port: u16,
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
struct LogBook<'a>(&'a [LogEntry]);

#[derive(Serialize, Debug)]
struct LogBookEntry<'a> {
    entry_type: &'a str,
    message: &'a str,
    #[serde(skip_serializing_if="Option::is_none")]
    detail: Option<&'a str>,
}

impl<'a> Serialize for LogBook<'a> {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where S: Serializer {
        let mut state = try!(serializer.serialize_seq(Some(self.0.len())));
        for log_entry in self.0 {
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

#[derive(Debug)]
struct Index<'a>(&'a LinkedHashMap<String, Vec<String>>);

impl<'a> Serialize for Index<'a> {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where S: Serializer {
        let mut state = try!(serializer.serialize_map(Some(self.0.len())));
        for (ref key, ref values) in self.0 {
            try!(serializer.serialize_map_key(&mut state, key));
            try!(serializer.serialize_map_value(&mut state, values));
        }
        try!(serializer.serialize_map_end(state));
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
    remote_address: AddressLogEntry<'a>,
    local_address: AddressLogEntry<'a>,
}
