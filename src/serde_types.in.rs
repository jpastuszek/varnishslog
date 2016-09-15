use serde::ser::Serialize;
use serde::ser::Serializer;
use linked_hash_map::LinkedHashMap;

pub trait EntryType: Serialize {
    fn type_name(&self) -> &str;
    fn remote_ip(&self) -> &str;
    fn timestamp(&self) -> f64;
    fn request_method(&self) -> &str;
    fn request_url(&self) -> &str;
    fn request_protocol(&self) -> &str;
    fn response_status(&self) -> Option<u32>;
    fn response_bytes(&self) -> Option<u64>;
}

#[derive(Serialize, Debug)]
pub struct ClientAccess<'a> {
    pub record_type: &'a str,
    pub vxid: u32,
    pub remote_address: Address<'a>,
    pub session_timestamp: f64,
    pub start_timestamp: f64,
    pub end_timestamp: f64,
    pub handling: &'a str,
    pub request: HttpRequest<'a>,
    pub response: HttpResponse<'a>,
    pub backend_access: Option<&'a BackendAccess<'a>>,
    pub process_duration: Option<f64>,
    pub fetch_duration: Option<f64>,
    pub ttfb_duration: f64,
    pub serve_duration: f64,
    pub recv_header_bytes: u64,
    pub recv_body_bytes: u64,
    pub recv_total_bytes: u64,
    pub sent_header_bytes: u64,
    pub sent_body_bytes: u64,
    pub sent_total_bytes: u64,
    pub esi_count: usize,
    pub restart_count: usize,
    pub restart_log: Option<Log<'a>>,
    pub log: Log<'a>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub request_header_index: Option<Index<'a>>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub response_header_index: Option<Index<'a>>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub log_vars_index: Option<Index<'a>>,
}

impl<'a> EntryType for ClientAccess<'a> {
    fn type_name(&self) -> &str {
        self.record_type
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
        Some(self.sent_body_bytes)
    }
}

#[derive(Serialize, Debug)]
pub struct BackendAccess<'a> {
    pub vxid: u32,
    pub remote_address: Address<'a>,
    pub session_timestamp: f64,
    pub start_timestamp: f64,
    pub end_timestamp: f64,
    pub handling: &'a str,
    pub request: HttpRequest<'a>,
    pub response: Option<HttpResponse<'a>>,
    pub send_duration: f64,
    pub wait_duration: Option<f64>,
    pub ttfb_duration: Option<f64>,
    pub fetch_duration: Option<f64>,
    pub sent_header_bytes: Option<u64>,
    pub sent_body_bytes: Option<u64>,
    pub sent_total_bytes: Option<u64>,
    pub recv_header_bytes: Option<u64>,
    pub recv_body_bytes: Option<u64>,
    pub recv_total_bytes: Option<u64>,
    pub retry: usize,
    pub backend_connection: Option<BackendConnection<'a>>,
    pub cache_object: Option<CacheObject<'a>>,
    pub log: Log<'a>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub request_header_index: Option<Index<'a>>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub response_header_index: Option<Index<'a>>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub cache_object_response_header_index: Option<Index<'a>>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub log_vars_index: Option<Index<'a>>,
}

#[derive(Serialize, Debug)]
pub struct PipeSession<'a> {
    pub record_type: &'a str,
    pub vxid: u32,
    pub remote_address: Address<'a>,
    pub session_timestamp: f64,
    pub start_timestamp: f64,
    pub end_timestamp: f64,
    pub backend_connection: BackendConnection<'a>,
    pub request: HttpRequest<'a>,
    pub backend_request: HttpRequest<'a>,
    pub process_duration: Option<f64>,
    pub ttfb_duration: f64,
    pub recv_total_bytes: u64,
    pub sent_total_bytes: u64,
    pub log: Log<'a>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub request_header_index: Option<Index<'a>>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub backend_request_header_index: Option<Index<'a>>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub log_vars_index: Option<Index<'a>>,
}

impl<'a> EntryType for PipeSession<'a> {
    fn type_name(&self) -> &str {
        self.record_type
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
pub struct Address<'a> {
    pub ip: &'a str,
    pub port: u16,
}

#[derive(Debug)]
pub enum Headers<'a> {
    Raw(&'a [(String, String)]),
    Indexed(Index<'a>)
}

impl<'a> Serialize for Headers<'a> {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where S: Serializer {
        match self {
            &Headers::Raw(slice) => slice.serialize(serializer),
            &Headers::Indexed(ref index) => index.serialize(serializer),
        }
    }
}

#[derive(Serialize, Debug)]
pub struct HttpRequest<'a> {
    pub protocol: &'a str,
    pub method: &'a str,
    pub url: &'a str,
    pub headers: Headers<'a>,
}

#[derive(Serialize, Debug)]
pub struct HttpResponse<'a> {
    pub status: u32,
    pub reason: &'a str,
    pub protocol: &'a str,
    pub headers: Headers<'a>,
}

#[derive(Debug)]
pub struct Log<'a>(pub &'a [VslLogEntry]);

#[derive(Serialize, Debug)]
pub struct LogEntry<'a> {
    pub entry_type: &'a str,
    pub message: &'a str,
    #[serde(skip_serializing_if="Option::is_none")]
    pub detail: Option<&'a str>,
}

impl<'a> Serialize for Log<'a> {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where S: Serializer {
        let mut state = try!(serializer.serialize_seq(Some(self.0.len())));
        for log_entry in self.0 {
            let (entry_type, message, detail) = match log_entry {
                &VslLogEntry::Vcl(ref msg) => ("VCL", msg.as_str(), None),
                &VslLogEntry::Debug(ref msg) => ("Debug", msg.as_str(), None),
                &VslLogEntry::Error(ref msg) => ("Error", msg.as_str(), None),
                &VslLogEntry::FetchError(ref msg) => ("Fetch Error", msg.as_str(), None),
                &VslLogEntry::Warning(ref msg) => ("Warning", msg.as_str(), None),
                &VslLogEntry::Acl(ref result, ref name, ref addr) => match result.as_str() {
                    "MATCH" => ("ACL Match", name.as_str(), addr.as_ref().map(String::as_str)),
                    "NO_MATCH" => ("ACL No Match", name.as_str(), addr.as_ref().map(String::as_str)),
                    _ => ("ACL Other", result.as_str(), Some(name.as_str())),
                },
            };

            try!(serializer.serialize_seq_elt(&mut state, &LogEntry {
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
pub struct Index<'a>(pub &'a LinkedHashMap<String, Vec<String>>);

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
pub struct CacheObject<'a> {
    pub storage_type: &'a str,
    pub storage_name: &'a str,
    pub ttl_duration: Option<f64>,
    pub grace_duration: Option<f64>,
    pub keep_duration: Option<f64>,
    pub since_timestamp: f64,
    pub origin_timestamp: f64,
    pub fetch_mode: &'a str,
    pub fetch_streamed: bool,
    pub response: HttpResponse<'a>,
}

#[derive(Serialize, Debug)]
pub struct BackendConnection<'a> {
    pub fd: isize,
    pub name: &'a str,
    pub remote_address: Address<'a>,
    pub local_address: Address<'a>,
}
