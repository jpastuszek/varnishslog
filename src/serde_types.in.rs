use serde::ser::Serialize;
use serde::ser::Serializer;
use linked_hash_map::LinkedHashMap;

pub trait EntryType: Serialize {
    fn type_name(&self) -> &str;
    fn remote_ip(&self) -> &str;
    fn timestamp(&self) -> f64;
    fn request_method(&self) -> Option<&str>;
    fn request_url(&self) -> Option<&str>;
    fn request_protocol(&self) -> Option<&str>;
    fn response_status(&self) -> Option<u32>;
    fn response_bytes(&self) -> Option<u64>;
}

#[derive(Serialize, Debug)]
pub struct ClientAccess<'a: 'i, 'i> {
    pub record_type: &'a str,
    pub vxid: u32,
    pub session: Option<SessionInfo<'a>>,
    pub remote_address: Address<'a>,
    pub start_timestamp: f64,
    pub end_timestamp: Option<f64>,
    pub handling: &'a str,
    pub request: Option<HttpRequest<'a, 'i>>,
    pub response: HttpResponse<'a, 'i>,
    pub backend_access: Option<&'i BackendAccess<'a, 'i>>,
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
    pub compression: Option<Compression>,
    pub restart_count: usize,
    #[serde(skip_serializing_if="Option::is_none")]
    pub restart_log: Option<Log<'a, 'i>>,
    pub log: Log<'a, 'i>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub request_header_index: Option<Index<'a, 'i>>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub response_header_index: Option<Index<'a, 'i>>,
}

impl<'a: 'i, 'i> EntryType for ClientAccess<'a, 'i> {
    fn type_name(&self) -> &str {
        self.record_type
    }
    fn remote_ip(&self) -> &str {
        self.remote_address.ip
    }
    fn timestamp(&self) -> f64 {
        self.end_timestamp.unwrap_or(self.start_timestamp)
    }
    fn request_method(&self) -> Option<&str> {
        self.request.as_ref().map(|request| request.method)
    }
    fn request_url(&self) -> Option<&str> {
        self.request.as_ref().map(|request| request.url)
    }
    fn request_protocol(&self) -> Option<&str> {
        self.request.as_ref().map(|request| request.protocol)
    }
    fn response_status(&self) -> Option<u32> {
        Some(self.response.status)
    }
    fn response_bytes(&self) -> Option<u64> {
        Some(self.sent_body_bytes)
    }
}

#[derive(Serialize, Debug)]
pub struct BackendAccess<'a: 'i, 'i> {
    pub vxid: u32,
    pub start_timestamp: Option<f64>,
    pub end_timestamp: Option<f64>,
    pub handling: &'a str,
    pub request: HttpRequest<'a, 'i>,
    pub response: Option<HttpResponse<'a, 'i>>,
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
    pub cache_object: Option<CacheObject<'a, 'i>>,
    pub compression: Option<Compression>,
    pub log: Log<'a, 'i>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub request_header_index: Option<Index<'a, 'i>>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub response_header_index: Option<Index<'a, 'i>>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub cache_object_response_header_index: Option<Index<'a, 'i>>,
    pub lru_nuked: u32,
}

#[derive(Serialize, Debug)]
pub struct PipeSession<'a: 'i, 'i> {
    pub record_type: &'a str,
    pub vxid: u32,
    pub remote_address: Address<'a>,
    pub start_timestamp: f64,
    pub end_timestamp: Option<f64>,
    pub backend_connection: Option<BackendConnection<'a>>,
    pub request: HttpRequest<'a, 'i>,
    pub backend_request: HttpRequest<'a, 'i>,
    pub process_duration: Option<f64>,
    pub ttfb_duration: Option<f64>,
    pub recv_total_bytes: u64,
    pub sent_total_bytes: u64,
    pub log: Log<'a, 'i>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub request_header_index: Option<Index<'a, 'i>>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub backend_request_header_index: Option<Index<'a, 'i>>,
}

impl<'a: 'i, 'i> EntryType for PipeSession<'a, 'i> {
    fn type_name(&self) -> &str {
        self.record_type
    }
    fn remote_ip(&self) -> &str {
        self.remote_address.ip
    }
    fn timestamp(&self) -> f64 {
        self.end_timestamp.unwrap_or(self.start_timestamp)
    }
    fn request_method(&self) -> Option<&str> {
        Some(self.request.method)
    }
    fn request_url(&self) -> Option<&str> {
        Some(self.request.url)
    }
    fn request_protocol(&self) -> Option<&str> {
        Some(self.request.protocol)
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
pub enum Headers<'a: 'i, 'i> {
    Raw(&'a [(String, String)]),
    Indexed(Index<'a, 'i>)
}

impl<'a: 'i, 'i> Serialize for Headers<'a, 'i> {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where S: Serializer {
        match self {
            &Headers::Raw(slice) => slice.serialize(serializer),
            &Headers::Indexed(ref index) => index.serialize(serializer),
        }
    }
}

#[derive(Serialize, Debug)]
pub struct Proxy<'a> {
    pub version: &'a str,
    pub client_address: Address<'a>,
    pub server_address: Address<'a>,
}

#[derive(Serialize, Debug)]
pub struct SessionInfo<'a> {
    pub vxid: u32,
    pub open_timestamp: f64,
    pub local_address: Option<Address<'a>>,
    pub remote_address: Address<'a>,
    pub proxy: Option<Proxy<'a>>,
}

#[derive(Serialize, Debug)]
pub struct HttpRequest<'a: 'i, 'i> {
    pub protocol: &'a str,
    pub method: &'a str,
    pub url: &'a str,
    pub headers: Headers<'a, 'i>,
}

#[derive(Serialize, Debug)]
pub struct HttpResponse<'a: 'i, 'i> {
    pub status: u32,
    pub reason: &'a str,
    pub protocol: &'a str,
    pub headers: Headers<'a, 'i>,
}

#[derive(Serialize, Debug)]
pub struct Compression {
    pub operation: &'static str,
    pub bytes_in: u64,
    pub bytes_out: u64,
}

#[derive(Serialize, Debug)]
pub struct Log<'a: 'i, 'i> {
    #[serde(skip_serializing_if="Option::is_none")]
    pub raw_log: Option<RawLog<'a>>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub vars: Option<LogVarsIndex<'a, 'i>>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub messages: Option<LogMessages<'a, 'i>>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub acl_matched: Option<LogMessages<'a, 'i>>,
    #[serde(skip_serializing_if="Option::is_none")]
    pub acl_not_matched: Option<LogMessages<'a, 'i>>,
}

#[derive(Debug)]
pub struct RawLog<'a>(pub &'a [VslLogEntry]);

#[derive(Serialize, Debug)]
pub struct RawLogEntry<'a> {
    pub entry_type: &'a str,
    pub message: &'a str,
    #[serde(skip_serializing_if="Option::is_none")]
    pub detail: Option<&'a str>,
}

impl<'a> Serialize for RawLog<'a> {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where S: Serializer {
        let mut state = try!(serializer.serialize_seq(Some(self.0.len())));
        for log_entry in self.0 {
            let (entry_type, message, detail) = match log_entry {
                &VslLogEntry::Vcl(ref msg) => ("VCL", msg.as_str(), None),
                &VslLogEntry::VclError(ref msg) => ("VCL Error", msg.as_str(), None),
                &VslLogEntry::Debug(ref msg) => ("Debug", msg.as_str(), None),
                &VslLogEntry::Error(ref msg) => ("Error", msg.as_str(), None),
                &VslLogEntry::FetchError(ref msg) => ("Fetch Error", msg.as_str(), None),
                &VslLogEntry::Warning(ref msg) => ("Warning", msg.as_str(), None),
                &VslLogEntry::Acl(ref result, ref name, ref addr) => match result {
                    &VslAclResult::Match => ("ACL Match", name.as_str(), addr.as_ref().map(String::as_str)),
                    &VslAclResult::NoMatch => ("ACL No Match", name.as_str(), addr.as_ref().map(String::as_str)),
                },
            };

            try!(serializer.serialize_seq_elt(&mut state, &RawLogEntry {
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
pub struct Index<'a: 'i, 'i>(pub &'i LinkedHashMap<String, Vec<&'a str>>);

impl<'a: 'i, 'i> Serialize for Index<'a, 'i> {
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

#[derive(Debug)]
pub struct LogVarsIndex<'a: 'i, 'i>(pub &'i LinkedHashMap<&'a str, &'a str>);

impl<'a: 'i, 'i> Serialize for LogVarsIndex<'a, 'i> {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where S: Serializer {
        let mut state = try!(serializer.serialize_map(Some(self.0.len())));
        for (key, value) in self.0 {
            try!(serializer.serialize_map_key(&mut state, key));
            try!(serializer.serialize_map_value(&mut state, value));
        }
        try!(serializer.serialize_map_end(state));
        Ok(())
    }
}

pub type LogMessages<'a, 'i> = &'i [&'a str];

#[derive(Serialize, Debug)]
pub struct CacheObject<'a: 'i, 'i> {
    pub storage_type: &'a str,
    pub storage_name: &'a str,
    pub ttl_duration: Option<f64>,
    pub grace_duration: Option<f64>,
    pub keep_duration: Option<f64>,
    pub since_timestamp: f64,
    pub origin_timestamp: f64,
    pub fetch_mode: Option<&'a str>,
    pub fetch_streamed: Option<bool>,
    pub response: Option<HttpResponse<'a, 'i>>,
}

#[derive(Serialize, Debug)]
pub struct BackendConnection<'a> {
    pub fd: isize,
    pub name: &'a str,
    pub remote_address: Option<Address<'a>>,
    pub local_address: Address<'a>,
}
