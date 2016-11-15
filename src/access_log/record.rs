use vsl::record::{
    VslIdent,
};
pub use vsl::record::message::{
    TimeStamp,
    Duration,
    ByteCount,
    FetchMode,
    Status,
    Port,
    FileDescriptor,
    AclResult,
    CompressionOperation,
    CompressionDirection,
};

pub type Address = (String, Port);

#[derive(Debug, Clone, PartialEq)]
pub enum LogEntry {
    /// VCL std.log logged messages
    Vcl(String),
    /// Debug messages that may be logged by Varnish or it's modules
    Debug(String),
    /// Varnish logged errors
    Error(String),
    /// Errors related to fetch operation
    FetchError(String),
    /// Problems with processing headers, log messages etc
    Warning(String),
    /// ACL match result, name and value
    Acl(AclResult, String, Option<String>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Accounting {
    pub recv_header: ByteCount,
    pub recv_body: ByteCount,
    pub recv_total: ByteCount,
    pub sent_header: ByteCount,
    pub sent_body: ByteCount,
    pub sent_total: ByteCount,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PipeAccounting {
    pub recv_total: ByteCount,
    pub sent_total: ByteCount,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Handling {
    /// Cache hit and served from cache
    Hit(VslIdent),
    /// Cache miss and served from backend response
    Miss,
    /// Served from backend as request was not cacheable
    Pass,
    /// Served from backend as previous response was not cacheable
    HitPass(VslIdent),
    /// Response generated internally by Varnish
    Synth,
    /// This request and any further communication is passed to the backend
    Pipe,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Compression {
    pub operation: CompressionOperation,
    pub bytes_in: ByteCount,
    pub bytes_out: ByteCount,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Link<T> {
    Unresolved(VslIdent, String),
    Resolved(Box<T>),
}

/// All Duration fields are in seconds (floating point values rounded to micro second precision)
#[derive(Debug, Clone, PartialEq)]
pub struct ClientAccessRecord {
    pub root: bool,
    pub ident: VslIdent,
    pub parent: VslIdent,
    pub reason: String,
    pub remote: Address,
    pub transaction: ClientAccessTransaction,
    /// Start of request processing
    pub start: TimeStamp,
    /// End of request processing
    pub end: Option<TimeStamp>,
    pub handling: Handling,
    pub compression: Option<Compression>,
    pub log: Vec<LogEntry>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ClientAccessTransaction {
    Full {
        request: HttpRequest,
        response: HttpResponse,
        esi_records: Vec<Link<ClientAccessRecord>>,
        backend_record: Option<Link<BackendAccessRecord>>,
        /// Time it took to process request; None for ESI subrequests as they have this done already
        process: Option<Duration>,
        /// Time waiting for backend response fetch to finish; None for HIT
        fetch: Option<Duration>,
        /// Time it took to get first byte of response
        ttfb: Duration,
        /// Total duration it took to serve the whole response
        serve: Duration,
        accounting: Accounting,
    },
    RestartedEarly {
        request: HttpRequest,
        /// Time it took to process request; None for ESI subrequests as they have this done already
        process: Option<Duration>,
        restart_record: Link<ClientAccessRecord>,
    },
    RestartedLate {
        request: HttpRequest,
        response: HttpResponse,
        backend_record: Option<Link<BackendAccessRecord>>,
        /// Time it took to process request; None for ESI subrequests as they have this done already
        process: Option<Duration>,
        restart_record: Link<ClientAccessRecord>,
    },
    Piped {
        request: HttpRequest,
        backend_record: Link<BackendAccessRecord>,
        /// Time it took to process request; None for ESI subrequests as they have this done already
        process: Option<Duration>,
        /// Time it took to get first byte of response
        ttfb: Option<Duration>,
        accounting: PipeAccounting,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct CacheObject {
    /// Type ("malloc", "file", "persistent" etc.)
    pub storage_type: String,
    /// Name of storage backend
    pub storage_name: String,
    /// TTL; None if unset
    pub ttl: Option<Duration>,
    /// Grace; None if unset
    pub grace: Option<Duration>,
    /// Keep; None if unset
    pub keep: Option<Duration>,
    /// Reference time for TTL
    pub since: TimeStamp,
    /// Reference time for object lifetime (now - Age)
    pub origin: TimeStamp,
    /// Text description of body fetch mode
    pub fetch_mode: String,
    pub fetch_streamed: bool,
    pub response: HttpResponse
}

#[derive(Debug, Clone, PartialEq)]
pub struct BackendConnection {
    pub fd: FileDescriptor,
    pub name: String,
    pub remote: Address,
    pub local: Address,
}

/// All Duration fields are in seconds (floating point values rounded to micro second precision)
#[derive(Debug, Clone, PartialEq)]
pub struct BackendAccessRecord {
    pub ident: VslIdent,
    pub parent: VslIdent,
    pub reason: String,
    pub transaction: BackendAccessTransaction,
    /// Start of backend request processing
    pub start: Option<TimeStamp>,
    /// End of response processing; None for aborted or piped response
    pub end: Option<TimeStamp>,
    pub compression: Option<Compression>,
    pub log: Vec<LogEntry>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BackendAccessTransaction {
    Full {
        request: HttpRequest,
        response: HttpResponse,
        /// Backend connection used/created
        backend_connection: BackendConnection,
        /// Response that was stored in memory
        cache_object: CacheObject,
        /// Time it took to send backend request, e.g. it may include backend access/connect time
        send: Duration,
        /// Time waiting for first byte of backend response after request was sent
        wait: Duration,
        /// Time it took to get first byte of backend response
        ttfb: Duration,
        /// Total duration it took to fetch the whole response
        fetch: Duration,
        accounting: Accounting,
    },
    Failed {
        request: HttpRequest,
        synth_response: HttpResponse,
        /// Some if this was retried
        retry_record: Option<Link<BackendAccessRecord>>,
        /// Total duration it took to synthesise response
        synth: Duration,
        accounting: Accounting,
    },
    /// Aborted before we have made a backend request
    Aborted {
        request: HttpRequest,
    },
    /// Varnish got the backend response but it did not like it: abort or retry
    Abandoned {
        request: HttpRequest,
        response: HttpResponse,
        /// Backend connection used/created
        backend_connection: BackendConnection,
        /// Some if this was a retry
        retry_record: Option<Link<BackendAccessRecord>>,
        /// Time it took to send backend request, e.g. it may include backend access/connect time
        send: Duration,
        /// Time waiting for first byte of backend response after request was sent
        wait: Duration,
        /// Time it took to get first byte of backend response
        ttfb: Duration,
        /// Total duration it took to fetch the whole response: Some for retry, None for abandon
        fetch: Option<Duration>,
    },
    Piped {
        request: HttpRequest,
        /// Backend connection used/created
        backend_connection: Option<BackendConnection>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct SessionRecord {
    pub ident: VslIdent,
    pub open: TimeStamp,
    pub duration: Duration,
    pub local: Option<Address>,
    pub remote: Address,
    pub client_records: Vec<Link<ClientAccessRecord>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HttpRequest {
    pub protocol: String,
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HttpResponse {
    pub status: Status,
    pub reason: String,
    pub protocol: String,
    pub headers: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Record {
    ClientAccess(ClientAccessRecord),
    BackendAccess(BackendAccessRecord),
    Session(SessionRecord),
}

impl<T> Link<T> {
    pub fn is_unresolved(&self) -> bool {
        match *self {
            Link::Unresolved(..) => true,
            _ => false
        }
    }
    pub fn unwrap_unresolved(self) -> VslIdent {
        match self {
            Link::Unresolved(ident, _) => ident,
            _ => panic!("unwrap_unresolved called on Link that was not Unresolved")
        }
    }
    pub fn get_unresolved(&self) -> Option<VslIdent> {
        match *self {
            Link::Unresolved(ident, _) => Some(ident),
            _ => None
        }
    }

    pub fn is_resolved(&self) -> bool {
        match *self {
            Link::Resolved(_) => true,
            _ => false
        }
    }
    pub fn unwrap_resolved(self) -> Box<T> {
        match self {
            Link::Resolved(t) => t,
            _ => panic!("unwrap_resolved called on Link that was not Resolved")
        }
    }
    pub fn get_resolved(&self) -> Option<&T> {
        match *self {
            Link::Resolved(ref t) => Some(t.as_ref()),
            _ => None
        }
    }
}

impl Record {
    pub fn is_client_access(&self) -> bool {
        match *self {
            Record::ClientAccess(_) => true,
            _ => false
        }
    }
    pub fn unwrap_client_access(self) -> ClientAccessRecord {
        match self {
            Record::ClientAccess(access_record) => access_record,
            _ => panic!("unwrap_client_access called on Record that was not ClientAccess")
        }
    }

    pub fn is_backend_access(&self) -> bool {
        match *self {
            Record::BackendAccess(_) => true,
            _ => false
        }
    }
    pub fn unwrap_backend_access(self) -> BackendAccessRecord {
        match self {
            Record::BackendAccess(access_record) => access_record,
            _ => panic!("unwrap_backend_access called on Record that was not BackendAccess")
        }
    }

    pub fn is_session(&self) -> bool {
        match *self {
            Record::Session(_) => true,
            _ => false,
        }
    }
    pub fn unwrap_session(self) -> SessionRecord {
        match self {
            Record::Session(session_record) => session_record,
            _ => panic!("unwrap_session called on Record that was not Session")
        }
    }
}
