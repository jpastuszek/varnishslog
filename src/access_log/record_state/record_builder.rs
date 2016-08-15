/// TODO:
/// * miss/hit etc
/// * Cache object info
///  * TTL
///  * SLT_Storage
/// * Call trace
/// * ACL trace
/// * more tests
/// * pipe sessions
/// * SLT_ExpBan         196625 banned lookup
///
/// Client headers:
/// ---
///
/// Req:
/// * What client set us (SLT_VCL_call RECV)
///
/// Resp:
/// * What we sent to the client (SLT_End)
///
/// Backend headers:
/// ---
///
/// Bereq:
/// * What we sent to the backend (SLT_VCL_call BACKEND_RESPONSE or BACKEND_ERROR)
/// * Note that (SLT_VCL_return fetch) is also used by req
///
/// Beresp:
/// * What backend sent us (SLT_VCL_call BACKEND_RESPONSE or BACKEND_ERROR)
///
/// Timestamps
/// ===
///
/// Req (logs/varnish20160805-3559-f6sifo45103025c06abad14.vsl):
/// ---
/// * parse (req_process) - Start to Req
/// * fetch (resp_fetch) - Req to Fetch
/// * ttfb (resp_ttfb) - Start to Process
/// * serve (req_took)- Start to Resp
///
/// Note that we may have no process time for ESI requests as they don't get Req: record
///
///     2 SLT_Timestamp      Start: 1470403414.647192 0.000000 0.000000
///     2 SLT_Timestamp      Req: 1470403414.647192 0.000000 0.000000
///     2 SLT_ReqStart       127.0.0.1 39792
///     2 SLT_VCL_call       RECV
///     2 SLT_VCL_call       HASH
///     2 SLT_VCL_return     lookup
///     2 SLT_VCL_call       SYNTH
///     2 SLT_Timestamp      Process: 1470403414.647272 0.000081 0.000081
///     2 SLT_VCL_return     deliver
///     2 SLT_RespHeader     Connection: keep-alive
///     2 SLT_Timestamp      Resp: 1470403414.647359 0.000167 0.000086
///     2 SLT_ReqAcct        148 0 148 185 25 210
///     2 SLT_End
///
///     4 SLT_Timestamp      Start: 1470403414.653332 0.000000 0.000000
///     4 SLT_Timestamp      Req: 1470403414.653332 0.000000 0.000000
///     4 SLT_ReqStart       127.0.0.1 39794
///     4 SLT_VCL_call       MISS
///     4 SLT_ReqHeader      X-Varnish-Result: miss
///     4 SLT_VCL_return     fetch
///     4 SLT_Link           bereq 5 fetch
///     4 SLT_Timestamp      Fetch: 1470403414.658863 0.005531 0.005531
///     4 SLT_VCL_call       DELIVER
///     4 SLT_VCL_return     deliver
///     4 SLT_Timestamp      Process: 1470403414.658956 0.005624 0.000093
///     4 SLT_Debug          RES_MODE 2
///     4 SLT_RespHeader     Connection: keep-alive
///     4 SLT_Timestamp      Resp: 1470403414.658984 0.005652 0.000028
///     4 SLT_ReqAcct 90 0 90 369 9 378 4 SLT_End
///
/// Bereq:
/// ---
/// Note that we may not have process time as backend request can be abandoned in vcl_backend_fetch.
///
/// * send (req_process) - Start to Bereq
/// * ttfb (resp_ttfb) - Start to Beresp
/// * wait (resp_fetch) - Bereq to Beresp
/// * fetch (req_took) - Start to BerespBody
///
///     5 SLT_Begin          bereq 4 fetch
///     5 SLT_Timestamp      Start: 1470403414.653455 0.000000 0.000000
///     5 SLT_VCL_return     fetch
///     5 SLT_BackendOpen    19 boot.default 127.0.0.1 42001 127.0.0.1 37606
///     5 SLT_BackendStart   127.0.0.1 42001
///     5 SLT_Timestamp      Bereq: 1470403414.653592 0.000137 0.000137
///     5 SLT_Timestamp      Beresp: 1470403414.658717 0.005262 0.005124
///     5 SLT_Timestamp      BerespBody: 1470403414.658833 0.005378 0.000116
///     5 SLT_Length         9
///     5 SLT_BereqAcct      504 0 504 351 9 360
///     5 SLT_End
///

use std::fmt::Debug;

use vsl::*;
use vsl::VslRecordTag::*;

#[derive(Debug, Clone, PartialEq)]
pub enum LogEntry {
    /// VCL std.log logged messages
    VCL(String),
    /// Debug messages that may be logged by Varnish or it's modules
    Debug(String),
    /// Varnish logged errors
    Error(String),
    /// Errors related to fetch operation
    FetchError(String),
    /// Problems with processing headers, log messages etc
    Warning(String),
}

/// All Duration fields are in seconds (floating point values rounded to micro second precision)
#[derive(Debug, Clone, PartialEq)]
pub struct ClientAccessRecord {
    pub ident: VslIdent,
    pub parent: VslIdent,
    pub reason: String,
    pub esi_requests: Vec<VslIdent>,
    pub backend_requests: Vec<VslIdent>,
    pub restart_request: Option<VslIdent>,
    pub http_transaction: HttpTransaction,
    /// Start of request processing
    pub start: TimeStamp,
    /// Time it took to parse request; Note that ESI requests are already parsed (None)
    pub parse: Option<Duration>,
    /// Time waiting for backend response fetch to finish
    pub fetch: Option<Duration>,
    /// Time it took to get first byte of response
    pub ttfb: Option<Duration>,
    /// Total duration it took to serve the whole response
    pub serve: Option<Duration>,
    /// End of request processing
    pub end: TimeStamp,
    pub recv_header: ByteCount,
    pub recv_body: ByteCount,
    pub recv_total: ByteCount,
    pub sent_header: ByteCount,
    pub sent_body: ByteCount,
    pub sent_total: ByteCount,
    pub log: Vec<LogEntry>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CacheObject {
    /*
    storage_type: String,
    storage_name: String,
    ttl: Duration,
    grace: Duration,
    keep: Duration,
    since: TimeStamp,
    */
    fetch_mode: String,
    fetch_streamed: bool,
    response: HttpResponse
}

/// All Duration fields are in seconds (floating point values rounded to micro second precision)
#[derive(Debug, Clone, PartialEq)]
pub struct BackendAccessRecord {
    pub ident: VslIdent,
    pub parent: VslIdent,
    pub reason: String,
    pub retry_request: Option<VslIdent>,
    pub http_transaction: HttpTransaction,
    pub cache_object: Option<CacheObject>,
    /// Start of backend request processing
    pub start: TimeStamp,
    /// Time it took to send backend request, e.g. it may include backend access/connect time
    pub send: Option<Duration>,
    /// Time waiting for first byte of backend response after request was sent
    pub wait: Option<Duration>,
    /// Time it took to get first byte of backend response
    pub ttfb: Option<Duration>,
    /// Total duration it took to fetch or synthesise the whole response
    pub fetch: Option<Duration>,
    /// End of response processing; may be None if it was abandoned
    pub end: Option<TimeStamp>,
    pub log: Vec<LogEntry>,
}

pub type Address = (String, Port);

#[derive(Debug, Clone, PartialEq)]
pub struct SessionRecord {
    pub ident: VslIdent,
    pub open: TimeStamp,
    pub duration: Duration,
    pub local: Option<Address>,
    pub remote: Address,
    pub client_requests: Vec<VslIdent>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HttpTransaction {
    pub request: HttpRequest,
    pub response: Option<HttpResponse>,
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
    pub protocol: String,
    pub status: Status,
    pub reason: String,
    pub headers: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Record {
    ClientAccess(ClientAccessRecord),
    BackendAccess(BackendAccessRecord),
    Session(SessionRecord),
}

impl Record {
    #[allow(dead_code)]
    pub fn is_client_access(&self) -> bool {
        match self {
            &Record::ClientAccess(_) => true,
            _ => false
        }
    }
    #[allow(dead_code)]
    pub fn is_backend_access(&self) -> bool {
        match self {
            &Record::BackendAccess(_) => true,
            _ => false
        }
    }
    #[allow(dead_code)]
    pub fn is_session(&self) -> bool {
        match self {
            &Record::Session(_) => true,
            _ => false,
        }
    }

    #[allow(dead_code)]
    pub fn unwrap_client_access(self) -> ClientAccessRecord {
        match self {
            Record::ClientAccess(access_record) => access_record,
            _ => panic!("unwrap_client_access called on Record that was not ClientAccess")
        }
    }
    #[allow(dead_code)]
    pub fn unwrap_backend_access(self) -> BackendAccessRecord {
        match self {
            Record::BackendAccess(access_record) => access_record,
            _ => panic!("unwrap_backend_access called on Record that was not BackendAccess")
        }
    }
    #[allow(dead_code)]
    pub fn unwrap_session(self) -> SessionRecord {
        match self {
            Record::Session(session_record) => session_record,
            _ => panic!("unwrap_session called on Record that was not Session")
        }
    }
}

// Builders

quick_error! {
    #[derive(Debug)]
    pub enum RecordBuilderError {
        UnimplementedTransactionType(record_type: String) {
            display("Unimplemented record type '{}'", record_type)
        }
        InvalidMessageFormat(err: VslRecordParseError) {
            display("Failed to parse VSL record data: {}", err)
            from()
        }
        DetailAlreadyBuilt(detail_name: &'static str) {
            display("Expected {} to be still building but got it complete", detail_name)
        }
        DetailIncomplete(detail_name: &'static str) {
            display("Expected {} to be complete but it was still building", detail_name)
        }
        RecordIncomplete(field_name: &'static str) {
            display("Failed to construct final access record due to missing field '{}'", field_name)
        }
    }
}

trait BytesExt {
    fn to_lossy_string(&self) -> String;
}

impl BytesExt for [u8] {
    fn to_lossy_string(&self) -> String {
        String::from_utf8_lossy(self).into_owned()
    }
}

#[derive(Debug)]
pub enum BuilderResult<B, C> {
    Building(B),
    Complete(C),
}

// So we can just type Building() and Complete()
use self::BuilderResult::*;

impl<B, C> BuilderResult<B, C> {
    #[allow(dead_code)]
    fn as_ref(&self) -> BuilderResult<&B, &C> {
        match self {
            &BuilderResult::Building(ref buidling) => BuilderResult::Building(buidling),
            &BuilderResult::Complete(ref complete) => BuilderResult::Complete(complete),
        }
    }

    #[allow(dead_code)]
    fn unwrap(self) -> C where B: Debug {
        match self {
            BuilderResult::Building(buidling) => panic!("Trying to unwrap BuilderResult::Building: {:?}", buidling),
            BuilderResult::Complete(complete) => complete,
        }
    }

    #[allow(dead_code)]
    fn unwrap_building(self) -> B where C: Debug {
        match self {
            BuilderResult::Building(buidling) => buidling,
            BuilderResult::Complete(complete) => panic!("Trying to unwrap BuilderResult::Complete: {:?}", complete),
        }
    }

    fn apply(self, vsl: &VslRecord) -> Result<BuilderResult<B, C>, RecordBuilderError> where B: DetailBuilder<C>
    {
        let builder_result = if let Building(builder) = self {
            Building(try!(builder.apply(vsl)))
        } else {
            debug!("Ignoring {} as we have finished building {}", vsl, B::result_name());
            self
        };

        Ok(builder_result)
    }

    fn complete(self) -> Result<BuilderResult<B, C>, RecordBuilderError> where B: DetailBuilder<C> {
        match self {
            Complete(_) => Err(RecordBuilderError::DetailAlreadyBuilt(B::result_name())),
            Building(builder) => Ok(Complete(try!(builder.complete()))),
        }
    }

    fn get_complete(self) -> Result<C, RecordBuilderError> where B: DetailBuilder<C> {
        match self {
            Complete(response) => Ok(response),
            Building(_) => Err(RecordBuilderError::DetailIncomplete(B::result_name())),
        }
    }
}

trait DetailBuilder<C>: Sized {
    fn result_name() -> &'static str;
    fn apply(self, vsl: &VslRecord) -> Result<Self, RecordBuilderError>;
    fn complete(self) -> Result<C, RecordBuilderError>;
}

#[derive(Debug)]
struct Headers {
    headers: Vec<(String, String)>,
}

impl Headers {
    fn new() -> Headers {
        Headers {
            headers: Vec::new()
        }
    }

    fn set(&mut self, name: String, value: String) {
        self.headers.push((name, value));
    }

    fn unset(&mut self, name: &str, value: &str) {
        self.headers.retain(|header| {
            let &(ref t_name, ref t_value) = header;
            (t_name.as_str(), t_value.as_str()) != (name, value)
        });
    }

    fn unwrap(self) -> Vec<(String, String)> {
        self.headers
    }
}

#[derive(Debug)]
struct HttpRequestBuilder {
    protocol: Option<String>,
    method: Option<String>,
    url: Option<String>,
    headers: Headers,
}

impl HttpRequestBuilder {
    fn new() -> HttpRequestBuilder {
        HttpRequestBuilder {
            protocol: None,
            method: None,
            url: None,
            headers: Headers::new(),
        }
    }
}

impl DetailBuilder<HttpRequest> for HttpRequestBuilder {
    fn result_name() -> &'static str {
        "HTTP Request"
    }

    fn apply(self, vsl: &VslRecord) -> Result<HttpRequestBuilder, RecordBuilderError> {
        let builder = match vsl.tag {
            SLT_BereqProtocol | SLT_ReqProtocol => {
                let protocol = try!(vsl.parse_data(slt_protocol));

                HttpRequestBuilder {
                    protocol: Some(protocol.to_lossy_string()),
                    .. self
                }
            }
            SLT_BereqMethod | SLT_ReqMethod => {
                let method = try!(vsl.parse_data(slt_method));

                HttpRequestBuilder {
                    method: Some(method.to_lossy_string()),
                    .. self
                }
            }
            SLT_BereqURL | SLT_ReqURL => {
                let url = try!(vsl.parse_data(slt_url));

                HttpRequestBuilder {
                    url: Some(url.to_lossy_string()),
                    .. self
                }
            }
            SLT_BereqHeader | SLT_ReqHeader => {
                if let (name, Some(value)) = try!(vsl.parse_data(slt_header)) {
                    let mut headers = self.headers;
                    headers.set(name.to_lossy_string(), value.to_lossy_string());

                    HttpRequestBuilder {
                        headers: headers,
                        .. self
                    }
                } else {
                    debug!("Not setting empty request header: {:?}", vsl);
                    self
                }
            }
            SLT_BereqUnset | SLT_ReqUnset => {
                if let (name, Some(value)) = try!(vsl.parse_data(slt_header)) {
                    let mut headers = self.headers;
                    headers.unset(&name.to_lossy_string(), &value.to_lossy_string());

                    HttpRequestBuilder {
                        headers: headers,
                        .. self
                    }
                } else {
                    debug!("Not unsetting empty request header: {:?}", vsl);
                    self
                }
            }
            _ => panic!("Got unexpected VSL record in request builder: {:?}", vsl)
        };

        Ok(builder)
    }

    fn complete(self) -> Result<HttpRequest, RecordBuilderError> {
        Ok(HttpRequest {
            protocol: try!(self.protocol.ok_or(RecordBuilderError::RecordIncomplete("Request.protocol"))),
            method: try!(self.method.ok_or(RecordBuilderError::RecordIncomplete("Request.method"))),
            url: try!(self.url.ok_or(RecordBuilderError::RecordIncomplete("Request.url"))),
            headers: self.headers.unwrap(),
        })
    }
}

#[derive(Debug)]
struct HttpResponseBuilder {
    protocol: Option<String>,
    status: Option<Status>,
    reason: Option<String>,
    headers: Headers,
}

impl HttpResponseBuilder {
    fn new() -> HttpResponseBuilder {
        HttpResponseBuilder {
            protocol: None,
            status: None,
            reason: None,
            headers: Headers::new(),
        }
    }
}

impl DetailBuilder<HttpResponse> for HttpResponseBuilder {
    fn result_name() -> &'static str {
        "HTTP Response"
    }

    fn apply(self, vsl: &VslRecord) -> Result<HttpResponseBuilder, RecordBuilderError> {
        let builder = match vsl.tag {
            SLT_BerespProtocol | SLT_RespProtocol | SLT_ObjProtocol => {
                let protocol = try!(vsl.parse_data(slt_protocol));

                HttpResponseBuilder {
                    protocol: Some(protocol.to_lossy_string()),
                    .. self
                }
            }
            SLT_BerespStatus | SLT_RespStatus | SLT_ObjStatus => {
                let status = try!(vsl.parse_data(slt_status));

                HttpResponseBuilder {
                    status: Some(status),
                    .. self
                }
            }
            SLT_BerespReason | SLT_RespReason | SLT_ObjReason => {
                let reason = try!(vsl.parse_data(slt_reason));

                HttpResponseBuilder {
                    reason: Some(reason.to_lossy_string()),
                    .. self
                }
            }
            SLT_BerespHeader | SLT_RespHeader | SLT_ObjHeader => {
                if let (name, Some(value)) = try!(vsl.parse_data(slt_header)) {
                    let mut headers = self.headers;
                    headers.set(name.to_lossy_string(), value.to_lossy_string());

                    HttpResponseBuilder {
                        headers: headers,
                        .. self
                    }
                } else {
                    debug!("Not setting empty response header: {:?}", vsl);
                    self
                }
            }
            SLT_BerespUnset | SLT_RespUnset | SLT_ObjUnset => {
                if let (name, Some(value)) = try!(vsl.parse_data(slt_header)) {
                    let mut headers = self.headers;
                    headers.unset(&name.to_lossy_string(), &value.to_lossy_string());

                    HttpResponseBuilder {
                        headers: headers,
                        .. self
                    }
                } else {
                    debug!("Not unsetting empty response header: {:?}", vsl);
                    self
                }
            }
            _ => panic!("Got unexpected VSL record in request builder: {:?}", vsl)
        };

        Ok(builder)
    }

    fn complete(self) -> Result<HttpResponse, RecordBuilderError> {
        Ok(HttpResponse {
            protocol: try!(self.protocol.ok_or(RecordBuilderError::RecordIncomplete("Response.protocol"))),
            status: try!(self.status.ok_or(RecordBuilderError::RecordIncomplete("Response.status"))),
            reason: try!(self.reason.ok_or(RecordBuilderError::RecordIncomplete("Response.reason"))),
            headers: self.headers.unwrap(),
        })
    }
}

#[derive(Debug)]
pub enum RecordType {
    ClientAccess {
        parent: VslIdent,
        reason: String,
    },
    BackendAccess {
        parent: VslIdent,
        reason: String,
    },
    Session
}

#[derive(Debug)]
struct ReqAcct {
    recv_header: ByteCount,
    recv_body: ByteCount,
    recv_total: ByteCount,
    sent_header: ByteCount,
    sent_body: ByteCount,
    sent_total: ByteCount,
}

#[derive(Debug)]
pub struct FetchBody {
    pub mode: String,
    pub streamed: bool,
}

#[derive(Debug)]
pub struct RecordBuilder {
    ident: VslIdent,
    record_type: Option<RecordType>,
    req_start: Option<TimeStamp>,
    http_request: BuilderResult<HttpRequestBuilder, HttpRequest>,
    http_response: BuilderResult<HttpResponseBuilder, HttpResponse>,
    cache_object: BuilderResult<HttpResponseBuilder, HttpResponse>,
    fetch_body: Option<FetchBody>,
    resp_fetch: Option<Duration>,
    req_process: Option<Duration>,
    resp_ttfb: Option<Duration>,
    req_took: Option<Duration>,
    resp_end: Option<TimeStamp>,
    req_acct: Option<ReqAcct>,
    sess_open: Option<TimeStamp>,
    sess_duration: Option<Duration>,
    sess_remote: Option<Address>,
    sess_local: Option<Address>,
    client_requests: Vec<VslIdent>,
    backend_requests: Vec<VslIdent>,
    restart_request: Option<VslIdent>,
    retry_request: Option<VslIdent>,
    log: Vec<LogEntry>,
}

impl RecordBuilder {
    pub fn new(ident: VslIdent) -> RecordBuilder {
        RecordBuilder {
            ident: ident,
            record_type: None,
            req_start: None,
            http_request: Building(HttpRequestBuilder::new()),
            http_response: Building(HttpResponseBuilder::new()),
            cache_object: Building(HttpResponseBuilder::new()),
            fetch_body: None,
            req_process: None,
            resp_fetch: None,
            resp_ttfb: None,
            req_took: None,
            resp_end: None,
            req_acct: None,
            sess_open: None,
            sess_duration: None,
            sess_remote: None,
            sess_local: None,
            client_requests: Vec::new(),
            backend_requests: Vec::new(),
            restart_request: None,
            retry_request: None,
            log: Vec::new(),
        }
    }

    pub fn apply<'r>(self, vsl: &'r VslRecord) -> Result<BuilderResult<RecordBuilder, Record>, RecordBuilderError> {
        let builder = match vsl.tag {
            SLT_Begin => {
                let (record_type, vxid, reason) = try!(vsl.parse_data(slt_begin));

                match record_type {
                    "bereq" => RecordBuilder {
                        record_type: Some(RecordType::BackendAccess {
                            parent: vxid,
                            reason: reason.to_owned()
                        }),
                        .. self
                    },
                    "req" => RecordBuilder {
                        record_type: Some(RecordType::ClientAccess {
                            parent: vxid,
                            reason: reason.to_owned()
                        }),
                        .. self
                    },
                    "sess" => RecordBuilder {
                        record_type: Some(RecordType::Session),
                        .. self
                    },
                    _ => return Err(RecordBuilderError::UnimplementedTransactionType(record_type.to_string()))
                }
            }
            SLT_Timestamp => {
                let (label, timestamp, since_work_start, since_last_timestamp) = try!(vsl.parse_data(slt_timestamp));
                match label {
                    "Start" => RecordBuilder {
                        req_start: Some(timestamp),
                        .. self
                    },
                    "Req" => RecordBuilder {
                        req_process: Some(since_work_start),
                        .. self
                    },
                    "Bereq" => RecordBuilder {
                        req_process: Some(since_work_start),
                        .. self
                    },
                    "Beresp" => RecordBuilder {
                        resp_ttfb: Some(since_work_start),
                        resp_fetch: Some(since_last_timestamp),
                        .. self
                    },
                    "Fetch" => RecordBuilder {
                        resp_fetch: Some(since_last_timestamp),
                        .. self
                    },
                    "Process" => RecordBuilder {
                        resp_ttfb: Some(since_work_start),
                        .. self
                    },
                    "Resp" => RecordBuilder {
                        req_took: Some(since_work_start),
                        resp_end: Some(timestamp),
                        .. self
                    },
                    "BerespBody" | "Retry" => RecordBuilder {
                        req_took: Some(since_work_start),
                        resp_end: Some(timestamp),
                        .. self
                    },
                    "Error" => RecordBuilder {
                        req_took: Some(since_work_start),
                        resp_end: Some(timestamp),
                        // this won't be correct if we got error while accessing backend
                        resp_ttfb: None,
                        resp_fetch: None,
                        .. self
                    },
                    "Restart" => RecordBuilder {
                        resp_end: Some(timestamp),
                        .. self
                    },
                    _ => {
                        warn!("Ignoring unknown SLT_Timestamp label variant: {}", label);
                        self
                    }
                }
            }
            SLT_Link => {
                let (reason, child_vxid, child_type) = try!(vsl.parse_data(slt_link));

                let child_vxid = child_vxid;

                match (reason, child_type) {
                    ("req", "restart") => {
                        RecordBuilder {
                            restart_request: Some(child_vxid),
                            .. self
                        }
                    },
                    ("req", _) => {
                        let mut client_requests = self.client_requests;
                        client_requests.push(child_vxid);

                        RecordBuilder {
                            client_requests: client_requests,
                            .. self
                        }
                    },
                    ("bereq", "retry") => {
                        RecordBuilder {
                            retry_request: Some(child_vxid),
                            .. self
                        }
                    },
                    ("bereq", _) => {
                        let mut backend_requests = self.backend_requests;
                        backend_requests.push(child_vxid);

                        RecordBuilder {
                            backend_requests: backend_requests,
                            .. self
                        }
                    },
                    _ => {
                        warn!("Ignoring unknown SLT_Link reason variant: {}", reason);
                        self
                    }
                }
            }
            SLT_VCL_Log => {
                let mut log = self.log;
                log.push(LogEntry::VCL(try!(vsl.parse_data(slt_log)).to_lossy_string()));

                RecordBuilder {
                    log: log,
                    .. self
                }
            }
            SLT_Debug => {
                let mut log = self.log;
                log.push(LogEntry::Debug(try!(vsl.parse_data(slt_log)).to_lossy_string()));

                RecordBuilder {
                    log: log,
                    .. self
                }
            }
            SLT_Error => {
                let mut log = self.log;
                log.push(LogEntry::Error(try!(vsl.parse_data(slt_log)).to_lossy_string()));

                RecordBuilder {
                    log: log,
                    .. self
                }
            }
            SLT_FetchError => {
                let mut log = self.log;
                log.push(LogEntry::FetchError(try!(vsl.parse_data(slt_log)).to_lossy_string()));

                RecordBuilder {
                    log: log,
                    .. self
                }
            }
            SLT_BogoHeader => {
                let mut log = self.log;
                log.push(LogEntry::Warning(format!("Bogus HTTP header received: {}", try!(vsl.parse_data(slt_log)).to_lossy_string())));
                RecordBuilder {
                    log: log,
                    .. self
                }
            }
            SLT_LostHeader => {
                let mut log = self.log;
                log.push(LogEntry::Warning(format!("Failed HTTP header operation due to resource exhaustion or configured limits; header was: {}", try!(vsl.parse_data(slt_log)).to_lossy_string())));
                RecordBuilder {
                    log: log,
                    .. self
                }
            }

            SLT_ReqAcct => {
                let (recv_header, recv_body, recv_total,
                     sent_header, sent_body, sent_total) =
                    try!(vsl.parse_data(slt_reqacc));

                RecordBuilder {
                    req_acct: Some(ReqAcct {
                        recv_header: recv_header,
                        recv_body: recv_body,
                        recv_total: recv_total,
                        sent_header: sent_header,
                        sent_body: sent_body,
                        sent_total: sent_total,
                    }),
                    .. self
                }
            }

            // Request
            SLT_BereqProtocol | SLT_ReqProtocol |
                SLT_BereqMethod | SLT_ReqMethod |
                SLT_BereqURL | SLT_ReqURL |
                SLT_BereqHeader | SLT_ReqHeader |
                SLT_BereqUnset | SLT_ReqUnset => {
                    RecordBuilder {
                        http_request: try!(self.http_request.apply(vsl)),
                        .. self
                    }
                }

            // Response
            SLT_BerespProtocol | SLT_RespProtocol |
                SLT_BerespStatus | SLT_RespStatus |
                SLT_BerespReason | SLT_RespReason |
                SLT_BerespHeader | SLT_RespHeader |
                SLT_BerespUnset | SLT_RespUnset => {
                    RecordBuilder {
                        http_response: try!(self.http_response.apply(vsl)),
                        .. self
                    }
                }

            // Cache Object
            SLT_ObjProtocol |
                SLT_ObjStatus |
                SLT_ObjReason |
                SLT_ObjHeader |
                SLT_ObjUnset => {
                    RecordBuilder {
                        cache_object: try!(self.cache_object.apply(vsl)),
                        .. self
                    }
                }

            // Session
            SLT_SessOpen => {
                let (remote_address, _listen_sock, local_address, timestamp, _fd)
                    = try!(vsl.parse_data(slt_sess_open));

                let remote_address = (remote_address.0.to_string(), remote_address.1);
                let local_address = local_address.map(|(ip, port)| (ip.to_string(), port));

                RecordBuilder {
                    sess_open: Some(timestamp),
                    sess_remote: Some(remote_address),
                    sess_local: local_address,
                    .. self
                }
            }
            SLT_SessClose => {
                let (_reason, duration) = try!(vsl.parse_data(slt_sess_close));

                RecordBuilder {
                    sess_duration: Some(duration),
                    .. self
                }
            }

            // Final
            SLT_VCL_call => {
                let method = try!(vsl.parse_data(slt_call));

                match method {
                    "RECV" => RecordBuilder {
                        http_request: try!(self.http_request.complete()),
                        .. self
                    },
                    "BACKEND_RESPONSE" | "BACKEND_ERROR" => RecordBuilder {
                        http_request: try!(self.http_request.complete()),
                        http_response: try!(self.http_response.complete()),
                        .. self
                    },
                    _ => {
                        debug!("Ignoring unknown {:?} method: {}", vsl.tag, method);
                        self
                    }
                }
            }
            SLT_Fetch_Body => {
                let (_fetch_mode, fetch_mode_name, streamed) =
                    try!(vsl.parse_data(slt_fetch_body));

                RecordBuilder {
                    cache_object: try!(self.cache_object.complete()),
                    fetch_body: Some( FetchBody {
                        mode: fetch_mode_name.to_string(),
                        streamed: streamed,
                    }),
                    .. self
                }
            }
            SLT_End => {
                let record_type = try!(self.record_type.ok_or(RecordBuilderError::RecordIncomplete("record_type")));
                match record_type {
                    RecordType::Session => {
                        // Try to build SessionRecord
                        let record = SessionRecord {
                            ident: self.ident,
                            open: try!(self.sess_open.ok_or(RecordBuilderError::RecordIncomplete("sess_open"))),
                            duration: try!(self.sess_duration.ok_or(RecordBuilderError::RecordIncomplete("sess_duration"))),
                            local: self.sess_local,
                            remote: try!(self.sess_remote.ok_or(RecordBuilderError::RecordIncomplete("sess_remote"))),
                            client_requests: self.client_requests,
                        };

                        return Ok(Complete(Record::Session(record)))
                    },
                    RecordType::ClientAccess { .. } | RecordType::BackendAccess { .. } => {
                        let request = try!(self.http_request.get_complete());
                        let response = if self.restart_request.is_some() {
                            // Restarted requests have no response
                            None
                        } else {
                            let response = if let RecordType::ClientAccess { .. } = record_type {
                                // SLT_End tag is completing the client response
                                try!(self.http_response.complete())
                            } else {
                                self.http_response
                            };
                            Some(try!(response.get_complete()))
                        };

                        let http_transaction = HttpTransaction {
                            request: request,
                            response: response,
                        };

                        match record_type {
                            RecordType::ClientAccess { parent, reason } => {
                                let req_acct = try!(self.req_acct.ok_or(RecordBuilderError::RecordIncomplete("req_acct")));
                                let record = ClientAccessRecord {
                                    ident: self.ident,
                                    parent: parent,
                                    reason: reason,
                                    esi_requests: self.client_requests,
                                    backend_requests: self.backend_requests,
                                    restart_request: self.restart_request,
                                    http_transaction: http_transaction,

                                    start: try!(self.req_start.ok_or(RecordBuilderError::RecordIncomplete("req_start"))),
                                    parse: self.req_process,
                                    fetch: self.resp_fetch,
                                    ttfb: self.resp_ttfb,
                                    serve: self.req_took,
                                    end: try!(self.resp_end.ok_or(RecordBuilderError::RecordIncomplete("resp_end"))),

                                    recv_header: req_acct.recv_header,
                                    recv_body: req_acct.recv_body,
                                    recv_total: req_acct.recv_total,
                                    sent_header: req_acct.sent_header,
                                    sent_body: req_acct.sent_body,
                                    sent_total: req_acct.sent_total,

                                    log: self.log,
                                };

                                return Ok(Complete(Record::ClientAccess(record)))
                            },
                            RecordType::BackendAccess { parent, reason } => {
                                //TODO: use proper predicate
                                let cache_object = if let Ok(cache_object) = self.cache_object.get_complete() {
                                    let fetch_body = try!(self.fetch_body.ok_or(RecordBuilderError::RecordIncomplete("fetch_body")));

                                    Some(CacheObject {
                                        response: cache_object,
                                        fetch_mode: fetch_body.mode,
                                        fetch_streamed: fetch_body.streamed,
                                    })
                                } else {
                                    None
                                };

                                let record = BackendAccessRecord {
                                    ident: self.ident,
                                    parent: parent,
                                    reason: reason,
                                    retry_request: self.retry_request,
                                    http_transaction: http_transaction,
                                    cache_object: cache_object,
                                    start: try!(self.req_start.ok_or(RecordBuilderError::RecordIncomplete("req_start"))),
                                    send: self.req_process,
                                    wait: self.resp_fetch,
                                    ttfb: self.resp_ttfb,
                                    fetch: self.req_took,
                                    end: self.resp_end,
                                    log: self.log,
                                };

                                return Ok(Complete(Record::BackendAccess(record)))
                            },
                            _ => unreachable!(),
                        }
                    },
                }
            }
            _ => {
                debug!("Ignoring unknown VSL tag: {:?}", vsl.tag);
                self
            }
        };

        Ok(Building(builder))
    }
}

#[cfg(test)]
mod tests {
    pub use super::*;
    pub use super::super::super::test_helpers::*;
    use vsl::VslRecord;

    macro_rules! apply {
        ($state:ident, $ident:expr, $tag:ident, $message:expr) => {{
            let res = $state.apply(&vsl($tag, $ident, $message));
            if let Err(err) = res {
                panic!("expected apply to return Ok after applying: `{}, {:?}, {};`; got: {}", $ident, $tag, $message, err)
            }
            res.unwrap().unwrap_building()
        }};
    }

    macro_rules! apply_last {
        ($state:ident, $ident:expr, $tag:ident, $message:expr) => {{
            let res = $state.apply(&vsl($tag, $ident, $message));
            if let Err(err) = res {
                panic!("expected apply to return Ok after applying: `{}, {:?}, {};`; got: {}", $ident, $tag, $message, err)
            }
            res.unwrap().unwrap()
        }};
    }

    macro_rules! apply_all {
        ($builder:ident, $ident:expr, $tag:ident, $message:expr;) => {{
            apply!($builder, $ident, $tag, $message)
        }};
        ($builder:ident, $ident:expr, $tag:ident, $message:expr; $($t_ident:expr, $t_tag:ident, $t_message:expr;)+) => {{
            let builder = apply!($builder, $ident, $tag, $message);
            apply_all!(builder, $($t_ident, $t_tag, $t_message;)*)
        }};
    }

    #[test]
    fn apply_begin() {
        let builder = RecordBuilder::new(123);

        let builder = builder.apply(&vsl(SLT_Begin, 123, "bereq 321 fetch"))
            .unwrap().unwrap_building();

        assert_matches!(builder.record_type,
            Some(RecordType::BackendAccess { parent: 321, ref reason }) if reason == "fetch");
    }

    #[test]
    fn apply_log() {
        let builder = RecordBuilder::new(1);

        let builder = apply_all!(builder,
                                 1, SLT_VCL_Log,        "X-Varnish-Privileged-Client: false";
                                 1, SLT_VCL_Log,        "X-Varnish-User-Agent-Class: Unknown-Bot";
                                 1, SLT_VCL_Log,        "X-Varnish-Force-Failure: false";
                                );
        assert_eq!(builder.log, &[
                   LogEntry::VCL("X-Varnish-Privileged-Client: false".to_string()),
                   LogEntry::VCL("X-Varnish-User-Agent-Class: Unknown-Bot".to_string()),
                   LogEntry::VCL("X-Varnish-Force-Failure: false".to_string()),
        ]);
    }

    #[test]
    fn apply_begin_unimpl_transaction_type() {
        let builder = RecordBuilder::new(123);

        let result = builder.apply(&vsl(SLT_Begin, 123, "foo 231 fetch"));
        assert_matches!(result.unwrap_err(),
            RecordBuilderError::UnimplementedTransactionType(ref record_type) if record_type == "foo");
    }

    #[test]
    fn apply_begin_parser_fail() {
        let builder = RecordBuilder::new(123);

        let result = builder.apply(&vsl(SLT_Begin, 123, "foo bar"));
        assert_matches!(result.unwrap_err(),
            RecordBuilderError::InvalidMessageFormat(_));
    }

    #[test]
    fn apply_begin_int_parse_fail() {
        let builder = RecordBuilder::new(123);

        let result = builder.apply(&vsl(SLT_Begin, 123, "bereq foo fetch"));
        assert_matches!(result.unwrap_err(),
            RecordBuilderError::InvalidMessageFormat(_));
    }

    #[test]
    fn apply_backend_request_response() {
        let builder = RecordBuilder::new(123);

        let builder = apply_all!(builder,
                                 123, SLT_BereqMethod,      "GET";
                                 123, SLT_BereqURL,         "/foobar";
                                 123, SLT_BereqProtocol,    "HTTP/1.1";
                                 123, SLT_BereqHeader,      "Host: localhost:8080";
                                 123, SLT_BereqHeader,      "User-Agent: curl/7.40.0";
                                 123, SLT_BereqHeader,      "Accept-Encoding: gzip";
                                 123, SLT_BereqUnset,       "Accept-Encoding: gzip";
                                 123, SLT_BerespProtocol,   "HTTP/1.1";
                                 123, SLT_BerespStatus,     "503";
                                 123, SLT_BerespReason,     "Service Unavailable";
                                 123, SLT_BerespReason,     "Backend fetch failed";
                                 123, SLT_BerespHeader,     "Date: Fri, 22 Jul 2016 09:46:02 GMT";
                                 123, SLT_BerespHeader,     "Server: Varnish";
                                 123, SLT_BerespHeader,     "Cache-Control: no-store";
                                 123, SLT_BerespUnset,      "Cache-Control: no-store";
                                 123, SLT_VCL_call,         "BACKEND_RESPONSE";
                                );

        let request = builder.http_request.as_ref().unwrap();
        assert_eq!(request.method, "GET".to_string());
        assert_eq!(request.url, "/foobar".to_string());
        assert_eq!(request.protocol, "HTTP/1.1".to_string());
        assert_eq!(request.headers, &[
                   ("Host".to_string(), "localhost:8080".to_string()),
                   ("User-Agent".to_string(), "curl/7.40.0".to_string())]);

        let response = builder.http_response.as_ref().unwrap();
        assert_eq!(response.protocol, "HTTP/1.1".to_string());
        assert_eq!(response.status, 503);
        assert_eq!(response.reason, "Backend fetch failed".to_string());
        assert_eq!(response.headers, &[
                   ("Date".to_string(), "Fri, 22 Jul 2016 09:46:02 GMT".to_string()),
                   ("Server".to_string(), "Varnish".to_string())]);
    }

    #[test]
    fn apply_request_header_updates() {
        let builder = RecordBuilder::new(123);

        // logs/varnish20160804-3752-1krgp8j808a493d5e74216e5.vsl
        let builder = apply_all!(builder,
                                 15, SLT_ReqMethod,     "GET";
                                 15, SLT_ReqURL,        "/test_page/abc";
                                 15, SLT_ReqProtocol,   "HTTP/1.1";
                                 15, SLT_ReqHeader,     "Host: 127.0.0.1:1209";
                                 15, SLT_ReqHeader,     "Test: 1";
                                 15, SLT_ReqHeader,     "Test: 2";
                                 15, SLT_ReqHeader,     "Test: 3";
                                 15, SLT_ReqHeader,     "X-Varnish-Data-Source: 1";
                                 15, SLT_ReqHeader,     "X-Varnish-Data-Source: 2";
                                 15, SLT_ReqHeader,     "X-Varnish-Data-Source: 3";
                                 15, SLT_ReqHeader,     "X-Forwarded-For: 127.0.0.1";
                                 15, SLT_ReqHeader,     "X-Varnish-User-Agent-Class: Other";
                                 15, SLT_ReqUnset,      "X-Varnish-Data-Source: 1";
                                 15, SLT_ReqUnset,      "X-Varnish-Data-Source: 2";
                                 15, SLT_ReqUnset,      "X-Varnish-Data-Source: 3";
                                 15, SLT_ReqHeader,     "X-Varnish-Data-Source: Backend";
                                 15, SLT_ReqUnset,      "X-Forwarded-For: 127.0.0.1";
                                 15, SLT_ReqHeader,     "X-Forwarded-For: 127.0.0.1";
                                 15, SLT_ReqUnset,      "X-Varnish-User-Agent-Class: Other";
                                 15, SLT_ReqHeader,     "X-Varnish-User-Agent-Class: Unknown-Bot";
                                 15, SLT_ReqHeader,     "X-Varnish-Client-Device: D";
                                 15, SLT_ReqHeader,     "X-Varnish-Client-Country:";
                                 15, SLT_ReqUnset,      "X-Varnish-Client-Country:";
                                 15, SLT_ReqHeader,     "X-Varnish-Client-Country: Unknown";
                                 15, SLT_ReqHeader,     "X-Varnish-Original-URL: /test_page/abc";
                                 15, SLT_ReqUnset,      "X-Varnish-Result: miss";
                                 15, SLT_ReqHeader,     "X-Varnish-Result: hit_for_pass";
                                 15, SLT_ReqUnset,      "X-Varnish-Decision: Cacheable";
                                 15, SLT_ReqHeader,     "X-Varnish-Decision: Uncacheable-NoCacheClass";
                                 );

        let request = builder.http_request.complete().unwrap().unwrap();
        assert_eq!(request.headers, &[
                   ("Host".to_string(), "127.0.0.1:1209".to_string()),
                   ("Test".to_string(), "1".to_string()),
                   ("Test".to_string(), "2".to_string()),
                   ("Test".to_string(), "3".to_string()),
                   ("X-Varnish-Data-Source".to_string(), "Backend".to_string()),
                   ("X-Forwarded-For".to_string(), "127.0.0.1".to_string()),
                   ("X-Varnish-User-Agent-Class".to_string(), "Unknown-Bot".to_string()),
                   ("X-Varnish-Client-Device".to_string(), "D".to_string()),
                   ("X-Varnish-Client-Country".to_string(), "Unknown".to_string()),
                   ("X-Varnish-Original-URL".to_string(), "/test_page/abc".to_string()),
                   ("X-Varnish-Result".to_string(), "hit_for_pass".to_string()),
                   ("X-Varnish-Decision".to_string(), "Uncacheable-NoCacheClass".to_string()),
        ]);
    }

    #[test]
    fn apply_response_header_updates() {
        let builder = RecordBuilder::new(123);

        // logs/varnish20160804-3752-1krgp8j808a493d5e74216e5.vsl
        let builder = apply_all!(builder,
                                 15, SLT_RespProtocol,   "HTTP/1.1";
                                 15, SLT_RespStatus,     "200";
                                 15, SLT_RespReason,     "OK";
                                 15, SLT_RespHeader,     "Content-Type: text/html; charset=utf-8";
                                 15, SLT_RespHeader,     "Test: 1";
                                 15, SLT_RespHeader,     "Test: 2";
                                 15, SLT_RespHeader,     "Test: 3";
                                 15, SLT_RespUnset,      "Test: 2";
                                 15, SLT_RespHeader,     "Age: 0";
                                 15, SLT_RespHeader,     "Via: 1.1 varnish-v4";
                                 15, SLT_RespUnset,      "x-url: /test_page/abc";
                                 15, SLT_RespUnset,      "Via: 1.1 varnish-v4";
                                 15, SLT_RespHeader,     "Via: 1.1 test-varnish (Varnish)";
                                 15, SLT_RespHeader,     "X-Request-ID: rid-15";
                                 15, SLT_RespUnset,      "X-Varnish: 15";
                                );

        let response = builder.http_response.complete().unwrap().unwrap();
        assert_eq!(response.headers, &[
                   ("Content-Type".to_string(), "text/html; charset=utf-8".to_string()),
                   ("Test".to_string(), "1".to_string()),
                   ("Test".to_string(), "3".to_string()),
                   ("Age".to_string(), "0".to_string()),
                   ("Via".to_string(), "1.1 test-varnish (Varnish)".to_string()),
                   ("X-Request-ID".to_string(), "rid-15".to_string()),
        ]);
    }

    #[test]
    fn apply_backend_request_locking() {
        let builder = RecordBuilder::new(123);

        let builder = apply_all!(builder,
                   123, SLT_BereqMethod,    "GET";
                   123, SLT_BereqURL,       "/foobar";
                   123, SLT_BereqProtocol,  "HTTP/1.1";
                   123, SLT_BereqHeader,    "Host: localhost:8080";
                   123, SLT_BereqHeader,    "User-Agent: curl/7.40.0";
                   123, SLT_BereqHeader,    "Accept-Encoding: gzip";
                   123, SLT_BereqUnset,     "Accept-Encoding: gzip";
                   123, SLT_BerespProtocol, "HTTP/1.1";
                   123, SLT_BerespStatus,   "503";
                   123, SLT_BerespReason,   "Service Unavailable";
                   123, SLT_BerespReason,   "Backend fetch failed";
                   123, SLT_BerespHeader,   "Date: Fri, 22 Jul 2016 09:46:02 GMT";
                   123, SLT_VCL_call,       "BACKEND_RESPONSE";

                   // try tp change headers after request (which can be done form VCL)
                   123, SLT_BereqMethod,    "POST";
                   123, SLT_BereqURL,       "/quix";
                   123, SLT_BereqProtocol,  "HTTP/2.0";
                   123, SLT_BereqHeader,    "Host: foobar:666";
                   123, SLT_BereqHeader,    "Baz: bar";
                   );

        let requests = builder.http_request.as_ref().unwrap();
        assert_eq!(requests.method, "GET".to_string());
        assert_eq!(requests.url, "/foobar".to_string());
        assert_eq!(requests.protocol, "HTTP/1.1".to_string());
        assert_eq!(requests.headers, &[
                   ("Host".to_string(), "localhost:8080".to_string()),
                   ("User-Agent".to_string(), "curl/7.40.0".to_string())]);
    }

    #[test]
    fn apply_backend_request_non_utf8() {
        let builder = RecordBuilder::new(123);

        let builder = apply_all!(builder,
                   123, SLT_BereqMethod,    "GET";
                   );

        let result = builder.apply(&VslRecord {
            tag: SLT_BereqURL,
            marker: 0,
            ident: 123,
            data: &[0, 159, 146, 150]
        });
        let builder = result.unwrap().unwrap_building();

        let builder = apply_all!(builder,
                   123, SLT_BereqProtocol, "HTTP/1.1";
                   );

        let result = builder.apply(&VslRecord {
            tag: SLT_BereqHeader,
            marker: 0,
            ident: 123,
            data: &[72, 111, 115, 116, 58, 32, 0, 159, 146, 150]
        });
        let builder = result.unwrap().unwrap_building();

        let builder = apply_all!(builder,
                   123, SLT_BerespProtocol, "HTTP/1.1";
                   123, SLT_BerespStatus,   "503";
                   123, SLT_BerespReason,   "Service Unavailable";
                   123, SLT_VCL_call,       "BACKEND_RESPONSE";
                   );

        let requests = builder.http_request.as_ref().unwrap();
        assert_eq!(requests.url, "\u{0}\u{fffd}\u{fffd}\u{fffd}".to_string());
        assert_eq!(requests.headers, vec![
                   ("Host".to_string(), "\u{0}\u{fffd}\u{fffd}\u{fffd}".to_string())
        ]);
    }

    #[test]
    fn apply_client_access_record_timing() {
        let builder = RecordBuilder::new(123);

        let builder = apply_all!(builder,
                                 7, SLT_Begin,        "req 6 rxreq";
                                 7, SLT_Timestamp,    "Start: 1470403413.664824 0.000000 0.000000";
                                 7, SLT_Timestamp,    "Req: 1470403414.664824 1.000000 1.000000";
                                 7, SLT_ReqStart,     "127.0.0.1 39798";
                                 7, SLT_ReqMethod,    "GET";
                                 7, SLT_ReqURL,       "/retry";
                                 7, SLT_ReqProtocol,  "HTTP/1.1";
                                 7, SLT_ReqHeader,    "Date: Fri, 05 Aug 2016 13:23:34 GMT";
                                 7, SLT_VCL_call,     "RECV";
                                 7, SLT_Link,         "bereq 8 fetch";
                                 7, SLT_Timestamp,    "Fetch: 1470403414.672315 1.007491 0.007491";
                                 7, SLT_RespProtocol, "HTTP/1.1";
                                 7, SLT_RespStatus,   "200";
                                 7, SLT_RespReason,   "OK";
                                 7, SLT_RespHeader,   "Content-Type: image/jpeg";
                                 7, SLT_VCL_return,   "deliver";
                                 7, SLT_Timestamp,    "Process: 1470403414.672425 1.007601 0.000111";
                                 7, SLT_RespHeader,   "Accept-Ranges: bytes";
                                 7, SLT_Debug,        "RES_MODE 2";
                                 7, SLT_RespHeader,   "Connection: keep-alive";
                                 7, SLT_Timestamp,    "Resp: 1470403414.672458 1.007634 0.000032";
                                 7, SLT_ReqAcct,      "82 0 82 304 6962 7266";
                                 );

        let record = apply_last!(builder, 7, SLT_End, "")
            .unwrap_client_access();

        assert_eq!(record.start, 1470403413.664824);
        assert_eq!(record.parse, Some(1.0));
        assert_eq!(record.fetch, Some(0.007491));
        assert_eq!(record.ttfb, Some(1.007601));
        assert_eq!(record.serve, Some(1.007634));
        assert_eq!(record.end, 1470403414.672458);
    }

    #[test]
    fn apply_backend_access_record_timing_retry() {
        let builder = RecordBuilder::new(123);

        let builder = apply_all!(builder,
                                 32769, SLT_Begin,            "bereq 8 retry";
                                 32769, SLT_Timestamp,        "Start: 1470403414.669375 0.004452 0.000000";
                                 32769, SLT_BereqMethod,      "GET";
                                 32769, SLT_BereqURL,         "/iss/v2/thumbnails/foo/4006450256177f4a/bar.jpg";
                                 32769, SLT_BereqProtocol,    "HTTP/1.1";
                                 32769, SLT_BereqHeader,      "Date: Fri, 05 Aug 2016 13:23:34 GMT";
                                 32769, SLT_BereqHeader,      "Host: 127.0.0.1:1200";
                                 32769, SLT_VCL_return,       "fetch";
                                 32769, SLT_Timestamp,        "Bereq: 1470403414.669471 0.004549 0.000096";
                                 32769, SLT_Timestamp,        "Beresp: 1470403414.672184 0.007262 0.002713";
                                 32769, SLT_BerespProtocol,   "HTTP/1.1";
                                 32769, SLT_BerespStatus,     "200";
                                 32769, SLT_BerespReason,     "OK";
                                 32769, SLT_BerespHeader,     "Content-Type: image/jpeg";
                                 32769, SLT_VCL_call,         "BACKEND_RESPONSE";
                                 32769, SLT_BackendReuse,     "19 boot.iss";
                                 32769, SLT_Timestamp,        "Retry: 1470403414.672290 0.007367 0.000105";
                                 32769, SLT_Link,             "bereq 32769 retry";
                                 );

       let record = apply_last!(builder, 32769, SLT_End, "")
           .unwrap_backend_access();

       assert_eq!(record.start, 1470403414.669375);
       assert_eq!(record.send, Some(0.004549));
       assert_eq!(record.ttfb, Some(0.007262));
       assert_eq!(record.wait, Some(0.002713));
       assert_eq!(record.fetch, Some(0.007367));
       assert_eq!(record.end, Some(1470403414.672290));
   }

    #[test]
    fn apply_backend_access_record_timing() {
        let builder = RecordBuilder::new(123);

        let builder = apply_all!(builder,
                                 32769, SLT_Begin,            "bereq 8 retry";
                                 32769, SLT_Timestamp,        "Start: 1470403414.669375 0.004452 0.000000";
                                 32769, SLT_BereqMethod,      "GET";
                                 32769, SLT_BereqURL,         "/iss/v2/thumbnails/foo/4006450256177f4a/bar.jpg";
                                 32769, SLT_BereqProtocol,    "HTTP/1.1";
                                 32769, SLT_BereqHeader,      "Date: Fri, 05 Aug 2016 13:23:34 GMT";
                                 32769, SLT_BereqHeader,      "Host: 127.0.0.1:1200";
                                 32769, SLT_VCL_return,       "fetch";
                                 32769, SLT_Timestamp,        "Bereq: 1470403414.669471 0.004549 0.000096";
                                 32769, SLT_Timestamp,        "Beresp: 1470403414.672184 0.007262 0.002713";
                                 32769, SLT_BerespProtocol,   "HTTP/1.1";
                                 32769, SLT_BerespStatus,     "200";
                                 32769, SLT_BerespReason,     "OK";
                                 32769, SLT_BerespHeader,     "Content-Type: image/jpeg";
                                 32769, SLT_VCL_call,         "BACKEND_RESPONSE";
                                 32769, SLT_BackendReuse,     "19 boot.iss";
                                 32769, SLT_Timestamp,        "BerespBody: 1470403414.672290 0.007367 0.000105";
                                 32769, SLT_Length,           "6962";
                                 32769, SLT_BereqAcct,        "1021 0 1021 608 6962 7570";
                                 );

       let record = apply_last!(builder, 32769, SLT_End, "")
           .unwrap_backend_access();

       assert_eq!(record.start, 1470403414.669375);
       assert_eq!(record.send, Some(0.004549));
       assert_eq!(record.ttfb, Some(0.007262));
       assert_eq!(record.wait, Some(0.002713));
       assert_eq!(record.fetch, Some(0.007367));
       assert_eq!(record.end, Some(1470403414.672290));
   }

    #[test]
    fn apply_backend_access_record_timing_error() {
        let builder = RecordBuilder::new(123);

        let builder = apply_all!(builder,
                                 32769, SLT_Begin,            "bereq 8 retry";
                                 32769, SLT_Timestamp,        "Start: 1470304835.059425 0.000000 0.000000";
                                 32769, SLT_BereqMethod,      "GET";
                                 32769, SLT_BereqURL,         "/iss/v2/thumbnails/foo/4006450256177f4a/bar.jpg";
                                 32769, SLT_BereqProtocol,    "HTTP/1.1";
                                 32769, SLT_BereqHeader,      "Date: Fri, 05 Aug 2016 13:23:34 GMT";
                                 32769, SLT_BereqHeader,      "Host: 127.0.0.1:1200";
                                 32769, SLT_VCL_return,       "fetch";
                                 32769, SLT_Timestamp,        "Beresp: 1470304835.059475 0.000050 0.000050";
                                 32769, SLT_Timestamp,        "Error: 1470304835.059479 0.000054 0.000004";
                                 32769, SLT_BerespProtocol,   "HTTP/1.1";
                                 32769, SLT_BerespStatus,     "503";
                                 32769, SLT_BerespReason,     "Service Unavailable";
                                 32769, SLT_BerespReason,     "Backend fetch failed";
                                 32769, SLT_BerespHeader,     "Content-Type: image/jpeg";
                                 32769, SLT_VCL_call,         "BACKEND_ERROR";
                                 32769, SLT_Length,           "6962";
                                 );

       let record = apply_last!(builder, 32769, SLT_End, "")
           .unwrap_backend_access();

       assert_eq!(record.start, 1470304835.059425);
       assert_eq!(record.send, None);
       assert_eq!(record.ttfb, None);
       assert_eq!(record.wait, None);
       assert_eq!(record.fetch, Some(0.000054));
       assert_eq!(record.end, Some(1470304835.059479));
   }

    #[test]
    fn apply_client_access_record_log() {
        let builder = RecordBuilder::new(123);

        let builder = apply_all!(builder,
                                 7, SLT_Begin,        "req 6 rxreq";
                                 7, SLT_Timestamp,    "Start: 1470403413.664824 0.000000 0.000000";
                                 7, SLT_Timestamp,    "Req: 1470403414.664824 1.000000 1.000000";
                                 7, SLT_ReqStart,     "127.0.0.1 39798";
                                 7, SLT_ReqMethod,    "GET";
                                 7, SLT_ReqURL,       "/retry";
                                 7, SLT_ReqProtocol,  "HTTP/1.1";
                                 7, SLT_ReqHeader,    "Date: Fri, 05 Aug 2016 13:23:34 GMT";
                                 7, SLT_VCL_call,     "RECV";
                                 7, SLT_Debug,        "geoip2.lookup: No entry for this IP address (127.0.0.1)";
                                 7, SLT_VCL_Log,      "X-Varnish-Privileged-Client: false";
                                 7, SLT_Link,         "bereq 8 fetch";
                                 7, SLT_Timestamp,    "Fetch: 1470403414.672315 1.007491 0.007491";
                                 7, SLT_RespProtocol, "HTTP/1.1";
                                 7, SLT_RespStatus,   "200";
                                 7, SLT_RespReason,   "OK";
                                 7, SLT_RespHeader,   "Content-Type: image/jpeg";
                                 7, SLT_VCL_return,   "deliver";
                                 7, SLT_VCL_Log,      "X-Varnish-User-Agent-Class: Unknown-Bot";
                                 7, SLT_Timestamp,    "Process: 1470403414.672425 1.007601 0.000111";
                                 7, SLT_RespHeader,   "Accept-Ranges: bytes";
                                 7, SLT_RespHeader,   "Connection: keep-alive";
                                 7, SLT_VCL_Log,      "X-Varnish-Force-Failure: false";
                                 7, SLT_Debug,        "RES_MODE 2";
                                 7, SLT_Timestamp,    "Resp: 1470403414.672458 1.007634 0.000032";
                                 7, SLT_Error,        "oh no!";
                                 7, SLT_LostHeader,   "SetCookie: foo=bar";
                                 7, SLT_ReqAcct,      "82 0 82 304 6962 7266";
                                 );

         let record = apply_last!(builder, 7, SLT_End, "")
             .unwrap_client_access();

         assert_eq!(record.log, &[
                    LogEntry::Debug("geoip2.lookup: No entry for this IP address (127.0.0.1)".to_string()),
                    LogEntry::VCL("X-Varnish-Privileged-Client: false".to_string()),
                    LogEntry::VCL("X-Varnish-User-Agent-Class: Unknown-Bot".to_string()),
                    LogEntry::VCL("X-Varnish-Force-Failure: false".to_string()),
                    LogEntry::Debug("RES_MODE 2".to_string()),
                    LogEntry::Error("oh no!".to_string()),
                    LogEntry::Warning("Failed HTTP header operation due to resource exhaustion or configured limits; header was: SetCookie: foo=bar".to_string()),
         ]);
    }

    #[test]
    fn apply_backend_access_record_log() {
        let builder = RecordBuilder::new(123);

        let builder = apply_all!(builder,
                                 32769, SLT_Begin,            "bereq 8 retry";
                                 32769, SLT_Timestamp,        "Start: 1470403414.669375 0.004452 0.000000";
                                 32769, SLT_BereqMethod,      "GET";
                                 32769, SLT_BereqURL,         "/iss/v2/thumbnails/foo/4006450256177f4a/bar.jpg";
                                 32769, SLT_BereqProtocol,    "HTTP/1.1";
                                 32769, SLT_BereqHeader,      "Date: Fri, 05 Aug 2016 13:23:34 GMT";
                                 32769, SLT_VCL_Log,          "X-Varnish-Privileged-Client: false";
                                 32769, SLT_BereqHeader,      "Host: 127.0.0.1:1200";
                                 32769, SLT_Debug,            "RES_MODE 2";
                                 32769, SLT_VCL_Log,          "X-Varnish-User-Agent-Class: Unknown-Bot";
                                 32769, SLT_VCL_return,       "fetch";
                                 32769, SLT_FetchError,       "no backend connection";
                                 32769, SLT_Timestamp,        "Bereq: 1470403414.669471 0.004549 0.000096";
                                 32769, SLT_Timestamp,        "Beresp: 1470403414.672184 0.007262 0.002713";
                                 32769, SLT_BerespProtocol,   "HTTP/1.1";
                                 32769, SLT_BerespStatus,     "503";
                                 32769, SLT_BerespReason,     "Service Unavailable";
                                 32769, SLT_BerespReason,     "Backend fetch failed";
                                 32769, SLT_BerespHeader,     "Content-Type: image/jpeg";
                                 32769, SLT_BogoHeader,       "foobar!";
                                 32769, SLT_VCL_call,         "BACKEND_ERROR";
                                 32769, SLT_Length,           "6962";
                                 32769, SLT_BereqAcct,        "1021 0 1021 608 6962 7570";
                                 );

       let record = apply_last!(builder, 32769, SLT_End, "")
           .unwrap_backend_access();

       assert_eq!(record.log, &[
                  LogEntry::VCL("X-Varnish-Privileged-Client: false".to_string()),
                  LogEntry::Debug("RES_MODE 2".to_string()),
                  LogEntry::VCL("X-Varnish-User-Agent-Class: Unknown-Bot".to_string()),
                  LogEntry::FetchError("no backend connection".to_string()),
                  LogEntry::Warning("Bogus HTTP header received: foobar!".to_string()),
       ]);
    }

    #[test]
    fn apply_client_access_record_byte_counts() {
        let builder = RecordBuilder::new(123);

        let builder = apply_all!(builder,
                                 7, SLT_Begin,        "req 6 rxreq";
                                 7, SLT_Timestamp,    "Start: 1470403413.664824 0.000000 0.000000";
                                 7, SLT_Timestamp,    "Req: 1470403414.664824 1.000000 1.000000";
                                 7, SLT_ReqStart,     "127.0.0.1 39798";
                                 7, SLT_ReqMethod,    "GET";
                                 7, SLT_ReqURL,       "/retry";
                                 7, SLT_ReqProtocol,  "HTTP/1.1";
                                 7, SLT_ReqHeader,    "Date: Fri, 05 Aug 2016 13:23:34 GMT";
                                 7, SLT_VCL_call,     "RECV";
                                 7, SLT_Link,         "bereq 8 fetch";
                                 7, SLT_Timestamp,    "Fetch: 1470403414.672315 1.007491 0.007491";
                                 7, SLT_RespProtocol, "HTTP/1.1";
                                 7, SLT_RespStatus,   "200";
                                 7, SLT_RespReason,   "OK";
                                 7, SLT_RespHeader,   "Content-Type: image/jpeg";
                                 7, SLT_VCL_return,   "deliver";
                                 7, SLT_Timestamp,    "Process: 1470403414.672425 1.007601 0.000111";
                                 7, SLT_RespHeader,   "Accept-Ranges: bytes";
                                 7, SLT_Debug,        "RES_MODE 2";
                                 7, SLT_RespHeader,   "Connection: keep-alive";
                                 7, SLT_Timestamp,    "Resp: 1470403414.672458 1.007634 0.000032";
                                 7, SLT_ReqAcct,      "82 2 84 304 6962 7266";
                                 );

        let record = apply_last!(builder, 7, SLT_End, "")
            .unwrap_client_access();

        assert_eq!(record.recv_header, 82);
        assert_eq!(record.recv_body, 2);
        assert_eq!(record.recv_total, 84);
        assert_eq!(record.sent_header, 304);
        assert_eq!(record.sent_body, 6962);
        assert_eq!(record.sent_total, 7266);
    }

    #[test]
    fn apply_backend_access_record_cache_object() {
        let builder = RecordBuilder::new(123);

        let builder = apply_all!(builder,
                                 32769, SLT_Begin,            "bereq 8 retry";
                                 32769, SLT_Timestamp,        "Start: 1470403414.669375 0.004452 0.000000";
                                 32769, SLT_BereqMethod,      "GET";
                                 32769, SLT_BereqURL,         "/iss/v2/thumbnails/foo/4006450256177f4a/bar.jpg";
                                 32769, SLT_BereqProtocol,    "HTTP/1.1";
                                 32769, SLT_BereqHeader,      "Date: Fri, 05 Aug 2016 13:23:34 GMT";
                                 32769, SLT_BereqHeader,      "Host: 127.0.0.1:1200";
                                 32769, SLT_VCL_return,       "fetch";
                                 32769, SLT_Timestamp,        "Bereq: 1470403414.669471 0.004549 0.000096";
                                 32769, SLT_Timestamp,        "Beresp: 1470403414.672184 0.007262 0.002713";
                                 32769, SLT_BerespProtocol,   "HTTP/1.1";
                                 32769, SLT_BerespStatus,     "200";
                                 32769, SLT_BerespReason,     "OK";
                                 32769, SLT_BerespHeader,     "Content-Type: image/jpeg";
                                 32769, SLT_VCL_call,         "BACKEND_RESPONSE";
                                 32769, SLT_BackendReuse,     "19 boot.iss";
                                 32769, SLT_ObjProtocol,      "HTTP/1.1";
                                 32769, SLT_ObjStatus,        "200";
                                 32769, SLT_ObjReason,        "OK";
                                 32769, SLT_ObjHeader,        "Content-Type: text/html; charset=utf-8";
                                 32769, SLT_ObjHeader,        "X-Aspnet-Version: 4.0.30319";
                                 32769, SLT_Fetch_Body,       "3 length stream";
                                 32769, SLT_Timestamp,        "BerespBody: 1470403414.672290 0.007367 0.000105";
                                 32769, SLT_Length,           "6962";
                                 32769, SLT_BereqAcct,        "1021 0 1021 608 6962 7570";
                                 );

       let record = apply_last!(builder, 32769, SLT_End, "")
           .unwrap_backend_access();

       let cache_object = record.cache_object.unwrap();
       assert_eq!(cache_object.fetch_mode, "length".to_string());
       assert_eq!(cache_object.fetch_streamed, true);

       let response = cache_object.response;
       assert_eq!(response.protocol, "HTTP/1.1".to_string());
       assert_eq!(response.status, 200);
       assert_eq!(response.reason, "OK".to_string());
       assert_eq!(response.headers, &[
                  ("Content-Type".to_string(), "text/html; charset=utf-8".to_string()),
                  ("X-Aspnet-Version".to_string(), "4.0.30319".to_string())]);
   }
}
