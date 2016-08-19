/// TODO:
/// * rename _request(s) to _access_record(s)
/// * ESI level
/// * result?: pipe, hit, miss, pass, synth
/// * Call trace
/// * ACL trace
/// * more tests
/// * backend info
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
/// Record types:
/// ---
/// * Client access transaction
///   * full
///   * restarted (logs-new/varnish20160816-4093-c0f5tz5609f5ab778e4a4eb.vsl)
///     * has SLT_VCL_return with restart [trigger]
///     * has SLT_Link with restart
///     * has SLT_Timestamp with Restart
///     * won't have response
///     * won't have certain timing info
///     * won't have accounting
///   * piped (logs-new/varnish20160816-4093-s54h6nb4b44b69f1b2c7ca2.vsl)
///     * won't have response
///     * will have special timing info (Pipe, PipeSess)
///     * will have special accounting (SLT_PipeAcct)
///   * ESI
///     * no processing time
///     * otherwise quite normal but linked
///
/// * Backend access transaction
///   * full
///   * aborted
///     * won't have response
///     * won't have end timestamp
///   * retried
///   * piped (logs-new/varnish20160816-4093-s54h6nb4b44b69f1b2c7ca2.vsl)
///     * won't have response
///     * will have special timing info
///     * won't have end timestamp
///
/// Timestamps
/// ===
///
/// Req (logs/varnish20160805-3559-f6sifo45103025c06abad14.vsl):
/// ---
/// * process (req_process) - Start to Req
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
/// Note that we may not have process time as backend request can be aborted in vcl_backend_fetch.
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
pub enum Link<T> {
    Unresolved(VslIdent),
    Resolved(Box<T>),
}

/// All Duration fields are in seconds (floating point values rounded to micro second precision)
#[derive(Debug, Clone, PartialEq)]
pub struct ClientAccessRecord {
    pub ident: VslIdent,
    pub parent: VslIdent,
    pub reason: String,
    pub transaction: ClientAccessTransaction,
    /// Start of request processing
    pub start: TimeStamp,
    /// End of request processing
    pub end: TimeStamp,
    pub log: Vec<LogEntry>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ClientAccessTransaction {
    Full {
        request: HttpRequest,
        response: HttpResponse,
        esi_requests: Vec<Link<ClientAccessRecord>>,
        backend_request: Option<Link<BackendAccessRecord>>,
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
    Restarted {
        request: HttpRequest,
        /// Time it took to process request; None for ESI subrequests as they have this done already
        process: Option<Duration>,
        restart_request: Link<ClientAccessRecord>,
    },
    Piped {
        request: HttpRequest,
        backend_request: Link<BackendAccessRecord>,
        /// Time it took to process request; None for ESI subrequests as they have this done already
        process: Option<Duration>,
        /// Time it took to get first byte of response
        ttfb: Duration,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct CacheObject {
    /// Type ("malloc", "file", "persistent" etc.)
    storage_type: String,
    /// Name of storage backend
    storage_name: String,
    /// TTL; None if unset
    ttl: Option<Duration>,
    /// Grace; None if unset
    grace: Option<Duration>,
    /// Keep; None if unset
    keep: Option<Duration>,
    /// Reference time for TTL
    since: TimeStamp,
    /// Reference time for object lifetime (now - Age)
    origin: TimeStamp,
    /// Text description of body fetch mode
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
    pub transaction: BackendAccessTransaction,
    /// Start of backend request processing
    pub start: TimeStamp,
    /// End of response processing; None for aborted or piped response
    pub end: Option<TimeStamp>,
    pub log: Vec<LogEntry>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BackendAccessTransaction {
    Full {
        request: HttpRequest,
        response: HttpResponse,
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
    },
    Failed {
        request: HttpRequest,
        synth_response: HttpResponse,
        retry_request: Option<Link<BackendAccessRecord>>,
        /// Total duration it took to synthesise response
        synth: Duration,
    },
    /// Aborted before we have made a backend request
    Aborted {
        request: HttpRequest,
    },
    /// Varnish got the backend response but it did not like it: abort or retry
    Abandoned {
        request: HttpRequest,
        response: HttpResponse,
        retry_request: Option<Link<BackendAccessRecord>>,
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
    },
}

pub type Address = (String, Port);

#[derive(Debug, Clone, PartialEq)]
pub struct SessionRecord {
    pub ident: VslIdent,
    pub open: TimeStamp,
    pub duration: Duration,
    pub local: Option<Address>,
    pub remote: Address,
    pub client_requests: Vec<Link<ClientAccessRecord>>,
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

// Access helpers

impl<T> Link<T> {
    #[allow(dead_code)]
    pub fn is_unresolved(&self) -> bool {
        match self {
            &Link::Unresolved(_) => true,
            _ => false
        }
    }
    #[allow(dead_code)]
    pub fn unwrap_unresolved(self) -> VslIdent {
        match self {
            Link::Unresolved(ident) => ident,
            _ => panic!("unwrap_unresolved called on Link that was not Unresolved")
        }
    }
    #[allow(dead_code)]
    pub fn get_unresolved(&self) -> Option<VslIdent> {
        match self {
            &Link::Unresolved(ident) => Some(ident),
            _ => None
        }
    }

    #[allow(dead_code)]
    pub fn is_resolved(&self) -> bool {
        match self {
            &Link::Resolved(_) => true,
            _ => false
        }
    }
    #[allow(dead_code)]
    pub fn unwrap_resolved(self) -> Box<T> {
        match self {
            Link::Resolved(t) => t,
            _ => panic!("unwrap_resolved called on Link that was not Resolved")
        }
    }
    #[allow(dead_code)]
    pub fn get_resolved(&self) -> Option<&T> {
        match self {
            &Link::Resolved(ref t) => Some(t.as_ref()),
            _ => None
        }
    }
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
    pub fn unwrap_client_access(self) -> ClientAccessRecord {
        match self {
            Record::ClientAccess(access_record) => access_record,
            _ => panic!("unwrap_client_access called on Record that was not ClientAccess")
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
    pub fn unwrap_backend_access(self) -> BackendAccessRecord {
        match self {
            Record::BackendAccess(access_record) => access_record,
            _ => panic!("unwrap_backend_access called on Record that was not BackendAccess")
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
        UnexpectedTransition(transition: &'static str, state: Box<Debug>) {
            display("Unexpected transition '{}' while building record: {:?}", transition, state)
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
            &Building(ref buidling) => Building(buidling),
            &Complete(ref complete) => Complete(complete),
        }
    }

    #[allow(dead_code)]
    fn unwrap(self) -> C where B: Debug {
        match self {
            Building(buidling) => panic!("Trying to unwrap BuilderResult::Building: {:?}", buidling),
            Complete(complete) => complete,
        }
    }

    #[allow(dead_code)]
    fn unwrap_building(self) -> B where C: Debug {
        match self {
            Building(buidling) => buidling,
            Complete(complete) => panic!("Trying to unwrap BuilderResult::Complete: {:?}", complete),
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

// Note: we need to use bytes here since we need to be 1 to 1 comparable with original byte value
#[derive(Debug)]
struct HeadersBuilder {
    headers: Vec<(MaybeString, MaybeString)>,
}

impl HeadersBuilder {
    fn new() -> HeadersBuilder {
        HeadersBuilder {
            headers: Vec::new()
        }
    }

    fn set(self, name: MaybeString, value: MaybeString) -> HeadersBuilder {
        let mut headers = self.headers;
        headers.push((name, value));

        HeadersBuilder {
            headers: headers,
            .. self
        }
    }

    fn unset(self, name: &MaybeStr, value: &MaybeStr) -> HeadersBuilder {
        let mut headers = self.headers;
        headers.retain(|header| {
            let &(ref t_name, ref t_value) = header;
            (t_name.as_maybe_str(), t_value.as_maybe_str()) != (name, value)
        });

        HeadersBuilder {
            headers: headers,
            .. self
        }
    }

    fn unwrap(self) -> Vec<(MaybeString, MaybeString)> {
        self.headers
    }
}

#[derive(Debug)]
struct HttpRequestBuilder {
    protocol: Option<String>,
    method: Option<String>,
    url: Option<String>,
    headers: HeadersBuilder,
}

impl HttpRequestBuilder {
    fn new() -> HttpRequestBuilder {
        HttpRequestBuilder {
            protocol: None,
            method: None,
            url: None,
            headers: HeadersBuilder::new(),
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
                    HttpRequestBuilder {
                        headers: self.headers.set(name.to_maybe_string(), value.to_maybe_string()),
                        .. self
                    }
                } else {
                    debug!("Not setting empty request header: {:?}", vsl);
                    self
                }
            }
            SLT_BereqUnset | SLT_ReqUnset => {
                if let (name, Some(value)) = try!(vsl.parse_data(slt_header)) {
                    HttpRequestBuilder {
                        headers: self.headers.unset(&name, &value),
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
            headers: self.headers.unwrap().into_iter()
                .map(|(name, value)| (name.to_lossy_string(), value.to_lossy_string()))
                .collect(),
        })
    }
}

#[derive(Debug)]
struct HttpResponseBuilder {
    protocol: Option<String>,
    status: Option<Status>,
    reason: Option<String>,
    headers: HeadersBuilder,
}

impl HttpResponseBuilder {
    fn new() -> HttpResponseBuilder {
        HttpResponseBuilder {
            protocol: None,
            status: None,
            reason: None,
            headers: HeadersBuilder::new(),
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
                    HttpResponseBuilder {
                        headers: self.headers.set(name.to_maybe_string(), value.to_maybe_string()),
                        .. self
                    }
                } else {
                    debug!("Not setting empty response header: {:?}", vsl);
                    self
                }
            }
            SLT_BerespUnset | SLT_RespUnset | SLT_ObjUnset => {
                if let (name, Some(value)) = try!(vsl.parse_data(slt_header)) {
                    HttpResponseBuilder {
                        headers: self.headers.unset(&name, &value),
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
            headers: self.headers.unwrap().into_iter()
                .map(|(name, value)| (name.to_lossy_string(), value.to_lossy_string()))
                .collect(),
        })
    }
}

#[derive(Debug)]
enum ClientAccessTransactionType {
    Full,
    Restarted,
    Piped,
}

#[derive(Debug)]
enum BackendAccessTransactionType {
    Full,
    Failed,
    Aborted,
    Abandoned,
    Piped,
}

#[derive(Debug)]
enum RecordType {
    Undefined,
    ClientAccess {
        parent: VslIdent,
        reason: String,
        transaction: ClientAccessTransactionType,
    },
    BackendAccess {
        parent: VslIdent,
        reason: String,
        transaction: BackendAccessTransactionType,
    },
    Session
}

impl RecordType {
    // hide this type behind Debug trait for error details
    fn into_debug(self) -> Box<Debug> {
        Box::new(self)
    }
}

#[derive(Debug)]
struct ObjStorage {
    stype: String,
    name: String,
}

#[derive(Debug)]
struct ObjTtl {
    ttl: Option<Duration>,
    grace: Option<Duration>,
    keep: Option<Duration>,
    since: TimeStamp,
    origin: Option<TimeStamp>,
}

#[derive(Debug)]
pub struct FetchBody {
    pub mode: String,
    pub streamed: bool,
}

#[derive(Debug)]
pub struct RecordBuilder {
    ident: VslIdent,
    record_type: RecordType,
    req_start: Option<TimeStamp>,
    pipe_start: Option<TimeStamp>,
    http_request: BuilderResult<HttpRequestBuilder, HttpRequest>,
    http_response: BuilderResult<HttpResponseBuilder, HttpResponse>,
    cache_object: BuilderResult<HttpResponseBuilder, HttpResponse>,
    obj_storage: Option<ObjStorage>,
    obj_ttl: Option<ObjTtl>,
    fetch_body: Option<FetchBody>,
    resp_fetch: Option<Duration>,
    req_process: Option<Duration>,
    resp_ttfb: Option<Duration>,
    req_took: Option<Duration>,
    resp_end: Option<TimeStamp>,
    accounting: Option<Accounting>,
    sess_open: Option<TimeStamp>,
    sess_duration: Option<Duration>,
    sess_remote: Option<Address>,
    sess_local: Option<Address>,
    client_requests: Vec<Link<ClientAccessRecord>>,
    backend_request: Option<Link<BackendAccessRecord>>,
    restart_request: Option<Link<ClientAccessRecord>>,
    retry_request: Option<Link<BackendAccessRecord>>,
    log: Vec<LogEntry>,
}

impl RecordBuilder {
    pub fn new(ident: VslIdent) -> RecordBuilder {
        RecordBuilder {
            ident: ident,
            record_type: RecordType::Undefined,
            req_start: None,
            pipe_start: None,
            http_request: Building(HttpRequestBuilder::new()),
            http_response: Building(HttpResponseBuilder::new()),
            cache_object: Building(HttpResponseBuilder::new()),
            obj_storage: None,
            obj_ttl: None,
            fetch_body: None,
            req_process: None,
            resp_fetch: None,
            resp_ttfb: None,
            req_took: None,
            resp_end: None,
            accounting: None,
            sess_open: None,
            sess_duration: None,
            sess_remote: None,
            sess_local: None,
            client_requests: Vec::new(),
            backend_request: None,
            restart_request: None,
            retry_request: None,
            log: Vec::new(),
        }
    }

    pub fn apply<'r>(self, vsl: &'r VslRecord) -> Result<BuilderResult<RecordBuilder, Record>, RecordBuilderError> {
        let builder = match vsl.tag {
            SLT_Begin => {
                let (record_type, vxid, reason) = try!(vsl.parse_data(slt_begin));
                if let RecordType::Undefined = self.record_type {
                    match record_type {
                        "bereq" => RecordBuilder {
                            record_type: RecordType::BackendAccess {
                                parent: vxid,
                                reason: reason.to_owned(),
                                transaction: BackendAccessTransactionType::Full,
                            },
                            .. self
                        },
                        "req" => RecordBuilder {
                            record_type: RecordType::ClientAccess {
                                parent: vxid,
                                reason: reason.to_owned(),
                                transaction: ClientAccessTransactionType::Full,
                            },
                            .. self
                        },
                        "sess" => RecordBuilder {
                            record_type: RecordType::Session,
                            .. self
                        },
                        _ => return Err(RecordBuilderError::UnimplementedTransactionType(record_type.to_string()))
                    }
                } else {
                    return Err(RecordBuilderError::UnexpectedTransition("SLT_Begin", self.record_type.into_debug()))
                }
            }
            SLT_Timestamp => {
                let (label, timestamp, since_work_start, since_last_timestamp) =
                    try!(vsl.parse_data(slt_timestamp));

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
                        pipe_start: Some(timestamp),
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
                    "Pipe" => RecordBuilder {
                        resp_ttfb: Some(since_work_start),
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
                    "BerespBody" |
                    "Retry" => RecordBuilder {
                        req_took: Some(since_work_start),
                        resp_end: Some(timestamp),
                        .. self
                    },
                    "PipeSess" => RecordBuilder {
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

                match (reason, child_type) {
                    ("req", "restart") => {
                        if let Some(link) = self.restart_request {
                            warn!("Already have restart client request link with ident {}; replacing with {}", link.unwrap_unresolved(), child_vxid);
                        }
                        RecordBuilder {
                            restart_request: Some(Link::Unresolved(child_vxid)),
                            .. self
                        }
                    },
                    ("req", _) => {
                        let mut client_requests = self.client_requests;
                        client_requests.push(Link::Unresolved(child_vxid));

                        RecordBuilder {
                            client_requests: client_requests,
                            .. self
                        }
                    },
                    ("bereq", "retry") => {
                        if let Some(link) = self.retry_request {
                            warn!("Already have retry backend request link with ident {}; replacing with {}", link.unwrap_unresolved(), child_vxid);
                        }
                        RecordBuilder {
                            retry_request: Some(Link::Unresolved(child_vxid)),
                            .. self
                        }
                    },
                    ("bereq", _) => {
                        if let Some(link) = self.backend_request {
                            warn!("Already have backend request link with ident {}; replacing with {}", link.unwrap_unresolved(), child_vxid);
                        }
                        RecordBuilder {
                            backend_request: Some(Link::Unresolved(child_vxid)),
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
                let log_entry = try!(vsl.parse_data(slt_log));

                let mut log = self.log;
                log.push(LogEntry::VCL(log_entry.to_lossy_string()));

                RecordBuilder {
                    log: log,
                    .. self
                }
            }
            SLT_Debug => {
                let log_entry = try!(vsl.parse_data(slt_log));

                let mut log = self.log;
                log.push(LogEntry::Debug(log_entry.to_lossy_string()));

                RecordBuilder {
                    log: log,
                    .. self
                }
            }
            SLT_Error => {
                let log_entry = try!(vsl.parse_data(slt_log));

                let mut log = self.log;
                log.push(LogEntry::Error(log_entry.to_lossy_string()));

                RecordBuilder {
                    log: log,
                    .. self
                }
            }
            SLT_FetchError => {
                let log_entry = try!(vsl.parse_data(slt_log));

                let mut log = self.log;
                log.push(LogEntry::FetchError(log_entry.to_lossy_string()));

                RecordBuilder {
                    log: log,
                    .. self
                }
            }
            SLT_BogoHeader => {
                let log_entry = try!(vsl.parse_data(slt_log));

                let mut log = self.log;
                log.push(LogEntry::Warning(format!("Bogus HTTP header received: {}", log_entry.to_lossy_string())));

                RecordBuilder {
                    log: log,
                    .. self
                }
            }
            SLT_LostHeader => {
                let log_entry = try!(vsl.parse_data(slt_log));

                let mut log = self.log;
                log.push(LogEntry::Warning(format!("Failed HTTP header operation due to resource exhaustion or configured limits; header was: {}", log_entry.to_lossy_string())));

                RecordBuilder {
                    log: log,
                    .. self
                }
            }

            SLT_Storage => {
                let (storage_type, storage_name) = try!(vsl.parse_data(slt_storage));

                RecordBuilder {
                    obj_storage: Some(ObjStorage {
                        stype: storage_type.to_string(),
                        name: storage_name.to_string(),
                    }),
                    .. self
                }
            }
            SLT_TTL => {
                let (_soruce, ttl, grace, keep, since, rfc) = try!(vsl.parse_data(slt_ttl));

                let origin = match (rfc, self.obj_ttl) {
                    (Some((origin, _date, _expires, _max_age)), _) => Some(origin),
                    (None, Some(obj_ttl)) => obj_ttl.origin,
                    _ => None,
                };

                RecordBuilder {
                    obj_ttl: Some(ObjTtl {
                        ttl: ttl,
                        grace: grace,
                        keep: keep,
                        since: since,
                        origin: origin,
                    }),
                    .. self
                }
            }
            SLT_ReqAcct => {
                let (recv_header, recv_body, recv_total,
                     sent_header, sent_body, sent_total) =
                    try!(vsl.parse_data(slt_reqacc));

                RecordBuilder {
                    accounting: Some(Accounting {
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
                    "BACKEND_RESPONSE" => RecordBuilder {
                        http_request: try!(self.http_request.complete()),
                        http_response: try!(self.http_response.complete()),
                        .. self
                    },
                    "BACKEND_ERROR" => {
                        match self.record_type {
                            RecordType::BackendAccess {
                                parent,
                                reason,
                                transaction: BackendAccessTransactionType::Full,
                            } => {
                                RecordBuilder {
                                    record_type: RecordType::BackendAccess {
                                        parent: parent,
                                        reason: reason,
                                        transaction: BackendAccessTransactionType::Failed,
                                    },
                                    http_request: try!(self.http_request.complete()),
                                    .. self
                                }
                            }
                            _ => return Err(RecordBuilderError::UnexpectedTransition("call BACKEND_ERROR", self.record_type.into_debug()))
                        }
                    },
                    _ => {
                        debug!("Ignoring unknown {:?} method: {}", vsl.tag, method);
                        self
                    }
                }
            }

            SLT_VCL_return => {
                let action = try!(vsl.parse_data(slt_return));

                match action {
                    "restart" => {
                        if let RecordType::ClientAccess {
                            parent,
                            reason,
                            transaction: ClientAccessTransactionType::Full,
                        } = self.record_type {
                            RecordBuilder {
                                record_type: RecordType::ClientAccess {
                                    parent: parent,
                                    reason: reason,
                                    transaction: ClientAccessTransactionType::Restarted,
                                },
                                .. self
                            }
                        } else {
                            return Err(RecordBuilderError::UnexpectedTransition("SLT_VCL_return restart", self.record_type.into_debug()))
                        }
                    },
                    "abandon" => {
                        // eary abandon will have request still Building
                        if let http_request @ Building(_) = self.http_request {
                            if let RecordType::BackendAccess {
                                parent,
                                reason,
                                transaction: BackendAccessTransactionType::Full,
                            } = self.record_type {
                                RecordBuilder {
                                    http_request: try!(http_request.complete()),
                                    record_type: RecordType::BackendAccess {
                                        parent: parent,
                                        reason: reason,
                                        transaction: BackendAccessTransactionType::Aborted,
                                    },
                                    .. self
                                }
                            } else {
                                return Err(RecordBuilderError::UnexpectedTransition("SLT_VCL_return abandon", self.record_type.into_debug()))
                            }
                        } else {
                            if let RecordType::BackendAccess {
                                parent,
                                reason,
                                transaction: BackendAccessTransactionType::Full,
                            } = self.record_type {
                                RecordBuilder {
                                    record_type: RecordType::BackendAccess {
                                        parent: parent,
                                        reason: reason,
                                        transaction: BackendAccessTransactionType::Abandoned,
                                    },
                                    .. self
                                }
                            } else {
                                return Err(RecordBuilderError::UnexpectedTransition("SLT_VCL_return abandon", self.record_type.into_debug()))
                            }
                        }
                    },
                    "retry" => {
                        if let RecordType::BackendAccess {
                            parent,
                            reason,
                            transaction: BackendAccessTransactionType::Full,
                        } = self.record_type {
                            RecordBuilder {
                                record_type: RecordType::BackendAccess {
                                    parent: parent,
                                    reason: reason,
                                    transaction: BackendAccessTransactionType::Abandoned,
                                },
                                .. self
                            }
                        } else {
                            return Err(RecordBuilderError::UnexpectedTransition("SLT_VCL_return retry", self.record_type.into_debug()))
                        }
                    },
                    "pipe" => {
                        match self.record_type {
                            RecordType::ClientAccess {
                                parent,
                                reason,
                                transaction: ClientAccessTransactionType::Full,
                            } => {
                                RecordBuilder {
                                    record_type: RecordType::ClientAccess {
                                        parent: parent,
                                        reason: reason,
                                        transaction: ClientAccessTransactionType::Piped,
                                    },
                                    .. self
                                }
                            }
                            RecordType::BackendAccess {
                                parent,
                                reason,
                                transaction: BackendAccessTransactionType::Full,
                            } => {
                                RecordBuilder {
                                    http_request: try!(self.http_request.complete()),
                                    record_type: RecordType::BackendAccess {
                                        parent: parent,
                                        reason: reason,
                                        transaction: BackendAccessTransactionType::Piped,
                                    },
                                    .. self
                                }
                            }
                            _ => return Err(RecordBuilderError::UnexpectedTransition("SLT_VCL_return pipe", self.record_type.into_debug()))
                        }
                    },
                    _ => {
                        debug!("Ignoring unknown {:?} return: {}", vsl.tag, action);
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
                match self.record_type {
                    RecordType::Undefined => return Err(RecordBuilderError::RecordIncomplete("record type is not known")),
                    RecordType::Session => {
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

                        match self.record_type {
                            RecordType::ClientAccess { parent, reason, transaction } => {
                                let transaction = match transaction {
                                    ClientAccessTransactionType::Full => {
                                        // SLT_End tag is completing the client response
                                        let http_response = try!(self.http_response.complete());

                                        ClientAccessTransaction::Full {
                                            request: request,
                                            response: try!(http_response.get_complete()),
                                            esi_requests: self.client_requests,
                                            backend_request: self.backend_request,
                                            process: self.req_process,
                                            fetch: self.resp_fetch,
                                            ttfb: try!(self.resp_ttfb.ok_or(RecordBuilderError::RecordIncomplete("resp_ttfb"))),
                                            serve: try!(self.req_took.ok_or(RecordBuilderError::RecordIncomplete("req_took"))),
                                            accounting: try!(self.accounting.ok_or(RecordBuilderError::RecordIncomplete("accounting"))),
                                        }
                                    },
                                    ClientAccessTransactionType::Restarted => {
                                        ClientAccessTransaction::Restarted {
                                            request: request,
                                            process: self.req_process,
                                            restart_request: try!(self.restart_request.ok_or(RecordBuilderError::RecordIncomplete("restart_request"))),
                                        }
                                    },
                                    ClientAccessTransactionType::Piped => {
                                        ClientAccessTransaction::Piped {
                                            request: request,
                                            backend_request: try!(self.backend_request.ok_or(RecordBuilderError::RecordIncomplete("backend_request"))),
                                            process: self.req_process,
                                            ttfb: try!(self.resp_ttfb.ok_or(RecordBuilderError::RecordIncomplete("resp_ttfb"))),
                                        }
                                    },
                                };

                                let record = ClientAccessRecord {
                                    ident: self.ident,
                                    parent: parent,
                                    reason: reason,
                                    transaction: transaction,
                                    start: try!(self.req_start.ok_or(RecordBuilderError::RecordIncomplete("req_start"))),
                                    end: try!(self.resp_end.ok_or(RecordBuilderError::RecordIncomplete("resp_end"))),
                                    log: self.log,
                                };

                                return Ok(Complete(Record::ClientAccess(record)))
                            },
                            RecordType::BackendAccess { parent, reason, transaction } => {
                                let transaction = match transaction {
                                    BackendAccessTransactionType::Full => {
                                        let cache_object = try!(self.cache_object.get_complete());

                                        let obj_storage = try!(self.obj_storage.ok_or(RecordBuilderError::RecordIncomplete("obj_storage")));
                                        let obj_ttl = try!(self.obj_ttl.ok_or(RecordBuilderError::RecordIncomplete("obj_ttl")));
                                        let fetch_body = try!(self.fetch_body.ok_or(RecordBuilderError::RecordIncomplete("fetch_body")));

                                        let cache_object = CacheObject {
                                            storage_type: obj_storage.stype,
                                            storage_name: obj_storage.name,
                                            ttl: obj_ttl.ttl,
                                            grace: obj_ttl.grace,
                                            keep: obj_ttl.keep,
                                            since: obj_ttl.since,
                                            origin: obj_ttl.origin.unwrap_or(obj_ttl.since),
                                            fetch_mode: fetch_body.mode,
                                            fetch_streamed: fetch_body.streamed,
                                            response: cache_object,
                                        };

                                        BackendAccessTransaction::Full {
                                            request: request,
                                            response: try!(self.http_response.get_complete()),
                                            cache_object: cache_object,
                                            send: try!(self.req_process.ok_or(RecordBuilderError::RecordIncomplete("req_process"))),
                                            wait: try!(self.resp_fetch.ok_or(RecordBuilderError::RecordIncomplete("resp_fetch"))),
                                            ttfb: try!(self.resp_ttfb.ok_or(RecordBuilderError::RecordIncomplete("resp_ttfb"))),
                                            fetch: try!(self.req_took.ok_or(RecordBuilderError::RecordIncomplete("req_took"))),
                                        }
                                    }
                                    BackendAccessTransactionType::Failed => {
                                        // We complete it here as it is syhth response - not a
                                        // backend response
                                        let http_response = try!(self.http_response.complete());

                                        BackendAccessTransaction::Failed {
                                            request: request,
                                            synth_response: try!(http_response.get_complete()),
                                            retry_request: self.retry_request,
                                            synth: try!(self.req_took.ok_or(RecordBuilderError::RecordIncomplete("req_took"))),
                                        }
                                    }
                                    BackendAccessTransactionType::Aborted => {
                                        BackendAccessTransaction::Aborted {
                                            request: request,
                                        }
                                    }
                                    BackendAccessTransactionType::Abandoned => {
                                        BackendAccessTransaction::Abandoned {
                                            request: request,
                                            response: try!(self.http_response.get_complete()),
                                            retry_request: self.retry_request,
                                            send: try!(self.req_process.ok_or(RecordBuilderError::RecordIncomplete("req_process"))),
                                            wait: try!(self.resp_fetch.ok_or(RecordBuilderError::RecordIncomplete("resp_fetch"))),
                                            ttfb: try!(self.resp_ttfb.ok_or(RecordBuilderError::RecordIncomplete("resp_ttfb"))),
                                            fetch: self.req_took,
                                        }
                                    }
                                    BackendAccessTransactionType::Piped => {
                                        BackendAccessTransaction::Piped {
                                            request: request,
                                        }
                                    }
                                };

                                let start = if let BackendAccessTransaction::Piped { .. } = transaction {
                                    // Note that piped backend requests don't have start timestamp
                                    try!(self.pipe_start.ok_or(RecordBuilderError::RecordIncomplete("pipe_start")))
                                } else {
                                    try!(self.req_start.ok_or(RecordBuilderError::RecordIncomplete("req_start")))
                                };

                                let record = BackendAccessRecord {
                                    ident: self.ident,
                                    parent: parent,
                                    reason: reason,
                                    transaction: transaction,
                                    start: start,
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
        use super::RecordType;
        let builder = RecordBuilder::new(123);

        let builder = builder.apply(&vsl(SLT_Begin, 123, "bereq 321 fetch"))
            .unwrap().unwrap_building();

        assert_matches!(builder.record_type,
            RecordType::BackendAccess { parent: 321, ref reason, .. } if reason == "fetch");
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
    fn apply_begin_unexpected_transition() {
        let builder = RecordBuilder::new(123);

        let builder = builder.apply(&vsl(SLT_Begin, 123, "bereq 231 fetch")).unwrap().unwrap_building();
        let err = builder.apply(&vsl(SLT_Begin, 123, "req 231 fetch")).unwrap_err();

        //println!("{}", &err);
        assert_matches!(err,
            RecordBuilderError::UnexpectedTransition("SLT_Begin", _));
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

    //TODO: _full test
    //TODO: _full HIT test
    //TODO: _full ESI test

    #[test]
    fn apply_client_access_record_full_timing() {
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
        assert_eq!(record.end, 1470403414.672458);

        assert_matches!(record.transaction, ClientAccessTransaction::Full {
            process: Some(1.0),
            fetch: Some(0.007491),
            ttfb: 1.007601,
            serve: 1.007634,
            ..
        });
    }

    #[test]
    fn apply_client_access_restarted() {
        let builder = RecordBuilder::new(123);

        // logs-new/varnish20160816-4093-c0f5tz5609f5ab778e4a4eb.vsl
        let builder = apply_all!(builder,
                                 4, SLT_Begin,          "req 3 rxreq";
                                 4, SLT_Timestamp,      "Start: 1471355414.450311 0.000000 0.000000";
                                 4, SLT_Timestamp,      "Req: 1471355414.450311 0.000000 0.000000";
                                 4, SLT_ReqStart,       "127.0.0.1 47912";
                                 4, SLT_ReqMethod,      "GET";
                                 4, SLT_ReqURL,         "/foo/thumbnails/foo/4006450256177f4a/bar.jpg?type=brochure";
                                 4, SLT_ReqProtocol,    "HTTP/1.1";
                                 4, SLT_ReqHeader,      "Host: 127.0.0.1:1245";
                                 4, SLT_VCL_call,       "RECV ";
                                 4, SLT_VCL_return,     "hash";
                                 4, SLT_VCL_call,       "HASH";
                                 4, SLT_VCL_return,     "lookup";
                                 4, SLT_Hit,            "32771";
                                 4, SLT_VCL_call,       "HIT";
                                 4, SLT_VCL_return,     "restart";
                                 4, SLT_Timestamp,      "Restart: 1471355414.450428 0.000117 0.000117";
                                 4, SLT_Link,           "req 5 restart";
                                );

        let record = apply_last!(builder, 4, SLT_End, "")
            .unwrap_client_access();

        assert_eq!(record.start, 1471355414.450311);
        assert_eq!(record.end, 1471355414.450428);

        assert_matches!(record.transaction, ClientAccessTransaction::Restarted {
            request: HttpRequest {
                ref url,
                ..
            },
            process: Some(0.0),
            restart_request: Link::Unresolved(5),
        } if url == "/foo/thumbnails/foo/4006450256177f4a/bar.jpg?type=brochure");
    }

    #[test]
    fn apply_client_access_piped() {
        let builder = RecordBuilder::new(123);

        // logs-new/varnish20160816-4093-s54h6nb4b44b69f1b2c7ca2.vsl
        let builder = apply_all!(builder,
                                 4, SLT_Begin,          "req 3 rxreq";
                                 4, SLT_Timestamp,      "Start: 1471355444.744141 0.000000 0.000000";
                                 4, SLT_Timestamp,      "Req: 1471355444.744141 0.000000 0.000000";
                                 4, SLT_ReqStart,       "127.0.0.1 59830";
                                 4, SLT_ReqMethod,      "GET";
                                 4, SLT_ReqURL,         "/websocket";
                                 4, SLT_ReqProtocol,    "HTTP/1.1";
                                 4, SLT_ReqHeader,      "Upgrade: websocket";
                                 4, SLT_ReqHeader,      "Connection: Upgrade";
                                 4, SLT_VCL_call,       "RECV";
                                 4, SLT_VCL_return,     "pipe";
                                 4, SLT_VCL_call,       "HASH";
                                 4, SLT_VCL_return,     "lookup";
                                 4, SLT_Link,           "bereq 5 pipe";
                                 4, SLT_ReqHeader,      "X-Varnish-Result: pipe";
                                 4, SLT_Timestamp,      "Pipe: 1471355444.744349 0.000209 0.000209";
                                 4, SLT_Timestamp,      "PipeSess: 1471355444.751368 0.007228 0.007019";
                                 4, SLT_PipeAcct,       "268 761 0 480";
                                );

        let record = apply_last!(builder, 4, SLT_End, "")
            .unwrap_client_access();

        assert_eq!(record.start, 1471355444.744141);
        assert_eq!(record.end, 1471355444.751368);

        assert_matches!(record.transaction, ClientAccessTransaction::Piped {
            request: HttpRequest {
                ref url,
                ref headers,
                ..
            },
            ref backend_request,
            process: Some(0.0),
            ttfb: 0.000209,
        } if
            url == "/websocket" &&
            headers == &[
                ("Upgrade".to_string(), "websocket".to_string()),
                ("Connection".to_string(), "Upgrade".to_string())] &&
            backend_request == &Link::Unresolved(5)
        );
    }

    //TODO: backend access record: Full, Failed, Aborted, Abandoned, Piped

    #[test]
    fn apply_backend_access_record_abandoned() {
        let builder = RecordBuilder::new(123);

        // logs/raw.vsl
        let builder = apply_all!(builder,
                                 5, SLT_Begin,          "bereq 4 fetch";
                                 5, SLT_Timestamp,      "Start: 1471354579.281173 0.000000 0.000000";
                                 5, SLT_BereqMethod,    "GET";
                                 5, SLT_BereqURL,       "/test_page/123.html";
                                 5, SLT_BereqProtocol,  "HTTP/1.1";
                                 5, SLT_BereqHeader,    "Date: Tue, 16 Aug 2016 13:36:19 GMT";
                                 5, SLT_BereqHeader,    "Host: 127.0.0.1:1202";
                                 5, SLT_VCL_call,       "BACKEND_FETCH";
                                 5, SLT_BereqUnset,     "Accept-Encoding: gzip";
                                 5, SLT_BereqHeader,    "Accept-Encoding: gzip";
                                 5, SLT_VCL_return,     "fetch";
                                 5, SLT_BackendOpen,    "19 boot.default 127.0.0.1 42000 127.0.0.1 51058";
                                 5, SLT_BackendStart,   "127.0.0.1 42000";
                                 5, SLT_Timestamp,      "Bereq: 1471354579.281302 0.000128 0.000128";
                                 5, SLT_Timestamp,      "Beresp: 1471354579.288697 0.007524 0.007396";
                                 5, SLT_BerespProtocol, "HTTP/1.1";
                                 5, SLT_BerespStatus,   "500";
                                 5, SLT_BerespReason,   "Internal Server Error";
                                 5, SLT_BerespHeader,   "Content-Type: text/html; charset=utf-8";
                                 5, SLT_TTL,            "RFC -1 10 -1 1471354579 1471354579 1340020138 0 0";
                                 5, SLT_VCL_call,       "BACKEND_RESPONSE";
                                 5, SLT_BerespHeader,   "X-Varnish-Decision: Cacheable";
                                 5, SLT_BerespHeader,   "x-url: /test_page/123.html";
                                 5, SLT_BerespHeader,   "X-Varnish-Content-Length: 9";
                                 5, SLT_BerespHeader,   "X-Varnish-ESI-Parsed: false";
                                 5, SLT_BerespHeader,   "X-Varnish-Compressable: false";
                                 5, SLT_VCL_Log,        "Backend Response Code: 500";
                                 5, SLT_VCL_return,     "abandon";
                                 5, SLT_BackendClose,   "19 boot.default";
                                 5, SLT_BereqAcct,      "541 0 541 375 0 375";
                                 );

       let record = apply_last!(builder, 5, SLT_End, "")
           .unwrap_backend_access();

       assert_eq!(record.start, 1471354579.281173);
       assert_eq!(record.end, None);

       assert_matches!(record.transaction, BackendAccessTransaction::Abandoned {
           send: 0.000128,
           ttfb: 0.007524,
           wait: 0.007396,
           fetch: None,
           ..
       });

        assert_matches!(record.transaction, BackendAccessTransaction::Abandoned {
            request: HttpRequest {
                ref method,
                ref url,
                ref protocol,
                ref headers,
            },
            ..
        } if
            method == "GET" &&
            url == "/test_page/123.html" &&
            protocol == "HTTP/1.1" &&
            headers == &[
                ("Date".to_string(), "Tue, 16 Aug 2016 13:36:19 GMT".to_string()),
                ("Host".to_string(), "127.0.0.1:1202".to_string()),
                ("Accept-Encoding".to_string(), "gzip".to_string())]
        );

       assert_matches!(record.transaction, BackendAccessTransaction::Abandoned {
           response: HttpResponse {
               ref protocol,
               status,
               ref reason,
               ref headers,
           },
           ..
       } if
           protocol == "HTTP/1.1" &&
           status == 500 &&
           reason == "Internal Server Error" &&
           headers == &[
               ("Content-Type".to_string(), "text/html; charset=utf-8".to_string())]
       );
    }

    #[test]
    fn apply_backend_access_record_full_timing() {
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
                                 32769, SLT_TTL,              "RFC 120 10 -1 1471339883 1471339880 1340020138 0 0";
                                 32769, SLT_VCL_call,         "BACKEND_RESPONSE";
                                 32769, SLT_BackendReuse,     "19 boot.iss";
                                 32769, SLT_Storage,          "malloc s0";
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

       assert_eq!(record.start, 1470403414.669375);
       assert_eq!(record.end, Some(1470403414.672290));

       assert_matches!(record.transaction, BackendAccessTransaction::Full {
           send: 0.004549,
           ttfb: 0.007262,
           wait: 0.002713,
           fetch: 0.007367,
           ..
       });
    }

    #[test]
    fn apply_backend_access_record_full_timing_retry() {
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
                                 32769, SLT_TTL,              "RFC 120 10 -1 1471339883 1471339880 1340020138 0 0";
                                 32769, SLT_VCL_call,         "BACKEND_RESPONSE";
                                 32769, SLT_VCL_return,       "retry";
                                 32769, SLT_BackendReuse,     "19 boot.iss";
                                 32769, SLT_Timestamp,        "Retry: 1470403414.672290 0.007367 0.000105";
                                 32769, SLT_Link,             "bereq 32769 retry";
                                 );

       let record = apply_last!(builder, 32769, SLT_End, "")
           .unwrap_backend_access();

       assert_eq!(record.start, 1470403414.669375);
       assert_eq!(record.end, Some(1470403414.672290));

       assert_matches!(record.transaction, BackendAccessTransaction::Abandoned {
           send: 0.004549,
           ttfb: 0.007262,
           wait: 0.002713,
           fetch: Some(0.007367),
           ..
       });
    }

    #[test]
    fn apply_backend_access_record_failed() {
        let builder = RecordBuilder::new(123);

        // logs-new/varnish20160816-4093-lmudum99608ad955ba43288.vsl
        let builder = apply_all!(builder,
                                 5, SLT_Begin,          "bereq 4 fetch";
                                 5, SLT_Timestamp,      "Start: 1471355385.239334 0.000000 0.000000";
                                 5, SLT_BereqMethod,    "GET";
                                 5, SLT_BereqURL,       "/test_page/123.html";
                                 5, SLT_BereqProtocol,  "HTTP/1.1";
                                 5, SLT_BereqHeader,    "Date: Tue, 16 Aug 2016 13:49:45 GMT";
                                 5, SLT_BereqHeader,    "Host: 127.0.0.1:1236";
                                 5, SLT_VCL_call,       "BACKEND_FETCH";
                                 5, SLT_VCL_return,     "fetch";
                                 5, SLT_FetchError,     "no backend connection";
                                 5, SLT_Timestamp,      "Beresp: 1471355385.239422 0.000087 0.000087";
                                 5, SLT_Timestamp,      "Error: 1471355385.239427 0.000093 0.000005";
                                 5, SLT_BerespProtocol, "HTTP/1.1";
                                 5, SLT_BerespStatus,   "503";
                                 5, SLT_BerespReason,   "Service Unavailable";
                                 5, SLT_BerespReason,   "Backend fetch failed";
                                 5, SLT_BerespHeader,   "Date: Tue, 16 Aug 2016 13:49:45 GMT";
                                 5, SLT_BerespHeader,   "Server: Varnish";
                                 5, SLT_VCL_call,       "BACKEND_ERROR";
                                 5, SLT_VCL_Log,        "Backend Error Code: 503";
                                 5, SLT_BerespHeader,   "Retry-After: 20";
                                 5, SLT_VCL_return,     "deliver";
                                 5, SLT_Storage,        "malloc Transient";
                                 5, SLT_ObjProtocol,    "HTTP/1.1";
                                 5, SLT_ObjStatus,      "503";
                                 5, SLT_ObjReason,      "Backend fetch failed";
                                 5, SLT_ObjHeader,      "Date: Tue, 16 Aug 2016 13:49:45 GMT";
                                 5, SLT_ObjHeader,      "Server: Varnish";
                                 5, SLT_ObjHeader,      "X-Varnish-Decision: Internal-UnavaliableError";
                                 5, SLT_ObjHeader,      "Content-Type: text/html; charset=utf-8";
                                 5, SLT_ObjHeader,      "Cache-Control: no-store";
                                 5, SLT_ObjHeader,      "Retry-After: 20";
                                 5, SLT_Length,         "1366";
                                 5, SLT_BereqAcct,      "0 0 0 0 0 0";
                                 );

       let record = apply_last!(builder, 5, SLT_End, "")
           .unwrap_backend_access();

       assert_eq!(record.start, 1471355385.239334);
       assert_eq!(record.end, Some(1471355385.239427));

       assert_matches!(record.transaction, BackendAccessTransaction::Failed {
           synth: 0.000093,
           ..
       });

        assert_matches!(record.transaction, BackendAccessTransaction::Failed {
            request: HttpRequest {
                ref method,
                ref url,
                ref protocol,
                ref headers,
            },
            ..
        } if
            method == "GET" &&
            url == "/test_page/123.html" &&
            protocol == "HTTP/1.1" &&
            headers == &[
                ("Date".to_string(), "Tue, 16 Aug 2016 13:49:45 GMT".to_string()),
                ("Host".to_string(), "127.0.0.1:1236".to_string())]
        );

       assert_matches!(record.transaction, BackendAccessTransaction::Failed {
           synth_response: HttpResponse {
               ref protocol,
               status,
               ref reason,
               ref headers,
           },
           ..
       } if
           protocol == "HTTP/1.1" &&
           status == 503 &&
           reason == "Backend fetch failed" &&
           headers == &[
               ("Date".to_string(), "Tue, 16 Aug 2016 13:49:45 GMT".to_string()),
               ("Server".to_string(), "Varnish".to_string()),
               ("Retry-After".to_string(), "20".to_string())]
       );
    }

    #[test]
    fn apply_backend_access_record_failed_timing() {
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
       assert_eq!(record.end, Some(1470304835.059479));

       assert_matches!(record.transaction, BackendAccessTransaction::Failed {
           synth: 0.000054,
           ..
       });
    }

    #[test]
    fn apply_backend_access_record_piped() {
        let builder = RecordBuilder::new(123);

        // logs-new/varnish20160816-4093-s54h6nb4b44b69f1b2c7ca2.vsl
        let builder = apply_all!(builder,
                                 5, SLT_Begin,          "bereq 4 pipe";
                                 5, SLT_BereqMethod,    "GET";
                                 5, SLT_BereqURL,       "/websocket";
                                 5, SLT_BereqProtocol,  "HTTP/1.1";
                                 5, SLT_BereqHeader,    "Connection: Upgrade";
                                 5, SLT_VCL_call,       "PIPE";
                                 5, SLT_BereqHeader,    "Upgrade: websocket";
                                 5, SLT_VCL_return,     "pipe";
                                 5, SLT_BackendOpen,    "20 boot.default 127.0.0.1 42000 127.0.0.1 54038";
                                 5, SLT_BackendStart,   "127.0.0.1 42000";
                                 5, SLT_Timestamp,      "Bereq: 1471355444.744344 0.000000 0.000000";
                                 5, SLT_BackendClose,   "20 boot.default";
                                 5, SLT_BereqAcct,      "0 0 0 0 0 0";
                                 );

       let record = apply_last!(builder, 5, SLT_End, "")
           .unwrap_backend_access();

       assert_eq!(record.start, 1471355444.744344);
       assert_eq!(record.end, None);

       assert_matches!(record.transaction, BackendAccessTransaction::Piped {
           request: HttpRequest {
               ref method,
               ref url,
               ref protocol,
               ref headers,
           },
           ..
       } if
           method == "GET" &&
           url == "/websocket" &&
           protocol == "HTTP/1.1" &&
           headers == &[
               ("Connection".to_string(), "Upgrade".to_string()),
               ("Upgrade".to_string(), "websocket".to_string())]
       );
    }

    #[test]
    fn apply_backend_access_record_aborted() {
        let builder = RecordBuilder::new(123);

        let builder = apply_all!(builder,
                                 5, SLT_Begin,          "bereq 2 fetch";
                                 5, SLT_Timestamp,      "Start: 1471449766.106695 0.000000 0.000000";
                                 5, SLT_BereqMethod,    "GET";
                                 5, SLT_BereqURL,       "/";
                                 5, SLT_BereqProtocol,  "HTTP/1.1";
                                 5, SLT_BereqHeader,    "User-Agent: curl/7.40.0";
                                 5, SLT_BereqHeader,    "Host: localhost:1080";
                                 5, SLT_VCL_call,       "BACKEND_FETCH";
                                 5, SLT_BereqUnset,     "Accept-Encoding: gzip";
                                 5, SLT_VCL_return,     "abandon";
                                 5, SLT_BereqAcct,      "0 0 0 0 0 0";
                                 );

       let record = apply_last!(builder, 5, SLT_End, "")
           .unwrap_backend_access();

       assert_eq!(record.start, 1471449766.106695);
       assert_eq!(record.end, None);

       assert_matches!(record.transaction, BackendAccessTransaction::Aborted {
           request: HttpRequest {
               ref method,
               ref url,
               ref protocol,
               ref headers,
           },
           ..
       } if
           method == "GET" &&
           url == "/" &&
           protocol == "HTTP/1.1" &&
           headers == &[
               ("User-Agent".to_string(), "curl/7.40.0".to_string()),
               ("Host".to_string(), "localhost:1080".to_string())]
       );
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
                                 32769, SLT_Timestamp,        "Error: 1470403414.669471 0.004549 0.000096";
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

        assert_matches!(record.transaction, ClientAccessTransaction::Full {
            accounting: Accounting {
                recv_header: 82,
                recv_body: 2,
                recv_total: 84,
                sent_header: 304,
                sent_body: 6962,
                sent_total: 7266,
            },
            ..
        });
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
                                 32769, SLT_TTL,              "RFC 120 10 -1 1471339883 1471339883 1340020138 0 0";
                                 32769, SLT_VCL_call,         "BACKEND_RESPONSE";
                                 32769, SLT_BackendReuse,     "19 boot.iss";
                                 32769, SLT_Storage,          "malloc s0";
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

       assert_matches!(record.transaction, BackendAccessTransaction::Full {
           cache_object: CacheObject {
               ref fetch_mode,
               fetch_streamed,
               response: HttpResponse {
                   ref protocol,
                   status,
                   ref reason,
                   ref headers
               },
               ..
           },
           ..
       } if
           fetch_streamed == true &&
           fetch_mode == "length" &&
           protocol == "HTTP/1.1" &&
           status == 200 &&
           reason == "OK" &&
           headers == &[
               ("Content-Type".to_string(), "text/html; charset=utf-8".to_string()),
               ("X-Aspnet-Version".to_string(), "4.0.30319".to_string())]
       );
   }

    #[test]
    fn apply_backend_access_record_cache_object_ttl() {
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
                                 32769, SLT_TTL,              "RFC 120 10 -1 1471339883 1471339880 1340020138 0 0";
                                 32769, SLT_VCL_call,         "BACKEND_RESPONSE";
                                 32769, SLT_BackendReuse,     "19 boot.iss";
                                 32769, SLT_Storage,          "malloc s0";
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

       assert_matches!(record.transaction, BackendAccessTransaction::Full {
           cache_object: CacheObject {
               ttl: Some(120.0),
               grace: Some(10.0),
               keep: None,
               since: 1471339883.0,
               origin: 1471339880.0,
               ..
           },
           ..
       });
   }

    #[test]
    fn apply_backend_access_record_cache_object_ttl_vcl() {
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
                                 32769, SLT_TTL,              "RFC 120 10 -1 1471339883 1471339880 1340020138 0 0";
                                 32769, SLT_VCL_call,         "BACKEND_RESPONSE";
                                 32769, SLT_TTL,              "VCL 12345 259200 0 1470304807";
                                 32769, SLT_BackendReuse,     "19 boot.iss";
                                 32769, SLT_Storage,          "malloc s0";
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

       assert_matches!(record.transaction, BackendAccessTransaction::Full {
           cache_object: CacheObject {
               ttl: Some(12345.0),
               grace: Some(259200.0),
               keep: Some(0.0),
               since: 1470304807.0,
               // Keep time from RFC so we can calculate origin TTL etc
               origin: 1471339880.0,
               ..
           },
           ..
       });
   }

    #[test]
    fn apply_backend_access_record_cache_object_storage() {
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
                                 32769, SLT_TTL,              "RFC 120 10 -1 1471339883 1471339880 1340020138 0 0";
                                 32769, SLT_VCL_call,         "BACKEND_RESPONSE";
                                 32769, SLT_TTL,              "VCL 12345 259200 0 1470304807";
                                 32769, SLT_BackendReuse,     "19 boot.iss";
                                 32769, SLT_Storage,          "malloc s0";
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

       assert_matches!(record.transaction, BackendAccessTransaction::Full {
           cache_object: CacheObject {
               ref storage_type,
               ref storage_name,
               ..
           },
           ..
       } if
           storage_type == "malloc" &&
           storage_name == "s0"
       );
   }
}
