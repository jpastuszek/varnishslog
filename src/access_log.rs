use std::collections::HashMap;
use std::str::Utf8Error;
use std::num::{ParseIntError, ParseFloatError};
use std::fmt::Debug;
use quick_error::ResultExt;
use linked_hash_map::LinkedHashMap;

use vsl::{VslRecord, VslIdent, VslRecordTag};
use vsl::VslRecordTag::*;

use nom::{self, IResult};

pub type TimeStamp = f64;
pub type Duration = f64;
pub type Address = (String, u16);

// TODO:
// * Collect Log messages
// * Collect errors: SLT_FetchError
// * Collect Debug messages: SLT_Debug
// * miss/hit etc
// * client IP: SLT_SessOpen
// * Call trace
// * ACL trace
// * Linking information: SLT_Link
// * Byte counts: SLT_ReqAcct
// * Handle the "<not set>" headers
//
// Request headers:
// ---
// Bereq:
// * We want to capture headers sent to the backend (SLT_VCL_return fetch)
// * They can be set after request was sent to the backend
//
// Req:
// * We want to capture original client request headers (SLT_VCL_call RECV)
// * The headers are used as variables
//
// ESI (logs/varnish20160804-3752-1lr56fj56c2d5925f217f012.vsl):
// ---
// 65539 SLT_Begin          req 65538 esi
//
// 65541 SLT_Begin          req 65538 esi
//
// 65542 SLT_Begin          bereq 65541 fetch
//
// 65538 SLT_Begin          req 65537 rxreq
// 65538 SLT_Link           req 65539 esi
// 65538 SLT_Link           req 65541 esi
//
// 65537 SLT_Begin          sess 0 HTTP/1
// 65537 SLT_SessOpen       127.0.0.1 57408 127.0.0.1:1221 127.0.0.1 1221 1470304807.389646 20
// 65537 SLT_Link           req 65538 rxreq
// 65537 SLT_SessClose      REM_CLOSE 3.228
// 65537 SLT_End
//
// Grace (logs/varnish20160804-3752-zmjq309e3d02a67cea67299.vsl)
// ---
//     4 SLT_Begin          req 3 rxreq
//     4 SLT_Link           bereq 5 bgfetch
//
//     5 SLT_Begin          bereq 4 bgfetch
//
//     3 SLT_Begin          sess 0 HTTP/1
//     3 SLT_SessOpen       127.0.0.1 59686 127.0.0.1:1230 127.0.0.1 1230 1470304835.029314 19
//     3 SLT_Link           req 4 rxreq
//     3 SLT_SessClose      RX_TIMEOUT 10.011
//     3 SLT_End
//
// Restarts (logs/varnish20160804-3752-1h9gf4h5609f5ab778e4a4eb.vsl)
// ---
// This can happen at any state of client requests/response handling
//
// 32770 SLT_Begin          req 32769 rxreq
// 32770 SLT_ReqHeader      X-Varnish-Decision: Refresh-NotBuildNumber
// 32770 SLT_VCL_return     restart
// // No response info
// 32770 SLT_Link           req 32771 restart
// 32770 SLT_Timestamp      Restart: 1470304882.576600 0.000136 0.000136
// 32770 SLT_End
//
// 32771 SLT_Begin          req 32770 restart
//
// 32769 SLT_Begin          sess 0 HTTP/1
// 32769 SLT_SessOpen       127.0.0.1 34560 127.0.0.1:1244 127.0.0.1 1244 1470304882.576266 14
// 32769 SLT_Link           req 32770 rxreq
// 32769 SLT_SessClose      REM_CLOSE 0.347
//
// Retry (varnish20160805-3559-f6sifo45103025c06abad14.vsl)
// ---
// Can be used to restart backend fetch in backend thread
//
//     8 SLT_Begin          bereq 7 fetch
//     8 SLT_BereqURL       /retry
//     8 SLT_Link           bereq 32769 retry
//
// 32769 SLT_Begin          bereq 8 retry
// 32769 SLT_BereqURL       /iss/v2/thumbnails/foo/4006450256177f4a/bar.jpg
//
//     7 SLT_Begin          req 6 rxreq
//     7 SLT_Link           bereq 8 fetch
//
//     6 SLT_Begin          sess 0 HTTP/1
//     6 SLT_SessOpen       127.0.0.1 39798 127.0.0.1:1200 127.0.0.1 1200 1470403414.664642 17
//     6 SLT_Link           req 7 rxreq
//     6 SLT_SessClose      REM_CLOSE 0.008
//     6 SLT_End
//
#[derive(Debug, Clone, PartialEq)]
pub struct ClientAccessRecord {
    pub ident: VslIdent,
    pub parent: VslIdent, // Session or anothre Client (ESI)
    pub reason: String,
    pub esi_requests: Vec<VslIdent>,
    pub backend_requests: Vec<VslIdent>,
    pub restart_request: Option<VslIdent>,
    pub http_transaction: HttpTransaction,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BackendAccessRecord {
    pub ident: VslIdent,
    pub parent: VslIdent, // Client
    pub reason: String,
    pub retry_request: Option<VslIdent>,
    pub http_transaction: HttpTransaction,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SessionRecord {
    pub ident: VslIdent,
    pub open: TimeStamp,
    pub duration: Duration,
    pub local: Option<Address>,
    pub remote: Address,
    pub client_requests: Vec<VslIdent>,
}

// TODO: store duration (use relative timing (?) from log as TS can go backwards)
// check Varnish code to see if relative timing is immune to clock going backwards
#[derive(Debug, Clone, PartialEq)]
pub struct HttpTransaction {
    pub start: TimeStamp,
    pub end: TimeStamp,
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
    pub status: u32,
    pub reason: String,
    pub headers: Vec<(String, String)>,
}

#[derive(Debug)]
struct HttpRequestBuilder {
    protocol: Option<String>,
    method: Option<String>,
    url: Option<String>,
    headers: LinkedHashMap<String, String>,
}

impl BuilderResult<HttpRequestBuilder, HttpRequest> {
    fn to_complete(self) -> Result<BuilderResult<HttpRequestBuilder, HttpRequest>, RecordBuilderError> {
        match self {
            Complete(request) => Err(RecordBuilderError::HttpRequestNotBuilding(request)),
            Building(builder) => Ok(Complete(HttpRequest {
                protocol: try!(builder.protocol.ok_or(RecordBuilderError::RecordIncomplete("Request.protocol"))),
                method: try!(builder.method.ok_or(RecordBuilderError::RecordIncomplete("Request.method"))),
                url: try!(builder.url.ok_or(RecordBuilderError::RecordIncomplete("Request.url"))),
                headers: builder.headers.into_iter().collect(),
            }))
        }
    }

    fn get_complete(self) -> Result<HttpRequest, RecordBuilderError> {
        match self {
            Complete(request) => Ok(request),
            Building(_) => Err(RecordBuilderError::HttpRequestNotComplete),
        }
    }
}

#[derive(Debug)]
struct RecordBuilder {
    ident: VslIdent,
    record_type: Option<RecordType>,
    req_start: Option<TimeStamp>,
    http_request: BuilderResult<HttpRequestBuilder, HttpRequest>,
    resp_protocol: Option<String>,
    resp_status: Option<u32>,
    resp_reason: Option<String>,
    resp_headers: LinkedHashMap<String, String>,
    resp_end: Option<TimeStamp>,
    sess_open: Option<TimeStamp>,
    sess_duration: Option<Duration>,
    sess_remote: Option<Address>,
    sess_local: Option<Address>,
    client_requests: Vec<VslIdent>,
    backend_requests: Vec<VslIdent>,
    restart_request: Option<VslIdent>,
    retry_request: Option<VslIdent>,
}

#[derive(Debug, Clone)]
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

#[derive(Debug)]
enum BuilderResult<B, C> {
    Building(B),
    Complete(C),
}

impl<B, C> BuilderResult<B, C> {
    fn as_ref(&self) -> BuilderResult<&B, &C> {
        match self {
            &BuilderResult::Building(ref buidling) => BuilderResult::Building(buidling),
            &BuilderResult::Complete(ref complete) => BuilderResult::Complete(complete),
        }
    }

    fn unwrap(self) -> C where B: Debug {
        match self {
            BuilderResult::Building(buidling) => panic!("Trying to unwrap BuilderResult::Building: {:?}", buidling),
            BuilderResult::Complete(complete) => complete,
        }
    }
}

// So we can just type Building() and Complete()
use self::BuilderResult::*;

trait IResultExt<O, E> {
    fn into_result(self) -> Result<O, E>;
}

impl<I, O, E> IResultExt<O, nom::Err<I, E>> for IResult<I, O, E> {
    fn into_result(self) -> Result<O, nom::Err<I, E>> {
        match self {
            IResult::Done(_, o) => Ok(o),
            IResult::Error(err) => Err(err),
            IResult::Incomplete(_) => panic!("got Incomplete IResult!"),
        }
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum RecordBuilderError {
        NonUtf8VslMessage(err: Utf8Error) {
            display("VSL record message is not valid UTF-8 encoded string: {}", err)
            cause(err)
        }
        UnimplementedTransactionType(record_type: String) {
            display("Unimplemented record type '{}'", record_type)
        }
        InvalidMessageFormat(err: String) {
            display("Failed to parse message: {}", err)
            // Note: using context() since from() does not support lifetimes
            context(tag: VslRecordTag ,err: nom::Err<&'a str>)
                -> (format!("Nom parser failed on VSL record {:?}: {}", tag, err))
        }
        InvalidMessageFieldFormat(field_name: &'static str, err: String) {
            display("Failed to parse message field '{}': {}", field_name, err)
            context(field_name: &'static str, err: ParseFloatError)
                -> (field_name, format!("Float parsing error: {}", err))
            context(field_name: &'static str, err: ParseIntError)
                -> (field_name, format!("Integer parsing error: {}", err))
        }
        HttpRequestNotBuilding(request: HttpRequest) {
            display("Expected HTTP request to be still building but got it complete: {:?}", request)
        }
        HttpRequestNotComplete {
            display("Expected HTTP request to be complete but it was still building")
        }
        RecordIncomplete(field_name: &'static str) {
            display("Failed to construct final access record due to missing field '{}'", field_name)
        }
    }
}

use nom::{rest_s, space, eof};
named!(label<&str, &str>, terminated!(take_until_s!(": "), tag_s!(": ")));
named!(space_terminated<&str, &str>, terminated!(is_not_s!(" "), space));
named!(space_terminated_eof<&str, &str>, terminated!(is_not_s!(" "), eof));

named!(slt_begin<&str, (&str, &str, &str)>, complete!(tuple!(
        space_terminated,           // Type ("sess", "req" or "bereq")
        space_terminated,           // Parent vxid
        space_terminated_eof)));    // Reason

named!(slt_timestamp<&str, (&str, &str, &str, &str)>, complete!(tuple!(
        label,                      // Event label
        space_terminated,           // Absolute time of event
        space_terminated,           // Time since start of work unit
        space_terminated_eof)));    // Time since last timestamp

named!(slt_method<&str, &str>, complete!(rest_s));
named!(slt_url<&str, &str>, complete!(rest_s));
named!(slt_protocol<&str, &str>, complete!(rest_s));
named!(slt_status<&str, &str>, complete!(rest_s));
named!(slt_reason<&str, &str>, complete!(rest_s));

named!(slt_header<&str, (&str, &str)>, complete!(tuple!(
        label,      // Header name
        rest_s)));  // Header value

named!(slt_session<&str, (&str, &str, &str, &str, &str, &str, &str)>, complete!(tuple!(
        space_terminated,           // Remote IPv4/6 address
        space_terminated,           // Remote TCP port
        space_terminated,           // Listen socket (-a argument)
        space_terminated,           // Local IPv4/6 address ('-' if !$log_local_addr)
        space_terminated,           // Local TCP port ('-' if !$log_local_addr)
        space_terminated,           // Time stamp (undocumented)
        space_terminated_eof)));    // File descriptor number

named!(slt_link<&str, (&str, &str, &str)>, complete!(tuple!(
        space_terminated,           // Child type ("req" or "bereq")
        space_terminated,           // Child vxid
        space_terminated_eof)));    // Reason

named!(slt_sess_close<&str, (&str, &str)>, complete!(tuple!(
        space_terminated,           // Why the connection closed
        space_terminated_eof)));    // How long the session was open

named!(stl_call<&str, &str>, complete!(space_terminated_eof));      // VCL method name

impl RecordBuilder {
    fn new(ident: VslIdent) -> RecordBuilder {
        RecordBuilder {
            ident: ident,
            record_type: None,
            req_start: None,
            http_request: Building(HttpRequestBuilder {
                protocol: None,
                method: None,
                url: None,
                headers: LinkedHashMap::new(),
            }),
            resp_protocol: None,
            resp_status: None,
            resp_reason: None,
            resp_headers: LinkedHashMap::new(),
            resp_end: None,
            sess_open: None,
            sess_duration: None,
            sess_remote: None,
            sess_local: None,
            client_requests: Vec::new(),
            backend_requests: Vec::new(),
            restart_request: None,
            retry_request: None,
        }
    }

    fn apply<'r>(self, vsl: &'r VslRecord) -> Result<BuilderResult<RecordBuilder, Record>, RecordBuilderError> {
        let builder = match vsl.message() {
            Ok(message) => match vsl.tag {
                SLT_Begin => {
                    let (record_type, parent, reason) = try!(slt_begin(message).into_result().context(vsl.tag));
                    let vxid = try!(parent.parse().context("vxid"));

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
                    let (label, timestamp, _sice_work_start, _since_last_timestamp) = try!(slt_timestamp(message).into_result().context(vsl.tag));
                    match label {
                        "Start" => RecordBuilder {
                            req_start: Some(try!(timestamp.parse().context("timestamp"))),
                            .. self
                        },
                        "Beresp" => RecordBuilder {
                            resp_end: Some(try!(timestamp.parse().context("timestamp"))),
                            .. self
                        },
                        "Resp" => RecordBuilder {
                            resp_end: Some(try!(timestamp.parse().context("timestamp"))),
                            .. self
                        },
                        "Restart" => RecordBuilder {
                            resp_end: Some(try!(timestamp.parse().context("timestamp"))),
                            .. self
                        },
                        _ => {
                            warn!("Ignoring unknown SLT_Timestamp label variant: {}", label);
                            self
                        }
                    }
                }

                // Request
                SLT_BereqProtocol | SLT_ReqProtocol => {
                    let protocol = try!(slt_protocol(message).into_result().context(vsl.tag));

                    if let Building(builder) = self.http_request {
                        RecordBuilder {
                            http_request: Building(HttpRequestBuilder {
                                protocol: Some(protocol.to_string()),
                                .. builder
                            }),
                            .. self
                        }
                    } else {
                        debug!("Ignoring {:?} with protocol '{}' as we have finished building the request", vsl.tag, protocol);
                        self
                    }
                }
                SLT_BereqMethod | SLT_ReqMethod => {
                    let method = try!(slt_method(message).into_result().context(vsl.tag));

                    if let Building(builder) = self.http_request {
                        RecordBuilder {
                            http_request: Building(HttpRequestBuilder {
                                method: Some(method.to_string()),
                                .. builder
                            }),
                            .. self
                        }
                    } else {
                        debug!("Ignoring {:?} with method '{}' as we have finished building the request", vsl.tag, method);
                        self
                    }
                }
                SLT_BereqURL | SLT_ReqURL => {
                    let url = try!(slt_url(message).into_result().context(vsl.tag));

                    if let Building(builder) = self.http_request {
                        RecordBuilder {
                            http_request: Building(HttpRequestBuilder {
                                url: Some(url.to_string()),
                                .. builder
                            }),
                            .. self
                        }
                    } else {
                        debug!("Ignoring {:?} with URL '{}' as we have finished building the request", vsl.tag, url);
                        self
                    }
                }
                //TODO: lock header manip after request/response was sent
                SLT_BereqHeader | SLT_ReqHeader => {
                    let (name, value) = try!(slt_header(message).into_result().context(vsl.tag));

                    if let BuilderResult::Building(builder) = self.http_request {
                        let mut headers = builder.headers;
                        headers.insert(name.to_string(), value.to_string());

                        RecordBuilder {
                            http_request: Building(HttpRequestBuilder {
                                headers: headers,
                                .. builder
                            }),
                            .. self
                        }
                    } else {
                        debug!("Ignoring {:?} with header '{}' of value '{}' as we have finished building the request", vsl.tag, name, value);
                        self
                    }
                }
                SLT_BereqUnset | SLT_ReqUnset => {
                    let (name, _) = try!(slt_header(message).into_result().context(vsl.tag));

                    if let Building(builder) = self.http_request {
                        let mut headers = builder.headers;
                        headers.remove(name);

                        RecordBuilder {
                            http_request: Building(HttpRequestBuilder {
                                headers: headers,
                                .. builder
                            }),
                            .. self
                        }
                    } else {
                        debug!("Ignoring {:?} with header '{}' as we have finished building the request", vsl.tag, name);
                        self
                    }
                }

                SLT_VCL_call | SLT_VCL_return => {
                    let method = try!(stl_call(message).into_result().context(vsl.tag));

                    match method {
                        "fetch" | "RECV" => RecordBuilder {
                            http_request: try!(self.http_request.to_complete()),
                            .. self
                        },
                        _ => {
                            warn!("Ignoring unknown {:?} method: {}", vsl.tag, method);
                            self
                        }
                    }
                }

                // Response
                SLT_BerespProtocol | SLT_RespProtocol => {
                    let protocol = try!(slt_protocol(message).into_result().context(vsl.tag));

                    RecordBuilder {
                        resp_protocol: Some(protocol.to_string()),
                        .. self
                    }
                }
                SLT_BerespStatus | SLT_RespStatus => {
                    let status = try!(slt_status(message).into_result().context(vsl.tag));

                    RecordBuilder {
                        resp_status: Some(try!(status.parse().context("status"))),
                        .. self
                    }
                }
                SLT_BerespReason | SLT_RespReason => {
                    let reason = try!(slt_reason(message).into_result().context(vsl.tag));

                    RecordBuilder {
                        resp_reason: Some(reason.to_string()),
                        .. self
                    }
                }
                SLT_BerespHeader | SLT_RespHeader => {
                    let (name, value) = try!(slt_header(message).into_result().context(vsl.tag));

                    let mut headers = self.resp_headers;
                    headers.insert(name.to_string(), value.to_string());

                    RecordBuilder {
                        resp_headers: headers,
                        .. self
                    }
                }
                SLT_BerespUnset | SLT_RespUnset => {
                    let (name, _) = try!(slt_header(message).into_result().context(vsl.tag));

                    let mut headers = self.resp_headers;
                    headers.remove(name);

                    RecordBuilder {
                        resp_headers: headers,
                        .. self
                    }
                }

                // Session
                SLT_SessOpen => {
                    let (remote_ip, remote_port, _listen_sock, local_ip, local_port, timestamp, _fd)
                        = try!(slt_session(message).into_result().context(vsl.tag));

                    RecordBuilder {
                        sess_open: Some(try!(timestamp.parse().context("timestamp"))),
                        sess_remote: if remote_ip != "-" && remote_port != "-" {
                            Some((remote_ip.to_string(), try!(remote_port.parse().context("remote_port"))))
                        } else {
                            None
                        },
                        sess_local: Some((local_ip.to_string(), try!(local_port.parse().context("local_port")))),
                        .. self
                    }
                }
                SLT_Link => {
                    let (reason, child_vxid, child_type) = try!(slt_link(message).into_result().context(vsl.tag));

                    let child_vxid = try!(child_vxid.parse().context("child_vxid"));

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
                SLT_SessClose => {
                    let (_reason, duration) = try!(slt_sess_close(message).into_result().context(vsl.tag));

                    RecordBuilder {
                        sess_duration: Some(try!(duration.parse().context("duration"))),
                        .. self
                    }
                }

                // Final
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

                            // We may not have a response in case of restart
                            let response = if self.resp_status.is_some() {
                                Some(HttpResponse {
                                    protocol: try!(self.resp_protocol.ok_or(RecordBuilderError::RecordIncomplete("resp_protocol"))),
                                    status: try!(self.resp_status.ok_or(RecordBuilderError::RecordIncomplete("resp_status"))),
                                    reason: try!(self.resp_reason.ok_or(RecordBuilderError::RecordIncomplete("resp_reason"))),
                                    headers: self.resp_headers.into_iter().collect(),
                                })
                            } else {
                                None
                            };

                            let http_transaction = HttpTransaction {
                                start: try!(self.req_start.ok_or(RecordBuilderError::RecordIncomplete("req_start"))),
                                end: try!(self.resp_end.ok_or(RecordBuilderError::RecordIncomplete("resp_end"))),
                                request: request,
                                response: response,
                            };

                            match record_type {
                                RecordType::ClientAccess { parent, reason } => {
                                    let record = ClientAccessRecord {
                                        ident: self.ident,
                                        parent: parent,
                                        reason: reason,
                                        esi_requests: self.client_requests,
                                        backend_requests: self.backend_requests,
                                        restart_request: self.restart_request,
                                        http_transaction: http_transaction,
                                    };

                                    return Ok(Complete(Record::ClientAccess(record)))
                                },
                                RecordType::BackendAccess { parent, reason } => {
                                    let record = BackendAccessRecord {
                                        ident: self.ident,
                                        parent: parent,
                                        reason: reason,
                                        retry_request: self.retry_request,
                                        http_transaction: http_transaction,
                                    };

                                    return Ok(Complete(Record::BackendAccess(record)))
                                },
                                _ => unreachable!(),
                            }
                        },
                    }
                }
                _ => {
                    warn!("Ignoring unknown VSL tag: {:?}", vsl.tag);
                    self
                }
            },
            Err(err) => return Err(RecordBuilderError::NonUtf8VslMessage(err))
        };

        Ok(Building(builder))
    }
}

#[derive(Debug)]
pub struct RecordState {
    builders: HashMap<VslIdent, RecordBuilder>
}

impl RecordState {
    pub fn new() -> RecordState {
        //TODO: some sort of expirity mechanism like LRU
        RecordState { builders: HashMap::new() }
    }

    pub fn apply(&mut self, vsl: &VslRecord) -> Option<Record> {
        let builder = match self.builders.remove(&vsl.ident) {
            Some(builder) => builder,
            None => RecordBuilder::new(vsl.ident),
        };

        match builder.apply(vsl) {
            Ok(result) => match result {
                Building(builder) => {
                    self.builders.insert(vsl.ident, builder);
                    return None
                }
                Complete(record) => return Some(record),
            },
            Err(err) => {
                error!("Error while building record with ident {} while applying VSL record with tag {:?} and message {:?}: {}", vsl.ident, vsl.tag, vsl.message(), err);
                return None
            }
        }
    }

    #[cfg(test)]
    fn get(&self, ident: VslIdent) -> Option<&RecordBuilder> {
        self.builders.get(&ident)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Session {
    pub record: SessionRecord,
    pub client_transactions: Vec<ClientTransaction>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClientTransaction {
    pub access_record: ClientAccessRecord,
    pub backend_transactions: Vec<BackendTransaction>,
    pub esi_transactions: Vec<ClientTransaction>,
    pub restart_transaction: Option<Box<ClientTransaction>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BackendTransaction {
    pub access_record: BackendAccessRecord,
    pub retry_transaction: Option<Box<BackendTransaction>>,
}

#[derive(Debug)]
pub struct SessionState {
    record_state: RecordState,
    client: HashMap<VslIdent, ClientAccessRecord>,
    backend: HashMap<VslIdent, BackendAccessRecord>,
}

impl SessionState {
    pub fn new() -> SessionState {
        //TODO: some sort of expirity mechanism like LRU
        SessionState {
            record_state: RecordState::new(),
            client: HashMap::new(),
            backend: HashMap::new(),
        }
    }

    fn build_backend_transaction(&mut self, session: &SessionRecord, client: &ClientAccessRecord, backend: BackendAccessRecord) -> BackendTransaction {
        let retry_transaction = backend.retry_request
            .and_then(|ident| self.backend.remove(&ident).or_else(|| {
                error!("Session {} references ClientAccessRecord {} which has BackendAccessRecord {} that was restarted into BackendAccessRecord {} wich was not found: {:?} in client: {:?} in session: {:?}", session.ident, client.ident, backend.ident, ident, backend, client, session);
                None}))
            .map(|retry| Box::new(self.build_backend_transaction(session, client, retry)));

        BackendTransaction {
            access_record: backend,
            retry_transaction: retry_transaction,
        }
    }

    // TODO: could use Cell to eliminate collect().into_iter() buffers
    fn build_client_transaction(&mut self, session: &SessionRecord, client: ClientAccessRecord) -> ClientTransaction {
        let backend_transactions = client.backend_requests.iter()
            .filter_map(|ident| self.backend.remove(ident).or_else(|| {
                error!("Session {} references ClientAccessRecord {} which references BackendAccessRecord {} that was not found: {:?} in session: {:?}", session.ident, client.ident, ident, client, session);
                None}))
            .collect::<Vec<_>>().into_iter()
            .map(|backend| self.build_backend_transaction(session, &client, backend))
            .collect();

        let esi_transactions = client.esi_requests.iter()
            .filter_map(|ident| self.client.remove(ident).or_else(|| {
                error!("Session {} references ClientAccessRecord {} which references ESI ClientAccessRecord {} wich was not found: {:?} in session: {:?}", session.ident, client.ident, ident, client, session);
                None}))
            .collect::<Vec<_>>().into_iter()
            .map(|client| self.build_client_transaction(session, client))
            .collect();

        let restart_transaction = client.restart_request
            .and_then(|ident| self.client.remove(&ident).or_else(|| {
                error!("Session {} references ClientAccessRecord {} which was restarted into ClientAccessRecord {} wich was not found: {:?} in session: {:?}", session.ident, client.ident, ident, client, session);
                None}))
            .map(|restart| Box::new(self.build_client_transaction(&session, restart)));

        ClientTransaction {
            access_record: client,
            backend_transactions: backend_transactions,
            esi_transactions: esi_transactions,
            restart_transaction: restart_transaction,
        }
    }

    pub fn apply(&mut self, vsl: &VslRecord) -> Option<Session> {
        match self.record_state.apply(vsl) {
            Some(Record::ClientAccess(record)) => {
                self.client.insert(record.ident, record);
                None
            }
            Some(Record::BackendAccess(record)) => {
                self.backend.insert(record.ident, record);
                None
            }
            Some(Record::Session(session)) => {
                let client_transactions = session.client_requests.iter()
                    .filter_map(|ident| self.client.remove(ident).or_else(|| {
                        error!("Session {} references ClientAccessRecord {} which was not found: {:?}", session.ident, ident, session);
                        None}))
                    .collect::<Vec<_>>().into_iter()
                    .map(|client| self.build_client_transaction(&session, client))
                    .collect();

                Some(Session {
                    record: session,
                    client_transactions: client_transactions,
                })
            },
            None => None,
        }
    }

    pub fn unmatched_client_access_records(&self) -> Vec<&ClientAccessRecord> {
        self.client.iter().map(|(_, record)| record).collect()
    }

    pub fn unmatched_backend_access_records(&self) -> Vec<&BackendAccessRecord> {
        self.backend.iter().map(|(_, record)| record).collect()
    }
}

#[cfg(test)]
mod access_log_request_state_tests {
    pub use super::*;
    use vsl::{VslRecord, VslRecordTag, VslIdent};
    use vsl::VslRecordTag::*;

    fn vsl(tag: VslRecordTag, ident: VslIdent, message: &str) -> VslRecord {
        VslRecord::from_str(tag, ident, message)
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

    #[test]
    fn apply_non_utf8() {
        let mut state = RecordState::new();

        state.apply(&VslRecord {
            tag: SLT_Begin,
            marker: 0,
            ident: 123,
            data: &[255, 0, 1, 2, 3]
        });

        assert_none!(state.get(123));
    }

    #[test]
    fn apply_begin() {
        let mut state = RecordState::new();

        state.apply(&vsl(SLT_Begin, 123, "bereq 321 fetch"));

        let builder = state.get(123).unwrap();
        let record_type = builder.record_type.as_ref().unwrap();

        assert_matches!(record_type, &RecordType::BackendAccess { parent: 321, ref reason } if reason == "fetch");
    }

    #[test]
    fn apply_begin_unimpl_transaction_type() {
        let mut state = RecordState::new();

        state.apply(&vsl(SLT_Begin, 123, "foo 231 fetch"));
        assert_none!(state.get(123));
    }

    #[test]
    fn apply_begin_parser_fail() {
        let mut state = RecordState::new();

        state.apply(&vsl(SLT_Begin, 123, "foo bar"));
        assert_none!(state.get(123));
    }

    #[test]
    fn apply_begin_float_parse_fail() {
        let mut state = RecordState::new();

        state.apply(&vsl(SLT_Begin, 123, "bereq bar fetch"));
        assert_none!(state.get(123));
    }

    #[test]
    fn apply_timestamp() {
        let mut state = RecordState::new();

        state.apply(&vsl(SLT_Timestamp, 123, "Start: 1469180762.484544 0.000000 0.000000"));

        let builder = state.get(123).unwrap().clone();
        assert_eq!(builder.req_start, Some(1469180762.484544));
    }

    #[test]
    fn apply_backend_request() {
        let mut state = RecordState::new();

        apply_all!(state,
               123, SLT_Timestamp,      "Start: 1469180762.484544 0.000000 0.000000";
               123, SLT_BereqMethod,    "GET";
               123, SLT_BereqURL,       "/foobar";
               123, SLT_BereqProtocol,  "HTTP/1.1";
               123, SLT_BereqHeader,    "Host: localhost:8080";
               123, SLT_BereqHeader,    "User-Agent: curl/7.40.0";
               123, SLT_BereqHeader,    "Accept-Encoding: gzip";
               123, SLT_BereqUnset,     "Accept-Encoding: gzip";
               123, SLT_VCL_return,     "fetch";
              );

        let builder = state.get(123).unwrap();
        assert_eq!(builder.req_start, Some(1469180762.484544));

        let builder = builder.http_request.as_ref().unwrap();
        assert_eq!(builder.method, "GET".to_string());
        assert_eq!(builder.url, "/foobar".to_string());
        assert_eq!(builder.protocol, "HTTP/1.1".to_string());
        assert_eq!(builder.headers, &[
                   ("Host".to_string(), "localhost:8080".to_string()),
                   ("User-Agent".to_string(), "curl/7.40.0".to_string())]);
    }

    #[test]
    fn apply_backend_response() {
        let mut state = RecordState::new();

        apply_all!(state,
               123, SLT_Timestamp, "Beresp: 1469180762.484544 0.000000 0.000000";
               123, SLT_BerespProtocol, "HTTP/1.1";
               123, SLT_BerespStatus, "503";
               123, SLT_BerespReason, "Service Unavailable";
               123, SLT_BerespReason, "Backend fetch failed";
               123, SLT_BerespHeader, "Date: Fri, 22 Jul 2016 09:46:02 GMT";
               123, SLT_BerespHeader, "Server: Varnish";
               123, SLT_BerespHeader, "Cache-Control: no-store";
               123, SLT_BerespUnset, "Cache-Control: no-store";
               );

        let builder = state.get(123).unwrap();
        assert_eq!(builder.resp_end, Some(1469180762.484544));
        assert_eq!(builder.resp_protocol, Some("HTTP/1.1".to_string()));
        assert_eq!(builder.resp_status, Some(503));
        assert_eq!(builder.resp_reason, Some("Backend fetch failed".to_string()));
        assert_eq!(builder.resp_headers.get("Date"), Some(&"Fri, 22 Jul 2016 09:46:02 GMT".to_string()));
        assert_eq!(builder.resp_headers.get("Server"), Some(&"Varnish".to_string()));
        assert_eq!(builder.resp_headers.get("Cache-Control"), None);
    }

    #[test]
    fn apply_backend_request_locking() {
        let mut state = RecordState::new();

        apply_all!(state,
               123, SLT_Timestamp,      "Start: 1469180762.484544 0.000000 0.000000";
               123, SLT_BereqMethod,    "GET";
               123, SLT_BereqURL,       "/foobar";
               123, SLT_BereqProtocol,  "HTTP/1.1";
               123, SLT_BereqHeader,    "Host: localhost:8080";
               123, SLT_BereqHeader,    "User-Agent: curl/7.40.0";
               123, SLT_BereqHeader,    "Accept-Encoding: gzip";
               123, SLT_BereqUnset,     "Accept-Encoding: gzip";
               123, SLT_VCL_return,     "fetch";
               123, SLT_BerespProtocol, "HTTP/1.1";
               123, SLT_BerespStatus,   "503";
               123, SLT_BerespReason,   "Service Unavailable";
               123, SLT_BerespReason,   "Backend fetch failed";
               123, SLT_BerespHeader,   "Date: Fri, 22 Jul 2016 09:46:02 GMT";

               // try tp change headers after request (which can be done form VCL)
               123, SLT_BereqMethod,    "POST";
               123, SLT_BereqURL,       "/quix";
               123, SLT_BereqProtocol,  "HTTP/2.0";
               123, SLT_BereqHeader,    "Host: foobar:666";
               123, SLT_BereqHeader,    "Baz: bar";
              );

        let builder = state.get(123).unwrap().clone();
        assert_eq!(builder.req_start, Some(1469180762.484544));

        let builder = builder.http_request.as_ref().unwrap();
        assert_eq!(builder.method, "GET".to_string());
        assert_eq!(builder.url, "/foobar".to_string());
        assert_eq!(builder.protocol, "HTTP/1.1".to_string());
        assert_eq!(builder.headers, &[
                   ("Host".to_string(), "localhost:8080".to_string()),
                   ("User-Agent".to_string(), "curl/7.40.0".to_string())]);
    }

    #[test]
    fn apply_record_state_client_access() {
        let mut state = RecordState::new();

        apply_all!(state,
               123, SLT_Begin, "req 321 rxreq";
               123, SLT_Timestamp, "Start: 1469180762.484544 0.000000 0.000000";
               123, SLT_ReqMethod, "GET";
               123, SLT_ReqURL, "/foobar";
               123, SLT_ReqProtocol, "HTTP/1.1";
               123, SLT_ReqHeader, "Host: localhost:8080";
               123, SLT_ReqHeader, "User-Agent: curl/7.40.0";
               123, SLT_ReqHeader, "Accept-Encoding: gzip";
               123, SLT_ReqUnset, "Accept-Encoding: gzip";

               123, SLT_Link, "bereq 32774 fetch";

               123, SLT_RespProtocol, "HTTP/1.1";
               123, SLT_RespStatus, "503";
               123, SLT_RespReason, "Service Unavailable";
               123, SLT_RespReason, "Backend fetch failed";
               123, SLT_RespHeader, "Date: Fri, 22 Jul 2016 09:46:02 GMT";
               123, SLT_RespHeader, "Server: Varnish";
               123, SLT_RespHeader, "Cache-Control: no-store";
               123, SLT_RespUnset, "Cache-Control: no-store";
               123, SLT_RespHeader, "Content-Type: text/html; charset=utf-8";
               123, SLT_Timestamp, "Resp: 1469180763.484544 0.000000 0.000000";
               );

        let record = apply_final!(state, 123, SLT_End, "");

        assert_none!(state.get(123));

        assert!(record.is_client_access());
        let client = record.unwrap_client_access();
        assert_matches!(client, ClientAccessRecord {
            ident: 123,
            parent: 321,
            ref reason,
            ref backend_requests,
            ref esi_requests,
            ..
        } if
            reason == "rxreq" &&
            backend_requests == &[32774] &&
            esi_requests.is_empty()
        );
        assert_matches!(client.http_transaction, HttpTransaction {
            start: 1469180762.484544,
            end: 1469180763.484544,
            ..
        });
        assert_eq!(client.http_transaction.request, HttpRequest {
            method: "GET".to_string(),
            url: "/foobar".to_string(),
            protocol: "HTTP/1.1".to_string(),
            headers: vec![
                ("Host".to_string(), "localhost:8080".to_string()),
                ("User-Agent".to_string(), "curl/7.40.0".to_string())]
        });
        assert_eq!(client.http_transaction.response, Some(HttpResponse {
            protocol: "HTTP/1.1".to_string(),
            status: 503,
            reason: "Backend fetch failed".to_string(),
            headers: vec![
                ("Date".to_string(), "Fri, 22 Jul 2016 09:46:02 GMT".to_string()),
                ("Server".to_string(), "Varnish".to_string()),
                ("Content-Type".to_string(), "text/html; charset=utf-8".to_string())]
        }));
    }

    #[test]
    fn apply_record_state_backend_access() {
        let mut state = RecordState::new();

        apply_all!(state,
               123, SLT_Begin,          "bereq 321 fetch";
               123, SLT_Timestamp,      "Start: 1469180762.484544 0.000000 0.000000";
               123, SLT_BereqMethod,    "GET";
               123, SLT_BereqURL,       "/foobar";
               123, SLT_BereqProtocol,  "HTTP/1.1";
               123, SLT_BereqHeader,    "Host: localhost:8080";
               123, SLT_BereqHeader,    "User-Agent: curl/7.40.0";
               123, SLT_BereqHeader,    "Accept-Encoding: gzip";
               123, SLT_BereqUnset,     "Accept-Encoding: gzip";
               123, SLT_VCL_return,     "fetch";
               123, SLT_Timestamp,      "Beresp: 1469180763.484544 0.000000 0.000000";
               123, SLT_BerespProtocol, "HTTP/1.1";
               123, SLT_BerespStatus,   "503";
               123, SLT_BerespReason,   "Service Unavailable";
               123, SLT_BerespReason,   "Backend fetch failed";
               123, SLT_BerespHeader,   "Date: Fri, 22 Jul 2016 09:46:02 GMT";
               123, SLT_BerespHeader,   "Server: Varnish";
               123, SLT_BerespHeader,   "Cache-Control: no-store";
               123, SLT_BerespUnset,    "Cache-Control: no-store";
               123, SLT_BerespHeader,   "Content-Type: text/html; charset=utf-8";
               );

        let record = apply_final!(state, 123, SLT_End, "");

        assert_none!(state.get(123));

        assert!(record.is_backend_access());
        let backend = record.unwrap_backend_access();

        assert_matches!(backend, BackendAccessRecord {
            ident: 123,
            parent: 321,
            ref reason,
            ..
        } if reason == "fetch");
        assert_matches!(backend.http_transaction, HttpTransaction {
            start: 1469180762.484544,
            end: 1469180763.484544,
            ..
        });
        assert_eq!(backend.http_transaction.request, HttpRequest {
            method: "GET".to_string(),
            url: "/foobar".to_string(),
            protocol: "HTTP/1.1".to_string(),
            headers: vec![
                ("Host".to_string(), "localhost:8080".to_string()),
                ("User-Agent".to_string(), "curl/7.40.0".to_string())]
        });
        assert_eq!(backend.http_transaction.response, Some(HttpResponse {
            protocol: "HTTP/1.1".to_string(),
            status: 503,
            reason: "Backend fetch failed".to_string(),
            headers: vec![
                ("Date".to_string(), "Fri, 22 Jul 2016 09:46:02 GMT".to_string()),
                ("Server".to_string(), "Varnish".to_string()),
                ("Content-Type".to_string(), "text/html; charset=utf-8".to_string())]
        }));
    }

    #[test]
    fn apply_record_state_session() {
        let mut state = RecordState::new();

        apply_all!(state,
               123, SLT_Begin, "sess 0 HTTP/1";
               123, SLT_SessOpen, "192.168.1.10 40078 localhost:1080 127.0.0.1 1080 1469180762.484344 18";
               123, SLT_Link, "req 32773 rxreq";
               123, SLT_SessClose, "REM_CLOSE 0.001";
              );

        let record = apply_final!(state, 123, SLT_End, "");

        assert_none!(state.get(123));

        assert!(record.is_session());
        let session = record.unwrap_session();
        assert_eq!(session, SessionRecord {
            ident: 123,
            open: 1469180762.484344,
            duration: 0.001,
            local: Some(("127.0.0.1".to_string(), 1080)),
            remote: ("192.168.1.10".to_string(), 40078),
            client_requests: vec![32773],
        });
    }

    #[test]
    fn apply_session_state() {
        let mut state = SessionState::new();

        apply_all!(state,
               100, SLT_Begin,          "req 10 rxreq";
               100, SLT_Timestamp,      "Start: 1469180762.484544 0.000000 0.000000";
               100, SLT_ReqMethod,      "GET";
               100, SLT_ReqURL,         "/foobar";
               100, SLT_ReqProtocol,    "HTTP/1.1";
               100, SLT_ReqHeader,      "Host: localhost:8080";
               100, SLT_ReqHeader,      "User-Agent: curl/7.40.0";
               100, SLT_ReqHeader,      "Accept-Encoding: gzip";
               100, SLT_ReqUnset,       "Accept-Encoding: gzip";

               100, SLT_Link,           "bereq 1000 fetch";

               100, SLT_RespProtocol,   "HTTP/1.1";
               100, SLT_RespStatus,     "503";
               100, SLT_RespReason,     "Service Unavailable";
               100, SLT_RespReason,     "Backend fetch failed";
               100, SLT_RespHeader,     "Date: Fri, 22 Jul 2016 09:46:02 GMT";
               100, SLT_RespHeader,     "Server: Varnish";
               100, SLT_RespHeader,     "Cache-Control: no-store";
               100, SLT_RespUnset,      "Cache-Control: no-store";
               100, SLT_RespHeader,     "Content-Type: text/html; charset=utf-8";
               100, SLT_Timestamp,      "Resp: 1469180763.484544 0.000000 0.000000";
               100, SLT_End,            "";

               1000, SLT_Begin,         "bereq 100 fetch";
               1000, SLT_Timestamp,     "Start: 1469180762.484544 0.000000 0.000000";
               1000, SLT_BereqMethod,   "GET";
               1000, SLT_BereqURL,      "/foobar";
               1000, SLT_BereqProtocol, "HTTP/1.1";
               1000, SLT_BereqHeader,   "Host: localhost:8080";
               1000, SLT_BereqHeader,   "User-Agent: curl/7.40.0";
               1000, SLT_BereqHeader,   "Accept-Encoding: gzip";
               1000, SLT_BereqUnset,    "Accept-Encoding: gzip";
               1000, SLT_VCL_return,    "fetch";
               1000, SLT_Timestamp,     "Beresp: 1469180763.484544 0.000000 0.000000";
               1000, SLT_BerespProtocol, "HTTP/1.1";
               1000, SLT_BerespStatus,  "503";
               1000, SLT_BerespReason,  "Service Unavailable";
               1000, SLT_BerespReason,  "Backend fetch failed";
               1000, SLT_BerespHeader,  "Date: Fri, 22 Jul 2016 09:46:02 GMT";
               1000, SLT_BerespHeader,  "Server: Varnish";
               1000, SLT_BerespHeader,  "Cache-Control: no-store";
               1000, SLT_BerespUnset,   "Cache-Control: no-store";
               1000, SLT_BerespHeader,  "Content-Type: text/html; charset=utf-8";
               1000, SLT_End,           "";

               10, SLT_Begin,       "sess 0 HTTP/1";
               10, SLT_SessOpen,    "192.168.1.10 40078 localhost:1080 127.0.0.1 1080 1469180762.484344 18";
               10, SLT_Link,        "req 100 rxreq";
               10, SLT_SessClose,   "REM_CLOSE 0.001";
               );

        let session = apply_final!(state, 10, SLT_End, "");

        let client = session.client_transactions.get(0).unwrap().access_record.clone();
        assert_matches!(client, ClientAccessRecord {
            ident: 100,
            parent: 10,
            ref reason,
            ref backend_requests,
            ref esi_requests,
            ..
        } if
            reason == "rxreq" &&
            backend_requests == &[1000] &&
            esi_requests.is_empty()
        );
        assert_matches!(client.http_transaction, HttpTransaction {
            start: 1469180762.484544,
            end: 1469180763.484544,
            ..
        });
        assert_eq!(client.http_transaction.request, HttpRequest {
            method: "GET".to_string(),
            url: "/foobar".to_string(),
            protocol: "HTTP/1.1".to_string(),
            headers: vec![
                ("Host".to_string(), "localhost:8080".to_string()),
                ("User-Agent".to_string(), "curl/7.40.0".to_string())]
        });
        assert_eq!(client.http_transaction.response, Some(HttpResponse {
            protocol: "HTTP/1.1".to_string(),
            status: 503,
            reason: "Backend fetch failed".to_string(),
            headers: vec![
                ("Date".to_string(), "Fri, 22 Jul 2016 09:46:02 GMT".to_string()),
                ("Server".to_string(), "Varnish".to_string()),
                ("Content-Type".to_string(), "text/html; charset=utf-8".to_string())]
        }));

        let backend_transaction = session.client_transactions.get(0).unwrap().backend_transactions.get(0).unwrap();
        assert!(backend_transaction.retry_transaction.is_none());
        let backend = &backend_transaction.access_record;
        assert_matches!(backend, &BackendAccessRecord {
            ident: 1000,
            parent: 100,
            ref reason,
            ..
        } if reason == "fetch");
        assert_matches!(backend.http_transaction, HttpTransaction {
            start: 1469180762.484544,
            end: 1469180763.484544,
            ..
        });
        assert_eq!(backend.http_transaction.request, HttpRequest {
            method: "GET".to_string(),
            url: "/foobar".to_string(),
            protocol: "HTTP/1.1".to_string(),
            headers: vec![
                ("Host".to_string(), "localhost:8080".to_string()),
                ("User-Agent".to_string(), "curl/7.40.0".to_string())]
        });
        assert_eq!(backend.http_transaction.response, Some(HttpResponse {
            protocol: "HTTP/1.1".to_string(),
            status: 503,
            reason: "Backend fetch failed".to_string(),
            headers: vec![
                ("Date".to_string(), "Fri, 22 Jul 2016 09:46:02 GMT".to_string()),
                ("Server".to_string(), "Varnish".to_string()),
                ("Content-Type".to_string(), "text/html; charset=utf-8".to_string())]
        }));

        assert_eq!(session.record, SessionRecord {
            ident: 10,
            open: 1469180762.484344,
            duration: 0.001,
            local: Some(("127.0.0.1".to_string(), 1080)),
            remote: ("192.168.1.10".to_string(), 40078),
            client_requests: vec![100],
        });
    }

    #[test]
    fn apply_session_state_esi() {
        let mut state = SessionState::new();

        apply_all!(state,
               65540, SLT_Begin,        "bereq 65539 fetch";
               65540, SLT_Timestamp,    "Start: 1470304807.390145 0.000000 0.000000";
               65540, SLT_BereqMethod,  "GET";
               65540, SLT_BereqURL,     "/esi/hello";
               65540, SLT_BereqProtocol,"HTTP/1.1";
               65540, SLT_BereqHeader,  "X-Backend-Set-Header-X-Accel-ESI: true";
               65540, SLT_VCL_return,   "fetch";
               65540, SLT_BackendOpen,  "19 boot.default 127.0.0.1 42000 127.0.0.1 41744";
               65540, SLT_BackendStart, "127.0.0.1 42000";
               65540, SLT_Timestamp,    "Bereq: 1470304807.390223 0.000078 0.000078";
               65540, SLT_Timestamp,    "Beresp: 1470304807.395378 0.005234 0.005155";
               65540, SLT_BerespProtocol, "HTTP/1.1";
               65540, SLT_BerespStatus, "200";
               65540, SLT_BerespReason, "OK";
               65540, SLT_BerespHeader, "Content-Type: text/html; charset=utf-8";
               65540, SLT_Timestamp,    "BerespBody: 1470304807.435149 0.045005 0.039771";
               65540, SLT_Length,       "5";
               65540, SLT_BereqAcct,    "637 0 637 398 5 403";
               65540, SLT_End,          "";

               65541, SLT_Begin,        "req 65538 esi";
               65541, SLT_ReqURL,       "/esi/world";
               65541, SLT_Timestamp,    "Start: 1470304807.435266 0.000000 0.000000";
               65541, SLT_ReqStart,     "127.0.0.1 57408";
               65541, SLT_ReqMethod,    "GET";
               65541, SLT_ReqURL,       "/esi/world";
               65541, SLT_ReqProtocol,  "HTTP/1.1";
               65541, SLT_ReqHeader,    "X-Backend-Set-Header-X-Accel-ESI: true";
               65541, SLT_Link,         "bereq 65542 fetch";
               65541, SLT_Timestamp,    "Fetch: 1470304807.479151 0.043886 0.043886";
               65541, SLT_RespProtocol, "HTTP/1.1";
               65541, SLT_RespStatus,   "200";
               65541, SLT_RespReason,   "OK";
               65541, SLT_RespHeader,   "Content-Type: text/html; charset=utf-8";
               65541, SLT_Timestamp,    "Process: 1470304807.479171 0.043905 0.000019";
               65541, SLT_RespHeader,   "Accept-Ranges: bytes";
               65541, SLT_Timestamp,    "Resp: 1470304807.479196 0.043930 0.000025";
               65541, SLT_ReqAcct,      "0 0 0 0 5 5";
               65541, SLT_End,          "";

               65542, SLT_Begin,        "bereq 65541 fetch";
               65542, SLT_Timestamp,    "Start: 1470304807.435378 0.000000 0.000000";
               65542, SLT_BereqMethod,  "GET";
               65542, SLT_BereqURL,     "/esi/world";
               65542, SLT_BereqProtocol, "HTTP/1.1";
               65542, SLT_BereqHeader,  "X-Backend-Set-Header-X-Accel-ESI: true";
               65542, SLT_VCL_return,   "fetch";
               65542, SLT_BackendOpen,  "19 boot.default 127.0.0.1 42000 127.0.0.1 41744";
               65542, SLT_BackendStart, "127.0.0.1 42000";
               65542, SLT_Timestamp,    "Bereq: 1470304807.435450 0.000072 0.000072";
               65542, SLT_Timestamp,    "Beresp: 1470304807.439882 0.004504 0.004432";
               65542, SLT_BerespProtocol, "HTTP/1.1";
               65542, SLT_BerespStatus, "200";
               65542, SLT_BerespReason, "OK";
               65542, SLT_BerespHeader, "Content-Type: text/html; charset=utf-8";
               65542, SLT_Fetch_Body,   "3 length -";
               65542, SLT_BackendReuse, "19 boot.default";
               65542, SLT_Timestamp,    "BerespBody: 1470304807.479137 0.043759 0.039255";
               65542, SLT_Length,       "5";
               65542, SLT_BereqAcct,    "637 0 637 398 5 403";
               65542, SLT_End,          "";

               65538, SLT_Begin,        "req 65537 rxreq";
               65538, SLT_Timestamp,    "Start: 1470304807.389831 0.000000 0.000000";
               65538, SLT_Timestamp,    "Req: 1470304807.389831 0.000000 0.000000";
               65538, SLT_ReqStart,     "127.0.0.1 57408";
               65538, SLT_ReqMethod,    "GET";
               65538, SLT_ReqURL,       "/esi/index";
               65538, SLT_ReqProtocol,  "HTTP/1.1";
               65538, SLT_ReqHeader,    "X-Backend-Set-Header-X-Accel-ESI: true";
               65538, SLT_VCL_return,   "deliver";
               65538, SLT_RespProtocol, "HTTP/1.1";
               65538, SLT_RespStatus,   "200";
               65538, SLT_RespReason,   "OK";
               65538, SLT_RespHeader,   "Content-Type: text/html; charset=utf-8";
               65538, SLT_Link,         "req 65539 esi";
               65538, SLT_Link,         "req 65541 esi";
               65538, SLT_Timestamp,    "Resp: 1470304807.479222 0.089391 0.089199";
               65538, SLT_ReqAcct,      "220 0 220 1423 29 1452";
               65538, SLT_End,          "";

               65537, SLT_Begin,        "sess 0 HTTP/1";
               65537, SLT_SessOpen,     "127.0.0.1 57408 127.0.0.1:1221 127.0.0.1 1221 1470304807.389646 20";
               65537, SLT_Link,         "req 65538 rxreq";
               65537, SLT_SessClose,    "REM_CLOSE 3.228";
              );

        let session = apply_final!(state, 65537, SLT_End, "");

        // We will have esi_transactions in client request
        assert_eq!(session.client_transactions[0].esi_transactions[0].access_record.reason, "esi".to_string());
        assert_eq!(session.client_transactions[0].esi_transactions[0].backend_transactions[0].access_record.reason, "fetch".to_string());
        assert!(session.client_transactions[0].esi_transactions[0].esi_transactions.is_empty());
    }

    #[test]
    fn apply_session_state_grace() {
        let mut state = SessionState::new();

        apply_all!(state,
               65540, SLT_Begin,        "req 65539 rxreq";
               65540, SLT_Timestamp,    "Start: 1470304835.059319 0.000000 0.000000";
               65540, SLT_Timestamp,    "Req: 1470304835.059319 0.000000 0.000000";
               65540, SLT_ReqStart,     "127.0.0.1 59694";
               65540, SLT_ReqMethod,    "GET";
               65540, SLT_ReqURL,       "/test_page/123.html";
               65540, SLT_ReqProtocol,  "HTTP/1.1";
               65540, SLT_ReqHeader,    "X-Varnish-Force-Zero-TTL: true";
               65540, SLT_Hit,          "98307";
               65540, SLT_ReqHeader,    "X-Varnish-Result: hit/sick_grace";
               65540, SLT_VCL_return,   "deliver";
               65540, SLT_Link,         "bereq 65541 bgfetch";
               65540, SLT_Timestamp,    "Fetch: 1470304835.059472 0.000154 0.000154";
               65540, SLT_RespProtocol, "HTTP/1.1";
               65540, SLT_RespStatus,   "200";
               65540, SLT_RespReason,   "OK";
               65540, SLT_RespHeader,   "Content-Type: text/html; charset=utf-8";
               65540, SLT_RespHeader,   "X-Varnish-Privileged-Client: true";
               65540, SLT_Timestamp,    "Process: 1470304835.059589 0.000270 0.000117";
               65540, SLT_Timestamp,    "Resp: 1470304835.059629 0.000311 0.000041";
               65540, SLT_End,          "";

               65541, SLT_Begin,        "bereq 65540 bgfetch";
               65541, SLT_Timestamp,    "Start: 1470304835.059425 0.000000 0.000000";
               65541, SLT_BereqMethod,  "GET";
               65541, SLT_BereqURL,     "/test_page/123.html";
               65541, SLT_BereqProtocol,"HTTP/1.1";
               65541, SLT_BereqHeader,  "X-Varnish-Force-Zero-TTL: true";
               65541, SLT_Timestamp,    "Beresp: 1470304835.059475 0.000050 0.000050";
               65541, SLT_Timestamp,    "Error: 1470304835.059479 0.000054 0.000004";
               65541, SLT_BerespProtocol, "HTTP/1.1";
               65541, SLT_BerespStatus, "503";
               65541, SLT_BerespReason, "Service Unavailable";
               65541, SLT_BerespReason, "Backend fetch failed";
               65541, SLT_BerespHeader, "Date: Thu, 04 Aug 2016 10:00:35 GMT";
               65541, SLT_BerespHeader, "Server: Varnish";
               65541, SLT_Length,       "1366";
               65541, SLT_BereqAcct,    "0 0 0 0 0 0";
               65541, SLT_End,          "";

               65539, SLT_Begin,        "sess 0 HTTP/1";
               65539, SLT_SessOpen,     "127.0.0.1 59694 127.0.0.1:1230 127.0.0.1 1230 1470304835.059145 22";
               65539, SLT_Link,         "req 65540 rxreq";
               65539, SLT_SessClose,    "RX_TIMEOUT 10.001";
               );

            let session = apply_final!(state, 65539, SLT_End, "");

            // It is handled as ususal; only difference is backend request reason
            assert_eq!(session.client_transactions[0].backend_transactions[0].access_record.reason, "bgfetch".to_string());
   }

    #[test]
    fn apply_session_state_restart() {
        let mut state = SessionState::new();

        apply_all!(state,
                   32770, SLT_Begin,        "req 32769 rxreq";
                   32770, SLT_Timestamp,    "Start: 1470304882.576464 0.000000 0.000000";
                   32770, SLT_Timestamp,    "Req: 1470304882.576464 0.000000 0.000000";
                   32770, SLT_ReqStart,     "127.0.0.1 34560";
                   32770, SLT_ReqMethod,    "GET";
                   32770, SLT_ReqURL,       "/foo/thumbnails/foo/4006450256177f4a/bar.jpg?type=brochure";
                   32770, SLT_ReqProtocol,  "HTTP/1.1";
                   32770, SLT_ReqHeader,    "X-Backend-Set-Header-Cache-Control: public, max-age=12345";
                   32770, SLT_VCL_return,   "restart";
                   32770, SLT_Timestamp,    "Restart: 1470304882.576600 0.000136 0.000136";
                   32770, SLT_Link,         "req 32771 restart";
                   32770, SLT_End,          "";

                   32771, SLT_Begin,        "req 32770 restart";
                   32771, SLT_Timestamp,    "Start: 1470304882.576600 0.000136 0.000000";
                   32771, SLT_ReqStart,     "127.0.0.1 34560";
                   32771, SLT_ReqMethod,    "GET";
                   32771, SLT_ReqURL,       "/iss/v2/thumbnails/foo/4006450256177f4a/bar.jpg?type=brochure";
                   32771, SLT_ReqProtocol,  "HTTP/1.1";
                   32771, SLT_ReqHeader,    "X-Backend-Set-Header-Cache-Control: public, max-age=12345";
                   32771, SLT_VCL_return,   "fetch";
                   32771, SLT_Link,         "bereq 32772 fetch";
                   32771, SLT_Timestamp,    "Fetch: 1470304882.579218 0.002754 0.002618";
                   32771, SLT_RespProtocol, "HTTP/1.1";
                   32771, SLT_RespStatus,   "200";
                   32771, SLT_RespReason,   "OK";
                   32771, SLT_RespHeader,   "Content-Type: image/jpeg";
                   32771, SLT_VCL_return,   "deliver";
                   32771, SLT_Timestamp,    "Process: 1470304882.579312 0.002848 0.000094";
                   32771, SLT_RespHeader,   "Accept-Ranges: bytes";
                   32771, SLT_Debug,        "RES_MODE 2";
                   32771, SLT_RespHeader,   "Connection: keep-alive";
                   32771, SLT_Timestamp,    "Resp: 1470304882.615250 0.038785 0.035938";
                   32771, SLT_ReqAcct,      "324 0 324 1445 6962 8407";
                   32771, SLT_End,          "";

                   32772, SLT_Begin,        "bereq 32771 fetch";
                   32772, SLT_Timestamp,    "Start: 1470304882.576644 0.000000 0.000000";
                   32772, SLT_BereqMethod,  "GET";
                   32772, SLT_BereqURL,     "/iss/v2/thumbnails/foo/4006450256177f4a/bar.jpg?type=brochure";
                   32772, SLT_BereqProtocol, "HTTP/1.1";
                   32772, SLT_BereqHeader,  "X-Backend-Set-Header-Cache-Control: public, max-age=12345";
                   32772, SLT_Timestamp,    "Bereq: 1470304882.576719 0.000074 0.000074";
                   32772, SLT_Timestamp,    "Beresp: 1470304882.579056 0.002412 0.002337";
                   32772, SLT_BerespProtocol, "HTTP/1.1";
                   32772, SLT_BerespStatus, "200";
                   32772, SLT_BerespReason, "OK";
                   32772, SLT_BerespHeader, "Content-Type: image/jpeg";
                   32772, SLT_Fetch_Body,   "3 length stream";
                   32772, SLT_BackendReuse, "19 boot.iss";
                   32772, SLT_Timestamp,    "BerespBody: 1470304882.615228 0.038584 0.036172";
                   32772, SLT_Length,       "6962";
                   32772, SLT_BereqAcct,    "792 0 792 332 6962 7294";
                   32772, SLT_End,          "";

                   32769, SLT_Begin,        "sess 0 HTTP/1";
                   32769, SLT_SessOpen,     "127.0.0.1 34560 127.0.0.1:1244 127.0.0.1 1244 1470304882.576266 14";
                   32769, SLT_Link,         "req 32770 rxreq";
                   32769, SLT_SessClose,    "REM_CLOSE 0.347";
                   );
        let session = apply_final!(state, 32769, SLT_End, "");

        // The first request won't have response as it got restarted
        assert!(session.client_transactions[0].access_record.http_transaction.response.is_none());

        // We should have restart transaction
        let restart_transaction = assert_some!(session.client_transactions[0].restart_transaction.as_ref());

        // It should have a response
        assert!(restart_transaction.access_record.http_transaction.response.is_some());
    }

    #[test]
    fn apply_session_state_retry() {
        let mut state = SessionState::new();

        apply_all!(state,
                   8, SLT_Begin,        "bereq 7 fetch";
                   8, SLT_Timestamp,    "Start: 1470403414.664923 0.000000 0.000000";
                   8, SLT_BereqMethod,  "GET";
                   8, SLT_BereqURL,     "/retry";
                   8, SLT_BereqProtocol,"HTTP/1.1";
                   8, SLT_BereqHeader,  "Date: Fri, 05 Aug 2016 13:23:34 GMT";
                   8, SLT_VCL_return,   "fetch";
                   8, SLT_Timestamp,    "Bereq: 1470403414.664993 0.000070 0.000070";
                   8, SLT_Timestamp,    "Beresp: 1470403414.669313 0.004390 0.004320";
                   8, SLT_BerespProtocol, "HTTP/1.1";
                   8, SLT_BerespStatus, "200";
                   8, SLT_BerespReason, "OK";
                   8, SLT_BerespHeader, "Content-Type: text/html; charset=utf-8";
                   8, SLT_BereqURL,     "/iss/v2/thumbnails/foo/4006450256177f4a/bar.jpg";
                   8, SLT_VCL_return,   "retry";
                   8, SLT_BackendClose, "19 boot.default";
                   8, SLT_Timestamp,    "Retry: 1470403414.669375 0.004452 0.000062";
                   8, SLT_Link,         "bereq 32769 retry";
                   8, SLT_End,          "";

                   32769, SLT_Begin,        "bereq 8 retry";
                   32769, SLT_Timestamp,    "Start: 1470403414.669375 0.004452 0.000000";
                   32769, SLT_BereqMethod,  "GET";
                   32769, SLT_BereqURL,     "/iss/v2/thumbnails/foo/4006450256177f4a/bar.jpg";
                   32769, SLT_BereqProtocol,"HTTP/1.1";
                   32769, SLT_BereqHeader,  "Date: Fri, 05 Aug 2016 13:23:34 GMT";
                   32769, SLT_BereqHeader,  "Host: 127.0.0.1:1200";
                   32769, SLT_VCL_return,   "fetch";
                   32769, SLT_Timestamp,    "Bereq: 1470403414.669471 0.004549 0.000096";
                   32769, SLT_Timestamp,    "Beresp: 1470403414.672184 0.007262 0.002713";
                   32769, SLT_BerespProtocol, "HTTP/1.1";
                   32769, SLT_BerespStatus, "200";
                   32769, SLT_BerespReason, "OK";
                   32769, SLT_BerespHeader, "Content-Type: image/jpeg";
                   32769, SLT_Fetch_Body,   "3 length stream";
                   32769, SLT_BackendReuse, "19 boot.iss";
                   32769, SLT_Timestamp,    "BerespBody: 1470403414.672290 0.007367 0.000105";
                   32769, SLT_Length,       "6962";
                   32769, SLT_BereqAcct,    "1021 0 1021 608 6962 7570";
                   32769, SLT_End,          "";

                   7, SLT_Begin,        "req 6 rxreq";
                   7, SLT_Timestamp,    "Start: 1470403414.664824 0.000000 0.000000";
                   7, SLT_Timestamp,    "Req: 1470403414.664824 0.000000 0.000000";
                   7, SLT_ReqStart,     "127.0.0.1 39798";
                   7, SLT_ReqMethod,    "GET";
                   7, SLT_ReqURL,       "/retry";
                   7, SLT_ReqProtocol,  "HTTP/1.1";
                   7, SLT_ReqHeader,    "Date: Fri, 05 Aug 2016 13:23:34 GMT";
                   7, SLT_VCL_call,     "RECV";
                   7, SLT_VCL_return,   "fetch";
                   7, SLT_Link,         "bereq 8 fetch";
                   7, SLT_Timestamp,    "Fetch: 1470403414.672315 0.007491 0.007491";
                   7, SLT_RespProtocol, "HTTP/1.1";
                   7, SLT_RespStatus,   "200";
                   7, SLT_RespReason,   "OK";
                   7, SLT_RespHeader,   "Content-Type: image/jpeg";
                   7, SLT_VCL_return,   "deliver";
                   7, SLT_Timestamp,    "Process: 1470403414.672425 0.007601 0.000111";
                   7, SLT_RespHeader,   "Accept-Ranges: bytes";
                   7, SLT_Debug,        "RES_MODE 2";
                   7, SLT_RespHeader,   "Connection: keep-alive";
                   7, SLT_Timestamp,    "Resp: 1470403414.672458 0.007634 0.000032";
                   7, SLT_ReqAcct,      "82 0 82 304 6962 7266";
                   7, SLT_End,          "";

                   6, SLT_Begin,        "sess 0 HTTP/1";
                   6, SLT_SessOpen,     "127.0.0.1 39798 127.0.0.1:1200 127.0.0.1 1200 1470403414.664642 17";
                   6, SLT_Link,         "req 7 rxreq";
                   6, SLT_SessClose,    "REM_CLOSE 0.008";
                   );
        let session = apply_final!(state, 6, SLT_End, "");

        // Backend transaction request record will be the one from before retry (triggering)
        assert_eq!(session.client_transactions[0].backend_transactions[0].access_record.http_transaction.request.url, "/retry");

        // Backend transaction will have retrys
        let retry_transaction = assert_some!(session.client_transactions[0].backend_transactions[0].retry_transaction.as_ref());

        // It will have "retry" reason
        assert_eq!(retry_transaction.access_record.reason, "retry".to_string());
        assert!(retry_transaction.retry_transaction.is_none());
        assert_eq!(retry_transaction.access_record.http_transaction.request.url, "/iss/v2/thumbnails/foo/4006450256177f4a/bar.jpg");
    }
}
