/// TODO:
/// * Collect Log messages
/// * Collect errors: SLT_FetchError
/// * Collect Debug messages: SLT_Debug
/// * miss/hit etc
/// * client IP: SLT_SessOpen
/// * Call trace
/// * ACL trace
/// * Linking information: SLT_Link
/// * Byte counts: SLT_ReqAcct
/// * Handle the "<not set>" headers
/// * Support for non-UTF8 data lines - log warnings?
/// * more tests
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

use std::fmt::Debug;
use std::str::Utf8Error;
use std::num::{ParseIntError, ParseFloatError};
use nom::{self, IResult};
use quick_error::ResultExt;
use linked_hash_map::LinkedHashMap;

use vsl::{VslRecord, VslIdent, VslRecordTag};
use vsl::VslRecordTag::*;

pub type TimeStamp = f64;
pub type Duration = f64;
pub type Address = (String, u16);

#[derive(Debug, Clone, PartialEq)]
pub struct ClientAccessRecord {
    pub ident: VslIdent,
    pub parent: VslIdent,
    pub reason: String,
    pub esi_requests: Vec<VslIdent>,
    pub backend_requests: Vec<VslIdent>,
    pub restart_request: Option<VslIdent>,
    pub http_transaction: HttpTransaction,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BackendAccessRecord {
    pub ident: VslIdent,
    pub parent: VslIdent,
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

// Parsers

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

// Builders

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

    fn apply(self, tag: VslRecordTag, message: &str) -> Result<BuilderResult<B, C>, RecordBuilderError> where B: DetailBuilder<C>
    {
        let builder_result = if let Building(builder) = self {
            Building(try!(builder.apply(tag, message)))
        } else {
            debug!("Ignoring VSL record with tag {:?} and message '{}' as we have finished building {}", tag, message, B::result_name());
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
    fn apply(self, tag: VslRecordTag, message: &str) -> Result<Self, RecordBuilderError>;
    fn complete(self) -> Result<C, RecordBuilderError>;
}

#[derive(Debug)]
struct HttpRequestBuilder {
    protocol: Option<String>,
    method: Option<String>,
    url: Option<String>,
    headers: LinkedHashMap<String, String>,
}

impl HttpRequestBuilder {
    fn new() -> HttpRequestBuilder {
        HttpRequestBuilder {
            protocol: None,
            method: None,
            url: None,
            headers: LinkedHashMap::new(),
        }
    }
}

impl DetailBuilder<HttpRequest> for HttpRequestBuilder {
    fn result_name() -> &'static str {
        "HTTP Request"
    }

    fn apply(self, tag: VslRecordTag, message: &str) -> Result<HttpRequestBuilder, RecordBuilderError> {
        let builder = match tag {
            SLT_BereqProtocol | SLT_ReqProtocol => {
                let protocol = try!(slt_protocol(message).into_result().context(tag));

                HttpRequestBuilder {
                    protocol: Some(protocol.to_string()),
                    .. self
                }
            }
            SLT_BereqMethod | SLT_ReqMethod => {
                let method = try!(slt_method(message).into_result().context(tag));

                HttpRequestBuilder {
                    method: Some(method.to_string()),
                    .. self
                }
            }
            SLT_BereqURL | SLT_ReqURL => {
                let url = try!(slt_url(message).into_result().context(tag));

                HttpRequestBuilder {
                    url: Some(url.to_string()),
                    .. self
                }
            }
            SLT_BereqHeader | SLT_ReqHeader => {
                let (name, value) = try!(slt_header(message).into_result().context(tag));

                let mut headers = self.headers;
                headers.insert(name.to_string(), value.to_string());

                HttpRequestBuilder {
                    headers: headers,
                    .. self
                }
            }
            SLT_BereqUnset | SLT_ReqUnset => {
                let (name, _) = try!(slt_header(message).into_result().context(tag));

                let mut headers = self.headers;
                headers.remove(name);

                HttpRequestBuilder {
                    headers: headers,
                    .. self
                }
            }
            _ => panic!("Got unexpected VSL record with tag {:?} in request builder", tag)
        };

        Ok(builder)
    }

    fn complete(self) -> Result<HttpRequest, RecordBuilderError> {
        Ok(HttpRequest {
            protocol: try!(self.protocol.ok_or(RecordBuilderError::RecordIncomplete("Request.protocol"))),
            method: try!(self.method.ok_or(RecordBuilderError::RecordIncomplete("Request.method"))),
            url: try!(self.url.ok_or(RecordBuilderError::RecordIncomplete("Request.url"))),
            headers: self.headers.into_iter().collect(),
        })
    }
}

#[derive(Debug)]
struct HttpResponseBuilder {
    protocol: Option<String>,
    status: Option<u32>,
    reason: Option<String>,
    headers: LinkedHashMap<String, String>,
}

impl HttpResponseBuilder {
    fn new() -> HttpResponseBuilder {
        HttpResponseBuilder {
            protocol: None,
            status: None,
            reason: None,
            headers: LinkedHashMap::new(),
        }
    }
}

impl DetailBuilder<HttpResponse> for HttpResponseBuilder {
    fn result_name() -> &'static str {
        "HTTP Response"
    }

    fn apply(self, tag: VslRecordTag, message: &str) -> Result<HttpResponseBuilder, RecordBuilderError> {
        let builder = match tag {
            SLT_BerespProtocol | SLT_RespProtocol => {
                let protocol = try!(slt_protocol(message).into_result().context(tag));

                HttpResponseBuilder {
                    protocol: Some(protocol.to_string()),
                    .. self
                }
            }
            SLT_BerespStatus | SLT_RespStatus => {
                let status = try!(slt_status(message).into_result().context(tag));

                HttpResponseBuilder {
                    status: Some(try!(status.parse().context("status"))),
                    .. self
                }
            }
            SLT_BerespReason | SLT_RespReason => {
                let reason = try!(slt_reason(message).into_result().context(tag));

                HttpResponseBuilder {
                    reason: Some(reason.to_string()),
                    .. self
                }
            }
            SLT_BerespHeader | SLT_RespHeader => {
                let (name, value) = try!(slt_header(message).into_result().context(tag));

                let mut headers = self.headers;
                headers.insert(name.to_string(), value.to_string());

                HttpResponseBuilder {
                    headers: headers,
                    .. self
                }
            }
            SLT_BerespUnset | SLT_RespUnset => {
                let (name, _) = try!(slt_header(message).into_result().context(tag));

                let mut headers = self.headers;
                headers.remove(name);

                HttpResponseBuilder {
                    headers: headers,
                    .. self
                }
            }
            _ => panic!("Got unexpected VSL record with tag {:?} in response builder", tag)
        };

        Ok(builder)
    }

    fn complete(self) -> Result<HttpResponse, RecordBuilderError> {
        Ok(HttpResponse {
            protocol: try!(self.protocol.ok_or(RecordBuilderError::RecordIncomplete("Response.protocol"))),
            status: try!(self.status.ok_or(RecordBuilderError::RecordIncomplete("Response.status"))),
            reason: try!(self.reason.ok_or(RecordBuilderError::RecordIncomplete("Response.reason"))),
            headers: self.headers.into_iter().collect(),
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
pub struct RecordBuilder {
    ident: VslIdent,
    record_type: Option<RecordType>,
    req_start: Option<TimeStamp>,
    http_request: BuilderResult<HttpRequestBuilder, HttpRequest>,
    http_response: BuilderResult<HttpResponseBuilder, HttpResponse>,
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

impl RecordBuilder {
    pub fn new(ident: VslIdent) -> RecordBuilder {
        RecordBuilder {
            ident: ident,
            record_type: None,
            req_start: None,
            http_request: Building(HttpRequestBuilder::new()),
            http_response: Building(HttpResponseBuilder::new()),
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

    pub fn apply<'r>(self, vsl: &'r VslRecord) -> Result<BuilderResult<RecordBuilder, Record>, RecordBuilderError> {
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

                // Request
                SLT_BereqProtocol | SLT_ReqProtocol |
                SLT_BereqMethod | SLT_ReqMethod |
                SLT_BereqURL | SLT_ReqURL |
                SLT_BereqHeader | SLT_ReqHeader |
                SLT_BereqUnset | SLT_ReqUnset => {
                    RecordBuilder {
                        http_request: try!(self.http_request.apply(vsl.tag, message)),
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
                        http_response: try!(self.http_response.apply(vsl.tag, message)),
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
                SLT_SessClose => {
                    let (_reason, duration) = try!(slt_sess_close(message).into_result().context(vsl.tag));

                    RecordBuilder {
                        sess_duration: Some(try!(duration.parse().context("duration"))),
                        .. self
                    }
                }

                // Final
                SLT_VCL_call => {
                    let method = try!(stl_call(message).into_result().context(vsl.tag));

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
                            warn!("Ignoring unknown {:?} method: {}", vsl.tag, method);
                            self
                        }
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

#[cfg(test)]
mod tests {
    pub use super::*;
    pub use super::super::super::test_helpers::*;

    macro_rules! apply {
        ($state:ident, $ident:expr, $tag:ident, $message:expr) => {{
            let res = $state.apply(&vsl($tag, $ident, $message));
            if let Err(err) = res {
                panic!("expected apply to return Ok after applying: `{}, {:?}, {};`; got: {}", $ident, $tag, $message, err)
            }
            res.unwrap().unwrap_building()
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
    fn apply_non_utf8() {
        let builder = RecordBuilder::new(1);

        use vsl::VslRecord;
        let result = builder.apply(&VslRecord {
            tag: SLT_Begin,
            marker: 0,
            ident: 123,
            data: &[255, 0, 1, 2, 3]
        });

        assert!(result.is_err());
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
            RecordBuilderError::InvalidMessageFieldFormat(field_name, _) if field_name == "vxid");
    }

    #[test]
    fn apply_timestamp() {
        let builder = RecordBuilder::new(123);

        let builder = builder.apply(&vsl(SLT_Timestamp, 123, "Start: 1469180762.484544 0.000000 0.000000"))
            .unwrap().unwrap_building();

        assert_eq!(builder.req_start, Some(1469180762.484544));
    }

    #[test]
    fn apply_backend_request_response() {
        let builder = RecordBuilder::new(123);

        let builder = apply_all!(builder,
                                 123, SLT_Timestamp,        "Start: 1469180762.484544 0.000000 0.000000";
                                 123, SLT_BereqMethod,      "GET";
                                 123, SLT_BereqURL,         "/foobar";
                                 123, SLT_BereqProtocol,    "HTTP/1.1";
                                 123, SLT_BereqHeader,      "Host: localhost:8080";
                                 123, SLT_BereqHeader,      "User-Agent: curl/7.40.0";
                                 123, SLT_BereqHeader,      "Accept-Encoding: gzip";
                                 123, SLT_BereqUnset,       "Accept-Encoding: gzip";
                                 123, SLT_Timestamp,        "Beresp: 1469180762.484544 0.000000 0.000000";
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

        assert_eq!(builder.req_start, Some(1469180762.484544));

        let request = builder.http_request.as_ref().unwrap();
        assert_eq!(request.method, "GET".to_string());
        assert_eq!(request.url, "/foobar".to_string());
        assert_eq!(request.protocol, "HTTP/1.1".to_string());
        assert_eq!(request.headers, &[
                   ("Host".to_string(), "localhost:8080".to_string()),
                   ("User-Agent".to_string(), "curl/7.40.0".to_string())]);

        assert_eq!(builder.resp_end, Some(1469180762.484544));

        let response = builder.http_response.as_ref().unwrap();
        assert_eq!(response.protocol, "HTTP/1.1".to_string());
        assert_eq!(response.status, 503);
        assert_eq!(response.reason, "Backend fetch failed".to_string());
        assert_eq!(response.headers, &[
                   ("Date".to_string(), "Fri, 22 Jul 2016 09:46:02 GMT".to_string()),
                   ("Server".to_string(), "Varnish".to_string())]);
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
}
