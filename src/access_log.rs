use std::collections::HashMap;
use std::str::Utf8Error;
use std::num::{ParseIntError, ParseFloatError};
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
#[derive(Debug, Clone, PartialEq)]
pub struct ClientAccessRecord {
    pub ident: VslIdent,
    pub parent: VslIdent, // Session or anothre Client (ESI)
    pub reason: String,
    pub esi_requests: Vec<VslIdent>,
    pub backend_requests: Vec<VslIdent>,
    pub http_transaction: HttpTransaction,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BackendAccessRecord {
    pub ident: VslIdent,
    pub parent: VslIdent, // Client
    pub reason: String,
    pub http_transaction: HttpTransaction,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SessionRecord {
    pub ident: VslIdent,
    pub open: TimeStamp,
    pub duration: Duration,
    pub local: Option<Address>,
    pub remote: Address,
    pub client_requests: Vec<VslIdent>
}

// TODO: store duration (use relative timing (?) from log as TS can go backwards)
// check Varnish code to see if relative timing is immune to clock going backwards
#[derive(Debug, Clone, PartialEq)]
pub struct HttpTransaction {
    pub start: TimeStamp,
    pub end: TimeStamp,
    pub request: HttpRequest,
    pub response: HttpResponse,
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

#[derive(Debug, Clone)]
struct RecordBuilder {
    ident: VslIdent,
    record_type: Option<RecordType>,
    req_start: Option<TimeStamp>,
    req_protocol: Option<String>,
    req_method: Option<String>,
    req_url: Option<String>,
    req_headers: LinkedHashMap<String, String>,
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
enum RecordBuilderResult {
    Building(RecordBuilder),
    Complete(Record),
}

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

impl RecordBuilder {
    fn new(ident: VslIdent) -> RecordBuilder {
        RecordBuilder {
            ident: ident,
            record_type: None,
            req_start: None,
            req_protocol: None,
            req_method: None,
            req_url: None,
            req_headers: LinkedHashMap::new(),
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
        }
    }

    fn apply<'r>(self, vsl: &'r VslRecord) -> Result<RecordBuilderResult, RecordBuilderError> {
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
                        _ => {
                            warn!("Ignoring unknown SLT_Timestamp label variant: {}", label);
                            self
                        }
                    }
                }

                // Request
                SLT_BereqProtocol | SLT_ReqProtocol => {
                    let protocol = try!(slt_protocol(message).into_result().context(vsl.tag));

                    RecordBuilder {
                        req_protocol: Some(protocol.to_string()),
                        .. self
                    }
                }
                SLT_BereqMethod | SLT_ReqMethod => {
                    let method = try!(slt_method(message).into_result().context(vsl.tag));

                    RecordBuilder {
                        req_method: Some(method.to_string()),
                        .. self
                    }
                }
                SLT_BereqURL | SLT_ReqURL => {
                    let url = try!(slt_url(message).into_result().context(vsl.tag));

                    RecordBuilder {
                        req_url: Some(url.to_string()),
                        .. self
                    }
                }
                //TODO: lock header manip after request/response was sent
                SLT_BereqHeader | SLT_ReqHeader => {
                    let (name, value) = try!(slt_header(message).into_result().context(vsl.tag));

                    let mut headers = self.req_headers;
                    headers.insert(name.to_string(), value.to_string());

                    RecordBuilder {
                        req_headers: headers,
                        .. self
                    }
                }
                SLT_BereqUnset | SLT_ReqUnset => {
                    let (name, _) = try!(slt_header(message).into_result().context(vsl.tag));

                    let mut headers = self.req_headers;
                    headers.remove(name);

                    RecordBuilder {
                        req_headers: headers,
                        .. self
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
                    let (reason, child_vxid, _child_type) = try!(slt_link(message).into_result().context(vsl.tag));

                    let vxid = try!(child_vxid.parse().context("vxid"));

                    match reason {
                        "req" => {
                            let mut client_requests = self.client_requests;
                            client_requests.push(vxid);

                            RecordBuilder {
                                client_requests: client_requests,
                                .. self
                            }
                        },
                        "bereq" => {
                            let mut backend_requests = self.backend_requests;
                            backend_requests.push(vxid);

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

                            return Ok(RecordBuilderResult::Complete(Record::Session(record)))
                        },
                        RecordType::ClientAccess { .. } | RecordType::BackendAccess { .. } => {
                            // Try to build AccessRecord
                            let request = HttpRequest {
                                protocol: try!(self.req_protocol.ok_or(RecordBuilderError::RecordIncomplete("req_protocol"))),
                                method: try!(self.req_method.ok_or(RecordBuilderError::RecordIncomplete("req_method"))),
                                url: try!(self.req_url.ok_or(RecordBuilderError::RecordIncomplete("req_url"))),
                                headers: self.req_headers.into_iter().collect(),
                            };

                            let response = HttpResponse {
                                protocol: try!(self.resp_protocol.ok_or(RecordBuilderError::RecordIncomplete("resp_protocol"))),
                                status: try!(self.resp_status.ok_or(RecordBuilderError::RecordIncomplete("resp_status"))),
                                reason: try!(self.resp_reason.ok_or(RecordBuilderError::RecordIncomplete("resp_reason"))),
                                headers: self.resp_headers.into_iter().collect(),
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
                                        http_transaction: http_transaction
                                    };

                                    return Ok(RecordBuilderResult::Complete(Record::ClientAccess(record)))
                                },
                                RecordType::BackendAccess { parent, reason } => {
                                    let record = BackendAccessRecord {
                                        ident: self.ident,
                                        parent: parent,
                                        reason: reason,
                                        http_transaction: http_transaction
                                    };

                                    return Ok(RecordBuilderResult::Complete(Record::BackendAccess(record)))
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

        Ok(RecordBuilderResult::Building(builder))
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
                RecordBuilderResult::Building(builder) => {
                    self.builders.insert(vsl.ident, builder);
                    return None
                }
                RecordBuilderResult::Complete(record) => return Some(record),
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

#[derive(Debug)]
pub struct ProxyTransaction {
    client: ClientAccessRecord,
    backend: Vec<BackendAccessRecord>,
    esi: Vec<ProxyTransaction>,
}

#[derive(Debug)]
pub struct Session {
    session_record: SessionRecord,
    proxy_transactions: Vec<ProxyTransaction>,
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

    fn build_proxy_transaction(&mut self, session: &SessionRecord, client: ClientAccessRecord) -> ProxyTransaction {
        let backend = client.backend_requests.iter()
            .map(|ident| self.backend.remove(ident).ok_or(ident))
            .inspect(|record| if let &Err(ident) = record {
                error!("Session {} references ClientAccessRecord {} which references BackendAccessRecord {} that was not found: {:?} in session: {:?}", session.ident, client.ident, ident, client, session) })
            .flat_map(Result::into_iter)
            .collect::<Vec<_>>();

        let esi_client = client.esi_requests.iter()
            .map(|ident| self.client.remove(ident).ok_or(ident))
            .inspect(|record| if let &Err(ident) = record {
                error!("Session {} references ClientAccessRecord {} which references ESI ClientAccessRecord {} wich was not found: {:?} in session: {:?}", session.ident, client.ident, ident, client, session) })
            .flat_map(Result::into_iter)
            .collect::<Vec<_>>();

        let esi = esi_client.into_iter()
            .map(|client| self.build_proxy_transaction(session, client))
            .collect();

        ProxyTransaction {
            client: client,
            backend: backend,
            esi: esi,
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
                let client_requests = session.client_requests.iter()
                    .map(|ident| self.client.remove(ident).ok_or(ident))
                    .inspect(|record| if let &Err(ident) = record {
                        error!("Session {} references ClientAccessRecord {} which was not found: {:?}", session.ident, ident, session) })
                    .flat_map(Result::into_iter)
                    .collect::<Vec<_>>();

                let proxy_transactions = client_requests.into_iter()
                    .map(|client| self.build_proxy_transaction(&session, client))
                    .collect();

                Some(Session {
                    session_record: session,
                    proxy_transactions: proxy_transactions,
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
        ($state:ident, $ident:expr, $tag:ident, $message:expr; $($t_ident:expr, $t_tag:ident, $t_message:expr;)+) => {{
            apply!($state, $ident, $tag, $message;);
            apply!($state, $($t_ident, $t_tag, $t_message;)*);
        }};

        ($state:ident, $ident:expr, $tag:ident, $message:expr;) => {{
            let opt: Option<_> = $state.apply(&vsl($tag, $ident, $message));
            assert!(opt.is_none(), "expected apply to return None after applying: `{:?}`", $tag);
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

        let builder = state.get(123).unwrap().clone();
        let record_type = builder.record_type.unwrap();

        assert_matches!(record_type, RecordType::BackendAccess { parent: 321, ref reason } if reason == "fetch");
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

        apply!(state,
               123, SLT_Timestamp, "Start: 1469180762.484544 0.000000 0.000000";
               123, SLT_BereqMethod, "GET";
               123, SLT_BereqURL, "/foobar";
               123, SLT_BereqProtocol, "HTTP/1.1";
               123, SLT_BereqHeader, "Host: localhost:8080";
               123, SLT_BereqHeader, "User-Agent: curl/7.40.0";
               123, SLT_BereqHeader, "Accept-Encoding: gzip";
               123, SLT_BereqUnset, "Accept-Encoding: gzip";
              );

        let builder = state.get(123).unwrap().clone();
        assert_eq!(builder.req_start, Some(1469180762.484544));
        assert_eq!(builder.req_method, Some("GET".to_string()));
        assert_eq!(builder.req_url, Some("/foobar".to_string()));
        assert_eq!(builder.req_protocol, Some("HTTP/1.1".to_string()));
        assert_eq!(builder.req_headers.get("Host"), Some(&"localhost:8080".to_string()));
        assert_eq!(builder.req_headers.get("User-Agent"), Some(&"curl/7.40.0".to_string()));
        assert_eq!(builder.req_headers.get("Accept-Encoding"), None);
    }

    #[test]
    fn apply_backend_response() {
        let mut state = RecordState::new();

        apply!(state,
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

        let builder = state.get(123).unwrap().clone();
        assert_eq!(builder.resp_end, Some(1469180762.484544));
        assert_eq!(builder.resp_protocol, Some("HTTP/1.1".to_string()));
        assert_eq!(builder.resp_status, Some(503));
        assert_eq!(builder.resp_reason, Some("Backend fetch failed".to_string()));
        assert_eq!(builder.resp_headers.get("Date"), Some(&"Fri, 22 Jul 2016 09:46:02 GMT".to_string()));
        assert_eq!(builder.resp_headers.get("Server"), Some(&"Varnish".to_string()));
        assert_eq!(builder.resp_headers.get("Cache-Control"), None);
    }

    #[test]
    fn apply_client_transaction() {
        let mut state = RecordState::new();

        apply!(state,
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
        assert_eq!(client.http_transaction.response, HttpResponse {
            protocol: "HTTP/1.1".to_string(),
            status: 503,
            reason: "Backend fetch failed".to_string(),
            headers: vec![
                ("Date".to_string(), "Fri, 22 Jul 2016 09:46:02 GMT".to_string()),
                ("Server".to_string(), "Varnish".to_string()),
                ("Content-Type".to_string(), "text/html; charset=utf-8".to_string())]
        });
    }

    #[test]
    fn apply_backend_transaction() {
        let mut state = RecordState::new();

        apply!(state,
               123, SLT_Begin, "bereq 321 fetch";
               123, SLT_Timestamp, "Start: 1469180762.484544 0.000000 0.000000";
               123, SLT_BereqMethod, "GET";
               123, SLT_BereqURL, "/foobar";
               123, SLT_BereqProtocol, "HTTP/1.1";
               123, SLT_BereqHeader, "Host: localhost:8080";
               123, SLT_BereqHeader, "User-Agent: curl/7.40.0";
               123, SLT_BereqHeader, "Accept-Encoding: gzip";
               123, SLT_BereqUnset, "Accept-Encoding: gzip";

               123, SLT_Timestamp, "Beresp: 1469180763.484544 0.000000 0.000000";
               123, SLT_BerespProtocol, "HTTP/1.1";
               123, SLT_BerespStatus, "503";
               123, SLT_BerespReason, "Service Unavailable";
               123, SLT_BerespReason, "Backend fetch failed";
               123, SLT_BerespHeader, "Date: Fri, 22 Jul 2016 09:46:02 GMT";
               123, SLT_BerespHeader, "Server: Varnish";
               123, SLT_BerespHeader, "Cache-Control: no-store";
               123, SLT_BerespUnset, "Cache-Control: no-store";
               123, SLT_BerespHeader, "Content-Type: text/html; charset=utf-8";
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
        assert_eq!(backend.http_transaction.response, HttpResponse {
            protocol: "HTTP/1.1".to_string(),
            status: 503,
            reason: "Backend fetch failed".to_string(),
            headers: vec![
                ("Date".to_string(), "Fri, 22 Jul 2016 09:46:02 GMT".to_string()),
                ("Server".to_string(), "Varnish".to_string()),
                ("Content-Type".to_string(), "text/html; charset=utf-8".to_string())]
        });
    }

    #[test]
    fn apply_session() {
        let mut state = RecordState::new();

        apply!(state,
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

        apply!(state,
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

        let client = session.proxy_transactions.get(0).unwrap().client.clone();
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
        assert_eq!(client.http_transaction.response, HttpResponse {
            protocol: "HTTP/1.1".to_string(),
            status: 503,
            reason: "Backend fetch failed".to_string(),
            headers: vec![
                ("Date".to_string(), "Fri, 22 Jul 2016 09:46:02 GMT".to_string()),
                ("Server".to_string(), "Varnish".to_string()),
                ("Content-Type".to_string(), "text/html; charset=utf-8".to_string())]
        });

        let backend = session.proxy_transactions.get(0).unwrap().backend.get(0).unwrap();
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
        assert_eq!(backend.http_transaction.response, HttpResponse {
            protocol: "HTTP/1.1".to_string(),
            status: 503,
            reason: "Backend fetch failed".to_string(),
            headers: vec![
                ("Date".to_string(), "Fri, 22 Jul 2016 09:46:02 GMT".to_string()),
                ("Server".to_string(), "Varnish".to_string()),
                ("Content-Type".to_string(), "text/html; charset=utf-8".to_string())]
        });

        assert_eq!(session.session_record, SessionRecord {
            ident: 10,
            open: 1469180762.484344,
            duration: 0.001,
            local: Some(("127.0.0.1".to_string(), 1080)),
            remote: ("192.168.1.10".to_string(), 40078),
            client_requests: vec![100],
        });
    }
}
