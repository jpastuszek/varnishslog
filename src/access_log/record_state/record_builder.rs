// Client headers:
// ---
//
// Req:
// * What client set us (SLT_VCL_call RECV)
//
// Resp:
// * What we sent to the client (SLT_End)
//
// Backend headers:
// ---
//
// Bereq:
// * What we sent to the backend (SLT_VCL_call BACKEND_RESPONSE or BACKEND_ERROR)
// * Note that (SLT_VCL_return fetch) is also used by req
//
// Beresp:
// * What backend sent us (SLT_VCL_call BACKEND_RESPONSE or BACKEND_ERROR)
//
// Record types:
// ---
// * Client access transaction
//   * full
//   * restarted (logs-new/varnish20160816-4093-c0f5tz5609f5ab778e4a4eb.vsl)
//     * has SLT_VCL_return with restart [trigger]
//     * has SLT_Link with restart
//     * has SLT_Timestamp with Restart
//     * won't have response
//     * won't have certain timing info
//     * won't have accounting
//   * piped (logs-new/varnish20160816-4093-s54h6nb4b44b69f1b2c7ca2.vsl)
//     * won't have response
//     * will have special timing info (Pipe, PipeSess)
//     * will have special accounting (SLT_PipeAcct)
//   * ESI
//     * no processing time
//     * otherwise quite normal but linked
//
// * Backend access transaction
//   * full
//   * aborted
//     * won't have response
//     * won't have end timestamp
//   * retried
//   * piped (logs-new/varnish20160816-4093-s54h6nb4b44b69f1b2c7ca2.vsl)
//     * won't have response
//     * will have special timing info
//     * won't have end timestamp
//
// Timestamps
// ===
//
// Req (logs/varnish20160805-3559-f6sifo45103025c06abad14.vsl):
// ---
// * process (req_process) - Start to Req
// * fetch (resp_fetch) - Req to Fetch
// * ttfb (resp_ttfb) - Start to Process
// * serve (req_took)- Start to Resp
//
// Note that we may have no process time for ESI requests as they don't get Req: record
//
//     2 SLT_Timestamp      Start: 1470403414.647192 0.000000 0.000000
//     2 SLT_Timestamp      Req: 1470403414.647192 0.000000 0.000000
//     2 SLT_ReqStart       127.0.0.1 39792
//     2 SLT_VCL_call       RECV
//     2 SLT_VCL_call       HASH
//     2 SLT_VCL_return     lookup
//     2 SLT_VCL_call       SYNTH
//     2 SLT_Timestamp      Process: 1470403414.647272 0.000081 0.000081
//     2 SLT_VCL_return     deliver
//     2 SLT_RespHeader     Connection: keep-alive
//     2 SLT_Timestamp      Resp: 1470403414.647359 0.000167 0.000086
//     2 SLT_ReqAcct        148 0 148 185 25 210
//     2 SLT_End
//
//     4 SLT_Timestamp      Start: 1470403414.653332 0.000000 0.000000
//     4 SLT_Timestamp      Req: 1470403414.653332 0.000000 0.000000
//     4 SLT_ReqStart       127.0.0.1 39794
//     4 SLT_VCL_call       MISS
//     4 SLT_ReqHeader      X-Varnish-Result: miss
//     4 SLT_VCL_return     fetch
//     4 SLT_Link           bereq 5 fetch
//     4 SLT_Timestamp      Fetch: 1470403414.658863 0.005531 0.005531
//     4 SLT_VCL_call       DELIVER
//     4 SLT_VCL_return     deliver
//     4 SLT_Timestamp      Process: 1470403414.658956 0.005624 0.000093
//     4 SLT_Debug          RES_MODE 2
//     4 SLT_RespHeader     Connection: keep-alive
//     4 SLT_Timestamp      Resp: 1470403414.658984 0.005652 0.000028
//     4 SLT_ReqAcct 90 0 90 369 9 378 4 SLT_End
//
// Bereq:
// ---
// Note that we may not have process time as backend request can be aborted in vcl_backend_fetch.
//
// * send (req_process) - Start to Bereq
// * ttfb (resp_ttfb) - Start to Beresp
// * wait (resp_fetch) - Bereq to Beresp
// * fetch (req_took) - Start to BerespBody
//
//     5 SLT_Begin          bereq 4 fetch
//     5 SLT_Timestamp      Start: 1470403414.653455 0.000000 0.000000
//     5 SLT_VCL_return     fetch
//     5 SLT_BackendOpen    19 boot.default 127.0.0.1 42001 127.0.0.1 37606
//     5 SLT_BackendStart   127.0.0.1 42001
//     5 SLT_Timestamp      Bereq: 1470403414.653592 0.000137 0.000137
//     5 SLT_Timestamp      Beresp: 1470403414.658717 0.005262 0.005124
//     5 SLT_Timestamp      BerespBody: 1470403414.658833 0.005378 0.000116
//     5 SLT_Length         9
//     5 SLT_BereqAcct      504 0 504 351 9 360
//     5 SLT_End
//

use maybe_string::{MaybeStr, MaybeString};
use vsl::record::{
    VslRecordTag,
    VslIdent,
    VslRecord,
    VslRecordParseError,
};
use vsl::record::VslRecordTag::*;
use vsl::record::message::parser::*;

use access_log::record::{
    TimeStamp,
    Duration,
    Status,
    Address,
    LogEntry,
    Compression,
    Accounting,
    PipeAccounting,
    Handling,
    Link,
    ClientAccessRecord,
    ClientAccessTransaction,
    CacheObject,
    BackendConnection,
    BackendAccessRecord,
    BackendAccessTransaction,
    SessionRecord,
    HttpRequest,
    HttpResponse,
    Record,
};

quick_error! {
    #[derive(Debug)]
    pub enum RecordBuilderError {
        UnexpectedTag(tag: VslRecordTag, expected: VslRecordTag) {
            display("Got unexpected tag: {:?} expected: {:?}", tag, expected)
        }
        UnimplementedTransactionType(record_type: String) {
            display("Unimplemented record type '{}'", record_type)
        }
        UnexpectedTransition(transition: &'static str) {
            display("Unexpected transition '{}' while building record", transition) // TODO: more info on state
        }
        InvalidMessageFormat(err: VslRecordParseError) {
            display("Failed to parse VSL record data: {}", err)
            from()
        }
        DetailIncomplete(detail_name: &'static str) {
            display("Expected {} to be complete but it was still building", detail_name)
        }
        RecordIncomplete(field_name: &'static str) {
            display("Failed to construct final access record due to missing field '{}'", field_name)
        }
    }
}

trait MutBuilder {
    type C;
    type E;

    // returns Ok(true) if complete
    fn apply<'r>(&mut self, mutagen: &VslRecord<'r>) -> Result<bool, Self::E>;

    // returns new value based on current
    fn build(self) -> Result<Self::C, Self::E>;
}

#[derive(Debug)]
struct MutBuilderState<B> {
    inner: B,
    complete: bool,
}

impl<B, C, E> MutBuilderState<B> where B: MutBuilder<C=C, E=E>  {
    fn new(inner: B) -> MutBuilderState<B> {
        MutBuilderState {
            inner: inner,
            complete: false,
        }
    }

    fn apply<'r>(&mut self, val: &VslRecord<'r>) -> Result<bool, E> {
        if !self.complete {
            self.complete = try!(self.inner.apply(val));
        }
        Ok(self.complete)
    }

    // complete needs to be callable with &mut self
    fn complete(&mut self) {
        self.complete = true;
    }

    fn is_building(&self) -> bool {
        !self.complete
    }
}

impl<B> MutBuilderState<B> {
    fn build<C>(self) -> Result<C, RecordBuilderError> where B: DetailBuilder<C>  {
        if self.complete {
            return self.inner.build()
        }
        Err(RecordBuilderError::DetailIncomplete(B::result_name()))
    }
}

trait DetailBuilder<C>: MutBuilder<C=C, E=RecordBuilderError> + Sized {
    fn result_name() -> &'static str;
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

    fn set(&mut self, name: MaybeString, value: MaybeString) {
        self.headers.push((name, value));
    }

    fn unset(&mut self, name: &MaybeStr, value: &MaybeStr) {
        self.headers.retain(|header| {
            let &(ref t_name, ref t_value) = header;
            (t_name.as_maybe_str(), t_value.as_maybe_str()) != (name, value)
        });
    }

    fn build(self) -> Vec<(MaybeString, MaybeString)> {
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
}

impl MutBuilder for HttpRequestBuilder {
    type C = HttpRequest;
    type E = RecordBuilderError;

    fn apply<'r>(&mut self, vsl: &VslRecord<'r>) -> Result<bool, RecordBuilderError> {
        match vsl.tag {
            SLT_BereqProtocol | SLT_ReqProtocol => {
                let protocol = try!(vsl.parse_data(slt_protocol));
                self.protocol = Some(protocol.to_lossy_string());
            }
            SLT_BereqMethod | SLT_ReqMethod => {
                let method = try!(vsl.parse_data(slt_method));
                self.method = Some(method.to_lossy_string());
            }
            SLT_BereqURL | SLT_ReqURL => {
                let url = try!(vsl.parse_data(slt_url));
                self.url = Some(url.to_lossy_string());
            }
            SLT_BereqHeader | SLT_ReqHeader => if let (name, Some(value)) = try!(vsl.parse_data(slt_header)) {
                self.headers.set(name.to_maybe_string(), value.to_maybe_string());
            } else {
                debug!("Not setting empty request header: {:?}", vsl);
            },
            SLT_BereqUnset | SLT_ReqUnset => if let (name, Some(value)) = try!(vsl.parse_data(slt_header)) {
                self.headers.unset(name, value);
            } else {
                debug!("Not unsetting empty request header: {:?}", vsl);
            },
            _ => panic!("Got unexpected VSL record in request builder: {:?}", vsl)
        };

        Ok(false)
    }

    fn build(self) -> Result<HttpRequest, RecordBuilderError> {
        Ok(HttpRequest {
            protocol: try!(self.protocol.ok_or(RecordBuilderError::RecordIncomplete("Request.protocol"))),
            method: try!(self.method.ok_or(RecordBuilderError::RecordIncomplete("Request.method"))),
            url: try!(self.url.ok_or(RecordBuilderError::RecordIncomplete("Request.url"))),
            headers: self.headers.build().into_iter()
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
}

impl MutBuilder for HttpResponseBuilder {
    type C = HttpResponse;
    type E = RecordBuilderError;

    fn apply<'r>(&mut self, vsl: &VslRecord<'r>) -> Result<bool, RecordBuilderError> {
        match vsl.tag {
            SLT_BerespProtocol | SLT_RespProtocol | SLT_ObjProtocol => {
                let protocol = try!(vsl.parse_data(slt_protocol));
                self.protocol= Some(protocol.to_lossy_string());
            }
            SLT_BerespStatus | SLT_RespStatus | SLT_ObjStatus => {
                let status = try!(vsl.parse_data(slt_status));
                self.status = Some(status);
            }
            SLT_BerespReason | SLT_RespReason | SLT_ObjReason => {
                let reason = try!(vsl.parse_data(slt_reason));
                self.reason = Some(reason.to_lossy_string());
            }
            SLT_BerespHeader | SLT_RespHeader | SLT_ObjHeader => {
                if let (name, Some(value)) = try!(vsl.parse_data(slt_header)) {
                    self.headers.set(name.to_maybe_string(), value.to_maybe_string());
                } else {
                    debug!("Not setting empty response header: {:?}", vsl);
                }
            }
            SLT_BerespUnset | SLT_RespUnset | SLT_ObjUnset => {
                if let (name, Some(value)) = try!(vsl.parse_data(slt_header)) {
                    self.headers.unset(name, value);
                } else {
                    debug!("Not unsetting empty response header: {:?}", vsl);
                }
            }
            _ => panic!("Got unexpected VSL record in request builder: {:?}", vsl)
        };

        Ok(false)
    }

    fn build(self) -> Result<HttpResponse, RecordBuilderError> {
        Ok(HttpResponse {
            protocol: try!(self.protocol.ok_or(RecordBuilderError::RecordIncomplete("Response.protocol"))),
            status: try!(self.status.ok_or(RecordBuilderError::RecordIncomplete("Response.status"))),
            reason: try!(self.reason.ok_or(RecordBuilderError::RecordIncomplete("Response.reason"))),
            headers: self.headers.build().into_iter()
                .map(|(name, value)| (name.to_lossy_string(), value.to_lossy_string()))
                .collect(),
        })
    }
}

#[derive(Debug)]
enum ClientAccessTransactionType {
    Full,
    RestartedEarly,
    RestartedLate,
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
        reason: String,
        parent: VslIdent,
        transaction: ClientAccessTransactionType,
    },
    BackendAccess {
        reason: String,
        parent: VslIdent,
        transaction: BackendAccessTransactionType,
    },
    Session
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
    http_request: MutBuilderState<HttpRequestBuilder>,
    http_response: MutBuilderState<HttpResponseBuilder>,
    cache_object: MutBuilderState<HttpResponseBuilder>,
    obj_storage: Option<ObjStorage>,
    obj_ttl: Option<ObjTtl>,
    backend_connection: Option<BackendConnection>,
    compression: Option<Compression>,
    fetch_body: Option<FetchBody>,
    resp_fetch: Option<Duration>,
    req_process: Option<Duration>,
    resp_ttfb: Option<Duration>,
    req_took: Option<Duration>,
    resp_end: Option<TimeStamp>,
    accounting: Option<Accounting>,
    pipe_accounting: Option<PipeAccounting>,
    client_addr: Option<Address>,
    sess_open: Option<TimeStamp>,
    sess_duration: Option<Duration>,
    sess_remote: Option<Address>,
    sess_local: Option<Address>,
    client_records: Vec<Link<ClientAccessRecord>>,
    backend_record: Option<Link<BackendAccessRecord>>,
    restart_record: Option<Link<ClientAccessRecord>>,
    retry_record: Option<Link<BackendAccessRecord>>,
    handling: Option<Handling>,
    // ture when we are in client processing after bereq (dliver, synth)
    late: bool,
    log: Vec<LogEntry>,
}

impl RecordBuilder {
    pub fn new(ident: VslIdent) -> RecordBuilder {
        RecordBuilder {
            ident: ident,
            record_type: RecordType::Undefined,
            req_start: None,
            pipe_start: None,
            http_request: MutBuilderState::new(HttpRequestBuilder::new()),
            http_response: MutBuilderState::new(HttpResponseBuilder::new()),
            cache_object: MutBuilderState::new(HttpResponseBuilder::new()),
            obj_storage: None,
            obj_ttl: None,
            backend_connection: None,
            compression: None,
            fetch_body: None,
            req_process: None,
            resp_fetch: None,
            resp_ttfb: None,
            req_took: None,
            resp_end: None,
            accounting: None,
            pipe_accounting: None,
            client_addr: None,
            sess_open: None,
            sess_duration: None,
            sess_remote: None,
            sess_local: None,
            client_records: Vec::new(),
            backend_record: None,
            restart_record: None,
            retry_record: None,
            handling: None,
            late: false,
            log: Vec::new(),
        }
    }

    pub fn apply<'r>(&mut self, vsl: &VslRecord<'r>) -> Result<bool, RecordBuilderError> {
        match self.record_type {
            RecordType::Undefined if vsl.tag != SLT_Begin => {
                // we have missed the SLT_Begin record - fail early
                return Err(RecordBuilderError::UnexpectedTag(vsl.tag, SLT_Begin))
            }
            _ => ()
        }

        match vsl.tag {
            SLT_Begin => match self.record_type {
                RecordType::Undefined => {
                    let (record_type, parent_ident, reason) = try!(vsl.parse_data(slt_begin));
                    match record_type {
                        "bereq" => self.record_type = RecordType::BackendAccess {
                            reason: reason.to_owned(),
                            parent: parent_ident,
                            transaction: BackendAccessTransactionType::Full,
                        },
                        "req" => self.record_type = RecordType::ClientAccess {
                            reason: reason.to_owned(),
                            parent: parent_ident,
                            transaction: ClientAccessTransactionType::Full,
                        },
                        "sess" => self.record_type = RecordType::Session,
                        _ => return Err(RecordBuilderError::UnimplementedTransactionType(record_type.to_string()))
                    }
                }
                _ => {
                    warn!("Got SLT_Begin while in process of building {:?}; restarting build", self.record_type);
                    *self = RecordBuilder::new(self.ident);
                    return self.apply(vsl)
                }
            },
            SLT_ReqStart => {
                let (client_ip, client_port) = try!(vsl.parse_data(slt_req_start));
                self.client_addr = Some((client_ip.to_string(), client_port));

                // Reset http_request on ReqStar
                self.http_request = MutBuilderState::new(HttpRequestBuilder::new());
            }
            SLT_Timestamp => {
                let (label, timestamp, since_work_start, since_last_timestamp) =
                    try!(vsl.parse_data(slt_timestamp));

                match label {
                    "Start" => self.req_start = Some(timestamp),
                    "Req" | "ReqBody" => self.req_process = Some(since_work_start),
                    "Bereq" => {
                        self.pipe_start = Some(timestamp);
                        self.req_process = Some(since_work_start);
                    }
                    "Beresp" => {
                        self.resp_ttfb = Some(since_work_start);
                        self.resp_fetch = Some(since_last_timestamp);
                    }
                    "Fetch" => self.resp_fetch = Some(since_last_timestamp),
                    "Pipe" | "Process" =>
                        self.resp_ttfb = Some(since_work_start),
                    "Resp" | "BerespBody" | "Retry" | "PipeSess" => {
                        self.req_took = Some(since_work_start);
                        self.resp_end = Some(timestamp);
                    }
                    "Error" => {
                        self.req_took = Some(since_work_start);
                        self.resp_end = Some(timestamp);
                        // this won't be correct if we got error while accessing backend
                        self.resp_ttfb = None;
                        self.resp_fetch = None;
                    }
                    "Restart" => self.resp_end = Some(timestamp),
                    _ => debug!("Ignoring unmatched SLT_Timestamp label variant: {}", label)
                };
            }
            SLT_Link => {
                let (reason, child_ident, child_type) = try!(vsl.parse_data(slt_link));

                match (reason, child_type) {
                    ("req", "restart") => {
                        if let Some(ref link) = self.restart_record {
                            warn!("Already have restart client request link with ident {}; replacing with {}", link.get_unresolved().as_ref().unwrap(), child_ident);
                        }
                        self.restart_record = Some(Link::Unresolved(child_ident, child_type.to_owned()));
                    }
                    ("req", _) => {
                        self.client_records.push(Link::Unresolved(child_ident, child_type.to_owned()));
                    }
                    ("bereq", "retry") => {
                        if let Some(ref link) = self.retry_record {
                            warn!("Already have retry backend request link with ident {}; replacing with {}", link.get_unresolved().as_ref().unwrap(), child_ident);
                        }
                        self.retry_record = Some(Link::Unresolved(child_ident, child_type.to_owned()));
                    }
                    ("bereq", _) => {
                        if let Some(ref link) = self.backend_record {
                            warn!("Already have backend request link with ident {}; replacing with {}", link.get_unresolved().as_ref().unwrap(), child_ident);
                        }
                        self.backend_record = Some(Link::Unresolved(child_ident, child_type.to_owned()));
                    }
                    _ => warn!("Ignoring unmatched SLT_Link reason variant: {}", reason)
                };
            }
            SLT_VCL_Log => {
                let log_entry = try!(vsl.parse_data(slt_vcl_log));

                self.log.push(LogEntry::Vcl(log_entry.to_lossy_string()));
            }
            SLT_Debug => {
                let log_entry = try!(vsl.parse_data(slt_vcl_log));

                self.log.push(LogEntry::Debug(log_entry.to_lossy_string()));
            }
            SLT_Error => {
                let log_entry = try!(vsl.parse_data(slt_vcl_log));

                self.log.push(LogEntry::Error(log_entry.to_lossy_string()));
            }
            SLT_FetchError => {
                let log_entry = try!(vsl.parse_data(slt_vcl_log));

                self.log.push(LogEntry::FetchError(log_entry.to_lossy_string()));
            }
            SLT_BogoHeader => {
                let log_entry = try!(vsl.parse_data(slt_vcl_log));

                self.log.push(LogEntry::Warning(format!("Bogus HTTP header received: {}", log_entry.to_lossy_string())));
            }
            SLT_LostHeader => {
                let log_entry = try!(vsl.parse_data(slt_vcl_log));

                self.log.push(LogEntry::Warning(format!("Failed HTTP header operation due to resource exhaustion or configured limits; header was: {}", log_entry.to_lossy_string())));
            }

            SLT_Storage => {
                let (storage_type, storage_name) = try!(vsl.parse_data(slt_storage));

                self.obj_storage = Some(ObjStorage {
                    stype: storage_type.to_string(),
                    name: storage_name.to_string(),
                });
            }
            SLT_VCL_acl => {
                let (result, name, addr) = try!(vsl.parse_data(slt_vcl_acl));

                self.log.push(LogEntry::Acl(result, name.to_string(), addr.map(|addr| addr.to_lossy_string())));
            }
            SLT_TTL => {
                let (_soruce, ttl, grace, keep, since, rfc) = try!(vsl.parse_data(slt_ttl));

                let origin = match (rfc, &self.obj_ttl) {
                    (Some((origin, _date, _expires, _max_age)), _) => Some(origin),
                    (None, &Some(ref obj_ttl)) => obj_ttl.origin,
                    _ => None,
                };

                self.obj_ttl = Some(ObjTtl {
                    ttl: ttl,
                    grace: grace,
                    keep: keep,
                    since: since,
                    origin: origin,
                });
            }
            SLT_ReqAcct => {
                // Note: recv are first
                let (recv_header, recv_body, recv_total,
                     sent_header, sent_body, sent_total) =
                    try!(vsl.parse_data(slt_req_acct));

                self.accounting = Some(Accounting {
                    recv_header: recv_header,
                    recv_body: recv_body,
                    recv_total: recv_total,
                    sent_header: sent_header,
                    sent_body: sent_body,
                    sent_total: sent_total,
                });
            }
            SLT_BereqAcct => {
                // Note: sent are first
                let (sent_header, sent_body, sent_total,
                    recv_header, recv_body, recv_total) =
                    try!(vsl.parse_data(slt_bereq_acct));

                self.accounting = Some(Accounting {
                    sent_header: sent_header,
                    sent_body: sent_body,
                    sent_total: sent_total,
                    recv_header: recv_header,
                    recv_body: recv_body,
                    recv_total: recv_total,
                });
            }
            SLT_Length => {
                // Logs the size of a fetch object body.
                // Looks the same as SLT_BereqAcct/recv_body
            }
            SLT_PipeAcct => {
                let (client_request_headers, _backend_request_headers,
                     piped_from_client, piped_to_client) =
                    try!(vsl.parse_data(slt_pipe_acct));

                self.pipe_accounting = Some(PipeAccounting {
                    recv_total: client_request_headers + piped_from_client,
                    sent_total: piped_to_client,
                });
            }
            SLT_BackendOpen => {
                let (fd, name, (remote_addr, remote_port), (local_addr, local_port)) =
                    try!(vsl.parse_data(slt_backend_open));

                self.backend_connection = Some(BackendConnection {
                    fd: fd,
                    name: name.to_string(),
                    remote: (remote_addr.to_string(), remote_port),
                    local: (local_addr.to_string(), local_port),
                });
            }
            SLT_BackendStart | SLT_BackendReuse | SLT_BackendClose => {
                // SLT_BackendStart: Start of backend processing. Logs the backend IP address and port
                // SLT_BackendReuse: Logged when a backend connection is put up for reuse by a later
                // SLT_BackendClose: Logged when a backend connection is closed
                // Not much more than in SLT_BackendOpen
            }

            // Request
            SLT_BereqProtocol | SLT_ReqProtocol |
            SLT_BereqMethod | SLT_ReqMethod |
            SLT_BereqURL | SLT_ReqURL |
            SLT_BereqHeader | SLT_ReqHeader |
            SLT_BereqUnset | SLT_ReqUnset => {
                try!(self.http_request.apply(vsl));
            }

            // Response
            SLT_BerespProtocol | SLT_RespProtocol |
            SLT_BerespStatus | SLT_RespStatus |
            SLT_BerespReason | SLT_RespReason |
            SLT_BerespHeader | SLT_RespHeader |
            SLT_BerespUnset | SLT_RespUnset => {
                try!(self.http_response.apply(vsl));
            }

            // Cache Object
            SLT_ObjProtocol |
            SLT_ObjStatus |
            SLT_ObjReason |
            SLT_ObjHeader |
            SLT_ObjUnset => {
                try!(self.cache_object.apply(vsl));
            }

            // Session
            SLT_SessOpen => {
                let (remote_address, _listen_sock, local_address, timestamp, _fd)
                    = try!(vsl.parse_data(slt_sess_open));

                let remote_address = (remote_address.0.to_string(), remote_address.1);
                let local_address = local_address.map(|(ip, port)| (ip.to_string(), port));

                self.sess_open = Some(timestamp);
                self.sess_remote = Some(remote_address);
                self.sess_local = local_address;
            }
            SLT_SessClose => {
                let (_reason, duration) = try!(vsl.parse_data(slt_sess_close));

                self.sess_duration = Some(duration);
            }

            SLT_Hit => {
                let object_ident = try!(vsl.parse_data(slt_hit));

                self.handling = Some(Handling::Hit(object_ident));
            }

            SLT_HitPass => {
                let object_ident = try!(vsl.parse_data(slt_hit_pass));

                self.handling = Some(Handling::HitPass(object_ident));
            }

            SLT_VCL_call => {
                let method = try!(vsl.parse_data(slt_vcl_call));

                match method {
                    "RECV" => self.http_request.complete(),
                    "MISS" => self.handling = Some(Handling::Miss),
                    "PASS" => if self.handling.is_none() {
                        self.handling = Some(Handling::Pass);
                    },
                    "SYNTH" => {
                        self.handling = Some(Handling::Synth);
                        self.late = true;
                    }
                    "BACKEND_RESPONSE" => {
                        self.http_request.complete();
                        self.http_response.complete();
                    }
                    "BACKEND_ERROR" => {
                        match self.record_type {
                            RecordType::BackendAccess {
                                transaction: ref mut transaction @ BackendAccessTransactionType::Full,
                                ..
                            } => {
                                self.http_request.complete();
                                *transaction = BackendAccessTransactionType::Failed;
                            }
                            _ => return Err(RecordBuilderError::UnexpectedTransition("call BACKEND_ERROR"))
                        }
                    }
                    "DELIVER" => self.late = true,
                    "BACKEND_FETCH" | "HASH" | "HIT" | "PIPE" => (),
                    _ => debug!("Ignoring unmatched {:?} method: {}", vsl.tag, method)
                };
            }

            SLT_VCL_return => {
                let action = try!(vsl.parse_data(slt_vcl_return));

                match action {
                    "restart" => if let RecordType::ClientAccess {
                        transaction: ref mut transaction @ ClientAccessTransactionType::Full,
                        ..
                    } = self.record_type {
                        if self.late {
                            *transaction = ClientAccessTransactionType::RestartedLate;
                        } else {
                            *transaction = ClientAccessTransactionType::RestartedEarly;
                        }
                    } else {
                        return Err(RecordBuilderError::UnexpectedTransition("SLT_VCL_return restart"))
                    },
                    "abandon" => if self.http_request.is_building() {
                        // eary abandon will have request still building
                        if let RecordType::BackendAccess {
                            transaction: ref mut transaction @ BackendAccessTransactionType::Full,
                            ..
                        } = self.record_type {
                            self.http_request.complete();
                            *transaction = BackendAccessTransactionType::Aborted;
                        } else {
                            return Err(RecordBuilderError::UnexpectedTransition("SLT_VCL_return abandon"))
                        }
                    } else if let RecordType::BackendAccess {
                        transaction: ref mut transaction @ BackendAccessTransactionType::Full,
                        ..
                    } = self.record_type {
                        *transaction = BackendAccessTransactionType::Abandoned;
                    } else {
                        return Err(RecordBuilderError::UnexpectedTransition("SLT_VCL_return abandon"))
                    },
                    "retry" => if let RecordType::BackendAccess {
                        transaction: ref mut transaction @ BackendAccessTransactionType::Full,
                        ..
                    } = self.record_type {
                        *transaction = BackendAccessTransactionType::Abandoned;
                    } else {
                        return Err(RecordBuilderError::UnexpectedTransition("SLT_VCL_return retry"))
                    },
                    "pipe" => match self.record_type {
                        RecordType::ClientAccess {
                            transaction: ref mut transaction @ ClientAccessTransactionType::Full,
                            ..
                        } => {
                            *transaction = ClientAccessTransactionType::Piped;
                            self.handling = Some(Handling::Pipe);
                        }
                        RecordType::BackendAccess {
                            transaction: ref mut transaction @ BackendAccessTransactionType::Full,
                            ..
                        } => {
                            self.http_request.complete();
                            *transaction = BackendAccessTransactionType::Piped;
                        }
                        _ => return Err(RecordBuilderError::UnexpectedTransition("SLT_VCL_return pipe"))
                    },
                    "synth" => self.http_response = MutBuilderState::new(HttpResponseBuilder::new()),
                    "deliver" | "fetch" | "hash" | "lookup" | "pass" => (),
                    _ => debug!("Ignoring unmatched {:?} return: {}", vsl.tag, action)
                };
            }
            SLT_Gzip => {
                // Note: direction and ESI values will be known form context
                match try!(vsl.parse_data(slt_gzip)) {
                    Ok((operation, _direction, _esi,
                       bytes_in, bytes_out,
                       _bit_first, _bit_last, _bit_len)) => self.compression = Some(Compression {
                        operation: operation,
                        bytes_in: bytes_in,
                        bytes_out: bytes_out,
                    }),
                    Err(message) => self.log.push(LogEntry::Error(message.to_lossy_string()))
                }
            }
            SLT_Fetch_Body => {
                let (_fetch_mode, fetch_mode_name, streamed) = try!(vsl.parse_data(slt_fetch_body));

                self.fetch_body = Some(FetchBody {
                    mode: fetch_mode_name.to_string(),
                    streamed: streamed,
                });
            }
            SLT_End => return Ok(true),
            SLT__Bogus | SLT__Reserved | SLT__Batch => warn!("Ignoring bogus tag: {:?}", vsl.tag),
            _ => debug!("Ignoring unmatched VSL tag: {:?}", vsl.tag)
        };

        Ok(false)
    }

    pub fn build(mut self) -> Result<Record, RecordBuilderError> {
        match self.record_type {
            RecordType::Undefined => Err(RecordBuilderError::RecordIncomplete("record type is not known - have we missed/lost SLT_Begin record?")),
            RecordType::Session => {
                let record = SessionRecord {
                    ident: self.ident,
                    open: try!(self.sess_open.ok_or(RecordBuilderError::RecordIncomplete("sess_open"))),
                    duration: try!(self.sess_duration.ok_or(RecordBuilderError::RecordIncomplete("sess_duration"))),
                    local: self.sess_local,
                    remote: try!(self.sess_remote.ok_or(RecordBuilderError::RecordIncomplete("sess_remote"))),
                    client_records: self.client_records,
                };

                Ok(Record::Session(record))
            },
            RecordType::ClientAccess { .. } | RecordType::BackendAccess { .. } => {
                let request = try!(self.http_request.build());

                match self.record_type {
                    RecordType::ClientAccess { reason, parent, transaction } => {
                        let transaction = match transaction {
                            ClientAccessTransactionType::Full => {
                                // SLT_End tag is completing the client response
                                self.http_response.complete();

                                ClientAccessTransaction::Full {
                                    request: request,
                                    response: try!(self.http_response.build()),
                                    esi_records: self.client_records,
                                    backend_record: self.backend_record,
                                    process: self.req_process,
                                    fetch: self.resp_fetch,
                                    ttfb: try!(self.resp_ttfb.ok_or(RecordBuilderError::RecordIncomplete("resp_ttfb"))),
                                    serve: try!(self.req_took.ok_or(RecordBuilderError::RecordIncomplete("req_took"))),
                                    accounting: try!(self.accounting.ok_or(RecordBuilderError::RecordIncomplete("accounting"))),
                                }
                            },
                            ClientAccessTransactionType::RestartedEarly => {
                                ClientAccessTransaction::RestartedEarly {
                                    request: request,
                                    process: self.req_process,
                                    restart_record: try!(self.restart_record.ok_or(RecordBuilderError::RecordIncomplete("restart_record"))),
                                }
                            },
                            ClientAccessTransactionType::RestartedLate => {
                                // SLT_End tag is completing the client response
                                self.http_response.complete();

                                ClientAccessTransaction::RestartedLate {
                                    request: request,
                                    response: try!(self.http_response.build()),
                                    backend_record: self.backend_record,
                                    process: self.req_process,
                                    restart_record: try!(self.restart_record.ok_or(RecordBuilderError::RecordIncomplete("restart_record"))),
                                }
                            },
                            ClientAccessTransactionType::Piped => {
                                ClientAccessTransaction::Piped {
                                    request: request,
                                    backend_record: try!(self.backend_record.ok_or(RecordBuilderError::RecordIncomplete("backend_record"))),
                                    process: self.req_process,
                                    ttfb: self.resp_ttfb,
                                    accounting: try!(self.pipe_accounting.ok_or(RecordBuilderError::RecordIncomplete("pipe_accounting"))),
                                }
                            },
                        };

                        let record = ClientAccessRecord {
                            root: reason == "rxreq",
                            ident: self.ident,
                            parent: parent,
                            reason: reason,
                            remote: try!(self.client_addr.ok_or(RecordBuilderError::RecordIncomplete("client_addr"))),
                            transaction: transaction,
                            start: try!(self.req_start.ok_or(RecordBuilderError::RecordIncomplete("req_start"))),
                            end: self.resp_end,
                            handling: try!(self.handling.ok_or(RecordBuilderError::RecordIncomplete("handling"))),
                            compression: self.compression,
                            log: self.log,
                        };

                        Ok(Record::ClientAccess(record))
                    }
                    RecordType::BackendAccess { reason, parent, transaction } => {
                        let transaction = match transaction {
                            BackendAccessTransactionType::Full => {
                                // safet to complete late
                                self.cache_object.complete();
                                let cache_object = try!(self.cache_object.build());

                                let obj_storage = try!(self.obj_storage.ok_or(RecordBuilderError::RecordIncomplete("obj_storage")));
                                let obj_ttl = try!(self.obj_ttl.ok_or(RecordBuilderError::RecordIncomplete("obj_ttl")));

                                let fetch_body =
                                    if reason == "bgfetch" {
                                        // HACK: bgfetch won't have SLT_Fetch_Body as the client is gone
                                        // already so we fake it to avoid having yet another transaction
                                        // type or Option
                                        FetchBody {
                                            mode: "bgfetch".to_string(),
                                            streamed: false
                                        }
                                    } else {
                                        try!(self.fetch_body.ok_or(RecordBuilderError::RecordIncomplete("fetch_body")))
                                    };

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
                                    response: try!(self.http_response.build()),
                                    backend_connection: try!(self.backend_connection.ok_or(RecordBuilderError::RecordIncomplete("backend_connection"))),
                                    cache_object: cache_object,
                                    send: try!(self.req_process.ok_or(RecordBuilderError::RecordIncomplete("req_process"))),
                                    wait: try!(self.resp_fetch.ok_or(RecordBuilderError::RecordIncomplete("resp_fetch"))),
                                    ttfb: try!(self.resp_ttfb.ok_or(RecordBuilderError::RecordIncomplete("resp_ttfb"))),
                                    fetch: try!(self.req_took.ok_or(RecordBuilderError::RecordIncomplete("req_took"))),
                                    accounting: try!(self.accounting.ok_or(RecordBuilderError::RecordIncomplete("accounting"))),
                                }
                            }
                            BackendAccessTransactionType::Failed => {
                                // We complete it here as it is syhth response - not a
                                // backend response
                                self.http_response.complete();

                                BackendAccessTransaction::Failed {
                                    request: request,
                                    synth_response: try!(self.http_response.build()),
                                    retry_record: self.retry_record,
                                    synth: try!(self.req_took.ok_or(RecordBuilderError::RecordIncomplete("req_took"))),
                                    accounting: try!(self.accounting.ok_or(RecordBuilderError::RecordIncomplete("accounting"))),
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
                                    response: try!(self.http_response.build()),
                                    backend_connection: try!(self.backend_connection.ok_or(RecordBuilderError::RecordIncomplete("backend_connection"))),
                                    retry_record: self.retry_record,
                                    send: try!(self.req_process.ok_or(RecordBuilderError::RecordIncomplete("req_process"))),
                                    wait: try!(self.resp_fetch.ok_or(RecordBuilderError::RecordIncomplete("resp_fetch"))),
                                    ttfb: try!(self.resp_ttfb.ok_or(RecordBuilderError::RecordIncomplete("resp_ttfb"))),
                                    fetch: self.req_took,
                                }
                            }
                            BackendAccessTransactionType::Piped => {
                                BackendAccessTransaction::Piped {
                                    request: request,
                                    backend_connection: self.backend_connection,
                                }
                            }
                        };

                        let start = if let BackendAccessTransaction::Piped { .. } = transaction {
                            // Note that piped backend requests don't have start timestamp
                            self.pipe_start
                        } else {
                            self.req_start
                        };

                        let record = BackendAccessRecord {
                            ident: self.ident,
                            parent: parent,
                            reason: reason,
                            transaction: transaction,
                            start: start,
                            end: self.resp_end,
                            compression: self.compression,
                            log: self.log,
                        };

                        Ok(Record::BackendAccess(record))
                    }
                    _ => unreachable!()
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    pub use super::*;
    pub use super::super::super::test_helpers::*;
    pub use access_log::record::*;
    pub use vsl::record::*;

    macro_rules! apply {
        ($state:ident, $ident:expr, $tag:ident, $message:expr) => {{
            $state.apply(&vsl($tag, $ident, $message)).expect(&format!("expected apply to return Ok after applying: `{}, {:?}, {};`", $ident, $tag, $message));
        }};
    }

    macro_rules! apply_last {
        ($state:ident, $ident:expr, $tag:ident, $message:expr) => {{
            $state.apply(&vsl($tag, $ident, $message)).expect(&format!("expected apply to return Ok after applying: `{}, {:?}, {};`", $ident, $tag, $message));
            $state.build().expect("build of builder failed in apply_last")
        }};
    }

    macro_rules! apply_all {
        ($builder:ident, $ident:expr, $tag:ident, $message:expr;) => {{
            apply!($builder, $ident, $tag, $message)
        }};
        ($builder:ident, $ident:expr, $tag:ident, $message:expr; $($t_ident:expr, $t_tag:ident, $t_message:expr;)+) => {{
            apply!($builder, $ident, $tag, $message);
            apply_all!($builder, $($t_ident, $t_tag, $t_message;)*)
        }};
    }

    #[test]
    fn apply_begin() {
        use super::RecordType;
        let mut builder = RecordBuilder::new(123);

        builder.apply(&vsl(SLT_Begin, 123, "bereq 321 fetch")).unwrap();

        assert_matches!(builder.record_type,
            RecordType::BackendAccess { parent: 321, ref reason, .. } if reason == "fetch");
    }

    #[test]
    fn apply_log() {
        let mut builder = RecordBuilder::new(1);

        apply_all!(builder,
                   1, SLT_Begin,          "bereq 6 rxreq";
                   1, SLT_VCL_Log,        "X-Varnish-Privileged-Client: false";
                   1, SLT_VCL_Log,        "X-Varnish-User-Agent-Class: Unknown-Bot";
                   1, SLT_VCL_Log,        "X-Varnish-Force-Failure: false";
                  );
        assert_eq!(builder.log, &[
                   LogEntry::Vcl("X-Varnish-Privileged-Client: false".to_string()),
                   LogEntry::Vcl("X-Varnish-User-Agent-Class: Unknown-Bot".to_string()),
                   LogEntry::Vcl("X-Varnish-Force-Failure: false".to_string()),
        ]);
    }

    #[test]
    fn apply_begin_unimpl_transaction_type() {
        let mut builder = RecordBuilder::new(123);

        let result = builder.apply(&vsl(SLT_Begin, 123, "foo 231 fetch"));
        assert_matches!(result.unwrap_err(),
            RecordBuilderError::UnimplementedTransactionType(ref record_type) if record_type == "foo");
    }

    #[test]
    fn apply_begin_missing_begin() {
        let mut builder = RecordBuilder::new(123);

        let result = builder.apply(&vsl(SLT_BereqURL, 123, "/foobar"));
        assert_matches!(result.unwrap_err(),
            RecordBuilderError::UnexpectedTag(SLT_BereqURL, SLT_Begin));
    }

    #[test]
    fn apply_begin_restart_build() {
        use super::RecordType;
        let mut builder = RecordBuilder::new(123);

        builder.apply(&vsl(SLT_Begin, 123, "bereq 231 fetch")).unwrap();
        builder.apply(&vsl(SLT_Begin, 123, "req 231 fetch")).unwrap();

        assert_matches!(builder.record_type, RecordType::ClientAccess { .. });
    }

    #[test]
    fn apply_begin_parser_fail() {
        let mut builder = RecordBuilder::new(123);

        let result = builder.apply(&vsl(SLT_Begin, 123, "foo bar"));
        assert_matches!(result.unwrap_err(),
            RecordBuilderError::InvalidMessageFormat(_));
    }

    #[test]
    fn apply_begin_int_parse_fail() {
        let mut builder = RecordBuilder::new(123);

        let result = builder.apply(&vsl(SLT_Begin, 123, "bereq foo fetch"));
        assert_matches!(result.unwrap_err(),
            RecordBuilderError::InvalidMessageFormat(_));
    }

    #[test]
    fn apply_backend_record_response() {
        let mut builder = RecordBuilder::new(123);

        apply_all!(builder,
                   123, SLT_Begin,            "bereq 6 rxreq";
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

        let request = builder.http_request.build().unwrap();
        assert_eq!(request.method, "GET".to_string());
        assert_eq!(request.url, "/foobar".to_string());
        assert_eq!(request.protocol, "HTTP/1.1".to_string());
        assert_eq!(request.headers, &[
                   ("Host".to_string(), "localhost:8080".to_string()),
                   ("User-Agent".to_string(), "curl/7.40.0".to_string())]);

        let response = builder.http_response.build().unwrap();
        assert_eq!(response.protocol, "HTTP/1.1".to_string());
        assert_eq!(response.status, 503);
        assert_eq!(response.reason, "Backend fetch failed".to_string());
        assert_eq!(response.headers, &[
                   ("Date".to_string(), "Fri, 22 Jul 2016 09:46:02 GMT".to_string()),
                   ("Server".to_string(), "Varnish".to_string())]);
    }

    #[test]
    fn apply_record_header_updates() {
        let mut builder = RecordBuilder::new(123);

        // logs/varnish20160804-3752-1krgp8j808a493d5e74216e5.vsl
        apply_all!(builder,
                   15, SLT_Begin,         "req 6 rxreq";
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
                   15, SLT_ReqHeader,     "X-Varnish-Result: ";
                   15, SLT_ReqUnset,      "X-Varnish-Decision: Cacheable";
                   15, SLT_ReqHeader,     "X-Varnish-Decision: Uncacheable-NoCacheClass";
                   15, SLT_ReqHeader,     "X-Varnish-Decision:";
                   );

        builder.http_request.complete();

        let request = builder.http_request.build().unwrap();
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
    fn apply_client_request_record_reset_esi() {
        let mut builder = RecordBuilder::new(123);

        // logs/varnish20160804-3752-1krgp8j808a493d5e74216e5.vsl
        apply_all!(builder,
                   15, SLT_Begin,          "req 6 rxreq";
                   15, SLT_ReqMethod,     "POST";
                   15, SLT_ReqURL,        "/foo/bar";
                   15, SLT_ReqProtocol,   "HTTP/1.0";
                   15, SLT_ReqHeader,     "Host: 127.0.0.1:666";
                   15, SLT_ReqHeader,     "Test: 42";
                   15, SLT_ReqStart,      "127.0.0.1 39792";
                   15, SLT_ReqMethod,     "GET";
                   15, SLT_ReqURL,        "/test_page/abc";
                   15, SLT_ReqProtocol,   "HTTP/1.1";
                   15, SLT_ReqHeader,     "Host: 127.0.0.1:1209";
                   15, SLT_ReqHeader,     "Test: 1";
                  );

        builder.http_request.complete();

        let request = builder.http_request.build().unwrap();
        assert_eq!(request.method, "GET".to_string());
        assert_eq!(request.url, "/test_page/abc".to_string());
        assert_eq!(request.protocol, "HTTP/1.1".to_string());
        assert_eq!(request.headers, &[
                   ("Host".to_string(), "127.0.0.1:1209".to_string()),
                   ("Test".to_string(), "1".to_string()),
        ]);
    }

    #[test]
    fn apply_response_header_updates() {
        let mut builder = RecordBuilder::new(123);

        // logs/varnish20160804-3752-1krgp8j808a493d5e74216e5.vsl
        apply_all!(builder,
                   15, SLT_Begin,          "req 6 rxreq";
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

        builder.http_response.complete();

        let response = builder.http_response.build().unwrap();
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
    fn apply_response_header_reset_synth() {
        let mut builder = RecordBuilder::new(123);

        // synth from deliver
        apply_all!(builder,
                   15, SLT_Begin,          "req 6 rxreq";
                   15, SLT_RespProtocol,   "HTTP/1.1";
                   15, SLT_RespStatus,     "500";
                   15, SLT_RespReason,     "Error";
                   15, SLT_RespHeader,     "Content-Type: text/html; charset=utf-8";
                   15, SLT_RespHeader,     "Test: 9";
                   15, SLT_RespHeader,     "Test: 8";
                   15, SLT_RespHeader,     "Test: 7";
                   15, SLT_RespUnset,      "Test: 6";
                   15, SLT_VCL_return,     "synth";
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

        builder.http_response.complete();

        let response = builder.http_response.build().unwrap();
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
    fn apply_backend_record_locking() {
        let mut builder = RecordBuilder::new(123);

        apply_all!(builder,
                   123, SLT_Begin,          "bereq 6 rxreq";
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

        let requests = builder.http_request.build().unwrap();
        assert_eq!(requests.method, "GET".to_string());
        assert_eq!(requests.url, "/foobar".to_string());
        assert_eq!(requests.protocol, "HTTP/1.1".to_string());
        assert_eq!(requests.headers, &[
                   ("Host".to_string(), "localhost:8080".to_string()),
                   ("User-Agent".to_string(), "curl/7.40.0".to_string())]);
    }

    #[test]
    fn apply_backend_record_non_utf8() {
        let mut builder = RecordBuilder::new(123);

        apply_all!(builder,
                   123, SLT_Begin,          "bereq 6 rxreq";
                   123, SLT_BereqMethod,    "GET";
                  );

        builder.apply(&VslRecord {
            tag: SLT_BereqURL,
            marker: VSL_BACKENDMARKER,
            ident: 123,
            data: &[0, 159, 146, 150]
        }).unwrap();

        apply_all!(builder,
                   123, SLT_BereqProtocol, "HTTP/1.1";
                  );

        builder.apply(&VslRecord {
            tag: SLT_BereqHeader,
            marker: VSL_BACKENDMARKER,
            ident: 123,
            data: &[72, 111, 115, 116, 58, 32, 0, 159, 146, 150]
        }).unwrap();

        apply_all!(builder,
                   123, SLT_BerespProtocol, "HTTP/1.1";
                   123, SLT_BerespStatus,   "503";
                   123, SLT_BerespReason,   "Service Unavailable";
                   123, SLT_VCL_call,       "BACKEND_RESPONSE";
                  );

        let requests = builder.http_request.build().unwrap();
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
        let mut builder = RecordBuilder::new(123);

        apply_all!(builder,
                   7, SLT_Begin,        "req 6 rxreq";
                   7, SLT_Timestamp,    "Start: 1470403413.664824 0.000000 0.000000";
                   7, SLT_Timestamp,    "Req: 1470403414.664824 1.000000 1.000000";
                   7, SLT_ReqStart,     "127.0.0.1 39798";
                   7, SLT_ReqMethod,    "GET";
                   7, SLT_ReqURL,       "/retry";
                   7, SLT_ReqProtocol,  "HTTP/1.1";
                   7, SLT_ReqHeader,    "Date: Fri, 05 Aug 2016 13:23:34 GMT";
                   7, SLT_VCL_call,     "RECV";
                   7, SLT_VCL_return,   "hash";
                   7, SLT_VCL_call,     "HASH";
                   7, SLT_VCL_return,   "lookup";
                   7, SLT_VCL_call,     "MISS";
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
        assert_eq!(record.end, Some(1470403414.672458));

        assert_eq!(record.handling, Handling::Miss);

        assert_matches!(record.transaction, ClientAccessTransaction::Full {
            process: Some(1.0),
            fetch: Some(0.007491),
            ttfb: 1.007601,
            serve: 1.007634,
            ..
        });
    }

    #[test]
    fn apply_client_access_restarted_early() {
        let mut builder = RecordBuilder::new(123);

        // logs-new/varnish20160816-4093-c0f5tz5609f5ab778e4a4eb.vsl
        apply_all!(builder,
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
        assert_eq!(record.end, Some(1471355414.450428));

        assert_matches!(record.transaction, ClientAccessTransaction::RestartedEarly {
            request: HttpRequest {
                ref url,
                ..
            },
            process: Some(0.0),
            restart_record: Link::Unresolved(5, _),
        } if url == "/foo/thumbnails/foo/4006450256177f4a/bar.jpg?type=brochure");
    }

    #[test]
    fn apply_client_access_restarted_late() {
        let mut builder = RecordBuilder::new(123);

        // logs-new/varnish20160816-4093-c0f5tz5609f5ab778e4a4eb.vsl
        apply_all!(builder,
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
                   4, SLT_VCL_call,       "MISS";
                   4, SLT_VCL_return,     "fetch";
                   4, SLT_Link,           "bereq 3 fetch";
                   4, SLT_Timestamp,      "Fetch: 1474021794.489401 0.002282 0.002282";
                   4, SLT_RespProtocol,   "HTTP/1.1";
                   4, SLT_RespStatus,     "301";
                   4, SLT_RespReason,     "Moved Permanently";
                   4, SLT_RespHeader,     "Date: Fri, 16 Sep 2016 10:29:53 GMT";
                   4, SLT_RespHeader,     "Server: Microsoft-IIS/7.5";
                   4, SLT_VCL_call,       "DELIVER";
                   4, SLT_VCL_return,     "restart";
                   4, SLT_Timestamp,      "Process: 1474021794.489489 0.002371 0.000088";
                   4, SLT_Timestamp,      "Restart: 1471355414.450428 0.000117 0.000117";
                   4, SLT_Link,           "req 5 restart";
                   );

        let record = apply_last!(builder, 4, SLT_End, "")
            .unwrap_client_access();

        assert_eq!(record.start, 1471355414.450311);
        assert_eq!(record.end, Some(1471355414.450428));

        assert_matches!(record.transaction, ClientAccessTransaction::RestartedLate {
            request: HttpRequest {
                ref url,
                ..
            },
            response: HttpResponse {
                status,
                ..
            },
            backend_record: Some(Link::Unresolved(3, _)),
            process: Some(0.0),
            restart_record: Link::Unresolved(5, _),
        } if
            url == "/foo/thumbnails/foo/4006450256177f4a/bar.jpg?type=brochure" &&
            status == 301
        );
    }

    #[test]
    fn apply_client_access_piped() {
        let mut builder = RecordBuilder::new(123);

        // logs-new/varnish20160816-4093-s54h6nb4b44b69f1b2c7ca2.vsl
        apply_all!(builder,
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
        assert_eq!(record.end, Some(1471355444.751368));

        assert_eq!(record.handling, Handling::Pipe);

        assert_matches!(record.transaction, ClientAccessTransaction::Piped {
            request: HttpRequest {
                ref url,
                ref headers,
                ..
            },
            ref backend_record,
            process: Some(0.0),
            ttfb: Some(0.000209),
            accounting: PipeAccounting {
                recv_total: 268,
                sent_total: 480,
            }
        } if
            url == "/websocket" &&
            headers == &[
                ("Upgrade".to_string(), "websocket".to_string()),
                ("Connection".to_string(), "Upgrade".to_string())] &&
            backend_record == &Link::Unresolved(5, "pipe".to_string())
        );
    }

    #[test]
    fn apply_client_access_piped_unavailable() {
        let mut builder = RecordBuilder::new(123);

        apply_all!(builder,
                   32785, SLT_Begin,          "req 32784 rxreq";
                   32785, SLT_Timestamp,      "Start: 1475491757.258461 0.000000 0.000000";
                   32785, SLT_Timestamp,      "Req: 1475491757.258461 0.000000 0.000000";
                   32785, SLT_ReqStart,       "192.168.1.115 55276";
                   32785, SLT_ReqMethod,      "GET";
                   32785, SLT_ReqURL,         "/sse";
                   32785, SLT_ReqProtocol,    "HTTP/1.1";
                   32785, SLT_ReqHeader,      "Host: staging.eod.example.net";
                   32785, SLT_ReqHeader,      "Accept: text/event-stream";
                   32785, SLT_VCL_call,       "RECV";
                   32785, SLT_VCL_Log,        "server_name: v4.dev.varnish";
                   32785, SLT_VCL_Log,        "data_source: WCC";
                   32785, SLT_VCL_Log,        "Server-Sent-Events connection request from: 192.168.1.115";
                   32785, SLT_VCL_Log,        "decision: Pipe-ServerSentEvents";
                   32785, SLT_VCL_Log,        "data_source: WCC";
                   32785, SLT_VCL_return,     "pipe";
                   32785, SLT_VCL_call,       "HASH";
                   32785, SLT_VCL_return,     "lookup";
                   32785, SLT_Link,           "bereq 32786 pipe";
                   32785, SLT_PipeAcct,       "350 0 0 0";
               );

        let record = apply_last!(builder, 32785, SLT_End, "")
            .unwrap_client_access();

        assert_eq!(record.start, 1475491757.258461);
        assert_eq!(record.end, None);

        assert_eq!(record.handling, Handling::Pipe);

        assert_matches!(record.transaction, ClientAccessTransaction::Piped {
            request: HttpRequest {
                ref url,
                ref headers,
                ..
            },
            ref backend_record,
            process: Some(0.0),
            ttfb: None,
            accounting: PipeAccounting {
                recv_total: 350,
                sent_total: 0,
            }
        } if
            url == "/sse" &&
            headers == &[
                ("Host".to_string(), "staging.eod.example.net".to_string()),
                ("Accept".to_string(), "text/event-stream".to_string())] &&
            backend_record == &Link::Unresolved(32786, "pipe".to_string())
        );
    }

    #[test]
    fn apply_client_access_record_byte_counts() {
        let mut builder = RecordBuilder::new(123);

        apply_all!(builder,
                   7, SLT_Begin,        "req 6 rxreq";
                   7, SLT_Timestamp,    "Start: 1470403413.664824 0.000000 0.000000";
                   7, SLT_Timestamp,    "Req: 1470403414.664824 1.000000 1.000000";
                   7, SLT_ReqStart,     "127.0.0.1 39798";
                   7, SLT_ReqMethod,    "GET";
                   7, SLT_ReqURL,       "/retry";
                   7, SLT_ReqProtocol,  "HTTP/1.1";
                   7, SLT_ReqHeader,    "Date: Fri, 05 Aug 2016 13:23:34 GMT";
                   7, SLT_VCL_call,     "RECV";
                   7, SLT_VCL_return,   "pass";
                   7, SLT_VCL_call,     "HASH";
                   7, SLT_VCL_return,   "lookup";
                   7, SLT_VCL_call,     "PASS";
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

        assert_eq!(record.handling, Handling::Pass);

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
    fn apply_client_access_record_gzip() {
        let mut builder = RecordBuilder::new(123);

        apply_all!(builder,
                   7, SLT_Begin,        "req 6 rxreq";
                   7, SLT_Timestamp,    "Start: 1470403413.664824 0.000000 0.000000";
                   7, SLT_Timestamp,    "Req: 1470403414.664824 1.000000 1.000000";
                   7, SLT_ReqStart,     "127.0.0.1 39798";
                   7, SLT_ReqMethod,    "GET";
                   7, SLT_ReqURL,       "/retry";
                   7, SLT_ReqProtocol,  "HTTP/1.1";
                   7, SLT_ReqHeader,    "Date: Fri, 05 Aug 2016 13:23:34 GMT";
                   7, SLT_VCL_call,     "RECV";
                   7, SLT_VCL_return,   "pass";
                   7, SLT_VCL_call,     "HASH";
                   7, SLT_VCL_return,   "lookup";
                   7, SLT_VCL_call,     "PASS";
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
                   7, SLT_Gzip,         "U D - 29 9 80 80 162";
                   7, SLT_Timestamp,    "Resp: 1470403414.672458 1.007634 0.000032";
                   7, SLT_ReqAcct,      "82 2 84 304 6962 7266";
                   );

        let record = apply_last!(builder, 7, SLT_End, "")
            .unwrap_client_access();

        assert_matches!(record.compression, Some(Compression {
            operation: CompressionOperation::Gunzip,
            bytes_in: 29,
            bytes_out: 9,
        }));
    }

    #[test]
    fn apply_client_access_record_hit_for_pass() {
        let mut builder = RecordBuilder::new(123);

        apply_all!(builder,
                   7, SLT_Begin,        "req 6 rxreq";
                   7, SLT_Timestamp,    "Start: 1470403413.664824 0.000000 0.000000";
                   7, SLT_Timestamp,    "Req: 1470403414.664824 1.000000 1.000000";
                   7, SLT_ReqStart,     "127.0.0.1 39798";
                   7, SLT_ReqMethod,    "GET";
                   7, SLT_ReqURL,       "/retry";
                   7, SLT_ReqProtocol,  "HTTP/1.1";
                   7, SLT_ReqHeader,    "Date: Fri, 05 Aug 2016 13:23:34 GMT";
                   7, SLT_VCL_call,     "RECV";
                   7, SLT_VCL_return,   "hash";
                   7, SLT_VCL_call,     "HASH";
                   7, SLT_VCL_return,   "lookup";
                   7, SLT_Debug,        "XXXX HIT-FOR-PASS";
                   7, SLT_HitPass,      "5";
                   7, SLT_VCL_call,     "PASS";
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

        assert_eq!(record.handling, Handling::HitPass(5));

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

    //TODO: test backend access record: Full, Failed, Aborted, Abandoned, Piped

    #[test]
    fn apply_backend_access_record_abandoned() {
        let mut builder = RecordBuilder::new(123);

        // logs/raw.vsl
        apply_all!(builder,
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

       assert_eq!(record.start, Some(1471354579.281173));
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
        let mut builder = RecordBuilder::new(123);

        apply_all!(builder,
                   32769, SLT_Begin,            "bereq 8 retry";
                   32769, SLT_Timestamp,        "Start: 1470403414.669375 0.004452 0.000000";
                   32769, SLT_BereqMethod,      "GET";
                   32769, SLT_BereqURL,         "/iss/v2/thumbnails/foo/4006450256177f4a/bar.jpg";
                   32769, SLT_BereqProtocol,    "HTTP/1.1";
                   32769, SLT_BereqHeader,      "Date: Fri, 05 Aug 2016 13:23:34 GMT";
                   32769, SLT_BereqHeader,      "Host: 127.0.0.1:1200";
                   32769, SLT_BackendOpen,      "19 boot.default 127.0.0.1 42000 127.0.0.1 51058";
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

       assert_eq!(record.start, Some(1470403414.669375));
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
        let mut builder = RecordBuilder::new(123);

        apply_all!(builder,
                   32769, SLT_Begin,            "bereq 8 retry";
                   32769, SLT_Timestamp,        "Start: 1470403414.669375 0.004452 0.000000";
                   32769, SLT_BereqMethod,      "GET";
                   32769, SLT_BereqURL,         "/iss/v2/thumbnails/foo/4006450256177f4a/bar.jpg";
                   32769, SLT_BereqProtocol,    "HTTP/1.1";
                   32769, SLT_BereqHeader,      "Date: Fri, 05 Aug 2016 13:23:34 GMT";
                   32769, SLT_BereqHeader,      "Host: 127.0.0.1:1200";
                   32769, SLT_VCL_return,       "fetch";
                   32769, SLT_BackendOpen,      "19 boot.default 127.0.0.1 42000 127.0.0.1 51058";
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
                   32769, SLT_BereqAcct,        "0 0 0 0 0 0";
                   );

       let record = apply_last!(builder, 32769, SLT_End, "")
           .unwrap_backend_access();

       assert_eq!(record.start, Some(1470403414.669375));
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
    fn apply_backend_access_record_gzip() {
        let mut builder = RecordBuilder::new(123);

        apply_all!(builder,
                   32769, SLT_Begin,            "bereq 8 retry";
                   32769, SLT_Timestamp,        "Start: 1470403414.669375 0.004452 0.000000";
                   32769, SLT_BereqMethod,      "GET";
                   32769, SLT_BereqURL,         "/iss/v2/thumbnails/foo/4006450256177f4a/bar.jpg";
                   32769, SLT_BereqProtocol,    "HTTP/1.1";
                   32769, SLT_BereqHeader,      "Date: Fri, 05 Aug 2016 13:23:34 GMT";
                   32769, SLT_BereqHeader,      "Host: 127.0.0.1:1200";
                   32769, SLT_BackendOpen,      "19 boot.default 127.0.0.1 42000 127.0.0.1 51058";
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
                   32769, SLT_Gzip,             "G F - 861 41 80 80 260";
                   32769, SLT_Timestamp,        "BerespBody: 1470403414.672290 0.007367 0.000105";
                   32769, SLT_Length,           "6962";
                   32769, SLT_BereqAcct,        "1021 0 1021 608 6962 7570";
                   );

       let record = apply_last!(builder, 32769, SLT_End, "")
           .unwrap_backend_access();

       assert_matches!(record.compression, Some(Compression {
           operation: CompressionOperation::Gzip,
           bytes_in: 861,
           bytes_out: 41,
       }));
    }

    #[test]
    fn apply_backend_access_record_failed() {
        let mut builder = RecordBuilder::new(123);

        // logs-new/varnish20160816-4093-lmudum99608ad955ba43288.vsl
        apply_all!(builder,
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

       assert_eq!(record.start, Some(1471355385.239334));
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
        let mut builder = RecordBuilder::new(123);

        apply_all!(builder,
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
                   32769, SLT_BereqAcct,        "0 0 0 0 0 0";
                  );

       let record = apply_last!(builder, 32769, SLT_End, "")
           .unwrap_backend_access();

       assert_eq!(record.start, Some(1470304835.059425));
       assert_eq!(record.end, Some(1470304835.059479));

       assert_matches!(record.transaction, BackendAccessTransaction::Failed {
           synth: 0.000054,
           ..
       });
    }

    #[test]
    fn apply_backend_access_record_piped() {
        let mut builder = RecordBuilder::new(123);

        // logs-new/varnish20160816-4093-s54h6nb4b44b69f1b2c7ca2.vsl
        apply_all!(builder,
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

       assert_eq!(record.start, Some(1471355444.744344));
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
    fn apply_backend_access_record_piped_unavailable() {
        let mut builder = RecordBuilder::new(123);

        apply_all!(builder,
                   32786, SLT_Begin,          "bereq 32785 pipe";
                   32786, SLT_BereqMethod,    "GET";
                   32786, SLT_BereqURL,       "/sse";
                   32786, SLT_BereqProtocol,  "HTTP/1.1";
                   32786, SLT_BereqHeader,    "Host: staging.eod.example.net";
                   32786, SLT_BereqHeader,    "Accept: text/event-stream";
                   32786, SLT_BereqHeader,    "Connection: close";
                   32786, SLT_VCL_call,       "PIPE";
                   32786, SLT_VCL_Log,        "proxy_host:";
                   32786, SLT_BereqUnset,     "Connection: close";
                   32786, SLT_BereqHeader,    "Connection: close";
                   32786, SLT_VCL_return,     "pipe";
                   32786, SLT_FetchError,     "no backend connection";
                   32786, SLT_BereqAcct,      "0 0 0 0 0 0";
                  );

       let record = apply_last!(builder, 32786, SLT_End, "")
           .unwrap_backend_access();

       assert_eq!(record.start, None);
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
           url == "/sse" &&
           protocol == "HTTP/1.1" &&
           headers == &[
               ("Host".to_string(), "staging.eod.example.net".to_string()),
               ("Accept".to_string(), "text/event-stream".to_string()),
               ("Connection".to_string(), "close".to_string())]
       );
    }

    #[test]
    fn apply_backend_access_record_aborted() {
        let mut builder = RecordBuilder::new(123);

        apply_all!(builder,
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

       assert_eq!(record.start, Some(1471449766.106695));
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
        let mut builder = RecordBuilder::new(123);

        apply_all!(builder,
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
                   7, SLT_VCL_acl,      "NO_MATCH trusted_networks";
                   7, SLT_VCL_acl,      "MATCH external_proxies \"127.0.0.1\"";
                   7, SLT_VCL_return,   "hash";
                   7, SLT_VCL_call,     "HASH";
                   7, SLT_VCL_return,   "lookup";
                   7, SLT_Debug,        "XXXX HIT-FOR-PASS";
                   7, SLT_HitPass,      "5";
                   7, SLT_VCL_call,     "PASS";
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
                    LogEntry::Vcl("X-Varnish-Privileged-Client: false".to_string()),
                    LogEntry::Acl(AclResult::NoMatch, "trusted_networks".to_string(), None),
                    LogEntry::Acl(AclResult::Match, "external_proxies".to_string(), Some("\"127.0.0.1\"".to_string())),
                    LogEntry::Debug("XXXX HIT-FOR-PASS".to_string()),
                    LogEntry::Vcl("X-Varnish-User-Agent-Class: Unknown-Bot".to_string()),
                    LogEntry::Vcl("X-Varnish-Force-Failure: false".to_string()),
                    LogEntry::Debug("RES_MODE 2".to_string()),
                    LogEntry::Error("oh no!".to_string()),
                    LogEntry::Warning("Failed HTTP header operation due to resource exhaustion or configured limits; header was: SetCookie: foo=bar".to_string()),
         ]);
    }

    #[test]
    fn apply_backend_access_record_log() {
        let mut builder = RecordBuilder::new(123);

        apply_all!(builder,
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
                  LogEntry::Vcl("X-Varnish-Privileged-Client: false".to_string()),
                  LogEntry::Debug("RES_MODE 2".to_string()),
                  LogEntry::Vcl("X-Varnish-User-Agent-Class: Unknown-Bot".to_string()),
                  LogEntry::FetchError("no backend connection".to_string()),
                  LogEntry::Warning("Bogus HTTP header received: foobar!".to_string()),
       ]);
    }
    #[test]
    fn apply_backend_access_record_cache_object() {
        let mut builder = RecordBuilder::new(123);

        apply_all!(builder,
                   32769, SLT_Begin,            "bereq 8 retry";
                   32769, SLT_Timestamp,        "Start: 1470403414.669375 0.004452 0.000000";
                   32769, SLT_BereqMethod,      "GET";
                   32769, SLT_BereqURL,         "/iss/v2/thumbnails/foo/4006450256177f4a/bar.jpg";
                   32769, SLT_BereqProtocol,    "HTTP/1.1";
                   32769, SLT_BereqHeader,      "Date: Fri, 05 Aug 2016 13:23:34 GMT";
                   32769, SLT_BereqHeader,      "Host: 127.0.0.1:1200";
                   32769, SLT_VCL_return,       "fetch";
                   32769, SLT_BackendOpen,      "19 boot.crm_v2 127.0.0.1 42005 127.0.0.1 53054";
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
        let mut builder = RecordBuilder::new(123);

        apply_all!(builder,
                   32769, SLT_Begin,            "bereq 8 retry";
                   32769, SLT_Timestamp,        "Start: 1470403414.669375 0.004452 0.000000";
                   32769, SLT_BereqMethod,      "GET";
                   32769, SLT_BereqURL,         "/iss/v2/thumbnails/foo/4006450256177f4a/bar.jpg";
                   32769, SLT_BereqProtocol,    "HTTP/1.1";
                   32769, SLT_BereqHeader,      "Date: Fri, 05 Aug 2016 13:23:34 GMT";
                   32769, SLT_BereqHeader,      "Host: 127.0.0.1:1200";
                   32769, SLT_VCL_return,       "fetch";
                   32769, SLT_BackendOpen,      "19 boot.crm_v2 127.0.0.1 42005 127.0.0.1 53054";
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
        let mut builder = RecordBuilder::new(123);

        apply_all!(builder,
                   32769, SLT_Begin,            "bereq 8 retry";
                   32769, SLT_Timestamp,        "Start: 1470403414.669375 0.004452 0.000000";
                   32769, SLT_BereqMethod,      "GET";
                   32769, SLT_BereqURL,         "/iss/v2/thumbnails/foo/4006450256177f4a/bar.jpg";
                   32769, SLT_BereqProtocol,    "HTTP/1.1";
                   32769, SLT_BereqHeader,      "Date: Fri, 05 Aug 2016 13:23:34 GMT";
                   32769, SLT_BereqHeader,      "Host: 127.0.0.1:1200";
                   32769, SLT_VCL_return,       "fetch";
                   32769, SLT_BackendOpen,      "19 boot.crm_v2 127.0.0.1 42005 127.0.0.1 53054";
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
        let mut builder = RecordBuilder::new(123);

        apply_all!(builder,
                   32769, SLT_Begin,            "bereq 8 retry";
                   32769, SLT_Timestamp,        "Start: 1470403414.669375 0.004452 0.000000";
                   32769, SLT_BereqMethod,      "GET";
                   32769, SLT_BereqURL,         "/iss/v2/thumbnails/foo/4006450256177f4a/bar.jpg";
                   32769, SLT_BereqProtocol,    "HTTP/1.1";
                   32769, SLT_BereqHeader,      "Date: Fri, 05 Aug 2016 13:23:34 GMT";
                   32769, SLT_BereqHeader,      "Host: 127.0.0.1:1200";
                   32769, SLT_VCL_return,       "fetch";
                   32769, SLT_BackendOpen,      "19 boot.crm_v2 127.0.0.1 42005 127.0.0.1 53054";
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

    #[test]
    fn apply_backend_access_record_bgfetch() {
        let mut builder = RecordBuilder::new(123);

        apply_all!(builder,
                   120386761, SLT_Begin,          "bereq 120386760 bgfetch";
                   120386761, SLT_Timestamp,      "Start: 1478876416.764800 0.000000 0.000000";
                   120386761, SLT_BereqMethod,    "GET";
                   120386761, SLT_BereqURL,       "/";
                   120386761, SLT_BereqProtocol,  "HTTP/1.1";
                   120386761, SLT_BereqHeader,    "Host: www.example.com";
                   120386761, SLT_BereqHeader,    "Accept: */*";
                   120386761, SLT_BereqHeader,    "User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1";
                   120386761, SLT_BereqHeader,    "Accept-Encoding: gzip";
                   120386761, SLT_BereqHeader,    "If-None-Match: W/\"371132087\"";
                   120386761, SLT_BereqHeader,    "X-Varnish: 120386761";
                   );
                   apply_all!(builder,
                   120386761, SLT_VCL_call,       "BACKEND_FETCH";
                   120386761, SLT_BereqUnset,     "Accept-Encoding: gzip";
                   120386761, SLT_BereqHeader,    "Accept-Encoding: gzip";
                   120386761, SLT_VCL_return,     "fetch";
                   120386761, SLT_BackendOpen,    "51 reload_2016-11-11T09:47:37.origin 10.1.1.22 2081 10.3.1.217 33152";
                   120386761, SLT_BackendStart,   "10.1.1.22 2081";
                   120386760, SLT_Link,           "bereq 120386761 bgfetch";
                   120386761, SLT_Timestamp,      "Bereq: 1478876416.764860 0.000060 0.000060";
                   120386761, SLT_Timestamp,      "Beresp: 1478876417.148921 0.384121 0.384061";
                   120386761, SLT_BerespProtocol, "HTTP/1.1";
                   120386761, SLT_BerespStatus,   "304";
                   120386761, SLT_BerespReason,   "Not Modified";
                   120386761, SLT_BerespHeader,   "Cache-Control: private, must-revalidate, s-maxage=3644";
                   120386761, SLT_BerespHeader,   "Content-Type: text/html; charset=utf-8";
                   120386761, SLT_TTL,            "RFC 3644 10 -1 1478876417 1478872579 1478872569 0 3644";
                   120386761, SLT_BerespProtocol, "HTTP/1.1";
                   120386761, SLT_BerespStatus,   "200";
                   120386761, SLT_BerespReason,   "OK";
                   120386761, SLT_BerespHeader,   "x-url: /";
                   120386761, SLT_VCL_call,       "BACKEND_RESPONSE";
                   120386761, SLT_TTL,            "VCL 3644 259200 0 1478872579";
                   120386761, SLT_VCL_return,     "deliver";
                   );
                   apply_all!(builder,
                   120386761, SLT_Storage,        "malloc s0";
                   120386761, SLT_ObjProtocol,    "HTTP/1.1";
                   120386761, SLT_ObjStatus,      "200";
                   120386761, SLT_ObjReason,      "OK";
                   120386761, SLT_ObjHeader,      "Cache-Control: private, must-revalidate, s-maxage=3644";
                   120386761, SLT_ObjHeader,      "Content-Type: text/html; charset=utf-8";
                   120386761, SLT_ObjHeader,      "Server: Microsoft-IIS/7.5";
                   120386761, SLT_BackendReuse,   "51 reload_2016-11-11T09:47:37.origin";
                   120386761, SLT_Timestamp,      "BerespBody: 1478876417.149090 0.384290 0.000168";
                   120386761, SLT_Length,         "182259";
                   120386761, SLT_BereqAcct,      "1041 0 1041 562 0 562";
                   );

        let record = apply_last!(builder, 120386761, SLT_End, "")
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

    #[test]
    fn apply_client_access_record_gzip_error() {
        let mut builder = RecordBuilder::new(123);

        apply_all!(builder,
                   7, SLT_Begin,        "req 6 rxreq";
                   7, SLT_Timestamp,    "Start: 1470403413.664824 0.000000 0.000000";
                   7, SLT_Timestamp,    "Req: 1470403414.664824 1.000000 1.000000";
                   7, SLT_ReqStart,     "127.0.0.1 39798";
                   7, SLT_ReqMethod,    "GET";
                   7, SLT_ReqURL,       "/retry";
                   7, SLT_ReqProtocol,  "HTTP/1.1";
                   7, SLT_ReqHeader,    "Date: Fri, 05 Aug 2016 13:23:34 GMT";
                   7, SLT_VCL_call,     "RECV";
                   7, SLT_VCL_return,   "pass";
                   7, SLT_VCL_call,     "HASH";
                   7, SLT_VCL_return,   "lookup";
                   7, SLT_VCL_call,     "PASS";
                   7, SLT_Link,         "bereq 8 fetch";
                   7, SLT_Timestamp,    "Fetch: 1470403414.672315 1.007491 0.007491";
                   7, SLT_RespProtocol, "HTTP/1.1";
                   7, SLT_RespStatus,   "200";
                   7, SLT_RespReason,   "OK";
                   7, SLT_RespHeader,   "Content-Type: image/jpeg";
                   7, SLT_VCL_return,   "deliver";
                   7, SLT_Timestamp,    "Process: 1470403414.672425 1.007601 0.000111";
                   7, SLT_RespHeader,   "Accept-Ranges: bytes";
                   7, SLT_RespHeader,   "Connection: keep-alive";
                   7, SLT_Gzip,         "G(un)zip error: -3 ((null))";
                   7, SLT_Timestamp,    "Resp: 1470403414.672458 1.007634 0.000032";
                   7, SLT_ReqAcct,      "82 2 84 304 6962 7266";
                   );

        let record = apply_last!(builder, 7, SLT_End, "")
            .unwrap_client_access();

        assert_matches!(record.compression, None);

        assert_eq!(record.log, &[
                   LogEntry::Error("G(un)zip error: -3 ((null))".to_string()),
        ]);
    }
}
