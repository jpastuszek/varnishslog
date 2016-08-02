use std::collections::HashMap;
use std::str::Utf8Error;
use std::num::{ParseIntError, ParseFloatError};
use quick_error::ResultExt;

use vsl::{VslRecord, VslRecordTag, VslIdent};

use nom::{self, IResult};

pub type TimeStamp = f64;

#[derive(Debug, Clone)]
pub struct AccessRecord {
    pub ident: VslIdent,
    pub start: TimeStamp,
    pub end: TimeStamp,
    pub transaction_type: TransactionType,
    pub transaction: HttpTransaction,
}

#[derive(Debug, Clone)]
pub enum TransactionType {
    Client,
    Backend {
        parent: VslIdent,
        reason: String,
    },
}

impl TransactionType {
    #[allow(dead_code)]
    pub fn is_backend(&self) -> bool {
        match self {
            &TransactionType::Backend { parent: _, reason: _ } => true,
            _ => false
        }
    }

    #[allow(dead_code)]
    pub fn get_backend_parent(&self) -> VslIdent {
        match self {
            &TransactionType::Backend { ref parent, reason: _ } => *parent,
            _ => panic!("unwrap_backend called on TransactionType that was Backend")
        }
    }

    #[allow(dead_code)]
    pub fn get_backend_reason(&self) -> &str {
        match self {
            &TransactionType::Backend { parent: _, ref reason } => reason,
            _ => panic!("unwrap_backend called on TransactionType that was Backend")
        }
    }
}

#[derive(Debug, Clone)]
pub struct HttpTransaction {
    pub request: HttpRequest,
    pub response: HttpResponse,
}

#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub method: String,
    pub url: String,
    pub protocol: String,
    pub headers: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status_name: String,
    pub status_code: u32,
    pub headers: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
struct RecordBuilder {
    ident: VslIdent,
    transaction_type: Option<TransactionType>,
    start: Option<TimeStamp>,
    end: Option<TimeStamp>,
    method: Option<String>,
    url: Option<String>,
    protocol: Option<String>,
    status_name: Option<String>,
    status_code: Option<u32>,
    reason: Option<String>,
    req_headers: HashMap<String, String>,
    resp_headers: HashMap<String, String>,
}

#[derive(Debug)]
enum RecordBuilderResult {
    Building(RecordBuilder),
    Ready(AccessRecord),
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
        UnimplementedTransactionType(transaction_type: String) {
            display("Unimplemented transaction type '{}'", transaction_type)
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

named!(slt_header<&str, (&str, &str)>, complete!(tuple!(
        label,      // Header name
        rest_s)));  // Header value

impl RecordBuilder {
    fn new(ident: VslIdent) -> RecordBuilder {
        RecordBuilder {
            ident: ident,
            transaction_type: None,
            start: None,
            end: None,
            method: None,
            url: None,
            protocol: None,
            status_name: None,
            status_code: None,
            reason: None,
            req_headers: HashMap::new(),
            resp_headers: HashMap::new()
        }
    }

    fn apply<'r>(self, vsl: &'r VslRecord) -> Result<RecordBuilderResult, RecordBuilderError> {
        let builder = match vsl.message() {
            Ok(message) => match vsl.tag {
                VslRecordTag::SLT_Begin => {
                    let (transaction_type, parent, reason) = try!(slt_begin(message).into_result().context(vsl.tag));
                    let parent = try!(parent.parse().context("parent vxid"));

                    let transaction_type = match transaction_type {
                        "bereq" => TransactionType::Backend {
                            parent: parent,
                            reason: reason.to_owned()
                        },
                        _ => return Err(RecordBuilderError::UnimplementedTransactionType(transaction_type.to_string()))
                    };

                    RecordBuilder {
                        transaction_type: Some(transaction_type),
                        .. self
                    }
                }
                VslRecordTag::SLT_Timestamp => {
                    let (label, timestamp, _sice_work_start, _since_last_timestamp) = try!(slt_timestamp(message).into_result().context(vsl.tag));
                    match label {
                        "Start" => RecordBuilder {
                                start: Some(try!(timestamp.parse().context("timestamp"))),
                                .. self
                            },
                            _ => {
                                warn!("Ignoring unknown SLT_Timestamp label variant: {}", label);
                                self
                            }
                    }
                }
                VslRecordTag::SLT_BereqMethod | VslRecordTag::SLT_ReqMethod => {
                    let method = try!(slt_method(message).into_result().context(vsl.tag));

                    RecordBuilder {
                        method: Some(method.to_string()),
                        .. self
                    }
                }
                VslRecordTag::SLT_BereqProtocol | VslRecordTag::SLT_ReqProtocol => {
                    let protocol = try!(slt_protocol(message).into_result().context(vsl.tag));

                    RecordBuilder {
                        protocol: Some(protocol.to_string()),
                        .. self
                    }
                }
                VslRecordTag::SLT_BereqURL | VslRecordTag::SLT_ReqURL => {
                    let url = try!(slt_url(message).into_result().context(vsl.tag));

                    RecordBuilder {
                        url: Some(url.to_string()),
                        .. self
                    }
                }
                VslRecordTag::SLT_BereqHeader | VslRecordTag::SLT_ReqHeader => {
                    let (name, value) = try!(slt_header(message).into_result().context(vsl.tag));

                    let mut headers = self.req_headers;
                    headers.insert(name.to_string(), value.to_string());

                    RecordBuilder {
                        req_headers: headers,
                        .. self
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
pub struct State {
    builders: HashMap<VslIdent, RecordBuilder>
}

impl State {
    pub fn new() -> State {
        State { builders: HashMap::new() }
    }

    pub fn apply(&mut self, vsl: &VslRecord) -> Option<AccessRecord> {
        //TODO: use entry API
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
                RecordBuilderResult::Ready(record) => return Some(record),
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

#[cfg(test)]
mod access_log_state_tests {
    pub use super::*;
    use vsl::{VslRecord, VslRecordTag};

    #[test]
    fn apply_non_utf8() {
        let mut state = State::new();

        state.apply(&VslRecord {
            tag: VslRecordTag::SLT_Begin,
            marker: 0,
            ident: 123,
            data: &[255, 0, 1, 2, 3]
        });

        assert!(state.get(123).is_none());
    }

    #[test]
    fn apply_begin() {
        let mut state = State::new();

        state.apply(&VslRecord::from_str(VslRecordTag::SLT_Begin, 123, "bereq 321 fetch"));

        let builder = state.get(123).unwrap().clone();
        let transaction_type = builder.transaction_type.unwrap();

        assert_eq!(transaction_type.get_backend_parent(), 321);
        assert_eq!(transaction_type.get_backend_reason(), "fetch");
    }

    #[test]
    fn apply_begin_unimpl_transaction_type() {
        let mut state = State::new();

        state.apply(&VslRecord::from_str(VslRecordTag::SLT_Begin, 123, "foo 231 fetch"));
        assert!(state.get(123).is_none());
    }

    #[test]
    fn apply_begin_parser_fail() {
        let mut state = State::new();

        state.apply(&VslRecord::from_str(VslRecordTag::SLT_Begin, 123, "foo bar"));
        assert!(state.get(123).is_none());
    }

    #[test]
    fn apply_begin_float_parse_fail() {
        let mut state = State::new();

        state.apply(&VslRecord::from_str(VslRecordTag::SLT_Begin, 123, "bereq bar fetch"));
        assert!(state.get(123).is_none());
    }

    #[test]
    fn apply_timestamp() {
        let mut state = State::new();

        state.apply(&VslRecord::from_str(VslRecordTag::SLT_Timestamp, 123, "Start: 1469180762.484544 0.000000 0.000000"));

        let builder = state.get(123).unwrap().clone();
        assert_eq!(builder.start, Some(1469180762.484544));
    }

    #[test]
    fn apply_backend_request() {
        let mut state = State::new();

        state.apply(&VslRecord::from_str(VslRecordTag::SLT_Timestamp, 123, "Start: 1469180762.484544 0.000000 0.000000"));
        state.apply(&VslRecord::from_str(VslRecordTag::SLT_BereqMethod, 123, "GET"));
        state.apply(&VslRecord::from_str(VslRecordTag::SLT_BereqURL, 123, "/foobar"));
        state.apply(&VslRecord::from_str(VslRecordTag::SLT_BereqProtocol, 123, "HTTP/1.1"));
        state.apply(&VslRecord::from_str(VslRecordTag::SLT_BereqHeader, 123, "Host: localhost:8080"));
        state.apply(&VslRecord::from_str(VslRecordTag::SLT_BereqHeader, 123, "User-Agent: curl/7.40.0"));

        let builder = state.get(123).unwrap().clone();
        assert_eq!(builder.start, Some(1469180762.484544));
        assert_eq!(builder.method, Some("GET".to_string()));
        assert_eq!(builder.url, Some("/foobar".to_string()));
        assert_eq!(builder.protocol, Some("HTTP/1.1".to_string()));
        assert_eq!(builder.req_headers.get("Host"), Some(&"localhost:8080".to_string()));
        assert_eq!(builder.req_headers.get("User-Agent"), Some(&"curl/7.40.0".to_string()));
    }
}
