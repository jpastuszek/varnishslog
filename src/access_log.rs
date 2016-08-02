use std::collections::HashMap;
use std::str::Utf8Error;
use std::num::ParseFloatError;
use std::fmt::{self, Display};

use vsl::{VslRecord, VslRecordTag, VslIdent};

use nom::IResult;

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
    headers: HashMap<String, String>,
}

#[derive(Debug)]
enum RecordBuilderResult {
    Building(RecordBuilder),
    Ready(AccessRecord),
}

#[derive(Debug)]
enum RecordBuilderError {
    VslBodyUtf8Error(Utf8Error),
}

impl Display for RecordBuilderError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &RecordBuilderError::VslBodyUtf8Error(ref e) => write!(f, "VSL record body is not valid UTF-8 encoded string: {}", e),
        }
    }
}

use nom::{space, eof};
named!(label<&str, &str>, terminated!(take_until_s!(": "), tag_s!(": ")));
named!(space_terminated<&str, &str>, terminated!(is_not_s!(" "), space));
named!(space_terminated_eof<&str, &str>, terminated!(is_not_s!(" "), eof));

named!(SLT_Timestamp<&str, (&str, &str, &str, &str)>, tuple!(
        label,                      // Event label
        space_terminated,           // Absolute time of event
        space_terminated,           // Time since start of work unit
        space_terminated_eof));     // Time since last timestamp

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
            headers: HashMap::new()
        }
    }

    fn apply<'r>(self, vsl: &'r VslRecord) -> Result<RecordBuilderResult, RecordBuilderError> {
        let builder = match vsl.body() {
            Ok(body) => match vsl.tag {
                VslRecordTag::SLT_Begin => {
                    // nom?
                    let mut elements = body.splitn(3, ' ');

                    //TODO: error handling
                    let transaction_type = elements.next().unwrap();
                    let parent = elements.next().unwrap();
                    let reason = elements.next().unwrap();

                    let parent = parent.parse().unwrap();

                    let transaction_type = match transaction_type {
                        "bereq" => TransactionType::Backend { parent: parent, reason: reason.to_owned() },
                        _ => panic!("unimpl transaction_type")
                    };

                    RecordBuilder { transaction_type: Some(transaction_type), .. self }
                }
                VslRecordTag::SLT_Timestamp => {
                    if let IResult::Done(_, (label, timestamp, _sice_work_start, _since_last_timestamp)) =  SLT_Timestamp(body) {
                        match label {
                            "Start" => RecordBuilder { start: Some(try!(timestamp.parse())), .. self },
                            _ => {
                                warn!("Unknown SLT_Timestamp label variant: {}", label);
                                self
                            }
                        }
                    } else {
                        panic!("foobar!")
                    }
                }
                _ => panic!("unimpl tag")
            },
            Err(err) => return Err(RecordBuilderError::VslBodyUtf8Error(err))
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
                //TODO: define proper error and return to main loop
                error!("Error while building record with ident {}: {}", vsl.ident, err);
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
    fn apply_begin() {
        let mut state = State::new();

        state.apply(&VslRecord::from_str(VslRecordTag::SLT_Begin, 123, "bereq 321 fetch"));

        let builder = state.get(123).unwrap().clone();
        let transaction_type = builder.transaction_type.unwrap();

        assert_eq!(transaction_type.get_backend_parent(), 321);
        assert_eq!(transaction_type.get_backend_reason(), "fetch");
    }

    #[test]
    fn apply_timestamp() {
        let mut state = State::new();

        state.apply(&VslRecord::from_str(VslRecordTag::SLT_Timestamp, 123, "Start: 1469180762.484544 0.000000 0.000000"));

        let builder = state.get(123).unwrap().clone();
        assert_eq!(builder.start, Some(1469180762.484544));
    }
}
