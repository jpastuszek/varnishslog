mod record_builder;
//TODO: don't use * imports as it makes things confusing
pub use self::record_builder::*;

#[derive(Debug)]
enum Slot {
    Builder(RecordBuilder),
    Tombstone(RecordBuilderError),
}
use self::Slot::*;

enum SlotAction {
    New,
    Finalize,
    Continue,
    Kill(RecordBuilderError),
}
use self::SlotAction::*;

// Note: tombstones will accumulate over time
// We need to remove Tombstone and RecordBuilder records after a while so they dont
// accumulate in memory. We need to store them long enought so that the will recive all the
// VSL records that are for them. The VslIdent may recycle after a while though so we need
// to make sure that old records that could potentially have same VslIdent are gone log
// beofore.
//
#[derive(Debug, Default)]
pub struct RecordState {
    builders: VslStore<Slot>
}

impl RecordState {
    pub fn new() -> RecordState {
        Default::default()
    }

    pub fn apply(&mut self, vsl: &VslRecord) -> Option<Record> {
        // Do not store 0 SLT_CLI Rd ping etc.
        if ! (vsl.is_client() || vsl.is_backend()) {
            debug!("Skipping non-client/backend record: {}", vsl);
            return None
        }

        let action = match self.builders.get_mut(&vsl.ident) {
            None => New,
            Some(&mut Builder(ref mut builder)) => {
                match builder.apply(vsl) {
                    Ok(true) => Finalize,
                    Ok(false) => Continue,
                    Err(err) => Kill(err),
                }
            }
            Some(&mut Tombstone(ref err)) => {
                debug!("Found tombstone for record with ident {}: ignoring {}; inscription: {}", &vsl.ident, &vsl, err);
                return None
            }
        };

        match action {
            New => {
                self.builders.insert(vsl.ident, Builder(RecordBuilder::new(vsl.ident)));
                return self.apply(vsl)
            }
            Finalize => {
                match self.builders.remove(&vsl.ident).unwrap() {
                    Builder(builder) => match builder.unwrap() {
                        Ok(record) => return Some(record),
                        Err(err) => {
                            error!("Error while finalizing record with ident {} after applying {}: {}", &vsl.ident, &vsl, &err);
                            self.builders.insert(vsl.ident, Tombstone(err));
                        }
                    },
                    _ => unreachable!()
                }
            }
            Kill(err) => {
                error!("Error while building record with ident {} while applying {}: {}", &vsl.ident, &vsl, &err);
                self.builders.insert(vsl.ident, Tombstone(err));
            }
            Continue => (),
        }

        None
    }

    pub fn building_count(&self) -> usize {
        self.builders.values().filter(|&v| if let Builder(_) = *v { true } else { false }).count()
    }

    pub fn tombstone_count(&self) -> usize {
        self.builders.values().filter(|&v| if let Tombstone(_) = *v { true } else { false }).count()
    }

    #[cfg(test)]
    fn get(&self, ident: VslIdent) -> Option<&RecordBuilder> {
        match self.builders.get(&ident) {
            Some(&Builder(ref builder)) => return Some(builder),
            Some(&Tombstone(ref err)) => panic!("Found Tombstone; inscription: {}", err),
            None => None,
        }
    }

    #[cfg(test)]
    fn is_tombstone(&self, ident: VslIdent) -> bool {
        match self.builders.get(&ident) {
            Some(&Tombstone(_)) => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    pub use super::*;
    pub use super::super::test_helpers::*;

    #[test]
    fn apply_record_state_client_access() {
        log();
        let mut state = RecordState::new();

        // logs-new/varnish20160816-4093-lmudum99608ad955ba43288.vsl
        apply_all!(state,
                   4, SLT_Begin,          "req 3 rxreq";
                   4, SLT_Timestamp,      "Start: 1471355385.239203 0.000000 0.000000";
                   4, SLT_Timestamp,      "Req: 1471355385.239203 0.000000 0.000000";
                   4, SLT_ReqStart,       "127.0.0.1 56842";
                   4, SLT_ReqMethod,      "GET";
                   4, SLT_ReqURL,         "/test_page/123.html";
                   4, SLT_ReqProtocol,    "HTTP/1.1";
                   4, SLT_ReqHeader,      "Date: Tue, 16 Aug 2016 13:49:45 GMT";
                   4, SLT_ReqHeader,      "Host: 127.0.0.1:1236";
                   4, SLT_VCL_call,       "RECV";
                   4, SLT_VCL_acl,        "NO_MATCH trusted_networks";
                   4, SLT_VCL_acl,        "NO_MATCH external_proxies";
                   4, SLT_VCL_return,     "hash";
                   4, SLT_VCL_call,       "HASH";
                   4, SLT_VCL_return,     "lookup";
                   4, SLT_VCL_call,       "MISS";
                   4, SLT_VCL_return,     "fetch";
                   4, SLT_Link,           "bereq 5 fetch";
                   4, SLT_Timestamp,      "Fetch: 1471355385.239520 0.000317 0.000317";
                   4, SLT_RespProtocol,   "HTTP/1.1";
                   4, SLT_RespStatus,     "503";
                   4, SLT_RespReason,     "Backend fetch failed";
                   4, SLT_RespHeader,     "Date: Tue, 16 Aug 2016 13:49:45 GMT";
                   4, SLT_RespHeader,     "Server: Varnish";
                   4, SLT_VCL_call,       "DELIVER";
                   4, SLT_VCL_return,     "deliver";
                   4, SLT_Timestamp,      "Process: 1471355385.239622 0.000419 0.000103";
                   4, SLT_RespHeader,     "Content-Length: 1366";
                   4, SLT_Debug,          "RES_MODE 2";
                   4, SLT_Timestamp,      "Resp: 1471355385.239652 0.000449 0.000029";
                   4, SLT_ReqAcct,        "95 0 95 1050 1366 2416";
               );

        let record = apply_final!(state, 4, SLT_End, "");

        assert_none!(state.get(4));

        assert!(record.is_client_access());
        let client = record.unwrap_client_access();
        assert_matches!(client, ClientAccessRecord {
            ident: 4,
            ref reason,
            transaction: ClientAccessTransaction::Full {
                accounting: Accounting {
                    recv_header: 95,
                    recv_body: 0,
                    recv_total: 95,
                    sent_header: 1050,
                    sent_body: 1366,
                    sent_total: 2416,
                },
                backend_record: Some(ref backend_record),
                ref esi_records,
                ..
            },
            start: 1471355385.239203,
            end: Some(1471355385.239652),
            ..
        } if
            reason == "rxreq" &&
            backend_record == &Link::Unresolved(5) &&
            esi_records.is_empty()
        );

        assert_matches!(client.transaction, ClientAccessTransaction::Full {
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

        assert_matches!(client.transaction, ClientAccessTransaction::Full {
            response: HttpResponse {
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
                ("Content-Length".to_string(), "1366".to_string())]
        );
    }

    #[test]
    fn apply_record_state_backend_access() {
        log();
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
               123, SLT_Timestamp,      "Error: 1469180763.484544 0.000000 0.000000";
               123, SLT_BerespProtocol, "HTTP/1.1";
               123, SLT_BerespStatus,   "503";
               123, SLT_BerespReason,   "Service Unavailable";
               123, SLT_BerespReason,   "Backend fetch failed";
               123, SLT_BerespHeader,   "Date: Fri, 22 Jul 2016 09:46:02 GMT";
               123, SLT_BerespHeader,   "Server: Varnish";
               123, SLT_BerespHeader,   "Cache-Control: no-store";
               123, SLT_BerespUnset,    "Cache-Control: no-store";
               123, SLT_BerespHeader,   "Content-Type: text/html; charset=utf-8";
               123, SLT_VCL_call,       "BACKEND_ERROR";
               123, SLT_BereqAcct,        "0 0 0 0 0 0";
               );

        let record = apply_final!(state, 123, SLT_End, "");

        assert_none!(state.get(123));

        assert!(record.is_backend_access());
        let backend = record.unwrap_backend_access();

        assert_matches!(backend, BackendAccessRecord {
            ident: 123,
            start: Some(1469180762.484544),
            end: Some(1469180763.484544),
            ref reason,
            ..
        } if reason == "fetch");

        assert_matches!(backend.transaction, BackendAccessTransaction::Failed {
            request: HttpRequest {
                ref method,
                ref url,
                ref protocol,
                ref headers,
            },
            ..
        } if
            method == "GET" &&
            url == "/foobar" &&
            protocol == "HTTP/1.1" &&
            headers == &[
                ("Host".to_string(), "localhost:8080".to_string()),
                ("User-Agent".to_string(), "curl/7.40.0".to_string())]
        );

        assert_matches!(backend.transaction, BackendAccessTransaction::Failed {
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
                ("Date".to_string(), "Fri, 22 Jul 2016 09:46:02 GMT".to_string()),
                ("Server".to_string(), "Varnish".to_string()),
                ("Content-Type".to_string(), "text/html; charset=utf-8".to_string())]
        );
    }

    #[test]
    fn apply_record_state_session() {
        log();
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
            client_records: vec![Link::Unresolved(32773)],
        });
    }

    #[test]
    fn apply_record_state_failed() {
        log();
        let mut state = RecordState::new();

        apply_all!(state,
               123, SLT_Begin,          "req 321 rxreq";
               123, SLT_Timestamp,      "Start: 1469180762.484544 0.000000 0.000000";
               123, SLT_ReqMethod,      "GET";
               123, SLT_ReqURL,         "/foobar";
               123, SLT_ReqProtocol,    "HTTP/1.1";
               123, SLT_ReqHeader,      "Host: localhost:8080";
               123, SLT_ReqHeader,      "User-Agent: curl/7.40.0";
               123, SLT_ReqHeader,      "Accept-Encoding: gzip";
               123, SLT_ReqUnset,       "Accept-Encoding: gzip";
               123, SLT_VCL_call,       "RECV";
               123, SLT_Link,           "bereq 32774 fetch";
               123, SLT_RespProtocol,   "HTTP/1.1";
               123, SLT_RespStatus,     "503";
               123, SLT_RespReason,     "Service Unavailable";
               123, SLT_RespReason,     "Backend fetch failed";
               123, SLT_RespHeader,     "Date: Fri, 22 Jul 2016 09:46:02 GMT";
               123, SLT_RespHeader,     "Server: Varnish";
               123, SLT_RespHeader,     "BOOM!";
               123, SLT_RespUnset,      "Cache-Control: no-store";
               123, SLT_RespHeader,     "Content-Type: text/html; charset=utf-8";
               123, SLT_Timestamp,      "Resp: 1469180763.484544 0.000000 0.000000";
               );

        apply!(state, 123, SLT_End, "");
        assert!(state.is_tombstone(123));
    }
}
