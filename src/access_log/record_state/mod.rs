mod record_builder;
use self::record_builder::{RecordBuilder, RecordBuilderError, SessionHead, Record};
use store::VslStore;
use store::Config as StoreConfig;
use vsl::record::VslRecord;
use access_log::record::AccessRecord;
use std::num::Wrapping;
use vsl::record::VslIdent;
use std::rc::Rc;
use std::cell::RefCell;

#[derive(Debug)]
enum Slot {
    Builder(RecordBuilder),
    Session(Rc<RefCell<SessionHead>>),
    Tombstone(RecordBuilderError),
}
use self::Slot::*;

enum SlotAction {
    New(RecordBuilder),
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
#[derive(Debug)]
pub struct RecordState {
    builders: VslStore<Slot>
}

impl Default for RecordState {
    fn default() -> Self {
        RecordState::new()
    }
}

impl RecordState {
    pub fn new() -> RecordState {
        RecordState::with_config(&Default::default())
    }

    pub fn with_config(store_config: &StoreConfig) -> RecordState {
        RecordState {
            builders: VslStore::with_config("builders", Some(Self::on_expire), None, store_config),
        }
    }

    fn on_expire(store_name: &str, current_epoch: Wrapping<u64>, record_epoch: Wrapping<u64>, record_ident: VslIdent, record: &Slot) -> () {
        if let &Slot::Tombstone(_) = record {
            return; // it is normal to expire Tombstone
        }
        VslStore::log_expire(store_name, current_epoch, record_epoch, record_ident, record);
    }

    pub fn apply(&mut self, vsl: &VslRecord) -> Option<AccessRecord> {
        // Do not store 0 SLT_CLI Rd ping etc.
        if ! (vsl.is_client() || vsl.is_backend()) {
            debug!("Skipping non-client/backend record: {}", vsl);
            return None
        }

        let action = match self.builders.get_mut(&vsl.ident) {
            None => {
                match RecordBuilder::new(vsl) {
                    Ok(builder) => New(builder),
                    Err(err) => Kill(err),
                }
            }
            Some(&mut Builder(ref mut builder)) => {
                match builder.apply(vsl) {
                    Ok(true) => Finalize,
                    Ok(false) => Continue,
                    Err(err) => Kill(err),
                }
            }
            Some(&mut Session(ref mut session)) => {
                match session.try_borrow_mut().expect("session already borrowed while trying to update").update(vsl) {
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
            New(mut builder) => {
                // here is the only moment we can look up session in builders since the builder is not yet part of it
                if let Some(session) = builder.session_ident().and_then(|ident| self.lookup_session(&ident)) {
                    builder.set_session(session.clone())
                }
                self.builders.insert(vsl.ident, Builder(builder));
                None
            }
            Finalize => {
                let session = match self.builders.remove(&vsl.ident).unwrap() {
                    Builder(builder) => match builder.build() {
                        Ok(Record::Session(session)) => session,
                        Ok(Record::ClientAccess(record)) => return Some(AccessRecord::ClientAccess(record)),
                        Ok(Record::BackendAccess(record)) => return Some(AccessRecord::BackendAccess(record)),
                        Err(err) => {
                            error!("Error while finalizing record with ident {} after applying {}: {}", &vsl.ident, &vsl, &err);
                            return None
                        }
                    },
                    Session(session) => {
                        let session = match Rc::try_unwrap(session) {
                            Ok(session) => session.into_inner(),
                            // bgfetch records may hold onto the session still
                            Err(session) => session.borrow().clone(),
                        };

                        match session.build() {
                            Ok(session_record) => {
                                return Some(AccessRecord::Session(session_record))
                            }
                            Err(err) => {
                                error!("Error while finalizing session record with ident {} after applying {}: {}", &vsl.ident, &vsl, &err);
                                return None
                            }
                        }
                    }
                    _ => unreachable!()
                };

                self.builders.insert(session.ident, Session(Rc::new(RefCell::new(session))));
                return None
            }
            Kill(err) => {
                match &err {
                    &RecordBuilderError::SpuriousBegin(_) =>
                        warn!("Cannot build record with ident {} after applying {}: {}", &vsl.ident, &vsl, &err),
                    _ =>
                        error!("Error while building record with ident {} while applying {}: {}", &vsl.ident, &vsl, &err)
                }
                // catch all following records
                self.builders.insert(vsl.ident, Tombstone(err));
                return None
            }
            Continue => return None
        }
    }

    pub fn lookup_session(&self, ident: &VslIdent) -> Option<Rc<RefCell<SessionHead>>> {
        match self.builders.get(ident) {
            Some(Builder(_)) |
            Some(Tombstone(_)) |
            None => None,
            Some(Session(session)) => {
                Some(session.clone())
            }
        }
    }

    pub fn building_count(&self) -> usize {
        self.builders.values().filter(|&v| if let Builder(_) = *v { true } else { false }).count()
    }

    pub fn tombstone_count(&self) -> usize {
        self.builders.values().filter(|&v| if let Tombstone(_) = *v { true } else { false }).count()
    }
}

#[cfg(test)]
mod tests {
    pub use super::*;
    pub use super::super::test_helpers::*;
    pub use access_log::record::*;
    pub use vsl::record::*;

    use super::record_builder::RecordBuilder;
    use super::Slot;
    impl RecordState {
        fn get(&self, ident: VslIdent) -> Option<&RecordBuilder> {
            match self.builders.get(&ident) {
                Some(&Slot::Builder(ref builder)) => return Some(builder),
                Some(&Slot::Session(ref session)) => panic!("Found Session: {:#?}", session), 
                Some(&Slot::Tombstone(ref err)) => panic!("Found Tombstone; inscription: {}", err),
                None => None,
            }
        }

        fn is_tombstone(&self, ident: VslIdent) -> bool {
            match self.builders.get(&ident) {
                Some(&Slot::Tombstone(_)) => true,
                _ => false,
            }
        }
    }

    #[test]
    fn apply_record_state_client_access() {
        log();
        let mut state = RecordState::new();

        // logs-new/varnish20160816-4093-lmudum99608ad955ba43288.vsl
        apply_all!(state,
            3, SLT_Begin,          "sess 0 HTTP/1";
            3, SLT_SessOpen,       "192.168.1.10 40078 localhost:1080 127.0.0.1 1080 1469180762.484344 18";
            3, SLT_Proxy,          "2 10.1.1.85 41504 10.1.1.70 443";
            3, SLT_Link,           "req 4 rxreq";
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
                parent: 3,
                ref reason,
                transaction: ClientAccessTransaction::Full {
                    accounting: Accounting {
                        recv_header,
                        recv_body,
                        recv_total,
                        sent_header,
                        sent_body,
                        sent_total,
                    },
                    backend_record: Some(ref backend_record),
                    ref esi_records,
                    ..
                },
                start,
                end: Some(end),
                ..
            } => {
                assert_eq!(recv_header, 95);
                assert_eq!(recv_body, 0);
                assert_eq!(recv_total, 95);
                assert_eq!(sent_header, 1050);
                assert_eq!(sent_body, 1366);
                assert_eq!(sent_total, 2416);
                assert_eq!(reason, "rxreq");
                assert_eq!(backend_record, &Link::Unresolved(5, "fetch".to_string()));
                assert!(esi_records.is_empty());
                assert_eq!(start, parse!("1471355385.239203"));
                assert_eq!(end, parse!("1471355385.239652"));
            }
        );

        assert_matches!(client.transaction, ClientAccessTransaction::Full {
                request: HttpRequest {
                    ref method,
                    ref url,
                    ref protocol,
                    ref headers,
                },
                ..
            } => {
                assert_eq!(method, "GET");
                assert_eq!(url, "/test_page/123.html");
                assert_eq!(protocol, "HTTP/1.1");
                assert_eq!(headers, &[
                    ("Date".to_string(), "Tue, 16 Aug 2016 13:49:45 GMT".to_string()),
                    ("Host".to_string(), "127.0.0.1:1236".to_string())]);
            }
        );

        assert_matches!(client.transaction, ClientAccessTransaction::Full {
                response: HttpResponse {
                    ref protocol,
                    status,
                    ref reason,
                    ref headers,
                },
                ..
            } => {
                assert_eq!(protocol, "HTTP/1.1");
                assert_eq!(status, 503);
                assert_eq!(reason, "Backend fetch failed");
                assert_eq!(headers, &[
                    ("Date".to_string(), "Tue, 16 Aug 2016 13:49:45 GMT".to_string()),
                    ("Server".to_string(), "Varnish".to_string()),
                    ("Content-Length".to_string(), "1366".to_string())]);
            }
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
                ident,
                parent,
                start: Some(start),
                end: Some(end),
                ref reason,
                ..
            } => {
                assert_eq!(ident, 123);
                assert_eq!(parent, 321);
                assert_eq!(reason, "fetch");
                assert_eq!(start, parse!("1469180762.484544"));
                assert_eq!(end, parse!("1469180763.484544"));
            }
        );

        assert_matches!(backend.transaction, BackendAccessTransaction::Failed {
                request: HttpRequest {
                    ref method,
                    ref url,
                    ref protocol,
                    ref headers,
                },
                ..
            } => {
                assert_eq!(method, "GET");
                assert_eq!(url, "/foobar");
                assert_eq!(protocol, "HTTP/1.1");
                assert_eq!(headers, &[
                    ("Host".to_string(), "localhost:8080".to_string()),
                    ("User-Agent".to_string(), "curl/7.40.0".to_string())]);
            }
        );

        assert_matches!(backend.transaction, BackendAccessTransaction::Failed {
                synth_response: HttpResponse {
                    ref protocol,
                    status,
                    ref reason,
                    ref headers,
                },
                ..
            } => {
                assert_eq!(protocol, "HTTP/1.1");
                assert_eq!(status, 503);
                assert_eq!(reason, "Backend fetch failed");
                assert_eq!(headers, &[
                    ("Date".to_string(), "Fri, 22 Jul 2016 09:46:02 GMT".to_string()),
                    ("Server".to_string(), "Varnish".to_string()),
                    ("Content-Type".to_string(), "text/html; charset=utf-8".to_string())]);
            }
        );
    }

    #[test]
    fn apply_record_state_session_head() {
        log();
        let mut state = RecordState::new();

        apply_all!(state,
                123, SLT_Begin,     "sess 0 HTTP/1";
                123, SLT_SessOpen,  "192.168.1.10 40078 localhost:1080 127.0.0.1 1080 1469180762.484344 18";
                123, SLT_Proxy,     "2 10.1.1.85 41504 10.1.1.70 443";
                123, SLT_Link,      "req 32773 rxreq"; // We should have SessionHead available after this
            );

        let record = state.lookup_session(&123);
        let session = assert_some!(record);

        {
            let session_ref = session.borrow();
            let session: &SessionHead = &session_ref;

            assert_matches!(session, &SessionHead {
                    ident,
                    open,
                    local: Some(ref local),
                    ref remote,
                    ref client_records,
                    proxy: Some(Proxy {
                        ref version,
                        ref client,
                        ref server,
                    }),
                    duration: None,
                    close_reason: None,
                } => {
                    assert_eq!(ident, 123);
                    assert_eq!(open, parse!("1469180762.484344"));
                    assert_eq!(local, &("127.0.0.1".to_string(), 1080));
                    assert_eq!(remote, &("192.168.1.10".to_string(), 40078));
                    assert_eq!(client_records, &[Link::Unresolved(32773, "rxreq".to_string())]);
                    assert_eq!(version, "2");
                    assert_eq!(client, &("10.1.1.85".to_string(), 41504));
                    assert_eq!(server, &("10.1.1.70".to_string(), 443));
                }
            );
        } // stop the borrow so we can continue applying VSL to session

        apply_all!(state,
                123, SLT_Link,      "req 32774 rxreq"; // Keep collecing links
                123, SLT_SessClose, "REM_CLOSE 0.001";
            );

        let record = apply_final!(state, 123, SLT_End, "");

        // Session should be built
        assert!(record.is_session());
        let session = record.unwrap_session();

        assert_none!(state.get(123));

        assert_matches!(session, SessionRecord {
                ident,
                open,
                local: Some(ref local),
                ref remote,
                ref client_records,
                proxy: Some(Proxy {
                    ref version,
                    ref client,
                    ref server,
                }),
                duration,
                ref close_reason,
            } => {
                assert_eq!(ident, 123);
                assert_eq!(open, parse!("1469180762.484344"));
                assert_eq!(local, &("127.0.0.1".to_string(), 1080));
                assert_eq!(remote, &("192.168.1.10".to_string(), 40078));
                assert_eq!(client_records, &[
                    Link::Unresolved(32773, "rxreq".to_string()), 
                    Link::Unresolved(32774, "rxreq".to_string())]);
                assert_eq!(version, "2");
                assert_eq!(client, &("10.1.1.85".to_string(), 41504));
                assert_eq!(server, &("10.1.1.70".to_string(), 443));
                assert_eq!(duration, parse!("0.001"));
                assert_eq!(close_reason, "REM_CLOSE");
            }
        );
    }

    #[test]
    fn apply_record_state_session_rx_junk() {
        log();
        let mut state = RecordState::new();

        apply_all!(state,
                69, SLT_Begin,          "sess 0 PROXY";
                69, SLT_SessOpen,       "127.0.0.1 32786 a1 127.0.0.1 2443 1542622357.198996 82";
                69, SLT_ReqAcct,        "126 22 148 351 6 357";
                69, SLT_End,            ""; // NOTE: spurious End
                69, SLT_SessClose,      "RX_JUNK 2.059";
            );

        let record = apply_final!(state, 69, SLT_End, "");

        // Session should be built
        assert!(record.is_session());
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
