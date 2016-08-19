/// Session Linking
/// ===
///
/// ESI (logs/varnish20160804-3752-1lr56fj56c2d5925f217f012.vsl):
/// ---
/// 65539 SLT_Begin          req 65538 esi
///
/// 65541 SLT_Begin          req 65538 esi
///
/// 65542 SLT_Begin          bereq 65541 fetch
///
/// 65538 SLT_Begin          req 65537 rxreq
/// 65538 SLT_Link           req 65539 esi
/// 65538 SLT_Link           req 65541 esi
///
/// 65537 SLT_Begin          sess 0 HTTP/1
/// 65537 SLT_SessOpen       127.0.0.1 57408 127.0.0.1:1221 127.0.0.1 1221 1470304807.389646 20
/// 65537 SLT_Link           req 65538 rxreq
/// 65537 SLT_SessClose      REM_CLOSE 3.228
/// 65537 SLT_End
///
/// Grace (logs/varnish20160804-3752-zmjq309e3d02a67cea67299.vsl)
/// ---
///     4 SLT_Begin          req 3 rxreq
///     4 SLT_Link           bereq 5 bgfetch
///
///     5 SLT_Begin          bereq 4 bgfetch
///
///     3 SLT_Begin          sess 0 HTTP/1
///     3 SLT_SessOpen       127.0.0.1 59686 127.0.0.1:1230 127.0.0.1 1230 1470304835.029314 19
///     3 SLT_Link           req 4 rxreq
///     3 SLT_SessClose      RX_TIMEOUT 10.011
///     3 SLT_End
///
/// Restarts (logs/varnish20160804-3752-1h9gf4h5609f5ab778e4a4eb.vsl)
/// ---
/// This can happen at any state of client requests/response handling
///
/// 32770 SLT_Begin          req 32769 rxreq
/// 32770 SLT_ReqHeader      X-Varnish-Decision: Refresh-NotBuildNumber
/// 32770 SLT_VCL_return     restart
/// // No response info
/// 32770 SLT_Link           req 32771 restart
/// 32770 SLT_Timestamp      Restart: 1470304882.576600 0.000136 0.000136
/// 32770 SLT_End
///
/// 32771 SLT_Begin          req 32770 restart
///
/// 32769 SLT_Begin          sess 0 HTTP/1
/// 32769 SLT_SessOpen       127.0.0.1 34560 127.0.0.1:1244 127.0.0.1 1244 1470304882.576266 14
/// 32769 SLT_Link           req 32770 rxreq
/// 32769 SLT_SessClose      REM_CLOSE 0.347
///
/// Retry (varnish20160805-3559-f6sifo45103025c06abad14.vsl)
/// ---
/// Can be used to restart backend fetch in backend thread
///
///     8 SLT_Begin          bereq 7 fetch
///     8 SLT_BereqURL       /retry
///     8 SLT_Link           bereq 32769 retry
///
/// 32769 SLT_Begin          bereq 8 retry
/// 32769 SLT_BereqURL       /iss/v2/thumbnails/foo/4006450256177f4a/bar.jpg
///
///     7 SLT_Begin          req 6 rxreq
///     7 SLT_Link           bereq 8 fetch
///
///     6 SLT_Begin          sess 0 HTTP/1
///     6 SLT_SessOpen       127.0.0.1 39798 127.0.0.1:1200 127.0.0.1 1200 1470403414.664642 17
///     6 SLT_Link           req 7 rxreq
///     6 SLT_SessClose      REM_CLOSE 0.008
///     6 SLT_End

use std::collections::HashMap;

use vsl::{VslRecord, VslIdent};
pub use super::record_state::*;

#[derive(Debug)]
pub struct SessionState {
    record_state: RecordState,
    client: HashMap<VslIdent, ClientAccessRecord>,
    backend: HashMap<VslIdent, BackendAccessRecord>,
    sessions: Vec<SessionRecord>,
}

impl SessionState {
    pub fn new() -> SessionState {
        //TODO: some sort of expirity mechanism like LRU
        SessionState {
            record_state: RecordState::new(),
            client: HashMap::new(),
            backend: HashMap::new(),
            sessions: Vec::new(),
        }
    }

    fn try_resolve_sessions(&mut self) -> Option<SessionRecord> {
        fn try_resolve_client_link(link: &mut Link<ClientAccessRecord>,
                              client_records: &mut HashMap<VslIdent, ClientAccessRecord>,
                              backend_records: &mut HashMap<VslIdent, BackendAccessRecord>) -> bool {
            if let Some(client_record) = if let &mut Link::Unresolved(ref ident) = link {
                client_records.remove(ident)
            } else {
                None
            } {
                *link = Link::Resolved(Box::new(client_record))
            }

            if let &mut Link::Resolved(ref mut client_record) = link {
                try_resolve_client_record(client_record, client_records, backend_records)
            } else {
                false
            }
        }

        fn try_resolve_backend_link(link: &mut Link<BackendAccessRecord>,
                               backend_records: &mut HashMap<VslIdent, BackendAccessRecord>) -> bool {
            if let Some(backend_record) = if let &mut Link::Unresolved(ref ident) = link {
                backend_records.remove(ident)
            } else {
                None
            } {
                *link = Link::Resolved(Box::new(backend_record))
            }

            if let &mut Link::Resolved(ref mut backend_record) = link {
                try_resolve_backend_record(backend_record, backend_records)
            } else {
                false
            }
        }

        fn try_resolve_backend_record(backend_record: &mut BackendAccessRecord,
                              backend_records: &mut HashMap<VslIdent, BackendAccessRecord>) -> bool {
            match backend_record.transaction {
                BackendAccessTransaction::Failed {
                    ref mut retry_request,
                    ..
                } |
                BackendAccessTransaction::Abandoned {
                    ref mut retry_request,
                    ..
                } => {
                    if let &mut Some(ref mut link) = retry_request {
                        try_resolve_backend_link(link, backend_records)
                    } else {
                        true
                    }
                }
                BackendAccessTransaction::Aborted { .. } => true,
                BackendAccessTransaction::Full { .. } => true,
                BackendAccessTransaction::Piped { .. } => true,
            }
        }

        fn try_resolve_client_record(client_record: &mut ClientAccessRecord,
                              client_records: &mut HashMap<VslIdent, ClientAccessRecord>,
                              backend_records: &mut HashMap<VslIdent, BackendAccessRecord>) -> bool {
            let backend_request_resolved = match client_record.transaction {
                ClientAccessTransaction::Full {
                    ref mut backend_request,
                    ..
                } |
                ClientAccessTransaction::Piped {
                    ref mut backend_request,
                    ..
                } => {
                    if let &mut Some(ref mut link) = backend_request {
                        try_resolve_backend_link(link, backend_records)
                    } else {
                        true
                    }
                }
                ClientAccessTransaction::Restarted { .. } => true,
            };

            let esi_requests_resolved = match client_record.transaction {
                ClientAccessTransaction::Full {
                    ref mut esi_requests,
                    ..
                } => {
                    esi_requests.iter_mut().all(|link|
                        try_resolve_client_link(link, client_records, backend_records)
                    )
                }
                ClientAccessTransaction::Restarted { .. } => true,
                ClientAccessTransaction::Piped { .. } => true,
            };

            let restart_request_resolved = match client_record.transaction {
                ClientAccessTransaction::Restarted {
                    restart_request: ref mut link,
                    ..
                } => {
                    try_resolve_client_link(link, client_records, backend_records)
                }
                ClientAccessTransaction::Full { .. } => true,
                ClientAccessTransaction::Piped { .. } => true,
            };

            backend_request_resolved && esi_requests_resolved && restart_request_resolved
        }

        fn try_resolve_session_record(session_record: &mut SessionRecord,
                               client_records: &mut HashMap<VslIdent, ClientAccessRecord>,
                               backend_records: &mut HashMap<VslIdent, BackendAccessRecord>) -> bool {
            session_record.client_requests.iter_mut().all(|link|
                try_resolve_client_link(link, client_records, backend_records)
            )
        }

        let sessions = self.sessions.split_off(0);

        let (mut resolved, unresolved): (Vec<(bool, SessionRecord)>, Vec<(bool, SessionRecord)>) =
            sessions.into_iter()
            .map(|mut session|
                (
                    try_resolve_session_record(&mut session, &mut self.client, &mut self.backend),
                    session
                )
            )
            .partition(|&(resolved, _)| resolved);

        self.sessions.extend(unresolved.into_iter().map(|(_, session)| session));

        assert!(resolved.is_empty() || resolved.len() == 1, "each new record may resolve only one session but got more!");

        resolved.pop().map(|(_, session)| session)
    }

    pub fn apply(&mut self, vsl: &VslRecord) -> Option<SessionRecord> {
        if match self.record_state.apply(vsl) {
            Some(Record::ClientAccess(record)) => {
                self.client.insert(record.ident, record);
                true
            }
            Some(Record::BackendAccess(record)) => {
                self.backend.insert(record.ident, record);
                true
            }
            Some(Record::Session(session)) => {
                self.sessions.push(session);
                true
            },
            None => false
        } {
            self.try_resolve_sessions()
        } else {
            None
        }
    }

    pub fn unmatched_client_access_records(&self) -> Vec<&ClientAccessRecord> {
        self.client.iter().map(|(_, record)| record).collect()
    }

    pub fn unmatched_backend_access_records(&self) -> Vec<&BackendAccessRecord> {
        self.backend.iter().map(|(_, record)| record).collect()
    }

    pub fn unresolved_sessions(&self) -> &[SessionRecord] {
        self.sessions.as_slice()
    }
}

#[cfg(test)]
mod tests {
    pub use super::*;
    pub use super::super::test_helpers::*;

    //TODO: testing too much here; should only test session state related structures and how they
    //are put together

    #[test]
    fn apply_session_state() {
        log();
        let mut state = SessionState::new();

        apply_all!(state,
               100, SLT_Begin,          "req 10 rxreq";
               100, SLT_Timestamp,      "Start: 1469180762.484544 0.000000 0.000000";
               100, SLT_Timestamp,      "Req: 1469180762.484544 0.000000 0.000000";
               100, SLT_ReqMethod,      "GET";
               100, SLT_ReqURL,         "/foobar";
               100, SLT_ReqProtocol,    "HTTP/1.1";
               100, SLT_ReqHeader,      "Host: localhost:8080";
               100, SLT_ReqHeader,      "User-Agent: curl/7.40.0";
               100, SLT_ReqHeader,      "Accept-Encoding: gzip";
               100, SLT_ReqUnset,       "Accept-Encoding: gzip";
               100, SLT_VCL_call,       "RECV";
               100, SLT_Link,           "bereq 1000 fetch";
               100, SLT_Timestamp,      "Fetch: 1469180763.484544 1.000000 0.000000";
               100, SLT_RespProtocol,   "HTTP/1.1";
               100, SLT_RespStatus,     "503";
               100, SLT_RespReason,     "Service Unavailable";
               100, SLT_RespReason,     "Backend fetch failed";
               100, SLT_RespHeader,     "Date: Fri, 22 Jul 2016 09:46:02 GMT";
               100, SLT_RespHeader,     "Server: Varnish";
               100, SLT_RespHeader,     "Cache-Control: no-store";
               100, SLT_RespUnset,      "Cache-Control: no-store";
               100, SLT_RespHeader,     "Content-Type: text/html; charset=utf-8";
               100, SLT_Timestamp,      "Process: 1469180765.484544 2.000000 1.000000";
               100, SLT_Timestamp,      "Resp: 1469180766.484544 3.000000 1.000000";
               100, SLT_ReqAcct,        "82 2 84 304 6962 7266";
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
               1000, SLT_Timestamp,     "Bereq: 1469180763.484544 1.000000 1.000000";
               1000, SLT_Timestamp,     "Error: 1469180764.484544 2.000000 1.000000";
               1000, SLT_BerespProtocol, "HTTP/1.1";
               1000, SLT_BerespStatus,  "503";
               1000, SLT_BerespReason,  "Service Unavailable";
               1000, SLT_BerespReason,  "Backend fetch failed";
               1000, SLT_BerespHeader,  "Date: Fri, 22 Jul 2016 09:46:02 GMT";
               1000, SLT_BerespHeader,  "Server: Varnish";
               1000, SLT_BerespHeader,  "Cache-Control: no-store";
               1000, SLT_BerespUnset,   "Cache-Control: no-store";
               1000, SLT_BerespHeader,  "Content-Type: text/html; charset=utf-8";
               1000, SLT_VCL_call,      "BACKEND_ERROR";
               1000, SLT_End,           "";

               10, SLT_Begin,       "sess 0 HTTP/1";
               10, SLT_SessOpen,    "192.168.1.10 40078 localhost:1080 127.0.0.1 1080 1469180762.484344 18";
               10, SLT_Link,        "req 100 rxreq";
               10, SLT_SessClose,   "REM_CLOSE 0.001";
               );

        let session_record = apply_final!(state, 10, SLT_End, "");

        let client_record = session_record.client_requests[0].get_resolved().unwrap();

        assert_matches!(client_record, &ClientAccessRecord {
            ident: 100,
            parent: 10,
            start: 1469180762.484544,
            end: 1469180766.484544,
            ref reason,
            transaction: ClientAccessTransaction::Full {
                backend_request: Some(_),
                ref esi_requests,
                ..
            },
            ..
        } if
            reason == "rxreq" &&
            esi_requests.is_empty()
        );

        assert_matches!(client_record.transaction, ClientAccessTransaction::Full {
            ref request,
            ..
        } if
            request == &HttpRequest {
                method: "GET".to_string(),
                url: "/foobar".to_string(),
                protocol: "HTTP/1.1".to_string(),
                headers: vec![
                    ("Host".to_string(), "localhost:8080".to_string()),
                    ("User-Agent".to_string(), "curl/7.40.0".to_string())]
        });

        assert_matches!(client_record.transaction, ClientAccessTransaction::Full {
            ref response,
            ..
        } if
            response == &HttpResponse {
                protocol: "HTTP/1.1".to_string(),
                status: 503,
                reason: "Backend fetch failed".to_string(),
                headers: vec![
                    ("Date".to_string(), "Fri, 22 Jul 2016 09:46:02 GMT".to_string()),
                    ("Server".to_string(), "Varnish".to_string()),
                    ("Content-Type".to_string(), "text/html; charset=utf-8".to_string())]
        });

        if let ClientAccessTransaction::Full { backend_request: Some(ref backend_request), .. } = client_record.transaction {
            let backend_record = backend_request.get_resolved().unwrap();

            assert_matches!(backend_record, &BackendAccessRecord {
                ident: 1000,
                parent: 100,
                start: 1469180762.484544,
                end: Some(1469180764.484544),
                ref reason,
                ..
            } if reason == "fetch");

            assert_matches!(backend_record.transaction, BackendAccessTransaction::Failed {
                ref request,
                ..
            } if
                request == &HttpRequest {
                    method: "GET".to_string(),
                    url: "/foobar".to_string(),
                    protocol: "HTTP/1.1".to_string(),
                    headers: vec![
                        ("Host".to_string(), "localhost:8080".to_string()),
                        ("User-Agent".to_string(), "curl/7.40.0".to_string())]
            });
            assert_matches!(backend_record.transaction, BackendAccessTransaction::Failed {
                ref synth_response,
                ..
            } if
                synth_response == &HttpResponse {
                    protocol: "HTTP/1.1".to_string(),
                    status: 503,
                    reason: "Backend fetch failed".to_string(),
                    headers: vec![
                        ("Date".to_string(), "Fri, 22 Jul 2016 09:46:02 GMT".to_string()),
                        ("Server".to_string(), "Varnish".to_string()),
                        ("Content-Type".to_string(), "text/html; charset=utf-8".to_string())]
            });
        } else {
            unreachable!()
        }

        assert_matches!(session_record, SessionRecord {
            ident: 10,
            open: 1469180762.484344,
            duration: 0.001,
            local: Some((ref local, 1080)),
            remote: (ref remote, 40078),
            ref client_requests,
        } if
            local == "127.0.0.1" &&
            remote == "192.168.1.10" &&
            client_requests.len() == 1
        );
    }

    #[test]
    fn apply_session_state_esi() {
        log();
        let mut state = SessionState::new();

        // logs/varnish20160804-3752-1lr56fj56c2d5925f217f012.vsl
        apply_all!(state,
               65540, SLT_Begin,            "bereq 65539 fetch";
               65540, SLT_Timestamp,        "Start: 1470304807.390145 0.000000 0.000000";
               65540, SLT_BereqMethod,      "GET";
               65540, SLT_BereqURL,         "/esi/hello";
               65540, SLT_BereqProtocol,    "HTTP/1.1";
               65540, SLT_BereqHeader,      "X-Backend-Set-Header-X-Accel-ESI: true";
               65540, SLT_VCL_return,       "fetch";
               65540, SLT_BackendOpen,      "19 boot.default 127.0.0.1 42000 127.0.0.1 41744";
               65540, SLT_BackendStart,     "127.0.0.1 42000";
               65540, SLT_Timestamp,        "Bereq: 1470304807.390223 0.000078 0.000078";
               65540, SLT_Timestamp,        "Beresp: 1470304807.395378 0.005234 0.005155";
               65540, SLT_BerespProtocol,   "HTTP/1.1";
               65540, SLT_BerespStatus,     "200";
               65540, SLT_BerespReason,     "OK";
               65540, SLT_BerespHeader,     "Content-Type: text/html; charset=utf-8";
               65540, SLT_VCL_call,         "BACKEND_RESPONSE";
               65540, SLT_TTL,              "RFC 12345 10 -1 1470304807 1470304807 1340020138 0 12345";
               65540, SLT_VCL_return,       "deliver";
               65540, SLT_Storage,          "malloc s0";
               65540, SLT_ObjProtocol,      "HTTP/1.1";
               65540, SLT_ObjStatus,        "200";
               65540, SLT_ObjReason,        "OK";
               65540, SLT_ObjHeader,        "Content-Type: text/html; charset=utf-8";
               65540, SLT_Fetch_Body,       "3 length -";
               65540, SLT_BackendReuse,     "19 boot.default";
               65540, SLT_Timestamp,        "BerespBody: 1470304807.435149 0.045005 0.039771";
               65540, SLT_Length,           "5";
               65540, SLT_BereqAcct,        "637 0 637 398 5 403";
               65540, SLT_End,              "";

               65541, SLT_Begin,            "req 65538 esi";
               65541, SLT_ReqURL,           "/esi/world";
               65541, SLT_Timestamp,        "Start: 1470304807.435266 0.000000 0.000000";
               65541, SLT_ReqStart,         "127.0.0.1 57408";
               65541, SLT_ReqMethod,        "GET";
               65541, SLT_ReqURL,           "/esi/world";
               65541, SLT_ReqProtocol,      "HTTP/1.1";
               65541, SLT_ReqHeader,        "X-Backend-Set-Header-X-Accel-ESI: true";
               65541, SLT_VCL_call,         "RECV";
               65541, SLT_Link,             "bereq 65542 fetch";
               65541, SLT_Timestamp,        "Fetch: 1470304807.479151 0.043886 0.043886";
               65541, SLT_RespProtocol,     "HTTP/1.1";
               65541, SLT_RespStatus,       "200";
               65541, SLT_RespReason,       "OK";
               65541, SLT_RespHeader,       "Content-Type: text/html; charset=utf-8";
               65541, SLT_Timestamp,        "Process: 1470304807.479171 0.043905 0.000019";
               65541, SLT_RespHeader,       "Accept-Ranges: bytes";
               65541, SLT_Timestamp,        "Resp: 1470304807.479196 0.043930 0.000025";
               65541, SLT_ReqAcct,          "0 0 0 0 5 5";
               65541, SLT_End,              "";

               65539, SLT_Begin,            "req 65538 esi";
               65539, SLT_ReqURL,           "/esi/world";
               65539, SLT_Timestamp,        "Start: 1470304807.435266 0.000000 0.000000";
               65539, SLT_ReqStart,         "127.0.0.1 57408";
               65539, SLT_ReqMethod,        "GET";
               65539, SLT_ReqURL,           "/esi/world";
               65539, SLT_ReqProtocol,      "HTTP/1.1";
               65539, SLT_ReqHeader,        "X-Backend-Set-Header-X-Accel-ESI: true";
               65539, SLT_VCL_call,         "RECV";
               65539, SLT_Link,             "bereq 65543 fetch";
               65539, SLT_Timestamp,        "Fetch: 1470304807.479151 0.043886 0.043886";
               65539, SLT_RespProtocol,     "HTTP/1.1";
               65539, SLT_RespStatus,       "200";
               65539, SLT_RespReason,       "OK";
               65539, SLT_RespHeader,       "Content-Type: text/html; charset=utf-8";
               65539, SLT_Timestamp,        "Process: 1470304807.479171 0.043905 0.000019";
               65539, SLT_RespHeader,       "Accept-Ranges: bytes";
               65539, SLT_Timestamp,        "Resp: 1470304807.479196 0.043930 0.000025";
               65539, SLT_ReqAcct,          "0 0 0 0 5 5";
               65539, SLT_End,              "";

               65542, SLT_Begin,            "bereq 65541 fetch";
               65542, SLT_Timestamp,        "Start: 1470304807.435378 0.000000 0.000000";
               65542, SLT_BereqMethod,      "GET";
               65542, SLT_BereqURL,         "/esi/world";
               65542, SLT_BereqProtocol,    "HTTP/1.1";
               65542, SLT_BereqHeader,      "X-Backend-Set-Header-X-Accel-ESI: true";
               65542, SLT_VCL_return,       "fetch";
               65542, SLT_BackendOpen,      "19 boot.default 127.0.0.1 42000 127.0.0.1 41744";
               65542, SLT_BackendStart,     "127.0.0.1 42000";
               65542, SLT_Timestamp,        "Bereq: 1470304807.435450 0.000072 0.000072";
               65542, SLT_Timestamp,        "Beresp: 1470304807.439882 0.004504 0.004432";
               65542, SLT_BerespProtocol,   "HTTP/1.1";
               65542, SLT_BerespStatus,     "200";
               65542, SLT_BerespReason,     "OK";
               65542, SLT_BerespHeader,     "Content-Type: text/html; charset=utf-8";
               65542, SLT_TTL,              "RFC 120 10 -1 1471339883 1471339880 1340020138 0 0";
               65542, SLT_VCL_call,         "BACKEND_RESPONSE";
               65542, SLT_Storage,          "malloc s0";
               65542, SLT_ObjProtocol,      "HTTP/1.1";
               65542, SLT_ObjStatus,        "200";
               65542, SLT_ObjReason,        "OK";
               65542, SLT_ObjHeader,        "Content-Type: text/html; charset=utf-8";
               65542, SLT_ObjHeader,        "X-Aspnet-Version: 4.0.30319";
               65542, SLT_Fetch_Body,       "3 length -";
               65542, SLT_BackendReuse,     "19 boot.default";
               65542, SLT_Timestamp,        "BerespBody: 1470304807.479137 0.043759 0.039255";
               65542, SLT_Length,           "5";
               65542, SLT_BereqAcct,        "637 0 637 398 5 403";
               65542, SLT_End,              "";

               65543, SLT_Begin,            "bereq 65539 fetch";
               65543, SLT_Timestamp,        "Start: 1470304807.435378 0.000000 0.000000";
               65543, SLT_BereqMethod,      "GET";
               65543, SLT_BereqURL,         "/esi/world";
               65543, SLT_BereqProtocol,    "HTTP/1.1";
               65543, SLT_BereqHeader,      "X-Backend-Set-Header-X-Accel-ESI: true";
               65543, SLT_VCL_return,       "fetch";
               65543, SLT_BackendOpen,      "19 boot.default 127.0.0.1 42000 127.0.0.1 41744";
               65543, SLT_BackendStart,     "127.0.0.1 42000";
               65543, SLT_Timestamp,        "Bereq: 1470304807.435450 0.000072 0.000072";
               65543, SLT_Timestamp,        "Beresp: 1470304807.439882 0.004504 0.004432";
               65543, SLT_BerespProtocol,   "HTTP/1.1";
               65543, SLT_BerespStatus,     "200";
               65543, SLT_BerespReason,     "OK";
               65543, SLT_BerespHeader,     "Content-Type: text/html; charset=utf-8";
               65543, SLT_TTL,              "RFC 120 10 -1 1471339883 1471339880 1340020138 0 0";
               65543, SLT_VCL_call,         "BACKEND_RESPONSE";
               65543, SLT_Storage,          "malloc s0";
               65543, SLT_ObjProtocol,      "HTTP/1.1";
               65543, SLT_ObjStatus,        "200";
               65543, SLT_ObjReason,        "OK";
               65543, SLT_ObjHeader,        "Content-Type: text/html; charset=utf-8";
               65543, SLT_ObjHeader,        "X-Aspnet-Version: 4.0.30319";
               65543, SLT_Fetch_Body,       "3 length -";
               65543, SLT_BackendReuse,     "19 boot.default";
               65543, SLT_Timestamp,        "BerespBody: 1470304807.479137 0.043759 0.039255";
               65543, SLT_Length,           "5";
               65543, SLT_BereqAcct,        "637 0 637 398 5 403";
               65543, SLT_End,              "";

               65538, SLT_Begin,            "req 65537 rxreq";
               65538, SLT_Timestamp,        "Start: 1470304807.389831 0.000000 0.000000";
               65538, SLT_Timestamp,        "Req: 1470304807.389831 0.000000 0.000000";
               65538, SLT_ReqStart,         "127.0.0.1 57408";
               65538, SLT_ReqMethod,        "GET";
               65538, SLT_ReqURL,           "/esi/index";
               65538, SLT_ReqProtocol,      "HTTP/1.1";
               65538, SLT_ReqHeader,        "X-Backend-Set-Header-X-Accel-ESI: true";
               65538, SLT_VCL_call,         "RECV";
               65538, SLT_RespProtocol,     "HTTP/1.1";
               65538, SLT_RespStatus,       "200";
               65538, SLT_RespReason,       "OK";
               65538, SLT_RespHeader,       "Content-Type: text/html; charset=utf-8";
               65538, SLT_VCL_return,       "deliver";
               65538, SLT_Timestamp,        "Process: 1470304807.390023 0.000192 0.000192";
               65538, SLT_Link,             "req 65539 esi";
               65538, SLT_Link,             "req 65541 esi";
               65538, SLT_Timestamp,        "Resp: 1470304807.479222 0.089391 0.089199";
               65538, SLT_ReqAcct,          "220 0 220 1423 29 1452";
               65538, SLT_End,              "";

               65537, SLT_Begin,            "sess 0 HTTP/1";
               65537, SLT_SessOpen,         "127.0.0.1 57408 127.0.0.1:1221 127.0.0.1 1221 1470304807.389646 20";
               65537, SLT_Link,             "req 65538 rxreq";
               65537, SLT_SessClose,        "REM_CLOSE 3.228";
              );

        let session_record = apply_final!(state, 65537, SLT_End, "");

        // We will have esi_transactions in client request
        if let ClientAccessTransaction::Full { ref esi_requests, .. } =
            session_record.client_requests[0].get_resolved().unwrap().transaction {
            assert_eq!(esi_requests[0].get_resolved().unwrap().reason, "esi".to_string());

            if let ClientAccessTransaction::Full {
                ref esi_requests,
                backend_request: Some(ref backend_request),
                ..
            } = esi_requests[0].get_resolved().unwrap().transaction {
                assert!(esi_requests.is_empty());
                assert_matches!(backend_request.get_resolved().unwrap(),
                    &BackendAccessRecord {
                        ref reason,
                        ..
                    } if reason == "fetch"
                );

            } else {
                unreachable!()
            }
        } else {
            unreachable!()
        }
    }

    #[test]
    fn apply_session_state_grace() {
        log();
        let mut state = SessionState::new();

        apply_all!(state,
               65539, SLT_Begin,            "sess 0 HTTP/1";
               65539, SLT_SessOpen,         "127.0.0.1 59694 127.0.0.1:1230 127.0.0.1 1230 1470304835.059145 22";

               65540, SLT_Begin,            "req 65539 rxreq";
               65540, SLT_Timestamp,        "Start: 1470304835.059319 0.000000 0.000000";
               65540, SLT_Timestamp,        "Req: 1470304835.059319 0.000000 0.000000";
               65540, SLT_ReqStart,         "127.0.0.1 59694";
               65540, SLT_ReqMethod,        "GET";
               65540, SLT_ReqURL,           "/test_page/123.html";
               65540, SLT_ReqProtocol,      "HTTP/1.1";
               65540, SLT_ReqHeader,        "X-Varnish-Force-Zero-TTL: true";
               65540, SLT_VCL_call,         "RECV";
               65540, SLT_Hit,              "98307";
               65540, SLT_ReqHeader,        "X-Varnish-Result: hit/sick_grace";
               65540, SLT_VCL_return,       "deliver";
               65540, SLT_Link,             "bereq 65541 bgfetch";
               65540, SLT_Timestamp,        "Fetch: 1470304835.059472 0.000154 0.000154";
               65540, SLT_RespProtocol,     "HTTP/1.1";
               65540, SLT_RespStatus,       "200";
               65540, SLT_RespReason,       "OK";
               65540, SLT_RespHeader,       "Content-Type: text/html; charset=utf-8";
               65540, SLT_RespHeader,       "X-Varnish-Privileged-Client: true";
               65540, SLT_Timestamp,        "Process: 1470304835.059589 0.000270 0.000117";
               65540, SLT_Timestamp,        "Resp: 1470304835.059629 0.000311 0.000041";
               65540, SLT_ReqAcct,          "82 2 84 304 6962 7266";
               65540, SLT_End,              "";

               //Note: session may end before bgfetch is finished!
               65539, SLT_Link,             "req 65540 rxreq";
               65539, SLT_SessClose,        "RX_TIMEOUT 10.001";
               65539, SLT_End,              "";

               65541, SLT_Begin,            "bereq 65540 bgfetch";
               65541, SLT_Timestamp,        "Start: 1470304835.059425 0.000000 0.000000";
               65541, SLT_BereqMethod,      "GET";
               65541, SLT_BereqURL,         "/test_page/123.html";
               65541, SLT_BereqProtocol,    "HTTP/1.1";
               65541, SLT_BereqHeader,      "X-Varnish-Force-Zero-TTL: true";
               65541, SLT_VCL_return,       "fetch";
               65541, SLT_Timestamp,        "Beresp: 1470304835.059475 0.000050 0.000050";
               65541, SLT_Timestamp,        "Error: 1470304835.059479 0.000054 0.000004";
               65541, SLT_BerespProtocol,   "HTTP/1.1";
               65541, SLT_BerespStatus,     "503";
               65541, SLT_BerespReason,     "Service Unavailable";
               65541, SLT_BerespReason,     "Backend fetch failed";
               65541, SLT_BerespHeader,     "Date: Thu, 04 Aug 2016 10:00:35 GMT";
               65541, SLT_BerespHeader,     "Server: Varnish";
               65541, SLT_VCL_call,         "BACKEND_ERROR";
               65541, SLT_Length,           "1366";
               65541, SLT_BereqAcct,        "0 0 0 0 0 0";
               );

       // Note that we are ending the bgfetch request as session is already closed
       let session_record = apply_final!(state, 65541, SLT_End, "");

       // It is handled as ususal; only difference is backend request reason
       if let ClientAccessTransaction::Full { backend_request: Some(ref backend_request), .. } =
           session_record.client_requests[0].get_resolved().unwrap().transaction {
           assert_eq!(backend_request.get_resolved().unwrap().reason, "bgfetch".to_string());
       } else {
           unreachable!()
       }
    }

    /*
    #[test]
    fn apply_session_state_restart() {
        log();
        let mut state = SessionState::new();

        apply_all!(state,
                   32770, SLT_Begin,            "req 32769 rxreq";
                   32770, SLT_Timestamp,        "Start: 1470304882.576464 0.000000 0.000000";
                   32770, SLT_Timestamp,        "Req: 1470304882.576464 0.000000 0.000000";
                   32770, SLT_ReqStart,         "127.0.0.1 34560";
                   32770, SLT_ReqMethod,        "GET";
                   32770, SLT_ReqURL,           "/foo/thumbnails/foo/4006450256177f4a/bar.jpg?type=brochure";
                   32770, SLT_ReqProtocol,      "HTTP/1.1";
                   32770, SLT_ReqHeader,        "X-Backend-Set-Header-Cache-Control: public, max-age=12345";
                   32770, SLT_VCL_call,         "RECV";
                   32770, SLT_VCL_return,       "restart";
                   32770, SLT_Timestamp,        "Restart: 1470304882.576600 0.000136 0.000136";
                   32770, SLT_Link,             "req 32771 restart";
                   32770, SLT_ReqAcct,          "82 2 84 304 6962 7266";
                   32770, SLT_End,              "";

                   32771, SLT_Begin,            "req 32770 restart";
                   32771, SLT_Timestamp,        "Start: 1470304882.576600 0.000136 0.000000";
                   32771, SLT_ReqStart,         "127.0.0.1 34560";
                   32771, SLT_ReqMethod,        "GET";
                   32771, SLT_ReqURL,           "/iss/v2/thumbnails/foo/4006450256177f4a/bar.jpg?type=brochure";
                   32771, SLT_ReqProtocol,      "HTTP/1.1";
                   32771, SLT_ReqHeader,        "X-Backend-Set-Header-Cache-Control: public, max-age=12345";
                   32771, SLT_VCL_call,         "RECV";
                   32771, SLT_Link,             "bereq 32772 fetch";
                   32771, SLT_Timestamp,        "Fetch: 1470304882.579218 0.002754 0.002618";
                   32771, SLT_RespProtocol,     "HTTP/1.1";
                   32771, SLT_RespStatus,       "200";
                   32771, SLT_RespReason,       "OK";
                   32771, SLT_RespHeader,       "Content-Type: image/jpeg";
                   32771, SLT_VCL_return,       "deliver";
                   32771, SLT_Timestamp,        "Process: 1470304882.579312 0.002848 0.000094";
                   32771, SLT_RespHeader,       "Accept-Ranges: bytes";
                   32771, SLT_Debug,            "RES_MODE 2";
                   32771, SLT_RespHeader,       "Connection: keep-alive";
                   32771, SLT_Timestamp,        "Resp: 1470304882.615250 0.038785 0.035938";
                   32771, SLT_ReqAcct,          "324 0 324 1445 6962 8407";
                   32771, SLT_End,              "";

                   32772, SLT_Begin,            "bereq 32771 fetch";
                   32772, SLT_Timestamp,        "Start: 1470304882.576644 0.000000 0.000000";
                   32772, SLT_BereqMethod,      "GET";
                   32772, SLT_BereqURL,         "/iss/v2/thumbnails/foo/4006450256177f4a/bar.jpg?type=brochure";
                   32772, SLT_BereqProtocol,    "HTTP/1.1";
                   32772, SLT_BereqHeader,      "X-Backend-Set-Header-Cache-Control: public, max-age=12345";
                   32772, SLT_VCL_return,       "fetch";
                   32772, SLT_Timestamp,        "Bereq: 1470304882.576719 0.000074 0.000074";
                   32772, SLT_Timestamp,        "Beresp: 1470304882.579056 0.002412 0.002337";
                   32772, SLT_BerespProtocol,   "HTTP/1.1";
                   32772, SLT_BerespStatus,     "200";
                   32772, SLT_BerespReason,     "OK";
                   32772, SLT_BerespHeader,     "Content-Type: image/jpeg";
                   32772, SLT_TTL,              "RFC 120 10 -1 1471339883 1471339880 1340020138 0 0";
                   32772, SLT_VCL_call,         "BACKEND_RESPONSE";
                   32772, SLT_Storage,          "malloc s0";
                   32772, SLT_ObjProtocol,      "HTTP/1.1";
                   32772, SLT_ObjStatus,        "200";
                   32772, SLT_ObjReason,        "OK";
                   32772, SLT_ObjHeader,        "Content-Type: text/html; charset=utf-8";
                   32772, SLT_ObjHeader,        "X-Aspnet-Version: 4.0.30319";
                   32772, SLT_Fetch_Body,       "3 length stream";
                   32772, SLT_BackendReuse,     "19 boot.iss";
                   32772, SLT_Timestamp,        "BerespBody: 1470304882.615228 0.038584 0.036172";
                   32772, SLT_Length,           "6962";
                   32772, SLT_BereqAcct,        "792 0 792 332 6962 7294";
                   32772, SLT_End,              "";

                   32769, SLT_Begin,            "sess 0 HTTP/1";
                   32769, SLT_SessOpen,         "127.0.0.1 34560 127.0.0.1:1244 127.0.0.1 1244 1470304882.576266 14";
                   32769, SLT_Link,             "req 32770 rxreq";
                   32769, SLT_SessClose,        "REM_CLOSE 0.347";
                   );
        let session = apply_final!(state, 32769, SLT_End, "");

        // The first request won't have response as it got restarted
        assert_matches!(session.client_transactions[0].access_record.transaction, ClientAccessTransaction::Restarted { .. });

        // We should have restart transaction
        let restart_transaction = assert_some!(session.client_transactions[0].restart_transaction.as_ref());

        // It should have a response
        assert_matches!(restart_transaction.access_record.transaction, ClientAccessTransaction::Full { .. });
        assert_matches!(restart_transaction.backend_transactions[0].access_record.transaction, BackendAccessTransaction::Full { .. });
    }

    #[test]
    fn apply_session_state_retry() {
        log();
        let mut state = SessionState::new();

        apply_all!(state,
                   8, SLT_Begin,            "bereq 7 fetch";
                   8, SLT_Timestamp,        "Start: 1470403414.664923 0.000000 0.000000";
                   8, SLT_BereqMethod,      "GET";
                   8, SLT_BereqURL,         "/retry";
                   8, SLT_BereqProtocol,    "HTTP/1.1";
                   8, SLT_BereqHeader,      "Date: Fri, 05 Aug 2016 13:23:34 GMT";
                   8, SLT_VCL_return,       "fetch";
                   8, SLT_Timestamp,        "Bereq: 1470403414.664993 0.000070 0.000070";
                   8, SLT_Timestamp,        "Beresp: 1470403414.669313 0.004390 0.004320";
                   8, SLT_BerespProtocol,   "HTTP/1.1";
                   8, SLT_BerespStatus,     "200";
                   8, SLT_BerespReason,     "OK";
                   8, SLT_BerespHeader,     "Content-Type: text/html; charset=utf-8";
                   8, SLT_VCL_call,         "BACKEND_RESPONSE";
                   8, SLT_BereqURL,         "/iss/v2/thumbnails/foo/4006450256177f4a/bar.jpg";
                   8, SLT_VCL_return,       "retry";
                   8, SLT_BackendClose,     "19 boot.default";
                   8, SLT_Timestamp,        "Retry: 1470403414.669375 0.004452 0.000062";
                   8, SLT_Link,             "bereq 32769 retry";
                   8, SLT_End,              "";

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
                   32769, SLT_Storage,          "malloc s0";
                   32769, SLT_ObjProtocol,      "HTTP/1.1";
                   32769, SLT_ObjStatus,        "200";
                   32769, SLT_ObjReason,        "OK";
                   32769, SLT_ObjHeader,        "Content-Type: text/html; charset=utf-8";
                   32769, SLT_ObjHeader,        "X-Aspnet-Version: 4.0.30319";
                   32769, SLT_Fetch_Body,       "3 length stream";
                   32769, SLT_BackendReuse,     "19 boot.iss";
                   32769, SLT_Timestamp,        "BerespBody: 1470403414.672290 0.007367 0.000105";
                   32769, SLT_Length,           "6962";
                   32769, SLT_BereqAcct,        "1021 0 1021 608 6962 7570";
                   32769, SLT_End,              "";

                   7, SLT_Begin,        "req 6 rxreq";
                   7, SLT_Timestamp,    "Start: 1470403414.664824 0.000000 0.000000";
                   7, SLT_Timestamp,    "Req: 1470403414.664824 0.000000 0.000000";
                   7, SLT_ReqStart,     "127.0.0.1 39798";
                   7, SLT_ReqMethod,    "GET";
                   7, SLT_ReqURL,       "/retry";
                   7, SLT_ReqProtocol,  "HTTP/1.1";
                   7, SLT_ReqHeader,    "Date: Fri, 05 Aug 2016 13:23:34 GMT";
                   7, SLT_VCL_call,     "RECV";
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
        assert_matches!(session.client_transactions[0].backend_transactions[0].access_record.transaction, BackendAccessTransaction::Abandoned {
            request: HttpRequest {
                ref url,
                ..
            },
            ..
        } if url == "/retry"
        );

        // Backend transaction will have retrys
        let retry_transaction = assert_some!(session.client_transactions[0].backend_transactions[0].retry_transaction.as_ref());

        // It will have "retry" reason
        assert_eq!(retry_transaction.access_record.reason, "retry".to_string());
        assert!(retry_transaction.retry_transaction.is_none());
        assert_matches!(retry_transaction.access_record.transaction, BackendAccessTransaction::Full {
            request: HttpRequest {
                ref url,
                ..
            },
            ..
        } if url == "/iss/v2/thumbnails/foo/4006450256177f4a/bar.jpg"
        );
    }

    #[test]
    fn apply_session_state_piped() {
        log();
        let mut state = SessionState::new();

        // logs-new/varnish20160816-4093-s54h6nb4b44b69f1b2c7ca2.vsl
        apply_all!(state,
                   3, SLT_Begin,          "sess 0 HTTP/1";
                   3, SLT_SessOpen,       "127.0.0.1 59830 127.0.0.1:1220 127.0.0.1 1220 1471355444.743889 18";
                   3, SLT_Link,           "req 4 rxreq";

                   5, SLT_Begin,          "bereq 4 pipe";
                   5, SLT_BereqMethod,    "GET";
                   5, SLT_BereqURL,       "/websocket";
                   5, SLT_BereqProtocol,  "HTTP/1.1";
                   5, SLT_VCL_call,       "PIPE ";
                   5, SLT_BereqHeader,    "Connection: Upgrade";
                   5, SLT_VCL_return,     "pipe";
                   5, SLT_BackendOpen,    "20 boot.default 127.0.0.1 42000 127.0.0.1 54038";
                   5, SLT_BackendStart,   "127.0.0.1 42000";
                   5, SLT_Timestamp,      "Bereq: 1471355444.744344 0.000000 0.000000";
                   5, SLT_BackendClose,   "20 boot.default";
                   5, SLT_BereqAcct,      "0 0 0 0 0 0";
                   5, SLT_End,            "";

                   4, SLT_Begin,          "req 3 rxreq";
                   4, SLT_Timestamp,      "Start: 1471355444.744141 0.000000 0.000000";
                   4, SLT_Timestamp,      "Req: 1471355444.744141 0.000000 0.000000";
                   4, SLT_ReqStart,       "127.0.0.1 59830";
                   4, SLT_ReqMethod,      "GET";
                   4, SLT_ReqURL,         "/websocket";
                   4, SLT_ReqProtocol,    "HTTP/1.1";
                   4, SLT_ReqHeader,      "Upgrade: websocket";
                   4, SLT_VCL_call,       "RECV";
                   4, SLT_VCL_return,     "pipe";
                   4, SLT_VCL_call,       "HASH";
                   4, SLT_VCL_return,     "lookup";
                   4, SLT_Link,           "bereq 5 pipe";
                   4, SLT_ReqHeader,      "X-Varnish-Result: pipe";
                   4, SLT_Timestamp,      "Pipe: 1471355444.744349 0.000209 0.000209";
                   4, SLT_Timestamp,      "PipeSess: 1471355444.751368 0.007228 0.007019";
                   4, SLT_PipeAcct,       "268 761 0 480";
                   4, SLT_End,            "";

                   3, SLT_SessClose,      "TX_PIPE 0.008";
              );

        let session = apply_final!(state, 3, SLT_End, "");

        assert_matches!(session.client_transactions[0].access_record.transaction, ClientAccessTransaction::Piped { .. });
        assert_matches!(session.client_transactions[0].backend_transactions[0].access_record.transaction, BackendAccessTransaction::Piped { .. });
    }
    */
}
