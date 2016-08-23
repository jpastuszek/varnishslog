use serde::ser::Serialize;

#[derive(Serialize, Debug)]
struct LogEntry<'a, S> where S: Serialize + 'a {
    record_type: &'a str,
    record: &'a S,
}

#[derive(Serialize, Debug)]
struct ClientAccessLogEntry<'a> {
    remote_address: (&'a str, u16),
    session_timestamp: f64,
    start_timestamp: f64,
    end_timestamp: f64,
    handing: &'a str,
    request: HttpRequestLogEntry<'a>,
    response: HttpResponseLogEntry<'a>,
}

#[derive(Serialize, Debug)]
struct HttpRequestLogEntry<'a> {
    protocol: &'a str,
    method: &'a str,
    url: &'a str,
    headers: &'a [(String, String)],
}

#[derive(Serialize, Debug)]
struct HttpResponseLogEntry<'a> {
    status: u32,
    reason: &'a str,
    protocol: &'a str,
    headers: &'a [(String, String)],
}
