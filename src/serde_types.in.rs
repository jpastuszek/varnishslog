use serde::ser::Serialize;
use serde::ser::Serializer;

#[derive(Serialize, Debug)]
struct Entry<'a, S> where S: Serialize + 'a {
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
    log: LogBook<'a>,
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

#[derive(Debug)]
struct LogBook<'a> {
    entries: &'a [LogEntry],
}

#[derive(Serialize, Debug)]
struct LogBookEntry<'a> {
    entry_type: &'a str,
    message: &'a str,
    #[serde(skip_serializing_if="Option::is_none")]
    detail: Option<&'a str>,
}

impl<'a> Serialize for LogBook<'a> {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where S: Serializer {
        let mut state = try!(serializer.serialize_seq(Some(self.entries.len())));
        for log_entry in self.entries {
            let (entry_type, message, detail) = match log_entry {
                &LogEntry::Vcl(ref msg) => ("VCL", msg.as_str(), None),
                &LogEntry::Debug(ref msg) => ("Debug", msg.as_str(), None),
                &LogEntry::Error(ref msg) => ("Error", msg.as_str(), None),
                &LogEntry::FetchError(ref msg) => ("Fetch Error", msg.as_str(), None),
                &LogEntry::Warning(ref msg) => ("Warning", msg.as_str(), None),
                &LogEntry::Acl(ref result, ref name, ref addr) => match result.as_str() {
                    "MATCH" => ("ACL Match", name.as_str(), addr.as_ref().map(String::as_str)),
                    "NO_MATCH" => ("ACL No Match", name.as_str(), addr.as_ref().map(String::as_str)),
                    _ => ("ACL Other", result.as_str(), Some(name.as_str())),
                },
            };

            try!(serializer.serialize_seq_elt(&mut state, &LogBookEntry {
                entry_type: entry_type,
                message: message,
                detail: detail,
            }));
        }
        try!(serializer.serialize_seq_end(state));
        Ok(())
    }
}
