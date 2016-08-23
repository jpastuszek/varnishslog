#[derive(Serialize, Debug)]
struct ClientAccessLogEntry<'a> {
    remote_address: (&'a str, u16),
    session_timestamp: f64,
    start_timestamp: f64,
    end_timestamp: f64,
    handing: &'a str
}
