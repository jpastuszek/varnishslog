#[cfg(test)]
#[macro_use]
mod test_helpers {
    use vsl::{VslRecord, VslRecordTag, VslIdent};
    use std::sync::{Once, ONCE_INIT};

    pub use vsl::VslRecordTag::*;

    pub fn vsl(tag: VslRecordTag, ident: VslIdent, message: &str) -> VslRecord {
        VslRecord::from_str(tag, ident, message)
    }

    static LOGGER: Once = ONCE_INIT;

    pub fn log() {
        use env_logger;

        LOGGER.call_once(|| {
            env_logger::init().unwrap();
        });
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
        ($state:ident, $ident:expr, $tag:ident, $message:expr) => {{
            let opt: Option<_> = $state.apply(&vsl($tag, $ident, $message));
            assert!(opt.is_none(), "expected apply to return None after applying: `{}, {:?}, {};`", $ident, $tag, $message);
        }};
    }

    macro_rules! apply_all {
        ($state:ident, $($t_ident:expr, $t_tag:ident, $t_message:expr;)+) => {{
            $(apply!($state, $t_ident, $t_tag, $t_message);)*
        }};
    }

    macro_rules! apply_final {
        ($state:ident, $ident:expr, $tag:ident, $message:expr) => {
            assert_some!($state.apply(&vsl($tag, $ident, $message)))
        };
    }
}

mod session_state;
mod record_state;

pub use self::record_state::*;
pub use self::session_state::SessionState;

include!(concat!(env!("OUT_DIR"), "/serde_types.rs"));

use serde_json::ser::to_writer as write_json;
use std::io::Write;
use std::fmt::Display;

pub enum Format {
    Json
}

use std::fmt;
impl<'a> fmt::Display for ClientAccessLogEntry<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Handling: {}", self.handing)
    }
}

trait AsStr {
    fn as_str(&self) -> &str;
}

impl AsStr for Handling {
    fn as_str(&self) -> &str {
        match self {
            &Handling::Hit(_) => "hit",
            &Handling::Miss => "miss",
            &Handling::Pass => "pass",
            &Handling::HitPass(_) => "hit_for_pass",
            &Handling::Synth => "synth",
            &Handling::Pipe => "pipe",
        }
    }
}

pub trait AccessLog {
    fn client_access_logs<W>(&self, format: Format, out: &mut W) where W: Write;
}

impl AccessLog for SessionRecord {
    fn client_access_logs<W>(&self, format: Format, out: &mut W) where W: Write {
        match format {
            Format::Json => {
                for record_link in self.client_records.iter() {
                    if let Some(record) = record_link.get_resolved() {
                        write_json(out, &ClientAccessLogEntry {
                            remote_address: (self.remote.0.as_str(), self.remote.1),
                            session_timestamp: self.open,
                            start_timestamp: record.start,
                            end_timestamp: record.end,
                            handing: record.handling.as_str(),
                        });
                    } else {
                        write_json(out, &"foobar!");
                    }
                    writeln!(out, "");
                }
            }
        }
    }
}
