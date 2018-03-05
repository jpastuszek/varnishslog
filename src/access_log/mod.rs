#[cfg(test)]
#[macro_use]
mod test_helpers {
    //TODO move to lib.rs?
    use vsl::record::{VslRecord, VslRecordTag, VslIdent};
    use std::sync::{Once, ONCE_INIT};

    pub use vsl::record::VslRecordTag::*;

    pub fn vsl(tag: VslRecordTag, ident: VslIdent, message: &str) -> VslRecord {
        VslRecord::from_str(tag, ident, message)
    }

    use vsl::record::VSL_CLIENTMARKER;
    impl<'s> VslRecord<'s> {
        pub fn from_str(tag: VslRecordTag, ident: VslIdent, message: &str) -> VslRecord {
            VslRecord {
                tag: tag,
                marker: VSL_CLIENTMARKER,
                ident: ident,
                data: message.as_ref()
            }
        }
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

    macro_rules! parse {
        ($e:expr) => {
            ::std::str::FromStr::from_str($e).expect(&format!("failed to parse '{:?}'", $e))
        }
    }

    macro_rules! assert_matches {
        ( $e:expr , $pat:pat ) => {
            match $e {
                $pat => (),
                ref e => panic!("assertion failed: `{:?}` does not match `{}`",
                    e, stringify!($pat))
            }
        };
        ( $e:expr , $pat:pat if $cond:expr ) => {
            match $e {
                $pat if $cond => (),
                ref e => panic!("assertion failed: `{:?}` does not match `{} if {}`",
                    e, stringify!($pat), stringify!($cond))
            }
        };
        ( $e:expr , $pat:pat => $($arm:tt)* ) => {
            match $e {
                $pat => ($($arm)*),
                ref e => panic!("assertion failed: `{:?}` does not match `{}`",
                    e, stringify!($pat))
            };
        };
        ( $e:expr , $pat:pat if $cond:expr => $($arm:tt)* ) => {
            match $e {
                $pat if $cond => ($($arm)*),
                ref e => panic!("assertion failed: `{:?}` does not match `{} if {}`",
                    e, stringify!($pat), stringify!($cond))
            }
        };
        ( $e:expr , $pat:pat , $arg:expr ) => {
            match $e {
                $pat => (),
                ref e => panic!("assertion failed: `{:?}` does not match `{}`: {}",
                    e, stringify!($pat), format_args!($arg))
            }
        };
        ( $e:expr , $pat:pat if $cond:expr , $arg:expr ) => {
            match $e {
                $pat if $cond => (),
                ref e => panic!("assertion failed: `{:?}` does not match `{} if {}`: {}",
                    e, stringify!($pat), stringify!($cond), format_args!($arg))
            }
        };
        ( $e:expr , $pat:pat , $arg:expr => $($arm:tt)* ) => {
            match $e {
                $pat => ($($arm)*),
                ref e => panic!("assertion failed: `{:?}` does not match `{}`: {}",
                    e, stringify!($pat), format_args!($arg))
            }
        };
        ( $e:expr , $pat:pat if $cond:expr , $arg:expr => $($arm:tt)* ) => {
            match $e {
                $pat if $cond => ($($arm)*),
                ref e => panic!("assertion failed: `{:?}` does not match `{} if {}`: {}",
                    e, stringify!($pat), stringify!($cond), format_args!($arg))
            }
        };
    }
}

pub mod record;
pub mod session_state;
pub mod record_state;
