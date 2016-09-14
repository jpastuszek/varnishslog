#[macro_use]
extern crate nom;
#[macro_use]
extern crate bitflags;
#[macro_use]
extern crate log;
extern crate stderrlog;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate assert_matches;

extern crate serde;
extern crate serde_json;

extern crate chrono;
#[cfg(test)]
extern crate env_logger;
extern crate linked_hash_map;
extern crate boolinator;

use std::io::{self, stdin};
use clap::{Arg, App};

#[macro_use]
mod stream_buf;
use stream_buf::{StreamBuf, ReadStreamBuf, FillError, FillApplyError};
use access_log::*;

mod vsl;

mod access_log;

arg_enum! {
    #[derive(Debug)]
    enum OutputFormat {
        Log,
        LogDebug,
        RecordDebug,
        SessionDebug,
        Json,
        JsonPretty,
        NcsaJson
    }
}

fn main() {
    let arguments = App::new("Varnish VSL log to syslog logger")
        .version(crate_version!())
        .author(crate_authors!())
        .about("Reads binary VSL log entreis, correlates them togeter and emits JSON log entry to syslog")
        .arg(Arg::with_name("quiet")
             .long("quiet")
             .short("q")
             .help("Don't log anything"))
        .arg(Arg::with_name("verbose")
             .long("verbose")
             .short("v")
             .help("Sets the level of verbosity; e.g. -vv for INFO level, -vvvv for TRACE level")
             .multiple(true))
        .arg(Arg::with_name("output")
             .long("output-format")
             .short("o")
             .help("Format of the output")
             .takes_value(true)
             .possible_values(&OutputFormat::variants())
             .default_value(OutputFormat::variants().last().unwrap()))
        .arg(Arg::with_name("make-indices")
             .long("make-indices")
             .short("i")
             .help("Make indices of request and response headers with normalized header names and VSL log vars"))
        .get_matches();

    stderrlog::new()
        .module(module_path!())
        .quiet(arguments.is_present("quiet"))
        .verbosity(arguments.occurrences_of("verbose") as usize)
        .init()
        .unwrap();

    let output_format = value_t!(arguments, "output", OutputFormat).unwrap_or_else(|e| e.exit());

    let stdin = stdin();
    let stdin = stdin.lock();
    // for testing
    //let mut rfb = ReadStreamBuf::with_capacity(stdin, 123);
    let mut rfb = ReadStreamBuf::new(stdin);

    loop {
        match rfb.fill_apply(binary_vsl_tag) {
            Err(err) => {
                error!("Error while reading VSL tag: {}", err);
                panic!("VSL tag error")
            }
            Ok(None) => continue,
            Ok(Some(Some(_))) => {
                info!("Found VSL tag");
                break
            }
            Ok(Some(_)) => break,
        }
    }

    rfb.recycle(); // TODO: VSL should benefit from alignment - bench test it

    let mut record_state = RecordState::new();
    let mut session_state = SessionState::new();

    let mut out = std::io::stdout();

    loop {
        let record = match rfb.fill_apply(vsl_record_v4) {
            Err(FillApplyError::FillError(FillError::Io(err))) => {
                if err.kind() == io::ErrorKind::UnexpectedEof {
                    info!("Reached end of stream");
                    break
                }
                error!("Got IO Error while reading stream: {}", err);
                panic!("Stream IO error")
            },
            Err(FillApplyError::FillError(err)) => {
                error!("Failed to fill parsing buffer: {}", err);
                panic!("Fill error")
            }
            Err(FillApplyError::Parser(err)) => {
                error!("Failed to parse VSL record: {}", err);
                panic!("Parser error")
            },
            Ok(None) => continue,
            Ok(Some(record)) => record,
        };

        match output_format {
            OutputFormat::Log => println!("{:#}", record),
            OutputFormat::LogDebug => println!("{:#?}", record),
            OutputFormat::RecordDebug => {
                if let Some(record) = record_state.apply(&record) {
                    println!("{:#?}", record)
                }
            }
            OutputFormat::SessionDebug => {
                if let Some(session) = session_state.apply(&record) {
                    println!("{:#?}", session)
                }
            }
            _ => {
                let format = match output_format {
                    OutputFormat::Json => Format::Json,
                    OutputFormat::JsonPretty => Format::JsonPretty,
                    OutputFormat::NcsaJson => Format::NcsaJson,
                    _ => unreachable!()
                };

                if let Some(session) = session_state.apply(&record) {
                    match log_session_record(&session, &format, &mut out, arguments.is_present("make-indices")) {
                        Ok(()) => (),
                        Err(OutputError::Io(err)) |
                        Err(OutputError::JsonSerialization(JsonError::Io(err))) => match err.kind() {
                            io::ErrorKind::BrokenPipe => {
                                info!("Broken pipe");
                                break
                            }
                            _ => error!("Failed to write out client access logs: {:?}: {}", record, err)
                        },
                        Err(err) => error!("Failed to serialize client access logs: {:?}: {}", record, err)
                    }
                }
            }
        }
    }

    for client in session_state.unmatched_client_access_records() {
        warn!("ClientAccessRecord without matching session left: {:?}", client)
    }

    for backend in session_state.unmatched_backend_access_records() {
        warn!("BackendAccessRecord without matching session left: {:?}", backend)
    }

    for session in session_state.unresolved_sessions() {
        warn!("SessionRecord with unresolved links to other objects left: {:?}", session)
    }

    info!("Done");
}
