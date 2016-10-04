#[macro_use]
extern crate varnishslog;

extern crate flexi_logger;
extern crate time;
#[macro_use]
extern crate log;

#[macro_use]
extern crate clap;

use std::io::{self, stdin};
use clap::{Arg, App};

use varnishslog::stream_buf::{StreamBuf, ReadStreamBuf, FillError, FillApplyError};
use varnishslog::access_log::*;

mod program;

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
        .arg(Arg::with_name("log-spec")
             .short("d")
             .long("log-sepc")
             .value_name("LOG_LEVEL_SPEC")
             .help("Logging level specification, e.g: varnishslog=info")
             .takes_value(true))
        .arg(Arg::with_name("output")
             .long("output-format")
             .short("o")
             .help("Format of the output")
             .takes_value(true)
             .possible_values(&OutputFormat::variants())
             .default_value(OutputFormat::variants().last().unwrap()))
        .arg(Arg::with_name("no-log-processing")
             .long("no-log-processing")
             .short("l")
             .help("Do not process VSL log into vars, messages and ACL matches"))
        .arg(Arg::with_name("keep-raw-log")
             .long("keep-raw-log")
             .short("L")
             .help("Include raw log messages"))
        .arg(Arg::with_name("no-header-indexing")
             .long("no-header-indexing")
             .short("i")
             .help("Do not make indices of request and response headers with normalized header names"))
        .arg(Arg::with_name("keep-raw-headers")
             .long("keep-raw-headers")
             .short("I")
             .help("Keep raw header name/value pairs; any indices are moved to top level"))
        .get_matches();

    program::init(arguments.value_of("log-spec"));

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
                program::exit_with_error("VSL tag error", 10)
            }
            Ok(None) => continue,
            Ok(Some(Some(_))) => {
                info!("Found VSL tag");
                break
            }
            Ok(Some(_)) => break,
        }
    }

    let mut record_state = RecordState::new();
    let mut session_state = SessionState::new();

    let mut out = std::io::stdout();

    let config = Config {
        no_log_processing: arguments.is_present("no-log-processing"),
        keep_raw_log: arguments.is_present("keep-raw-log"),
        no_header_indexing: arguments.is_present("no-header-indexing"),
        keep_raw_headers: arguments.is_present("keep-raw-headers"),
    };

    loop {
        let record = match rfb.fill_apply(vsl_record_v4) {
            Err(FillApplyError::FillError(FillError::Io(err))) => {
                if err.kind() == io::ErrorKind::UnexpectedEof {
                    info!("Reached end of stream");
                    break
                }
                error!("Got IO Error while reading stream: {}", err);
                program::exit_with_error("Stream IO error", 20)
            },
            Err(FillApplyError::FillError(err)) => {
                error!("Failed to fill parsing buffer: {}", err);
                program::exit_with_error("Fill error", 21)
            }
            Err(FillApplyError::Parser(err)) => {
                error!("Failed to parse VSL record: {}", err);
                program::exit_with_error("Parser error", 22)
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
                    match log_session_record(&session, &format, &mut out, &config) {
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
