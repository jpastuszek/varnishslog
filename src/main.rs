#![recursion_limit = "500"]
#[macro_use]
extern crate nom;
#[macro_use]
extern crate log;
extern crate stderrlog;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate quick_error;
extern crate linked_hash_map;
#[macro_use]
extern crate assert_matches;

use std::io::{self, stdin};
use clap::{Arg, App};

#[macro_use]
mod stream_buf;
use stream_buf::{StreamBuf, ReadStreamBuf, FillError, FillApplyError};
use access_log::{RecordState, SessionState};

// Generated with ./mk_vsl_tag from Varnish headers: include/tbl/vsl_tags.h include/tbl/vsl_tags_http.h include/vsl_int.h
// https://github.com/varnishcache/varnish-cache/blob/master/include/vapi/vsl_int.h
// https://github.com/varnishcache/varnish-cache/blob/master/include/tbl/vsl_tags.h
// https://github.com/varnishcache/varnish-cache/blob/master/include/tbl/vsl_tags_http.h
mod vsl_tag_e;
mod vsl;
use vsl::{binary_vsl_tag, vsl_record_v3, vsl_record_v4};
use vsl::VslRecord;

mod access_log;

fn main() {
    let arguments = App::new("Varnish VSL log to syslog logger")
        .version(crate_version!())
        .author(crate_authors!())
        .about("Reads binary VSL log entreis, correlates them togeter and emits JSON log entry to syslog")
        .arg(Arg::with_name("v3")
             .long("varnish-v3")
             .short("3")
             .help("Parse Varnish v3 binary log"))
        .arg(Arg::with_name("quiet")
             .long("quiet")
             .short("q")
             .help("Don't log anything"))
        .arg(Arg::with_name("verbose")
             .long("verbose")
             .short("v")
             .multiple(true)
             .help("Sets the level of verbosity; e.g. -vv for INFO level, -vvvv for TRACE level"))
        .arg(Arg::with_name("output-log")
             .long("output-log")
             .short("l")
             .conflicts_with_all(&["output-debug-records", "output-debug-sessions"])
             .help("Output log content"))
        .arg(Arg::with_name("output-debug-records")
             .long("output-debug-records")
             .short("r")
             .conflicts_with_all(&["output-log", "output-debug-sessions"])
             .help("Output debug format records"))
        .arg(Arg::with_name("output-debug-sessions")
             .long("output-debug-session")
             .short("s")
             .conflicts_with_all(&["output-log", "output-debug-records"]) // TODO: use Set
             .help("Output debug format sessions"))
        .get_matches();

    stderrlog::new()
        .module(module_path!())
        .quiet(arguments.is_present("quiet"))
        .verbosity(arguments.occurrences_of("verbose") as usize)
        .init()
        .unwrap();

    let stdin = stdin();
    let stdin = stdin.lock();
    // for testing
    //let mut rfb = ReadStreamBuf::with_capacity(stdin, 123);
    let mut rfb = ReadStreamBuf::new(stdin);

    let vsl_record: fn(&[u8]) -> nom::IResult<&[u8], VslRecord>;

    if ! arguments.is_present("v3") {
        loop {
            match rfb.fill_apply(binary_vsl_tag) {
                Err(FillApplyError::Parser(_)) => {
                    error!("Input is not Varnish v4 VSL binary format");
                    panic!("Bad input format")
                }
                Err(err) => {
                    error!("Error while reading VSL tag: {}", err);
                    panic!("VSL tag error")
                }
                Ok(None) => continue,
                Ok(Some(_)) => break,
            }
        }
        vsl_record = vsl_record_v4;
    } else {
        vsl_record = vsl_record_v3;
    }

    rfb.recycle(); // TODO: VSL should benefit from alignment - bench test it

    let mut record_state = RecordState::new();
    let mut session_state = SessionState::new();

    loop {
        let record = match rfb.fill_apply(vsl_record) {
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

        if arguments.is_present("output-log") {
            println!("{}", record);
        } else if arguments.is_present("output-debug-records") {
            if let Some(record) = record_state.apply(&record) {
                println!("{:#?}", record)
            }
        } else if arguments.is_present("output-debug-sessions") {
            if let Some(session) = session_state.apply(&record) {
                println!("{:#?}", session)
            }
        } else {
            panic!("default output unipl")
        }
    }

    for client in session_state.unmatched_client_access_records() {
        warn!("ClientAccessRecord without matching session left: {:?}", client)
    }

    for backend in session_state.unmatched_backend_access_records() {
        warn!("BackendAccessRecord without matching session left: {:?}", backend)
    }

    info!("Done");
}
