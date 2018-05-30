extern crate flexi_logger;
extern crate time;
#[macro_use]
extern crate log;
#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate clap;
extern crate varnishslog;

use std::io::{self, stdin, Read, Write};
use std::fs::File;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};
use std::num::Wrapping;
use std::time::Duration;
use std::thread;

use clap::{Arg, App};

use varnishslog::stream_buf::{StreamBuf, ReadStreamBuf, FillError, FillApplyError};
use varnishslog::vsl::record::VslRecord;
use varnishslog::vsl::record::parser::{binary_vsl_tag, vsl_record_v4};
use varnishslog::store::Config as StoreConfig;
use varnishslog::access_log::session_state::SessionState;
use varnishslog::access_log::record_state::RecordState;
use varnishslog::serialization::{log_client_record, Config, Format, OutputError, JsonError};

mod program;

static EPOCH: AtomicUsize = ATOMIC_USIZE_INIT;

fn spawn_epoch_timer() -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name("epoch timer".to_string())
        .spawn(|| {
        loop {
            thread::sleep(Duration::from_secs(1));
            let epoch = Wrapping(EPOCH.load(Ordering::Relaxed) as u64) + Wrapping(1);
            EPOCH.store(epoch.0 as usize, Ordering::Relaxed);
        }
    }).expect("timer thread")
}

fn global_epoch(_epoch: Wrapping<u64>) -> Wrapping<u64> {
    Wrapping(EPOCH.load(Ordering::Relaxed) as u64)
}

quick_error! {
    #[derive(Debug)]
    pub enum ProcessingError {
        IO(err: io::Error) {
            display("IO Error while processing records: {}", err)
            description("I/O error")
            from()
        }
        InputBuffer(err: FillError) {
            display("Failed to fill parsing buffer: {}", err)
            description("Input buffer error")
        }
        Parsing(err: String) {
            display("Failed to parse VSL record: {}", err)
            description("Parser error")
        }
        Serialization(err: String) {
            display("Failed to serialize log record: {}", err)
            description("Serialization error")
        }
    }
}

impl<'b> From<FillApplyError<&'b[u8], u32>> for ProcessingError {
    fn from(err: FillApplyError<&'b[u8], u32>) -> ProcessingError {
        match err {
            FillApplyError::FillError(FillError::Io(err)) => ProcessingError::IO(err),
            FillApplyError::FillError(err) => ProcessingError::InputBuffer(err),
            // we need to convert it to string due to input reference
            FillApplyError::Parser(err) => ProcessingError::Parsing(format!("nom parser error: {}", err)),
        }
    }
}

impl From<OutputError> for ProcessingError {
    fn from(err: OutputError) -> ProcessingError {
        match err {
            OutputError::JsonSerialization(JsonError::Io(err)) |
            OutputError::Io(err) => ProcessingError::IO(err),
            err => ProcessingError::Serialization(format!("Serialization error: {}", err)),
        }
    }
}

impl ProcessingError {
    fn to_exit_code(&self) -> i32 {
        match *self {
            ProcessingError::IO(_) => 10,
            ProcessingError::InputBuffer(_) => 11,
            ProcessingError::Parsing(_) => 20,
            ProcessingError::Serialization(_) => 30,
        }
    }
}

impl ProcessingError {
    fn is_brokend_pipe(&self) -> bool {
        match *self {
            ProcessingError::IO(ref err) =>
                err.kind() == io::ErrorKind::UnexpectedEof || err.kind() == io::ErrorKind::BrokenPipe,
            _ => false
        }
    }
}

fn try_read_vsl_tag<R: Read>(stream: &mut ReadStreamBuf<R>) -> Result<(), ProcessingError> {
    loop {
        match try!(stream.fill_apply(binary_vsl_tag)) {
            None => continue,
            Some(Some(_)) => {
                info!("Found VSL tag");
                break
            }
            Some(_) => break,
        }
    }
    Ok(())
}

trait WriteRecord {
    fn write_record<W>(&mut self, record: VslRecord, output: &mut W) -> Result<(), ProcessingError> where W: Write;
    fn log_reports(&self) {}
}

fn process_vsl_records<R, W, P>(stream: &mut ReadStreamBuf<R>, mut writer: P, output: &mut W) -> Result<(), ProcessingError> where R: Read, W: Write, P: WriteRecord {
    loop {
        match stream.fill_apply(vsl_record_v4).map_err(ProcessingError::from) {
            Ok(None) => continue,
            Ok(Some(record)) => try!(writer.write_record(record, output)),
            Err(err) => {
                //TODO: need better tracking of orphan records and other stats
                if err.is_brokend_pipe() {
                    writer.log_reports();
                }
                return Err(err)
            }
        }
    }
}

fn process_vsl_stream<R, W>(input: R, mut output: W, stream_buf_size: usize, output_format: OutputFormat, config: Config, store_config: &StoreConfig) -> Result<(), ProcessingError> where R: Read, W: Write {
    //TODO: make buffer size configurable
    let mut stream = ReadStreamBuf::with_capacity(input, stream_buf_size);

    try!(try_read_vsl_tag(&mut stream));

    match output_format {
        OutputFormat::Log => process_vsl_records(&mut stream, LogWriter::default(), &mut output),
        OutputFormat::LogDebug => process_vsl_records(&mut stream, LogDebugWriter::default(), &mut output),
        OutputFormat::RecordDebug => process_vsl_records(&mut stream, RecordDebugWriter::new(store_config), &mut output),
        OutputFormat::SessionDebug => process_vsl_records(&mut stream, SessionDebugWriter::new(store_config), &mut output),
        OutputFormat::Json => process_vsl_records(&mut stream, SerdeWriter::new(Format::Json, config, store_config), &mut output),
        OutputFormat::JsonPretty => process_vsl_records(&mut stream, SerdeWriter::new(Format::JsonPretty, config, store_config), &mut output),
        OutputFormat::NcsaJson => process_vsl_records(&mut stream, SerdeWriter::new(Format::NcsaJson, config, store_config), &mut output),
    }
}

#[derive(Default)]
struct LogWriter;
impl WriteRecord for LogWriter {
    fn write_record<W>(&mut self, record: VslRecord, output: &mut W) -> Result<(), ProcessingError> where W: Write {
        writeln!(output, "{:#}", record).map_err(From::from)
    }
}

#[derive(Default)]
struct LogDebugWriter;
impl WriteRecord for LogDebugWriter {
    fn write_record<W>(&mut self, record: VslRecord, output: &mut W) -> Result<(), ProcessingError> where W: Write {
        writeln!(output, "{:#?}", record).map_err(From::from)
    }
}

struct RecordDebugWriter {
    state: RecordState,
}

impl RecordDebugWriter {
    fn new(store_config: &StoreConfig) -> RecordDebugWriter {
        RecordDebugWriter {
            state: RecordState::with_config(store_config)
        }
    }
}

impl WriteRecord for RecordDebugWriter {
    fn write_record<W>(&mut self, record: VslRecord, output: &mut W) -> Result<(), ProcessingError> where W: Write {
        if let Some(record) = self.state.apply(&record) {
            writeln!(output, "{:#?}", record).map_err(From::from)
        } else {
            Ok(())
        }
    }
}

struct SessionDebugWriter {
    state: SessionState,
}

impl SessionDebugWriter {
    fn new(store_config: &StoreConfig) -> SessionDebugWriter {
        SessionDebugWriter {
            state: SessionState::with_config(store_config)
        }
    }
}

impl WriteRecord for SessionDebugWriter {
    fn write_record<W>(&mut self, record: VslRecord, output: &mut W) -> Result<(), ProcessingError> where W: Write {
        if let Some(record) = self.state.apply(&record) {
            writeln!(output, "{:#?}", record).map_err(From::from)
        } else {
            Ok(())
        }
    }
}

struct SerdeWriter {
    state: SessionState,
    format: Format,
    config: Config,
}

impl SerdeWriter {
    fn new(format: Format, config: Config, store_config: &StoreConfig) -> SerdeWriter {
        SerdeWriter {
            state: SessionState::with_config(store_config),
            format: format,
            config: config,
        }
    }
}

impl WriteRecord for SerdeWriter {
    fn write_record<W>(&mut self, record: VslRecord, output: &mut W) -> Result<(), ProcessingError> where W: Write {
        if let Some(client) = self.state.apply(&record) {
            log_client_record(&client, &self.format, output, &self.config).map_err(From::from)
        } else {
            Ok(())
        }
    }

    fn log_reports(&self) {
        for client in self.state.unresolved_root_client_access_records() {
            warn!("Root ClientAccessRecord left unresolved:\n{:#?}", client)
        }

        for client in self.state.unresolved_client_access_records() {
            warn!("ClientAccessRecord left unresolved:\n{:#?}", client)
        }

        for backend in self.state.unresolved_backend_access_records() {
            warn!("BackendAccessRecord left unresolved:\n{:#?}", backend)
        }
    }
}

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

fn validate_max_record_slots(value: String) -> Result<(), String> {
    let v = try!(value.parse::<usize>().map_err(|_| format!("max-record-slots expected to be an integer; got: {:?}", value)));

    if !(v > 0) {
        Err(format!("max-record-slots must be greater than zero; got: {}", v))
    } else {
        Ok(())
    }
}

fn validate_max_epoch_diff(value: String) -> Result<(), String> {
    let v = try!(value.parse::<u64>().map_err(|_| format!("max-epoch-diff expected to be an integer; got: {:?}", value)));

    if !(v > 0) {
        Err(format!("max-epoch-diff must be greater than zero; got: {}", v))
    } else {
        Ok(())
    }
}

fn validate_evict_factor(value: String) -> Result<(), String> {
    let v = try!(value.parse::<f32>().map_err(|_| format!("evict-factor expected to be an integer; got: {:?}", value)));

    if !(v > 0.0) {
        Err(format!("evict-factor must be greater than zero; got: {}", v))
    } else {
        Ok(())
    }
}

fn validate_stat_epoch_inverval(value: String) -> Result<(), String> {
    let v = try!(value.parse::<u64>().map_err(|_| format!("stat-epoch-interval expected to be an integer; got: {:?}", value)));

    if !(v > 0) {
        Err(format!("stat-epoch-interval must be greater than zero; got: {}", v))
    } else {
        Ok(())
    }
}

fn main() {
    let arguments = App::new("Varnish Structured Logger")
        .version(crate_version!())
        .author(crate_authors!())
        .about("Reads Varnish VSL (binary) log stream and emits JSON log entries to standard output")
        .arg(Arg::with_name("log-spec")
             .short("d")
             .long("log-sepc")
             .value_name("LOG_LEVEL_SPEC")
             .help("Logging level specification, e.g: info")
             .takes_value(true)
             .default_value("info"))
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
        .arg(Arg::with_name("stat-epoch-interval")
             .long("stat-epoch-interval")
             .short("s")
             .help("Log store stats every epoch interval (~seconds) on record insert")
             .takes_value(true)
             .validator(validate_stat_epoch_inverval)
             .default_value("600"))
        .arg(Arg::with_name("stream-buffer-size")
             .long("stream-buffer-size")
             .display_order(2000)
             .help("Size of stream buffer in bytes - must be bigger than biggest VSL record")
             .default_value("262144"))
        .arg(Arg::with_name("max-record-slots")
             .long("max-record-slots")
             .display_order(2000)
             .help("Maximum number of records being built or correlated at the same time")
             .validator(validate_max_record_slots)
             .default_value("4000"))
        .arg(Arg::with_name("max-epoch-diff")
             .long("max-epoch-diff")
             .display_order(2000)
             .help("Consider record being built or correlated as stale after that many epoch (~seconds) elapsed")
             .validator(validate_max_epoch_diff)
             .default_value("43200"))
        .arg(Arg::with_name("evict-factor")
             .long("evict-factor")
             .help("Remove that many records at a time when doing expiration or nucking (ratio to max-record-slots)")
             .display_order(2000)
             .validator(validate_evict_factor)
             .default_value("0.01"))
        .arg(Arg::with_name("vsl-file")
             .value_name("VSL_FILE")
             .help("VSL file to process (read from standard input if not specified)"))
        .get_matches();

    program::init(arguments.value_of("log-spec"));

    let output_format = value_t!(arguments, "output", OutputFormat).unwrap_or_else(|e| e.exit());
    let stream_buf_size = value_t!(arguments, "stream-buffer-size", usize).unwrap_or_else(|e| e.exit());

    let output = std::io::stdout();

    let config = Config {
        no_log_processing: arguments.is_present("no-log-processing"),
        keep_raw_log: arguments.is_present("keep-raw-log"),
        no_header_indexing: arguments.is_present("no-header-indexing"),
        keep_raw_headers: arguments.is_present("keep-raw-headers"),
    };

    let stat_epoch_interval = if arguments.is_present("stat-epoch-interval") {
        Some(value_t!(arguments, "stat-epoch-interval", u64).unwrap_or_else(|e| e.exit()))
    } else {
        None
    };

    let _epoch_timer = spawn_epoch_timer();

    let store_config = StoreConfig::new(
        value_t!(arguments, "max-record-slots", usize).unwrap_or_else(|e| e.exit()),
        value_t!(arguments, "max-epoch-diff", u64).unwrap_or_else(|e| e.exit()),
        value_t!(arguments, "evict-factor", f32).unwrap_or_else(|e| e.exit()),
        stat_epoch_interval,
        Some(global_epoch)
    ).unwrap();

    let result = if let Some(path) = arguments.value_of("vsl-file") {
        let file = File::open(path);
        match file {
            Ok(file) => process_vsl_stream(file, output, stream_buf_size, output_format, config, &store_config),
            Err(err) => program::exit_with_error(&format!("Failed to open VSL file: {}: {}", path, err), 1),
        }
    } else {
        let stdin = stdin();
        let stdin = stdin.lock();
        process_vsl_stream(stdin, output, stream_buf_size, output_format, config, &store_config)
    };

    if let Err(err) = result {
        if err.is_brokend_pipe() {
            info!("Broken pipe")
        } else {
            error!("{}", err);
            program::exit_with_error(err.description(), err.to_exit_code())
        }
    }

    info!("Done");
}
