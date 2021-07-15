//use flexi_logger::{init as log_init, LogConfig, LogRecord};
use flexi_logger;
use std;
use log::error;
use std::io::{Write, Error};

fn app_format(write: &mut dyn Write, now: &mut flexi_logger::DeferredNow, record: &flexi_logger::Record<'_>) -> Result<(), Error> {
    //2016-02-09 10:20:15.784 [varnishband] INFO  src/varnishband.rs - Processing update events
    write!(write, "{} [{}] {:<5} {} - {}",
        now.now().format("%Y-%m-%d %H:%M:%S%.3f"),
        record.module_path().unwrap_or("-"),
        record.level(),
        record.file().unwrap_or("-"),
        &record.args())
}

pub fn init(spec: Option<&str>) {
    // let mut log_config = LogConfig::new();
    // log_config.format = app_format;
    // log_init(log_config, spec.map(|s| s.into())).unwrap();

    flexi_logger::Logger::try_with_str(spec.unwrap_or("info")).unwrap()
        .format(app_format)
        .start().unwrap();
}

pub fn exit_with_error(msg: &str, code: i32) -> ! {
    error!("Exiting: {}", msg);
    std::process::exit(code);
}
