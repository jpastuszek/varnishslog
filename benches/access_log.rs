#[macro_use]
extern crate bencher;
use bencher::Bencher;

use std::fs::File;
use std::io::Cursor;
use std::io::{Read, ErrorKind};

extern crate varnishslog;

use varnishslog::stream_buf::{ReadStreamBuf, StreamBuf, FillApplyError, FillError};
use varnishslog::vsl::record::VslRecord;
use varnishslog::vsl::record::parser::{binary_vsl_tag, vsl_record_v4};
use varnishslog::access_log::session_state::SessionState;
use varnishslog::access_log::record_state::RecordState;
use varnishslog::serialization::{log_client_record, Format, Config};

fn parse_each_vsl_record<R: Read, C>(mut rfb: ReadStreamBuf<R>, mut block: C) where C: FnMut(&VslRecord) {
    rfb.fill_apply(binary_vsl_tag).unwrap();

    loop {
        match rfb.fill_apply(binary_vsl_tag) {
            Err(err) => panic!("VSL tag error: {}", err),
            Ok(None) => continue,
            Ok(Some(_)) => break,
        }
    }

    loop {
        match rfb.fill_apply(vsl_record_v4) {
            Err(FillApplyError::FillError(FillError::Io(err))) => {
                if err.kind() == ErrorKind::UnexpectedEof {
                    break
                }
                panic!("I/O error: {}", err);
            },
            Err(err) => panic!("error: {}", err),
            Ok(None) => continue,
            Ok(Some(record)) => block(&record),
        }
    }
}

fn open_data() -> File {
    // 13480 VSL records making up 211 access records and 86 sessions and 86 client access records
    File::open("benches/varnish20160816-4093-1xh1jbx808a493d5e74216e5.vsl").unwrap()
}

fn load_data() -> Vec<u8> {
    let mut data = Vec::new();
    open_data().read_to_end(&mut data).unwrap();
    data
}

fn record_state(bench: &mut Bencher) {
    let mut cursor = Cursor::new(load_data());
    bench.iter(|| {
        let mut rs = RecordState::new();
        {
            parse_each_vsl_record(ReadStreamBuf::new(&mut cursor), |vsl_record| {
                bencher::black_box(rs.apply(vsl_record));
            });
        }
        cursor.set_position(0);
    });
}

fn session_state(bench: &mut Bencher) {
    let mut cursor = Cursor::new(load_data());
    bench.iter(|| {
        let mut ss = SessionState::new();
        {
            parse_each_vsl_record(ReadStreamBuf::new(&mut cursor), |vsl_record| {
                bencher::black_box(ss.apply(vsl_record));
            });
        }
        cursor.set_position(0);
    });
}

fn log_session_record_ncsa_json(bench: &mut Bencher) {
    let mut cursor = Cursor::new(load_data());
    let config = Config {
        no_log_processing: false,
        keep_raw_log: false,
        no_header_indexing: false,
        keep_raw_headers: false,
    };
    let format = Format::NcsaJson;

    bench.iter(|| {
        let mut out = Cursor::new(Vec::new());
        let mut ss = SessionState::new();
        {
            parse_each_vsl_record(ReadStreamBuf::new(&mut cursor), |vsl_record| {
                if let Some(session) = ss.apply(vsl_record) {
                    log_client_record(&session, &format, &mut out, &config).unwrap()
                }
            });
        }
        cursor.set_position(0);
        assert!(out.into_inner().len() > 100_000);
    });
}

fn log_session_record_json(bench: &mut Bencher) {
    let mut cursor = Cursor::new(load_data());
    let config = Config {
        no_log_processing: false,
        keep_raw_log: false,
        no_header_indexing: false,
        keep_raw_headers: false,
    };
    let format = Format::NcsaJson;

    bench.iter(|| {
        let mut out = Cursor::new(Vec::new());
        let mut ss = SessionState::new();
        {
            parse_each_vsl_record(ReadStreamBuf::new(&mut cursor), |vsl_record| {
                if let Some(session) = ss.apply(vsl_record) {
                    log_client_record(&session, &format, &mut out, &config).unwrap()
                }
            });
        }
        cursor.set_position(0);
    });
}

fn log_session_record_json_raw(bench: &mut Bencher) {
    let mut cursor = Cursor::new(load_data());
    let config = Config {
        no_log_processing: true,
        keep_raw_log: true,
        no_header_indexing: true,
        keep_raw_headers: true,
    };
    let format = Format::NcsaJson;

    bench.iter(|| {
        let mut out = Cursor::new(Vec::new());
        let mut ss = SessionState::new();
        {
            parse_each_vsl_record(ReadStreamBuf::new(&mut cursor), |vsl_record| {
                if let Some(session) = ss.apply(vsl_record) {
                    log_client_record(&session, &format, &mut out, &config).unwrap()
                }
            });
        }
        cursor.set_position(0);
    });
}

benchmark_group!(benches,
                 record_state,
                 session_state,
                 log_session_record_ncsa_json,
                 log_session_record_json,
                 log_session_record_json_raw);
benchmark_main!(benches);
