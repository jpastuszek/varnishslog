#[macro_use]
extern crate bencher;
use bencher::Bencher;

use std::fs::File;
use std::io::Cursor;
use std::io::{Read, ErrorKind};

extern crate varnishslog;

use varnishslog::stream_buf::{ReadStreamBuf, StreamBuf, FillApplyError, FillError};
use varnishslog::access_log::{binary_vsl_tag, vsl_record_v4, VslRecord};
use varnishslog::access_log::RecordState;
use varnishslog::access_log::SessionState;
use varnishslog::access_log::{log_session_record, Format, Config};

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

fn access_record_parsing(bench: &mut Bencher) {
    let mut cursor = Cursor::new(load_data());
    bench.iter(|| {
        let mut rs = RecordState::new();
        // use larger data sample
        for _ in 0..4 {
            {
                parse_each_vsl_record(ReadStreamBuf::new(&mut cursor), |vsl_record| {
                    bencher::black_box(rs.apply(vsl_record));
                });
            }
            cursor.set_position(0);
        }
        assert_eq!(rs.building_count(), 0);
        assert_eq!(rs.tombstone_count(), 0);
    });
}

fn access_session_parsing(bench: &mut Bencher) {
    let mut cursor = Cursor::new(load_data());
    bench.iter(|| {
        let mut ss = SessionState::new();
        // use larger data sample
        for _ in 0..4 {
            {
                parse_each_vsl_record(ReadStreamBuf::new(&mut cursor), |vsl_record| {
                    bencher::black_box(ss.apply(vsl_record));
                });
            }
            cursor.set_position(0);
        }
        assert_eq!(ss.unmatched_client_access_records().len(), 0);
        assert_eq!(ss.unmatched_backend_access_records().len(), 0);
        assert_eq!(ss.unresolved_sessions().len(), 0);
    });
}

fn access_record_ncsa_json(bench: &mut Bencher) {
    let mut cursor = Cursor::new(load_data());
    let config = Config {
        no_log_processing: false,
        keep_raw_log: false,
        no_header_indexing: false,
        keep_raw_headers: false,
    };
    let format = Format::NcsaJson;

    bench.iter(|| {
        let mut ss = SessionState::new();
        let mut out = Cursor::new(Vec::new());

        // use larger data sample
        for _ in 0..4 {
            {
                parse_each_vsl_record(ReadStreamBuf::new(&mut cursor), |vsl_record| {
                    if let Some(session) = ss.apply(vsl_record) {
                        log_session_record(&session, &format, &mut out, &config).unwrap()
                    }
                });
            }
            cursor.set_position(0);
        }
        //println!("written {} bytes of output", out.into_inner().len());
        //println!("written {} records", out.into_inner().into_iter().filter(|b| *b == b'\n').count());
    });
}

fn access_record_json(bench: &mut Bencher) {
    let mut cursor = Cursor::new(load_data());
    let config = Config {
        no_log_processing: false,
        keep_raw_log: false,
        no_header_indexing: false,
        keep_raw_headers: false,
    };
    let format = Format::NcsaJson;

    bench.iter(|| {
        let mut ss = SessionState::new();
        let mut out = Cursor::new(Vec::new());

        // use larger data sample
        for _ in 0..4 {
            {
                parse_each_vsl_record(ReadStreamBuf::new(&mut cursor), |vsl_record| {
                    if let Some(session) = ss.apply(vsl_record) {
                        log_session_record(&session, &format, &mut out, &config).unwrap()
                    }
                });
            }
            cursor.set_position(0);
        }
    });
}

fn access_record_json_raw(bench: &mut Bencher) {
    let mut cursor = Cursor::new(load_data());
    let config = Config {
        no_log_processing: true,
        keep_raw_log: true,
        no_header_indexing: true,
        keep_raw_headers: true,
    };
    let format = Format::NcsaJson;

    bench.iter(|| {
        let mut ss = SessionState::new();
        let mut out = Cursor::new(Vec::new());

        // use larger data sample
        for _ in 0..4 {
            {
                parse_each_vsl_record(ReadStreamBuf::new(&mut cursor), |vsl_record| {
                    if let Some(session) = ss.apply(vsl_record) {
                        log_session_record(&session, &format, &mut out, &config).unwrap()
                    }
                });
            }
            cursor.set_position(0);
        }
    });
}

benchmark_group!(benches,
                 access_record_parsing,
                 access_session_parsing,
                 access_record_ncsa_json,
                 access_record_json,
                 access_record_json_raw);
benchmark_main!(benches);
