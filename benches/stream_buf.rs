#[macro_use]
extern crate bencher;
use bencher::Bencher;

use std::fs::File;
use std::io::Read;
use std::io::Cursor;
use std::io::ErrorKind;

extern crate varnishslog;

use varnishslog::stream_buf::{ReadStreamBuf, StreamBuf, FillApplyError, FillError};
use varnishslog::access_log::{binary_vsl_tag, vsl_record_v4};

fn parse_vsl<R: Read>(mut rfb: ReadStreamBuf<R>) {
    rfb.fill_apply(binary_vsl_tag).unwrap();

    loop {
        match rfb.fill_apply(binary_vsl_tag) {
            Err(err) => panic!("VSL tag error: {}", err),
            Ok(None) => continue,
            Ok(Some(_)) => break,
        }
    }

    loop {
        let record = match rfb.fill_apply(vsl_record_v4) {
            Err(FillApplyError::FillError(FillError::Io(err))) => {
                if err.kind() == ErrorKind::UnexpectedEof {
                    break
                }
                panic!("I/O error: {}", err);
            },
            Err(err) => panic!("error: {}", err),
            Ok(None) => continue,
            Ok(Some(record)) => record,
        };
        bencher::black_box(record);
    }
}

fn load_data() -> Vec<u8> {
    let mut data = Vec::new();
    File::open("benches/varnish20160816-4093-1xh1jbx808a493d5e74216e5.vsl").unwrap().read_to_end(&mut data).unwrap();
    data
}

fn default_buffer(bench: &mut Bencher) {
    let mut cursor = Cursor::new(load_data());

    bench.iter(|| {
        parse_vsl(ReadStreamBuf::new(&mut cursor));
        cursor.set_position(0);
    })
}

fn tiny_buffer(bench: &mut Bencher) {
    let mut cursor = Cursor::new(load_data());

    bench.iter(|| {
        parse_vsl(ReadStreamBuf::with_capacity(&mut cursor, 303));
        cursor.set_position(0);
    })
}

fn large_buffer(bench: &mut Bencher) {
    let mut cursor = Cursor::new(load_data());

    bench.iter(|| {
        parse_vsl(ReadStreamBuf::with_capacity(&mut cursor, 1024 * 1024));
        cursor.set_position(0);
    })
}

benchmark_group!(benches, default_buffer, tiny_buffer, large_buffer);
benchmark_main!(benches);
