#[macro_use]
extern crate bencher;
use bencher::Bencher;

use std::fs::File;
use std::io::Cursor;
use std::io::{Seek, SeekFrom};
use std::io::{Read, ErrorKind};

extern crate varnishslog;

use varnishslog::stream_buf::{ReadStreamBuf, StreamBuf, FillApplyError, FillError};
use varnishslog::vsl::record::parser::{binary_vsl_tag, vsl_record_v4};

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

fn open_data() -> File {
    File::open("benches/varnish20160816-4093-1xh1jbx808a493d5e74216e5.vsl").unwrap()
}

fn load_data() -> Vec<u8> {
    let mut data = Vec::new();
    open_data().read_to_end(&mut data).unwrap();
    data
}

fn default_buffer(bench: &mut Bencher) {
    let mut cursor = Cursor::new(load_data());

    bench.iter(|| {
        parse_vsl(ReadStreamBuf::new(&mut cursor));
        cursor.set_position(0);
    })
}

fn default_buffer_from_file(bench: &mut Bencher) {
    let mut file = open_data();

    bench.iter(|| {
        parse_vsl(ReadStreamBuf::new(&mut file));
        file.seek(SeekFrom::Start(0)).unwrap();
    })
}

fn custom_buffer_from_file_303b(bench: &mut Bencher) {
    let mut file = open_data();

    bench.iter(|| {
        parse_vsl(ReadStreamBuf::with_capacity(&mut file, 303));
        file.seek(SeekFrom::Start(0)).unwrap();
    })
}

fn custom_buffer_from_file_1mib(bench: &mut Bencher) {
    let mut file = open_data();

    bench.iter(|| {
        parse_vsl(ReadStreamBuf::with_capacity(&mut file, 1024 * 1024));
        file.seek(SeekFrom::Start(0)).unwrap();
    })
}

fn default_buffer_no_prefetch(bench: &mut Bencher) {
    let mut cursor = Cursor::new(load_data());

    bench.iter(|| {
        {
            let mut rsb = ReadStreamBuf::new(&mut cursor);
            rsb.disable_prefetch();
            parse_vsl(rsb);
        }
        cursor.set_position(0);
    })
}

benchmark_group!(benches,
                 default_buffer,
                 default_buffer_from_file,
                 custom_buffer_from_file_303b,
                 custom_buffer_from_file_1mib,
                 default_buffer_no_prefetch);
benchmark_main!(benches);
