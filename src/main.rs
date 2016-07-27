#[macro_use]
extern crate nom;

use std::cell::Cell;
use std::io::stdin;
use std::io::Read;
use std::io;
use std::fmt::{self, Display, Debug};
use std::ffi::{CStr, FromBytesWithNulError};
//use std::io::BufRead;
//use nom::{Producer, Move, Input, Consumer, ConsumerState};
use nom::{le_u32};

/*
pub struct BufReadProducer<R: Read + BufRead> {
    reader: R
}

impl<R: Read + BufRead> BufReadProducer<R> {
    pub fn new(reader: R) -> BufReadProducer<R> {
        BufReadProducer { reader: reader }
    }
}

impl<'x, R: Read + BufRead> Producer<'x, &'x [u8], Move> for BufReadProducer<R> {
    fn apply<'a,O,E>(&'x mut self, consumer: &'a mut Consumer<&'x[u8],O,E,Move>) -> &'a ConsumerState<O,E,Move> {
        match consumer.state() {
            &ConsumerState::Continue(ref m) => {
                println!("{:?}", m);
                match *m {
                    Move::Await(_) => (),
                    Move::Consume(bytes) => self.reader.consume(bytes),
                    Move::Seek(position) => panic!("Can't seek BufReadProducer: {:?}", position)
                }

            },
            _  => return consumer.state()
        }

        match self.reader.fill_buf() {
            //TODO: it will probably not give any more data if buf is full -
            //infinite loop!?
            Ok(ref data) => consumer.handle(Input::Element(data)),
            Err(_) => consumer.handle(Input::Eof(None))
        }
    }

    fn run<'a: 'x,O,E: 'x>(&'x mut self, consumer: &'a mut Consumer<&'x[u8],O,E,Move>)   -> Option<&O> {
        //TODO: keep calling apply until we get Done or Err
        //TODO: handle Eof
        if let &ConsumerState::Done(_,ref o) = self.apply(consumer) {
            Some(o)
        } else {
            None
        }
    }
}

consumer_from_parser!(VslConsumer<VslRecordHeader>, vsl_record_header);
consumer_from_parser!(VslTagConsumer<()>, vsl_tag);
*/

/*
 * Shared memory log format
 *
 * The log member points to an array of 32bit unsigned integers containing
 * log records.
 *
 * Each logrecord consist of:
 *    [n]               = ((type & 0xff) << 24) | (length & 0xffff)
 *    [n + 1]           = ((marker & 0x03) << 30) | (identifier & 0x3fffffff)
 *    [n + 2] ... [m]   = content (NUL-terminated)
 *
 * Logrecords are NUL-terminated so that string functions can be run
 * directly on the shmlog data.
 *
 * Notice that the constants in these macros cannot be changed without
 * changing corresponding magic numbers in varnishd/cache/cache_shmlog.c
 */

const VSL_LENOFFSET: u32 = 24;
const VSL_LENMASK: u32 = 0xffff;
const VSL_MARKERMASK: u32 = 0x03;
//const VSL_CLIENTMARKER: u32 = 1 << 30;
//const VSL_BACKENDMARKER: u32 = 1 << 31;
const VSL_IDENTOFFSET: u32 = 30;
const VSL_IDENTMASK: u32 = !(3 << VSL_IDENTOFFSET);

// https://github.com/varnishcache/varnish-cache/blob/master/include/vapi/vsl_int.h
// https://github.com/varnishcache/varnish-cache/blob/master/include/tbl/vsl_tags.h
// https://github.com/varnishcache/varnish-cache/blob/master/include/tbl/vsl_tags_http.h
/* TODO: generate with build.rs from the header files
enum VslTag {
    SLT__Bogus = 0,
    SLT__Reserved = 254,
    SLT__Batch = 255
}
*/

named!(binary_vsl_tag<&[u8], &[u8]>, tag!(b"VSL\0"));

#[derive(Debug)]
struct VslRecordHeader {
    pub tag: u8,
    pub len: u16,
    pub marker: u8,
    pub ident: u32,
}

fn vsl_record_header<'b>(input: &'b[u8]) -> nom::IResult<&'b[u8], VslRecordHeader, u32> {
    chain!(
        input, r1: le_u32 ~ r2: le_u32,
        || {
            VslRecordHeader {
                tag: (r1 >> VSL_LENOFFSET) as u8,
                len: (r1 & VSL_LENMASK) as u16,
                marker: (r2 & VSL_MARKERMASK >> VSL_IDENTOFFSET) as u8,
                ident: r2 & VSL_IDENTMASK,
            }
        })
}

struct VslRecord<'b> {
    pub tag: u8,
    pub marker: u8,
    pub ident: u32,
    pub data: &'b[u8],
}

impl<'b> VslRecord<'b> {
    fn body(&'b self) -> Result<&'b CStr, FromBytesWithNulError> {
        CStr::from_bytes_with_nul(self.data)
    }
}

impl<'b> Debug for VslRecord<'b> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.debug_struct("VSL Record")
            .field("tag", &self.tag)
            .field("marker", &self.marker)
            .field("ident", &self.ident)
            .field("body", &self.body())
            .finish()
    }
}

fn is_zero(i: u8) -> bool { i == 0 }

fn vsl_record<'b>(input: &'b[u8]) -> nom::IResult<&'b[u8], VslRecord<'b>, u32> {
    chain!(
        input,
        header: vsl_record_header ~ data: take!(header.len) ~ take_while!(is_zero),
        || {
            VslRecord { tag: header.tag, marker: header.marker, ident: header.ident, data: data }
        })
}

fn binary_vsl_records<'b>(input: &'b[u8]) -> nom::IResult<&'b[u8], Vec<VslRecord<'b>>, u32> {
    chain!(
        input, binary_vsl_tag ~ records: many1!(vsl_record),
        || { records })
}

#[derive(Debug)]
enum VslError<I, E> {
    IO(io::Error),
    NomErr(nom::Err<I, E>),
    NomNeeded(nom::Needed)
}

impl<I, E> From<io::Error> for VslError<I, E> {
    fn from(e: io::Error) -> VslError<I, E> {
        VslError::IO(e)
    }
}

impl<I, E> From<nom::Err<I, E>> for VslError<I, E> {
    fn from(e: nom::Err<I, E>) -> VslError<I, E> {
        VslError::NomErr(e)
    }
}

impl<I, E> From<nom::Needed> for VslError<I, E> {
    fn from(e: nom::Needed) -> VslError<I, E> {
        VslError::NomNeeded(e)
    }
}

trait ToVslResult<I, O, E> {
    fn into_vsl_result(self) -> Result<O, VslError<I, E>>;
}

impl<I, O, E> ToVslResult<I, O, E> for nom::IResult<I, O, E> {
    fn into_vsl_result(self) -> Result<O, VslError<I, E>> {
        match self {
            nom::IResult::Done(_, out) => Ok(out),
            nom::IResult::Error(e) => Err(From::from(e)),
            nom::IResult::Incomplete(n) => Err(From::from(n)),
        }
    }
}

impl<I, E> Display for VslError<I, E> where I: Debug, E: Debug {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &VslError::IO(ref e) => write!(f, "Failed to read VSL data: {}", e),
            &VslError::NomErr(ref e) => write!(f, "Failed to parse VSL data: {}", e),
            &VslError::NomNeeded(ref e) => write!(f, "Not enought data to parse VSL: {:?}", e),
        }
    }
}

const DEFAULT_BUF_SIZE: usize = 8 * 1024;

trait StreamBuf<O> {
    fn fill(&mut self, count: usize) -> Result<(), io::Error>;
    fn recycle(&mut self);
    fn consume(&mut self, count: usize);
    fn data<'b>(&'b self) -> &'b[O];
    fn needed<C, CO>(&self, combinator: C) -> Option<usize>
         where C: Fn(&[O]) -> nom::IResult<&[O], CO>;
    fn apply<'b, C, CO>(&'b mut  self, combinator: C) -> Result<CO, ()>
        where O: 'b, C: Fn(&'b [O]) -> nom::IResult<&'b [O], CO>;
}

struct ReadStreamBuf<R: Read> {
    reader: R,
    buf: Vec<u8>,
    cap: usize,
    offset: Cell<usize>,
}

impl<R: Read> ReadStreamBuf<R> {
    fn new(reader: R) -> ReadStreamBuf<R> {
        ReadStreamBuf::with_capacity(reader, DEFAULT_BUF_SIZE)
    }

    fn with_capacity(reader: R, cap: usize) -> ReadStreamBuf<R> {
        ReadStreamBuf {
            reader: reader,
            buf: Vec::with_capacity(cap),
            cap: cap,
            offset: Cell::new(0),
        }
    }
}

impl<R: Read> StreamBuf<u8> for ReadStreamBuf<R> {
    fn fill(&mut self, count: usize) -> Result<(), io::Error> {
        let len = self.buf.len();
        let have = len - self.offset.get();
        let need_more = count - have;

        if have >= count {
            return Ok(())
        }

        //TODO: enforce cap
        self.buf.resize(len + need_more, 0);
        let result = self.reader.read_exact(&mut self.buf[len..len + need_more]);

        if result.is_err() {
            self.buf.resize(len, 0);
        }
        result
    }

    fn recycle(&mut self) {
        if self.offset.get() == 0 {
            return
        }
        self.buf = self.buf.split_off(self.offset.get());
        self.offset.set(0);
    }

    fn consume(&mut self, count: usize) {
        let len = self.buf.len();

        let consume = if self.offset.get() + count > len {
            len - self.offset.get()
        } else {
            count
        };
        self.offset.set(self.offset.get() + consume);
    }

    fn data<'b>(&'b self) -> &'b[u8] {
        &self.buf[self.offset.get()..self.buf.len()]
    }

    fn needed<C, CO>(&self, combinator: C) -> Option<usize>
         where C: Fn(&[u8]) -> nom::IResult<&[u8], CO> {
        let result = combinator(self.data());
        if result.is_incomplete() {
            match result.unwrap_inc() {
                nom::Needed::Size(needed) => return Some(needed),
                nom::Needed::Unknown => panic!("ReadStreamBuf does not know how much data to read for stream!"),
            }
        }
        None
    }

    fn apply<'b, C, CO>(&'b mut self, combinator: C) -> Result<CO, ()>
        where C: Fn(&'b [u8]) -> nom::IResult<&'b [u8], CO> {
        // TODO: error handling
        let data = self.data();
        let (left, out) = combinator(data).unwrap();
        let consumed = data.len() - left.len();
        // Need Cell to modify offset after parsing is done
        self.offset.set(self.offset.get() + consumed);
        return Ok(out)
        /*
        let need;
        {
            use nom::{IResult, Needed};
            let data = self.data();
            match combinator(data) {
                IResult::Done(left, out) => {
                    let consumed = data.len() - left.len();
                    //Need Cell
                    self.offset.set(self.offset.get() + consumed);
                    return Ok(out)
                },
                IResult::Incomplete(Needed::Size(needed)) => need = needed,
                //TODO should I just return it?
                IResult::Incomplete(Needed::Unknown) => panic!("ReadStreamBuf does not know how much data to reed for stream!"),
                result => panic!("TODO: can't provide borrow of data here") //return Err(result)
            }
        }

        assert!(need > 0, "ReadStreamBuf does not make any progress applying combinator (need(0))");
        //TODO: how to signal IO errors?
        self.fill(need).unwrap();

        //TODO: use loop
        self.apply(combinator)
        */
    }
}

fn main() {
    let stdin = stdin();
    let mut stdin = stdin.lock();

    let mut buf = Vec::new();
    stdin.read_to_end(&mut buf).unwrap();

    let records = binary_vsl_records(buf.as_slice()).into_vsl_result().unwrap();
    for record in records {
        println!("{:?}", &record);
    }
}

#[cfg(test)]
mod resd_stream_buf_test {
    use super::StreamBuf;
    use super::ReadStreamBuf;
    use super::nom::IResult;
    use std::io::Cursor;

    fn subject(data: Vec<u8>) -> ReadStreamBuf<Cursor<Vec<u8>>> {
        ReadStreamBuf::new(Cursor::new(data))
    }

    fn subject_with_default_data() -> ReadStreamBuf<Cursor<Vec<u8>>> {
        subject(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    }

    #[test]
    fn reading() {
        let mut rsb = subject_with_default_data();
        assert_eq!(rsb.data(), [].as_ref());

        rsb.fill(2).unwrap();
        assert_eq!(rsb.data(), [0, 1].as_ref());

        rsb.fill(3).unwrap();
        assert_eq!(rsb.data(), [0, 1, 2].as_ref());
    }

    #[test]
    fn recycle() {
        let mut rsb = subject_with_default_data();

        rsb.fill(3).unwrap();
        assert_eq!(rsb.data(), [0, 1, 2].as_ref());

        rsb.recycle();
        assert_eq!(rsb.data(), [0, 1, 2].as_ref());

        rsb.fill(5).unwrap();
        rsb.fill(7).unwrap();
        assert_eq!(rsb.data(), [0, 1, 2, 3, 4, 5, 6].as_ref());
    }

    #[test]
    fn consume() {
        let mut rsb = subject_with_default_data();

        rsb.fill(5).unwrap();
        assert_eq!(rsb.data(), [0, 1, 2, 3, 4].as_ref());

        rsb.consume(2);
        assert_eq!(rsb.data(), [2, 3, 4].as_ref());
    }

    #[test]
    fn consume_more_than_we_have() {
        let mut rsb = subject_with_default_data();

        // We don't consume more than we have already in the buffor
        rsb.fill(2).unwrap();
        rsb.consume(3);
        rsb.fill(2).unwrap();
        assert_eq!(rsb.data(), [2, 3].as_ref());
    }

    #[test]
    fn apply_function() {
        use nom::be_u8;
        let mut rsb = subject_with_default_data();

        rsb.fill(1).unwrap();
        assert_eq!(rsb.apply(be_u8), Ok(0));
    }

    #[test]
    fn apply_should_consume() {
        use nom::be_u8;
        let mut rsb = subject_with_default_data();

        rsb.fill(1).unwrap();
        assert_eq!(rsb.apply(be_u8), Ok(0));
        rsb.fill(1).unwrap();
        assert_eq!(rsb.apply(be_u8), Ok(1));
    }

    #[test]
    fn apply_converted_macro() {
        let mut rsb = subject_with_default_data();

        rsb.fill(2).unwrap();
        assert_eq!(rsb.apply(closure!(tag!([0, 1]))), Ok([0, 1].as_ref()));
    }

    #[test]
    fn apply_closure() {
        let mut rsb = subject_with_default_data();

        rsb.fill(2).unwrap();
        assert_eq!(rsb.apply(|i| tag!(i, [0, 1])), Ok([0, 1].as_ref()));
    }

    #[test]
    fn apply_custom_fuction_with_refs() {
        let mut rsb = subject_with_default_data();
        fn tag<'i>(input: &'i [u8]) -> IResult<&'i [u8], &[u8]> {
            tag!(input, [0, 1, 2])
        }

        rsb.fill(3).unwrap();
        assert_eq!(rsb.apply(tag), Ok([0, 1, 2].as_ref()));
    }
}
