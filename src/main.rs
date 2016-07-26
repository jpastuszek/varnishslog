#[macro_use]
extern crate nom;

use std::io::stdin;
use std::io::Read;
use std::io;
use std::fmt::{self, Display, Debug};
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
const VSL_CLIENTMARKER: u32 = 1 << 30;
const VSL_BACKENDMARKER: u32 = 1 << 31;
const VSL_IDENTOFFSET: u32 = 30;
const VSL_IDENTMASK: u32 = !(3 << VSL_IDENTOFFSET);

#[derive(Debug)]
enum VslTag {
    BinaryVsl
}

named!(vsl_tag<&[u8], VslTag>, chain!(
        tag!(b"VSL\0"),
        || {
            VslTag::BinaryVsl
        }));

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

#[derive(Debug)]
struct VslRecord<'b> {
    pub tag: u8,
    pub marker: u8,
    pub ident: u32,
    pub data: &'b str,
    pub body: &'b[u8]
}

fn is_zero(i: u8) -> bool {
    i == 0
}

fn vsl_record<'b>(input: &'b[u8]) -> nom::IResult<&'b[u8], VslRecord<'b>, u32> {
    chain!(
        input,
        header: vsl_record_header ~ body: take!(header.len) ~ take_while!(is_zero),
        || {
            VslRecord { tag: header.tag, marker: header.marker, ident: header.ident, data: "foobar", body: body }
        })
}

fn vsl_records<'b>(input: &'b[u8]) -> nom::IResult<&'b[u8], Vec<VslRecord<'b>>, u32> {
    many1!(input, vsl_record)
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

fn read_data_tag<'b, R: Read>(reader: &mut R, buf: &'b mut [u8; 4]) -> Result<VslTag, VslError<&'b[u8], u32>> {
    try!(reader.read_exact(buf));
    vsl_tag(buf).into_vsl_result()
}

fn main() {
    let stdin = stdin();
    let mut stdin = stdin.lock();

    let mut buf = [0u8; 4];
    let tag = read_data_tag(&mut stdin, &mut buf).expect("Failed to parse data tag");

    println!("Tag: {:?}", &tag);
    match tag {
        VslTag::BinaryVsl => {
            let mut buf = Vec::new();
            stdin.read_to_end(&mut buf);

            let records = vsl_records(buf.as_slice()).into_vsl_result();
            println!("Record: {:?}", &records);
        }
    }
}
