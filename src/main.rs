#[macro_use]
extern crate nom;

use std::io::stdin;
use std::io::Read;
use std::io;
use std::fmt::{self, Display, Debug};
//use std::io::BufRead;
//use nom::{Producer, Move, Input, Consumer, ConsumerState};
use nom::{be_u8, be_u32, rest};

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

consumer_from_parser!(VslConsumer<VslHeader>, vsl_record_header);
consumer_from_parser!(VslTagConsumer<()>, vsl_tag);
*/

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
struct VslHeader {
    pub tag: u8,
    pub len: u32
}

named!(vsl_record_header<&[u8], VslHeader>, chain!(
        tag: peek!(be_u8)  ~
        len: be_u32 ,
        || {
            VslHeader { tag: tag, len: len & 0x00ffffff}
        }));

#[derive(Debug)]
struct VslRecord<'b> {
    pub xid: u32,
    pub data: &'b str
}

fn is_zero(i: &u8) -> bool {
    *i == 0
}
fn is_zero2(i: u8) -> bool {
    i == 0
}

fn vsl_record_data_s<'b>(input: &'b[u8]) -> nom::IResult<&'b[u8], &'b str, u32> {
    use ::std::str::from_utf8;
    map_res!(input, take_till!(is_zero), from_utf8)
}

fn vsl_record<'b>(input: &'b[u8]) -> nom::IResult<&'b[u8], VslRecord<'b>, u32> {
    chain!(
        input,
        xid: be_u32  ~
        data: vsl_record_data_s ~
        take_while!(is_zero2),
        || {
            VslRecord { xid: xid, data: data }
        })
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

fn read_record_header<'b, R: Read>(reader: &mut R, buf: &'b mut [u8; 4]) -> Result<VslHeader, VslError<&'b[u8], u32>> {
    try!(reader.read_exact(buf));
    vsl_record_header(buf).into_vsl_result()
}

fn read_record<'b, R: Read>(reader: &mut R, buf: &'b mut Vec<u8>, len: u32) -> Result<VslRecord<'b>, VslError<&'b[u8], u32>> {
    try!(reader.take(len as u64).read_to_end(buf));

    let len = buf.len();
    let vsl_record_bytes: Box<Fn(&'b[u8]) -> nom::IResult<&[u8], &[u8]>> =
        Box::new(closure!(&'b[u8], take!(len)));
    let bytes = try!(vsl_record_bytes(buf).into_vsl_result());
    vsl_record(&bytes).into_vsl_result()
}

fn main() {
    let stdin = stdin();
    let mut stdin = stdin.lock();

    let mut buf = [0u8; 4];
    let tag = read_data_tag(&mut stdin, &mut buf).expect("Failed to parse data tag");

    println!("Tag: {:?}", &tag);
    match tag {
        VslTag::BinaryVsl => {
            let header = read_record_header(&mut stdin, &mut buf).expect("Invalid header");
            println!("Header: {:?}", &header);

            let mut buf = Vec::with_capacity(header.len as usize);
            let record = read_record(&mut stdin, &mut buf, header.len).expect("Invalid record");
            println!("Record: {:?}", &record);

        }
    }
}
