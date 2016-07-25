#[macro_use]
extern crate nom;

use std::io::stdin;
use std::io::Read;
use std::io;
use std::fmt::{self, Display, Debug};
//use std::io::BufRead;
//use nom::{Producer, Move, Input, Consumer, ConsumerState};
use nom::{le_u8, le_u32};

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
enum VSLTag {
    BinaryVSL
}

named!(vsl_tag<&[u8], VSLTag>, chain!(tag!(b"VSL\0"), ||{VSLTag::BinaryVSL}));

#[derive(Debug)]
struct VslHeader {
    tag: u8,
    len: u32
}

named!(vsl_record_header<&[u8], VslHeader>, chain!(
        tag: peek!(le_u8)  ~
        len: le_u32 ,
        ||{
            VslHeader { tag: tag, len: len & 0x00ffffff}
        }));

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


fn read_data_tag<'b, R: Read>(reader: &mut R, buf: &'b mut [u8; 4]) -> Result<VSLTag, VslError<&'b[u8], u32>> {
    try!(reader.read_exact(buf));
    vsl_tag(buf).into_vsl_result()
}

fn main() {
    let stdin = stdin();
    let mut stdin = stdin.lock();

    let mut buf = [0u8; 4];
    let tag = read_data_tag(&mut stdin, &mut buf).expect("Failed to parse data tag");

    println!("Tag: {:?}", tag);
}
