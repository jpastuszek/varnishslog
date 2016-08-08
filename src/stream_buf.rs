use std::fmt::{self, Display, Debug};
use std::error::Error;
use std::any::Any;
use std::cell::Cell;
use std::io::Read;
use std::io;
use nom;

#[allow(dead_code)]
pub const DEFAULT_BUF_SIZE: usize = 256 * 1024;

pub trait StreamBuf<O> {
    fn fill(&mut self, min_count: usize) -> Result<(), FillError>;
    //TODO: rename to relocate
    fn recycle(&mut self);
    fn consume(&mut self, count: usize);
    fn data<'b>(&'b self) -> &'b[O];
    fn needed(&self) -> Option<nom::Needed>;
    fn apply<'b, C, CO, E>(&'b self, combinator: C) -> Result<Option<CO>, nom::Err<&'b [O], E>> where
        O: 'b, C: Fn(&'b [O]) -> nom::IResult<&'b [O], CO, E>;

    fn fill_apply<'b, C, CO, E>(&'b mut self, combinator: C) -> Result<Option<CO>, FillApplyError<&'b [O], E>> where
        C: Fn(&'b [O]) -> nom::IResult<&'b [O], CO, E> {

        match self.needed() {
            Some(nom::Needed::Size(bytes)) => try!(self.fill(bytes)),
            Some(nom::Needed::Unknown) => try!(self.fill(1)),
            None => ()
        }

        Ok(try!(self.apply(combinator)))
    }
}

#[derive(Debug)]
pub enum FillError {
    Io(io::Error),
    BufferOverflow(usize, usize),
}

impl From<io::Error> for FillError {
    fn from(e: io::Error) -> FillError {
        FillError::Io(e)
    }
}

impl Display for FillError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &FillError::Io(ref e) => write!(f, "Failed to read data: {}", e),
            &FillError::BufferOverflow(needed, capacity) => write!(f, "Cannot fill buffer of size {} bytes with {} bytes of data", capacity, needed),
        }
    }
}

impl Error for FillError {
    fn description(&self) -> &str {
        match self {
            &FillError::Io(_) => "I/O error",
            &FillError::BufferOverflow(_, _) => "buffer overflow",
        }
    }
    fn cause(&self) -> Option<&Error> {
        match self {
            &FillError::Io(ref e) => Some(e),
            &FillError::BufferOverflow(_, _) => None,
        }
    }
}

#[derive(Debug)]
pub enum FillApplyError<I, E> {
    Parser(nom::Err<I, E>),
    FillError(FillError),
}

impl<I, E> From<nom::Err<I, E>> for FillApplyError<I, E> {
    fn from(e: nom::Err<I, E>) -> FillApplyError<I, E> {
        FillApplyError::Parser(e)
    }
}

impl<I, E> From<FillError> for FillApplyError<I, E> {
    fn from(e: FillError) -> FillApplyError<I, E> {
        FillApplyError::FillError(e)
    }
}

impl<I, E> Display for FillApplyError<I, E> where I: Debug, E: Debug {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &FillApplyError::Parser(ref e) => write!(f, "Failed to parse data: {}", e),
            &FillApplyError::FillError(ref e) => write!(f, "Failed to fill buffer with amout of data requested by parser: {}", e),
        }
    }
}

impl<I, E> Error for FillApplyError<I, E> where I: Debug + Display + Any, E: Error {
    fn description(&self) -> &str {
        match self {
            &FillApplyError::Parser(_) => "parsing faield",
            &FillApplyError::FillError(_) => "buffer fill error",
        }
    }
    fn cause(&self) -> Option<&Error> {
        match self {
            &FillApplyError::Parser(_) => None, // e contains reference to data
            &FillApplyError::FillError(ref e) => Some(e),
        }
    }
}

pub struct ReadStreamBuf<R: Read> {
    reader: R,
    //TODO: alignment?
    buf: Vec<u8>,
    cap: usize,
    needed: Cell<Option<nom::Needed>>,
    offset: Cell<usize>,
}

impl<R: Read> ReadStreamBuf<R> {
    #[allow(dead_code)]
    pub fn new(reader: R) -> ReadStreamBuf<R> {
        ReadStreamBuf::with_capacity(reader, DEFAULT_BUF_SIZE)
    }

    pub fn with_capacity(reader: R, cap: usize) -> ReadStreamBuf<R> {
        ReadStreamBuf {
            reader: reader,
            buf: Vec::with_capacity(cap),
            cap: cap,
            needed: Cell::new(Some(nom::Needed::Unknown)),
            offset: Cell::new(0),
        }
    }
}

impl<R: Read> StreamBuf<u8> for ReadStreamBuf<R> {
    fn fill(&mut self, min_bytes: usize) -> Result<(), FillError> {
        let len = self.buf.len();
        let have = len - self.offset.get();

        if have >= min_bytes {
            return Ok(())
        }

        let needed = min_bytes - have;

        if min_bytes > self.cap {
            return Err(FillError::BufferOverflow(min_bytes, self.cap))
        }
        if len + needed > self.cap {
            self.recycle();
            assert_eq!(self.offset.get(), 0);
            assert_eq!(self.buf.len(), have);
            return self.fill(min_bytes)
        }

        self.buf.resize(self.cap, 0);
        trace!("reading exactly {} bytes into buf blocking: {}..{} ({}); have: {} will have: {}", needed, len, len + needed, self.buf[len..len + needed].len(), have, have + needed);
        if let Err(err) = self.reader.read_exact(&mut self.buf[len..len + needed]) {
            self.buf.resize(len, 0);
            return Err(From::from(err));
        }

        // Try to read to the end of the buffer if we can
        trace!("reading up to {} extra bytes into buf non blocking", self.cap - (len + needed));
        match self.reader.read(&mut self.buf[len + needed..self.cap]) {
            Err(err) => {
                self.buf.resize(len + needed, 0);
                return Err(From::from(err));
            },
            Ok(bytes_read) => {
                trace!("got extra {} bytes", bytes_read);
                self.buf.resize(len + needed + bytes_read, 0);
            }
        }

        //trace!("buf has: {:?}", self.data());
        trace!("buf has {} bytes", self.data().len());
        Ok(())
    }

    fn recycle(&mut self) {
        if self.offset.get() == 0 {
            return
        }
        self.buf = self.buf.split_off(self.offset.get());
        self.offset.set(0);
        //TODO: expand buffer OR use memcpy - bench test it
    }

    fn consume(&mut self, bytes: usize) {
        let len = self.buf.len();

        let consume = if self.offset.get() + bytes > len {
            len - self.offset.get()
        } else {
            bytes
        };
        self.offset.set(self.offset.get() + consume);
    }

    fn data<'b>(&'b self) -> &'b[u8] {
        &self.buf[self.offset.get()..self.buf.len()]
    }

    fn needed(&self) -> Option<nom::Needed> {
        self.needed.get().clone()
    }

    fn apply<'b, C, CO, E>(&'b self, combinator: C) -> Result<Option<CO>, nom::Err<&'b [u8], E>> where
        C: Fn(&'b [u8]) -> nom::IResult<&'b [u8], CO, E> {
        let data = self.data();
        match combinator(data) {
            nom::IResult::Done(left, out) => {
                let consumed = data.len() - left.len();
                trace!("done: consumed: {}", consumed);

                // Move the offset
                self.offset.set(self.offset.get() + consumed);
                // We don't know how much we will need now
                self.needed.set(None);
                Ok(Some(out))
            },
            nom::IResult::Error(err) => Err(err),
            nom::IResult::Incomplete(needed) => {
                trace!("incomplete: needed: {:?}", needed);
                self.needed.set(Some(needed));
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod resd_stream_buf_tests {
    use super::StreamBuf;
    use super::ReadStreamBuf;
    use super::FillError;
    use nom::{IResult, Needed};
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
        assert!(rsb.data().is_empty());

        // fill reads as much as it can non-blocking
        // TODO: how to test blocking streaming?
        rsb.fill(3).unwrap();
        assert_eq!(rsb.data(), [0, 1, 2, 3, 4, 5, 6, 7, 8 ,9].as_ref());
    }

    #[test]
    fn recycle() {
        let mut rsb = subject_with_default_data();

        //TODO: this test does nothing...
        rsb.fill(10).unwrap();
        rsb.recycle();
        assert_eq!(rsb.data(), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9].as_ref());
    }

    #[test]
    fn consume() {
        let mut rsb = subject_with_default_data();

        rsb.fill(10).unwrap();
        rsb.consume(8);
        assert_eq!(rsb.data(), [8, 9].as_ref());
    }

    #[test]
    fn consume_more_than_we_have() {
        let mut rsb = subject_with_default_data();

        rsb.consume(3);
        rsb.fill(10).unwrap();
        assert_eq!(rsb.data(), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9].as_ref());
    }

    #[test]
    fn apply_function() {
        use nom::be_u8;
        let mut rsb = subject_with_default_data();

        rsb.fill(1).unwrap();
        assert_eq!(rsb.apply(be_u8).unwrap(), Some(0));
    }

    #[test]
    fn apply_should_consume() {
        use nom::be_u8;
        let mut rsb = subject_with_default_data();

        rsb.fill(1).unwrap();
        assert_eq!(rsb.apply(be_u8).unwrap(), Some(0));
        rsb.fill(1).unwrap();
        assert_eq!(rsb.apply(be_u8).unwrap(), Some(1));
    }

    #[test]
    fn apply_converted_macro() {
        let mut rsb = subject_with_default_data();

        rsb.fill(2).unwrap();
        assert_eq!(rsb.apply(closure!(tag!([0, 1]))).unwrap(), Some([0, 1].as_ref()));
    }

    #[test]
    fn apply_closure() {
        let mut rsb = subject_with_default_data();

        rsb.fill(2).unwrap();
        assert_eq!(rsb.apply(|i| tag!(i, [0, 1])).unwrap(), Some([0, 1].as_ref()));
    }

    #[test]
    fn apply_custom_fuction_with_refs() {
        let mut rsb = subject_with_default_data();
        fn comb(input: &[u8]) -> IResult<&[u8], &[u8]> {
            tag!(input, [0, 1, 2])
        }

        rsb.fill(3).unwrap();
        assert_eq!(rsb.apply(comb).unwrap(), Some([0, 1, 2].as_ref()));
    }

    #[test]
    fn needed_with_apply() {
        let mut rsb = subject_with_default_data();

        fn comb<'a>(input: &'a[u8]) -> IResult<&'a[u8], &'a[u8]> {
            tag!(input, [0, 1, 2])
        }

        assert!(rsb.apply(comb).unwrap().is_none());

        let needed = rsb.needed().unwrap();
        assert_eq!(needed, Needed::Size(3));

        if let Needed::Size(bytes) = needed {
            rsb.fill(bytes).unwrap();
            assert_eq!(rsb.apply(comb).unwrap(), Some([0, 1, 2].as_ref()));
        }
    }

    #[test]
    fn fill_apply() {
        use nom::be_u8;
        let mut rsb = subject_with_default_data();

        fn comb(input: &[u8]) -> IResult<&[u8], &[u8]> {
            tag!(input, [0, 1, 2])
        }

        //TODO: test None scenario
        rsb.fill(10).unwrap();
        assert_eq!(rsb.fill_apply(comb).unwrap(), Some([0, 1, 2].as_ref()));
        assert_eq!(rsb.fill_apply(be_u8).unwrap(), Some(3));
    }

    #[test]
    fn fill_over_buf() {
        let mut rsb = subject_with_default_data();
        let error = rsb.fill(super::DEFAULT_BUF_SIZE + 1).unwrap_err();
        if let FillError::BufferOverflow(needed, capacity) = error {
            assert_eq!(needed, super::DEFAULT_BUF_SIZE + 1);
            assert_eq!(capacity, super::DEFAULT_BUF_SIZE);
        } else {
            panic!("was expecing BufferOverflow error");
        }
    }
}
