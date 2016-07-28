use std::cell::Cell;
use std::io::Read;
use std::io;
use nom;

const DEFAULT_BUF_SIZE: usize = 8 * 1024;

pub trait StreamBuf<O> {
    fn fill(&mut self, count: usize) -> Result<(), io::Error>;
    fn recycle(&mut self);
    fn consume(&mut self, count: usize);
    fn data<'b>(&'b self) -> &'b[O];
    fn needed<'b, C, CO>(&'b self, combinator: C) -> Option<usize> where
        O: 'b, C: Fn(&'b [O]) -> nom::IResult<&'b [O], CO>;
    fn apply<'b, C, CO>(&'b self, combinator: C) -> Result<CO, ()> where
        O: 'b, C: Fn(&'b [O]) -> nom::IResult<&'b [O], CO>;
}

// Note: Need to use macro as this cannot be represented in the type system.
//       It would require generic combinator outpt parameter to have for<'r> life time
// TODO: Makie it smarter and able to handle errors and unknown amount of needed data
#[macro_export]
macro_rules! apply_stream (
    ($sb:expr, $comb:expr) => ({
        use stream_buf::StreamBuf;
        let needed = $sb.needed($comb).unwrap();
        $sb.fill(needed).unwrap();
        $sb.apply($comb)
    })
);

pub struct ReadStreamBuf<R: Read> {
    reader: R,
    buf: Vec<u8>,
    cap: usize,
    offset: Cell<usize>,
}

impl<R: Read> ReadStreamBuf<R> {
    pub fn new(reader: R) -> ReadStreamBuf<R> {
        ReadStreamBuf::with_capacity(reader, DEFAULT_BUF_SIZE)
    }

    pub fn with_capacity(reader: R, cap: usize) -> ReadStreamBuf<R> {
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

    fn needed<'b, C, CO>(&'b self, combinator: C) -> Option<usize> where
        C: Fn(&'b [u8]) -> nom::IResult<&'b [u8], CO> {
        let result = combinator(self.data());
        if result.is_incomplete() {
            match result.unwrap_inc() {
                nom::Needed::Size(needed) => return Some(needed),
                nom::Needed::Unknown => panic!("ReadStreamBuf does not know how much data to read for stream!"),
            }
        }
        None
    }

    fn apply<'b, C, CO>(&'b self, combinator: C) -> Result<CO, ()> where
        C: Fn(&'b [u8]) -> nom::IResult<&'b [u8], CO> {
        // TODO: error handling
        let data = self.data();
        let (left, out) = combinator(data).unwrap();
        let consumed = data.len() - left.len();
        // Need Cell to modify offset after parsing is done
        self.offset.set(self.offset.get() + consumed);
        return Ok(out)
    }
}

#[cfg(test)]
mod resd_stream_buf_tests {
    use super::StreamBuf;
    use super::ReadStreamBuf;
    use nom::IResult;
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
        fn comb(input: &[u8]) -> IResult<&[u8], &[u8]> {
            tag!(input, [0, 1, 2])
        }

        rsb.fill(3).unwrap();
        assert_eq!(rsb.apply(comb), Ok([0, 1, 2].as_ref()));
    }

    #[test]
    fn needed_with_apply() {
        let mut rsb = subject_with_default_data();

        fn comb<'a>(input: &'a[u8]) -> IResult<&'a[u8], &'a[u8]> {
            tag!(input, [0, 1, 2])
        }

        let needed = rsb.needed(comb);
        assert_eq!(needed, Some(3));

        rsb.fill(needed.unwrap()).unwrap();
        assert_eq!(rsb.apply(comb), Ok([0, 1, 2].as_ref()));
    }

    #[test]
    fn fill_and_apply() {
        use nom::be_u8;
        let mut rsb = subject_with_default_data();

        fn comb(input: &[u8]) -> IResult<&[u8], &[u8]> {
            tag!(input, [0, 1, 2])
        }

        assert_eq!(apply_stream!(rsb, comb), Ok([0, 1, 2].as_ref()));
        assert_eq!(apply_stream!(rsb, be_u8), Ok(3));
    }
}
