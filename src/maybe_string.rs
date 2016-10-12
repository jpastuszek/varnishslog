use std::str::from_utf8;
use std::fmt::{self, Debug, Display};
use std::ops::Deref;

#[derive(PartialEq)]
pub struct MaybeStr([u8]);

impl MaybeStr {
    pub fn from_bytes(bytes: &[u8]) -> &MaybeStr {
        //TODO: can this be done without unsafe?
        // Deref cannot be implemented for [u8] as we don't own it
        // We can't construct unsized struct so we need to be able to create pointer stright away
        unsafe { &*((bytes as *const [u8]) as *const MaybeStr)}
    }

    pub fn as_bytes(&self) -> &[u8] {
        self
    }

    pub fn to_maybe_string(&self) -> MaybeString {
        MaybeString(self.as_bytes().to_owned())
    }

    pub fn to_lossy_string(&self) -> String {
        String::from_utf8_lossy(self.as_bytes()).into_owned()
    }
}

impl Debug for MaybeStr {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        if let Ok(string) = from_utf8(self.as_bytes()) {
            write!(f, "{:?}", string)
        } else {
            write!(f, "{:?}<non-UTF-8 data: {:?}>", String::from_utf8_lossy(&self.as_bytes()), &self.as_bytes())
        }
    }
}

impl Display for MaybeStr {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", String::from_utf8_lossy(&self.as_bytes()))
    }
}

impl Deref for MaybeStr {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

#[derive(PartialEq)]
pub struct MaybeString(pub Vec<u8>);

impl MaybeString {
    pub fn as_bytes(&self) -> &[u8] {
        self
    }

    pub fn as_maybe_str(&self) -> &MaybeStr {
        self
    }

    pub fn into_lossy_string(self) -> String {
        match String::from_utf8(self.0) {
            Ok(string) => string,
            Err(err) => {
                String::from_utf8_lossy(err.into_bytes().as_slice()).into_owned()
            }
        }
    }
}

impl Deref for MaybeString {
    type Target = MaybeStr;

    fn deref(&self) -> &MaybeStr {
        MaybeStr::from_bytes(self.0.as_slice())
    }
}

impl Debug for MaybeString {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{:?}", self.as_maybe_str())
    }
}

impl Display for MaybeString {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.as_maybe_str())
    }
}
