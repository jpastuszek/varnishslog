// From Varnish source code:
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
 *
 * VSL_CLIENT(ptr)
 *   Non-zero if this is a client transaction
 *
 * VSL_BACKEND(ptr)
 *   Non-zero if this is a backend transaction
 */

use std::mem;

use nom::{self, le_u32};

use super::{
    VslRecord,
    VslRecordHeader,
    Marker,
    VslRecordTag,
};

const VSL_LENOFFSET: u32 = 24;
const VSL_LENMASK: u32 = 0xffff;
const VSL_IDENTOFFSET: u8 = 30;
const VSL_IDENTMASK: u32 = !(0b0000_0011 << VSL_IDENTOFFSET);

named!(pub binary_vsl_tag<&[u8], Option<&[u8]> >, opt!(complete!(tag!(b"VSL\0"))));

fn vsl_record_header(input: &[u8]) -> nom::IResult<&[u8], VslRecordHeader, u32> {
    chain!(
        input, r1: le_u32 ~ r2: le_u32,
        || VslRecordHeader {
            tag: (r1 >> VSL_LENOFFSET) as u8,
            len: (r1 & VSL_LENMASK) as u16,
            marker: Marker::from_bits_truncate(((r2 & !VSL_IDENTMASK) >> VSL_IDENTOFFSET) as u8),
            ident: r2 & VSL_IDENTMASK,
        })
}

fn to_vsl_record_tag(num: u8) -> VslRecordTag {
    let num = num as u32;

    // Warning: we need to make sure that num is an existing VslRecordTag variant or program will crash!
    // TODO: there needs to be a better way than this!
    if num > VslRecordTag::SLT_VCL_use as u32 && num < VslRecordTag::SLT__Reserved as u32 {
        return VslRecordTag::SLT__Bogus
    }

    unsafe { mem::transmute(num as u32) }
}

pub fn vsl_record_v4(input: &[u8]) -> nom::IResult<&[u8], VslRecord, u32> {
    chain!(
        input,
        header: vsl_record_header ~
        data: take!(header.len - 1) ~ take!(1) ~ take!((4 - header.len % 4) % 4),
        || VslRecord {
            tag: to_vsl_record_tag(header.tag),
            marker: header.marker,
            ident: header.ident,
            data: data
        })
}
