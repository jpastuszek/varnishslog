/* automatically generated by rust-bindgen */

#![allow(dead_code, non_camel_case_types, non_upper_case_globals, non_snake_case)]

pub const VSL_CLASS: &'static [u8; 4usize] = b"Log\0";
pub const VSL_SEGMENTS: u32 = 8;
pub const VSL_CLIENTMARKER: u32 = 1073741824;
pub const VSL_BACKENDMARKER: u32 = 2147483648;
pub const VSL_IDENTMASK: i64 = -3221225473;
pub const VSL_LENMASK: u32 = 65535;
pub const SLT__MAX: u32 = 256;
pub const NODEF_NOTICE: &'static [u8; 46usize] =
    b"NB: This log record is masked by default.\\n\\n\0";
pub const SLT_F_UNUSED: u32 = 1;
pub const SLT_F_BINARY: u32 = 2;
#[repr(u32)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum VSL_tag_e {
    SLT__Bogus = 0,
    SLT_Debug = 1,
    SLT_Error = 2,
    SLT_CLI = 3,
    SLT_SessOpen = 4,
    SLT_SessClose = 5,
    SLT_BackendOpen = 6,
    SLT_BackendReuse = 7,
    SLT_BackendClose = 8,
    SLT_HttpGarbage = 9,
    SLT_Proxy = 10,
    SLT_ProxyGarbage = 11,
    SLT_Backend = 12,
    SLT_Length = 13,
    SLT_FetchError = 14,
    SLT_ReqMethod = 15,
    SLT_ReqURL = 16,
    SLT_ReqProtocol = 17,
    SLT_ReqStatus = 18,
    SLT_ReqReason = 19,
    SLT_ReqHeader = 20,
    SLT_ReqUnset = 21,
    SLT_ReqLost = 22,
    SLT_RespMethod = 23,
    SLT_RespURL = 24,
    SLT_RespProtocol = 25,
    SLT_RespStatus = 26,
    SLT_RespReason = 27,
    SLT_RespHeader = 28,
    SLT_RespUnset = 29,
    SLT_RespLost = 30,
    SLT_BereqMethod = 31,
    SLT_BereqURL = 32,
    SLT_BereqProtocol = 33,
    SLT_BereqStatus = 34,
    SLT_BereqReason = 35,
    SLT_BereqHeader = 36,
    SLT_BereqUnset = 37,
    SLT_BereqLost = 38,
    SLT_BerespMethod = 39,
    SLT_BerespURL = 40,
    SLT_BerespProtocol = 41,
    SLT_BerespStatus = 42,
    SLT_BerespReason = 43,
    SLT_BerespHeader = 44,
    SLT_BerespUnset = 45,
    SLT_BerespLost = 46,
    SLT_ObjMethod = 47,
    SLT_ObjURL = 48,
    SLT_ObjProtocol = 49,
    SLT_ObjStatus = 50,
    SLT_ObjReason = 51,
    SLT_ObjHeader = 52,
    SLT_ObjUnset = 53,
    SLT_ObjLost = 54,
    SLT_BogoHeader = 55,
    SLT_LostHeader = 56,
    SLT_TTL = 57,
    SLT_Fetch_Body = 58,
    SLT_VCL_acl = 59,
    SLT_VCL_call = 60,
    SLT_VCL_trace = 61,
    SLT_VCL_return = 62,
    SLT_ReqStart = 63,
    SLT_Hit = 64,
    SLT_HitPass = 65,
    SLT_ExpBan = 66,
    SLT_ExpKill = 67,
    SLT_WorkThread = 68,
    SLT_ESI_xmlerror = 69,
    SLT_Hash = 70,
    SLT_Backend_health = 71,
    SLT_VCL_Log = 72,
    SLT_VCL_Error = 73,
    SLT_Gzip = 74,
    SLT_Link = 75,
    SLT_Begin = 76,
    SLT_End = 77,
    SLT_VSL = 78,
    SLT_Storage = 79,
    SLT_Timestamp = 80,
    SLT_ReqAcct = 81,
    SLT_PipeAcct = 82,
    SLT_BereqAcct = 83,
    SLT_VfpAcct = 84,
    SLT_Witness = 85,
    SLT_BackendStart = 86,
    SLT__Reserved = 254,
    SLT__Batch = 255,
}
