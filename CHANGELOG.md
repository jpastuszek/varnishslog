# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [7.0.0] - 2018-11-23
### Fixed
- Added workaround for spurious End tag before SessionClose
- Properly handling case when Varnish is changing PIPE handling into PASS handling as PIPE is not supported with HTTP/2 to HTTP/1.1 translation
### Added
- Support for PROXY protocol client and server address logging (Proxy VSL tag)
- Remote address information is now taken from session for root client access records
- Client access records now have session information when available
- Support for bad request logging when there is no request information available
- SessionRecord now has close_reason string
- Logging of VCL_Error, ProxyGarbage and HttpGarbage messages