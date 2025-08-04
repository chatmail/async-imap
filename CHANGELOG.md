# Changelog 

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.11.1] - 2025-08-04

- Close IMAP stream after an error.

## [0.11.0] - 2025-07-21

- Breaking change: `read_response()` returns `Result<Option<_>>` instead of `Option<Result<_>>`.
- Add `get_ref()` and `get_mut()` APIs to get references to underlying stream.

## [0.10.4] - 2025-04-07

- Remove unused `once_cell` dependency.
- Update base64 from 0.21 to 0.22.1.
- Use write_all() to write space after a tag.

## [0.10.3] - 2025-03-10

- Don't panic if the server returns 0 as the quota limit. [#116](https://github.com/chatmail/async-imap/pull/116)

## [0.10.2] - 2024-10-14

- Implement IMAP COMPRESS

## [0.10.1] - 2024-09-09

- Correctly parse unsolicited APPEND responses

## [0.10.0] - 2024-08-30

- Support optional arguments for APPEND command. [#107](https://github.com/async-email/async-imap/pull/107)
- Print unparseable input as a string. [#106](https://github.com/async-email/async-imap/pull/106)

## [0.9.7] - 2023-01-30

- Fix parsing of METADATA results with NIL values. [#103](https://github.com/async-email/async-imap/pull/103)

## [0.9.6] - 2023-01-27

### Changes

- Add GETMETADATA command. [#102](https://github.com/async-email/async-imap/pull/102)

## [0.9.5] - 2023-12-11

### Fixes

- Reset IDLE timeout when keepalive is received

## [0.9.4] - 2023-11-15

### Fixes

- Do not ignore EOF errors when reading FETCH responses. [#94](https://github.com/async-email/async-imap/pull/94)

## [0.9.3] - 2023-10-20

### Dependencies

- Update `async-channel` to version 2.

## [0.9.2] - 2023-10-20

### Fixes

- Fix STATUS command response parsing. [#92](https://github.com/async-email/async-imap/pull/92)

## [0.9.1] - 2023-08-28

### Fixes

- Replace byte pool with bytes to fix memory leak. [#79](https://github.com/async-email/async-imap/pull/79)

### Documentation

- Remove outdated reference to `rustls.rs` example

### Miscellaneous Tasks

- Fix beta (1.73) clippy

## [0.9.0] - 2023-06-13

### Fixes

- Switch from `ouroboros` to `self_cell`. [#86](https://github.com/async-email/async-imap/pull/86)

  `ouroboros` is [no longer maintained](https://github.com/joshua-maros/ouroboros/issues/88) and has a [RUSTSEC-2023-0042 advisory](https://rustsec.org/advisories/RUSTSEC-2023-0042) suggesting switch to [`self_cell`](https://github.com/Voultapher/self_cell).

## [0.8.0] - 2023-04-17

### Changed

- Remove `async-native-tls` dependency. TLS streams should be created by the library users as documented in `lib.rs`. #68
- Do not generate artificial "broken pipe" errors when attempting to send a request
  after reaching EOF on the response stream. #73
- Do not attempt to track if the stream is closed or not.
  `ImapStream` can wrap any kinds of streams, including streams which may become open again later,
  like files which can be rewinded after reaching end of file or appended to.

### Fixes

- Update byte-pool to 0.2.4 to fix `ensure_capacity()`.
  Previously this bug could have resulted in an attempt to read into a buffer of zero size
  and erronous detection of the end of stream.

## [0.7.0] - 2023-04-03

### Added

- Added changelog.
- Add `ID` extension support.

### Fixed

- Fix parsing of long responses by correctly setting the `decode_needs` variable. [#74](https://github.com/async-email/async-imap/pull/74).

### Changed

- Make `async-native-tls` dependency optional.
- Update to `base64` 0.21.

## [0.6.0] - 2022-06-27

### Added

- Add `QUOTA` support.
- Add `CONDSTORE` support: add `Session.select_condstore()`.
- Full tokio support.

### Fixed

- Do not ignore `SELECT` command errors.

### Changed

- Replace `rental` with `ouroboros`.
- Replace `lazy_static` with `once_cell`.

## [0.5.0] - 2021-03-23

### Changed

- Update async-std, stop-token, migrate to stable channels.

## [0.4.1] - 2020-10-14

### Fixed

- Fix response handling in authentication. [#36](https://github.com/async-email/async-imap/pull/36)

### Changed

- Update `base64` to 0.13.

## [0.3.3] - 2020-08-04

### Fixed

- [Refactor buffering, fixing infinite loop](https://github.com/async-email/async-imap/commit/9a7097dd446784257ad9a546c6f77188e983acd6). [#33](https://github.com/async-email/async-imap/pull/33)
- Updated `byte-pool` from 0.2.1 to 0.2.2 due to important bugfix.

### Changed

- [Do not try to send data when the stream is closed](https://github.com/async-email/async-imap/commit/68f21e5921a002e172d5ffadc45c62bf882a68d6).

## [0.3.2] - 2020-06-11

### Changed

- Bump `base64` to 0.12.

## [0.3.1] - 2020-05-24

### Fixed

- Ignore unsolicited responses if the channel is full.

## [0.3.0] - 2020-05-23

### Added

- Make streams and futures `Send`.

## [0.2.0] - 2020-01-04

### Added

- Added tracing logs for traffic.

### Fixed

- Correctly decode incomplete reads of long IMAP messages.
- Avoid infinite loop in decoding.
- Correct response value for manual interrupt in IDLE.
- Handle OAuth responses without challenge.
- Don't crash if we can't read the greeting from the server.
- Improved handling of unsolicited responses and errors.

### Changed

- Use thiserror for error handling.

## [0.1.1] - 2019-11-16

### Fixed

- Ensure there is enough space available when encoding.

## 0.1.0 - 2019-11-11

[0.11.1]: https://github.com/async-email/async-imap/compare/v0.11.0...v0.11.1
[0.11.0]: https://github.com/async-email/async-imap/compare/v0.10.4...v0.11.0
[0.10.4]: https://github.com/async-email/async-imap/compare/v0.10.3...v0.10.4
[0.10.3]: https://github.com/async-email/async-imap/compare/v0.10.2...v0.10.3
[0.10.2]: https://github.com/async-email/async-imap/compare/v0.10.1...v0.10.2
[0.10.1]: https://github.com/async-email/async-imap/compare/v0.10.0...v0.10.1
[0.10.0]: https://github.com/async-email/async-imap/compare/v0.9.7...v0.10.0
[0.9.7]: https://github.com/async-email/async-imap/compare/v0.9.6...v0.9.7
[0.9.6]: https://github.com/async-email/async-imap/compare/v0.9.5...v0.9.6
[0.9.5]: https://github.com/async-email/async-imap/compare/v0.9.4...v0.9.5
[0.9.4]: https://github.com/async-email/async-imap/compare/v0.9.3...v0.9.4
[0.9.3]: https://github.com/async-email/async-imap/compare/v0.9.2...v0.9.3
[0.9.2]: https://github.com/async-email/async-imap/compare/v0.9.1...v0.9.2
[0.9.1]: https://github.com/async-email/async-imap/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/async-email/async-imap/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/async-email/async-imap/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/async-email/async-imap/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/async-email/async-imap/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/async-email/async-imap/compare/v0.4.1...v0.5.0
[0.4.1]: https://github.com/async-email/async-imap/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/async-email/async-imap/compare/v0.3.3...v0.4.0
[0.3.3]: https://github.com/async-email/async-imap/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/async-email/async-imap/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/async-email/async-imap/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/async-email/async-imap/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/async-email/async-imap/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/async-email/async-imap/compare/v0.1.0...v0.1.1
