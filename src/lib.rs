//! # Async IMAP
//!
//! This crate lets you connect to and interact with servers
//! that implement the IMAP protocol ([RFC 3501](https://tools.ietf.org/html/rfc3501) and extensions).
//! After authenticating with the server,
//! IMAP lets you list, fetch, and search for e-mails,
//! as well as monitor mailboxes for changes.
//!
//! ## Connecting
//!
//! Connect to the server, for example using TLS connection on port 993
//! or plain TCP connection on port 143 if you plan to use STARTTLS.
//! can be used.
//! Pass the stream to [`Client::new()`].
//! This gives you an unauthenticated [`Client`].
//!
//! Then read the server greeting:
//! ```ignore
//! let _greeting = client
//!     .read_response().await?
//!     .expect("unexpected end of stream, expected greeting");
//! ```
//!
//! ## STARTTLS
//!
//! If you connected on a non-TLS port, upgrade the connection using STARTTLS:
//! ```ignore
//! client.run_command_and_check_ok("STARTTLS", None).await?;
//! let stream = client.into_inner();
//! ```
//! Convert this stream into a TLS stream using a library
//! such as [`async-native-tls`](https://crates.io/crates/async-native-tls)
//! or [Rustls](`https://crates.io/crates/rustls`).
//! Once you have a TLS stream, wrap it back into a [`Client`]:
//! ```ignore
//! let client = Client::new(tls_stream);
//! ```
//! Note that there is no server greeting after STARTTLS.
//!
//! ## Authentication and session usage
//!
//! Once you have an established connection,
//! authenticate using [`Client::login`] or [`Client::authenticate`]
//! to perform username/password or challenge/response authentication respectively.
//! This in turn gives you an authenticated
//! [`Session`], which lets you access the mailboxes at the server.
//! For example:
//! ```ignore
//! let mut session = client
//!     .login("alice@example.org", "password").await
//!     .map_err(|(err, _client)| err)?;
//! session.select("INBOX").await?;
//!
//! // Fetch message number 1 in this mailbox, along with its RFC 822 field.
//! // RFC 822 dictates the format of the body of e-mails.
//! let messages_stream = imap_session.fetch("1", "RFC822").await?;
//! let messages: Vec<_> = messages_stream.try_collect().await?;
//! let message = messages.first().expect("found no messages in the INBOX");
//!
//! // Extract the message body.
//! let body = message.body().expect("message did not have a body!");
//! let body = std::str::from_utf8(body)
//!     .expect("message was not valid utf-8")
//!     .to_string();
//!
//! session.logout().await?;
//! ```
//!
//! The documentation within this crate borrows heavily from the various RFCs,
//! but should not be considered a complete reference.
//! If anything is unclear,
//! follow the links to the RFCs embedded in the documentation
//! for the various types and methods and read the raw text there!
//!
//! See the `examples/` directory for usage examples.
#![warn(missing_docs)]
#![deny(rust_2018_idioms, unsafe_code)]

#[cfg(not(any(feature = "runtime-tokio", feature = "runtime-async-std")))]
compile_error!("one of 'runtime-async-std' or 'runtime-tokio' features must be enabled");

#[cfg(all(feature = "runtime-tokio", feature = "runtime-async-std"))]
compile_error!("only one of 'runtime-async-std' or 'runtime-tokio' features must be enabled");
#[macro_use]
extern crate pin_utils;

// Reexport imap_proto for easier access.
pub use imap_proto;

mod authenticator;
mod client;
pub mod error;
pub mod extensions;
mod imap_stream;
pub use imap_stream::{LiteralAwareResponse, LiteralPrefix};
mod parse;
pub mod types;

#[cfg(feature = "compress")]
pub use crate::extensions::compress::DeflateStream;

pub use crate::authenticator::Authenticator;
pub use crate::client::*;

#[cfg(test)]
mod mock_stream;
