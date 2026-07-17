//! IMAP COMPRESS extension specified in [RFC4978](https://www.rfc-editor.org/rfc/rfc4978.html).

use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project::pin_project;

use crate::Connection;
use crate::client::Session;
use crate::error::Result;
use crate::imap_stream::ImapStream;
use crate::types::IdGenerator;

#[cfg(feature = "runtime-async-std")]
use async_std::io::{IoSlice, IoSliceMut, Read, Write};
#[cfg(feature = "runtime-async-std")]
use futures::io::BufReader;
#[cfg(feature = "runtime-tokio")]
use tokio::io::{AsyncRead as Read, AsyncWrite as Write, BufReader, ReadBuf};

#[cfg(feature = "runtime-tokio")]
use async_compression::tokio::bufread::DeflateDecoder;
#[cfg(feature = "runtime-tokio")]
use async_compression::tokio::write::DeflateEncoder;

#[cfg(feature = "runtime-async-std")]
use async_compression::futures::bufread::DeflateDecoder;
#[cfg(feature = "runtime-async-std")]
use async_compression::futures::write::DeflateEncoder;

/// Network stream compressed with DEFLATE.
#[derive(Debug)]
#[pin_project]
pub struct DeflateStream<T: Read + Write + Unpin + fmt::Debug> {
    #[pin]
    inner: DeflateDecoder<BufReader<DeflateEncoder<T>>>,
}

impl<T: Read + Write + Unpin + fmt::Debug> DeflateStream<T> {
    pub(crate) fn new(stream: T) -> Self {
        let stream = DeflateEncoder::new(stream);
        let stream = BufReader::new(stream);
        let stream = DeflateDecoder::new(stream);
        Self { inner: stream }
    }

    /// Gets a reference to the underlying stream.
    pub fn get_ref(&self) -> &T {
        self.inner.get_ref().get_ref().get_ref()
    }

    /// Gets a mutable reference to the underlying stream.
    pub fn get_mut(&mut self) -> &mut T {
        self.inner.get_mut().get_mut().get_mut()
    }

    /// Consumes `DeflateStream` and returns underlying stream.
    pub fn into_inner(self) -> T {
        self.inner.into_inner().into_inner().into_inner()
    }
}

#[cfg(feature = "runtime-tokio")]
impl<T: Read + Write + Unpin + fmt::Debug> Read for DeflateStream<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.project().inner.poll_read(cx, buf)
    }
}

#[cfg(feature = "runtime-async-std")]
impl<T: Read + Write + Unpin + fmt::Debug> Read for DeflateStream<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<async_std::io::Result<usize>> {
        self.project().inner.poll_read(cx, buf)
    }

    fn poll_read_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<async_std::io::Result<usize>> {
        self.project().inner.poll_read_vectored(cx, bufs)
    }
}

#[cfg(feature = "runtime-tokio")]
impl<T: Read + Write + Unpin + fmt::Debug> Write for DeflateStream<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.project().inner.get_pin_mut().poll_write(cx, buf)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.project().inner.poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        self.project().inner.poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

#[cfg(feature = "runtime-async-std")]
impl<T: Read + Write + Unpin + fmt::Debug> Write for DeflateStream<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<async_std::io::Result<usize>> {
        self.project().inner.as_mut().poll_write(cx, buf)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<async_std::io::Result<()>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<async_std::io::Result<()>> {
        self.project().inner.poll_close(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<async_std::io::Result<usize>> {
        self.project().inner.poll_write_vectored(cx, bufs)
    }
}

impl<T: Read + Write + Unpin + fmt::Debug + Send> Session<T> {
    /// Runs `COMPRESS DEFLATE` command.
    pub async fn compress<F, S>(self, f: F) -> Result<Session<S>>
    where
        S: Read + Write + Unpin + fmt::Debug,
        F: FnOnce(DeflateStream<T>) -> S,
    {
        let Self {
            mut conn,
            unsolicited_responses_tx,
            unsolicited_responses,
        } = self;
        conn.run_command_and_check_ok("COMPRESS DEFLATE", Some(unsolicited_responses_tx.clone()))
            .await?;

        let stream = conn.into_inner();
        let deflate_stream = DeflateStream::new(stream);
        let stream = ImapStream::new(f(deflate_stream));
        let conn = Connection {
            stream,
            request_ids: IdGenerator::new(),
        };
        let session = Session {
            conn,
            unsolicited_responses_tx,
            unsolicited_responses,
        };
        Ok(session)
    }
}
