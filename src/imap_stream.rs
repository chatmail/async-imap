use std::mem;
use std::pin::Pin;

#[cfg(feature = "runtime-async-std")]
use async_std::io::{Read, Write, WriteExt};
use bytes::BytesMut;
use futures_util::stream::Stream;
use futures_util::task::{Context, Poll};
use futures_util::{io, ready};
use nom::Needed;
#[cfg(feature = "runtime-tokio")]
use tokio::io::{AsyncRead as Read, AsyncWrite as Write, AsyncWriteExt};
#[cfg(feature = "runtime-tokio")]
use tokio_util::io::poll_read_buf;

use crate::types::{Request, ResponseData};

/// Wraps a stream, and parses incoming data as imap server messages. Writes outgoing data
/// as imap client messages.
#[derive(Debug)]
pub struct ImapStream<R: Read + Write> {
    /// The underlying stream
    pub(crate) inner: R,
    /// Number of bytes the next decode operation needs if known.
    /// If the buffer contains less than this, it is a waste of time to try to parse it.
    /// If unknown, set it to 1 so that decoding is attempted when any data is available.
    decode_needs: usize,
    /// The buffer.
    buffer: BytesMut,

    /// True if the stream should not return any more items.
    ///
    /// This is set when reading from a stream
    /// returns an error.
    /// Afterwards the stream returns only `None`
    /// and `poll_next()` does not read
    /// from the underlying stream.
    read_closed: bool,
}

impl<R: Read + Write + Unpin> ImapStream<R> {
    const CAPACITY_INITIAL: usize = 1024 * 4;
    const CAPACITY_MAX: usize = 512 * 1024 * 1024; // 512 MiB

    /// Creates a new `ImapStream` based on the given `Read`er.
    pub fn new(inner: R) -> Self {
        ImapStream {
            inner,
            buffer: BytesMut::with_capacity(Self::CAPACITY_INITIAL),
            decode_needs: 1,
            read_closed: false,
        }
    }

    pub async fn encode(&mut self, msg: Request) -> Result<(), io::Error> {
        log::trace!(
            "encode: input: {:?}, {:?}",
            msg.0,
            std::str::from_utf8(&msg.1)
        );

        if let Some(tag) = msg.0 {
            self.inner.write_all(tag.as_bytes()).await?;
            self.inner.write_all(b" ").await?;
        }
        self.inner.write_all(&msg.1).await?;
        self.inner.write_all(b"\r\n").await?;

        Ok(())
    }

    /// Gets a reference to the underlying stream.
    pub fn get_ref(&self) -> &R {
        &self.inner
    }

    /// Gets a mutable reference to the underlying stream.
    pub fn get_mut(&mut self) -> &mut R {
        &mut self.inner
    }

    /// Returns underlying stream.
    pub fn into_inner(self) -> R {
        self.inner
    }

    /// Flushes the underlying stream.
    pub async fn flush(&mut self) -> Result<(), io::Error> {
        self.inner.flush().await
    }

    pub fn as_mut(&mut self) -> &mut R {
        &mut self.inner
    }

    /// Attempts to decode a single response from the buffer.
    ///
    /// Returns `None` if the buffer does not contain enough data.
    fn decode(&mut self) -> io::Result<Option<ResponseData>> {
        if self.buffer.len() < self.decode_needs {
            // We know that there is not enough data to decode anything
            // from previous attempts.
            return Ok(None);
        }

        let block = mem::take(&mut self.buffer);

        let res = ResponseData::try_new_or_recover(block, |buf| {
            log::trace!("decode: input: {:?}", std::str::from_utf8(buf));
            match imap_proto::Response::parse(buf) {
                Ok((remaining, response)) => {
                    // TODO: figure out if we can use a minimum required size for a response.
                    self.decode_needs = 1;
                    // Would be better to use `buf.split_off()` instead of copying, but the main
                    // problem is, even if `buf` were `&mut`, `imap_proto::Response::parse()`
                    // borrowing the whole buffer immutably, so we can't pass it as `&mut` to
                    // `split_off()`. `parse()` should take an owned buffer and perform splitting on
                    // its own.
                    //
                    // Ok, reserve at least `CAPACITY_INITIAL` to avoid extra reallocations if the
                    // remainder is a partial next response.
                    if remaining.len() < Self::CAPACITY_INITIAL {
                        self.buffer.reserve(Self::CAPACITY_INITIAL);
                    }
                    self.buffer
                        .extend_from_slice(&buf[buf.len() - remaining.len()..]);
                    Ok(response)
                }
                Err(nom::Err::Incomplete(Needed::Size(min))) => {
                    log::trace!("decode: incomplete data, need minimum {min} bytes");
                    let received = buf.len();
                    if received == 0 {
                        return Err(Some(io::Error::other(
                            "decode: internal error: received == 0".to_string(),
                        )));
                    }
                    let decode_needs = received.saturating_add(usize::from(min));
                    match decode_needs > Self::CAPACITY_MAX {
                        true => Err(Some(io::Error::other(format!(
                            "decode: decode_needs ({decode_needs}) > CAPACITY_MAX ({})",
                            Self::CAPACITY_MAX,
                        )))),
                        false => {
                            self.decode_needs = decode_needs;
                            Err(None)
                        }
                    }
                }
                Err(nom::Err::Incomplete(_)) => {
                    log::trace!("decode: incomplete data, need unknown number of bytes");
                    self.decode_needs = buf.len() + 1;
                    Err(None)
                }
                Err(other) => Err(Some(io::Error::other(format!(
                    "{:?} during parsing of {:?}",
                    other,
                    String::from_utf8_lossy(buf)
                )))),
            }
        });
        match res {
            Ok(response) => Ok(Some(response)),
            Err((heads, err)) => {
                self.buffer = heads;
                match err {
                    Some(err) => Err(err),
                    None => Ok(None),
                }
            }
        }
    }

    fn do_poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<io::Result<ResponseData>>> {
        let this = &mut *self;
        if let Some(response) = this.decode()? {
            return Poll::Ready(Some(Ok(response)));
        }
        loop {
            this.buffer
                .reserve(this.decode_needs.saturating_sub(this.buffer.len()));
            if this.buffer.capacity() == this.buffer.len() {
                this.buffer.reserve(Self::CAPACITY_INITIAL);
            }

            #[cfg(feature = "runtime-async-std")]
            let num_bytes_read = {
                let start = this.buffer.len();
                let capacity = this.buffer.capacity();
                // Unfortunate, but async-std has no way to read into MaybeUninit
                this.buffer.resize(capacity, 0);
                let spare_capacity = &mut this.buffer[start..capacity];
                match Pin::new(&mut this.inner).poll_read(cx, spare_capacity) {
                    Poll::Ready(Ok(n)) => {
                        this.buffer.truncate(start + n);
                        Ok(n)
                    }
                    Poll::Ready(Err(e)) => {
                        this.buffer.truncate(start);
                        Err(e)
                    }
                    Poll::Pending => {
                        this.buffer.truncate(start);
                        return Poll::Pending;
                    }
                }?
            };

            #[cfg(feature = "runtime-tokio")]
            let num_bytes_read = {
                let start = this.buffer.len();
                ready!(poll_read_buf(
                    Pin::new(&mut this.inner),
                    cx,
                    &mut this.buffer
                ))?;
                this.buffer.len() - start
            };

            if num_bytes_read == 0 {
                if !this.buffer.is_empty() {
                    return Poll::Ready(Some(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "bytes remaining in stream",
                    ))));
                }
                return Poll::Ready(None);
            }
            if let Some(response) = this.decode()? {
                return Poll::Ready(Some(Ok(response)));
            }
        }
    }
}

impl<R: Read + Write + Unpin> Stream for ImapStream<R> {
    type Item = io::Result<ResponseData>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.read_closed {
            return Poll::Ready(None);
        }
        let res = match ready!(self.as_mut().do_poll_next(cx)) {
            None => None,
            Some(Err(err)) => {
                self.read_closed = true;
                Some(Err(err))
            }
            Some(Ok(item)) => Some(Ok(item)),
        };
        Poll::Ready(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use pin_project::pin_project;

    #[cfg(feature = "runtime-tokio")]
    type PollRead = Poll<tokio::io::Result<()>>;
    #[cfg(feature = "runtime-async-std")]
    type PollRead = Poll<async_std::io::Result<usize>>;

    #[cfg(feature = "runtime-tokio")]
    type ReadBuf<'a> = tokio::io::ReadBuf<'a>;
    #[cfg(feature = "runtime-async-std")]
    type ReadBuf<'a> = [u8];

    /// Wrapper for a stream that injects `PollRead` on a first read.
    ///
    /// Writes are discarded.
    #[pin_project]
    struct ReadInjectingStream {
        #[pin]
        inner: &'static [u8],
        injection: Option<PollRead>,
    }

    impl ReadInjectingStream {
        fn new(buf: &'static [u8], injection: PollRead) -> Self {
            Self {
                inner: buf,
                injection: Some(injection),
            }
        }
    }

    impl Read for ReadInjectingStream {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> PollRead {
            let this = self.project();
            match this.injection.take() {
                Some(Poll::Pending) => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Some(i) => i,
                None => this.inner.poll_read(cx, buf),
            }
        }
    }

    #[cfg(feature = "runtime-tokio")]
    impl Write for ReadInjectingStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<tokio::io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<tokio::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<tokio::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[cfg(feature = "runtime-async-std")]
    impl Write for ReadInjectingStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<async_std::io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<async_std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<async_std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// Tests that stream returns `None` after
    /// a single error of the underlying stream.
    ///
    /// This is need to prevent accidental
    /// reading from a network stream
    /// after a temporary error such as a timeout
    /// or returning an inifinite stream of errors.
    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn test_imap_stream_error() {
        use futures_util::StreamExt;

        let mock_stream = ReadInjectingStream::new(
            b"* OK\r\n",
            Poll::Ready(Err(std::io::Error::other("Failure"))),
        );
        let mut imap_stream = ImapStream::new(mock_stream);

        // First call is an error because underlying stream fails.
        assert!(imap_stream.next().await.unwrap().is_err());

        // IMAP stream should end even though underlying stream fails only once.
        assert!(imap_stream.next().await.is_none());
    }

    /// Tests that an underlying stream returning `Poll::Pending` doesn't lead to received data
    /// corruption which is possible e.g. if `ImapStream::do_poll_next()` forgets to truncate back
    /// its buffer.
    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn test_imap_stream_read_pending() {
        use futures_util::StreamExt;

        let mock_stream = ReadInjectingStream::new(b"* OK\r\n", Poll::Pending);
        let mut imap_stream = ImapStream::new(mock_stream);
        let Some(Ok(_response_data)) = imap_stream.next().await else {
            unreachable!();
        };
        assert!(imap_stream.next().await.is_none());
    }
}
