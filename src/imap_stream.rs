use std::fmt;
use std::num::NonZeroUsize;
use std::pin::Pin;

#[cfg(feature = "runtime-async-std")]
use async_std::io::{Read, Write, WriteExt};
use bytes::BytesMut;
use futures_util::stream::Stream;
use futures_util::task::{Context, Poll};
use futures_util::{future::poll_fn, io, ready};
use nom::Needed;
#[cfg(feature = "runtime-tokio")]
use tokio::io::{AsyncRead as Read, AsyncWrite as Write, AsyncWriteExt};

use crate::types::{Request, ResponseData};

/// One parsed IMAP response or the retained prefix of a noncompliant oversized literal.
#[derive(Debug)]
pub enum LiteralAwareResponse {
    /// A complete parsed IMAP response.
    Parsed(ResponseData),
    /// A body prefix retained from a server-declared literal larger than the configured limit.
    LiteralPrefix(LiteralPrefix),
}

/// The bounded body prefix retained from an oversized server-declared literal.
#[derive(Debug, PartialEq, Eq)]
pub struct LiteralPrefix {
    declared_size: usize,
    data: Vec<u8>,
}

impl LiteralPrefix {
    /// Returns the literal size declared by the server.
    pub fn declared_size(&self) -> usize {
        self.declared_size
    }

    /// Returns exactly the retained prefix bytes.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Consumes the outcome and returns exactly the retained prefix bytes.
    pub fn into_data(self) -> Vec<u8> {
        self.data
    }
}

enum DecodeOutcome {
    Parsed(ResponseData),
    Incomplete,
    OversizedLiteral(OversizedLiteralBoundary),
}

#[derive(Clone, Copy)]
struct OversizedLiteralBoundary {
    literal_start: usize,
    declared_size: usize,
}

enum RecoverableDecodeError {
    Incomplete,
    OversizedLiteral(OversizedLiteralBoundary),
    Invalid(io::Error),
}

/// Wraps a stream, and parses incoming data as imap server messages. Writes outgoing data
/// as imap client messages.
#[derive(Debug)]
pub struct ImapStream<R: Read + Write> {
    /// The underlying stream
    pub(crate) inner: R,
    /// Number of bytes the next decode operation needs if known.
    /// If the buffer contains less than this, it is a waste of time to try to parse it.
    /// If unknown, set it to 0, so decoding is always attempted.
    decode_needs: usize,
    /// The buffer.
    buffer: Buffer,

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
    /// Creates a new `ImapStream` based on the given `Read`er.
    pub fn new(inner: R) -> Self {
        Self::new_with_max_response_size(
            inner,
            NonZeroUsize::new(Buffer::DEFAULT_MAX_CAPACITY)
                .expect("default IMAP response limit is nonzero"),
        )
    }

    /// Creates an `ImapStream` whose response buffer cannot grow past `max_response_size`.
    pub fn new_with_max_response_size(inner: R, max_response_size: NonZeroUsize) -> Self {
        ImapStream {
            inner,
            buffer: Buffer::new_with_max_response_size(max_response_size),
            decode_needs: 0,
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
    fn decode(&mut self, literal_prefix_limit: Option<NonZeroUsize>) -> io::Result<DecodeOutcome> {
        if self.buffer.used() < self.decode_needs {
            // We know that there is not enough data to decode anything
            // from previous attempts.
            return Ok(DecodeOutcome::Incomplete);
        }

        let block = self.buffer.take_block();
        // Be aware, now self.buffer is invalid until block is returned or reset!

        let res = ResponseData::try_new_or_recover(block, |buf| {
            let buf = &buf[..self.buffer.used()];
            log::trace!("decode: input: {:?}", std::str::from_utf8(buf));
            match imap_proto::parser::parse_response(buf) {
                Ok((remaining, response)) => {
                    // TODO: figure out if we can use a minimum required size for a response.
                    self.decode_needs = 0;
                    self.buffer.reset_with_data(remaining);
                    Ok(response)
                }
                Err(nom::Err::Incomplete(Needed::Size(min))) => {
                    log::trace!("decode: incomplete data, need minimum {min} bytes");
                    if let Some(limit) = literal_prefix_limit
                        && let Some(boundary) =
                            oversized_literal_boundary(buf, usize::from(min), limit)
                    {
                        self.decode_needs = 0;
                        return Err(RecoverableDecodeError::OversizedLiteral(boundary));
                    }
                    self.decode_needs = self.buffer.used() + usize::from(min);
                    Err(RecoverableDecodeError::Incomplete)
                }
                Err(nom::Err::Incomplete(_)) => {
                    log::trace!("decode: incomplete data, need unknown number of bytes");
                    self.decode_needs = 0;
                    Err(RecoverableDecodeError::Incomplete)
                }
                Err(other) => {
                    self.decode_needs = 0;
                    Err(RecoverableDecodeError::Invalid(io::Error::other(format!(
                        "{:?} during parsing of {:?}",
                        other,
                        String::from_utf8_lossy(buf)
                    ))))
                }
            }
        });
        match res {
            Ok(response) => Ok(DecodeOutcome::Parsed(response)),
            Err((heads, err)) => {
                self.buffer.return_block(heads);
                match err {
                    RecoverableDecodeError::Incomplete => Ok(DecodeOutcome::Incomplete),
                    RecoverableDecodeError::OversizedLiteral(boundary) => {
                        Ok(DecodeOutcome::OversizedLiteral(boundary))
                    }
                    RecoverableDecodeError::Invalid(error) => Err(error),
                }
            }
        }
    }

    fn do_poll_next_with_literal_prefix(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        literal_prefix_limit: Option<NonZeroUsize>,
    ) -> Poll<Option<io::Result<LiteralAwareResponse>>> {
        let this = &mut *self;
        match this.decode(literal_prefix_limit)? {
            DecodeOutcome::Parsed(response) => {
                return Poll::Ready(Some(Ok(LiteralAwareResponse::Parsed(response))));
            }
            DecodeOutcome::OversizedLiteral(boundary) => {
                if let Some(response) =
                    this.read_literal_prefix(cx, boundary, literal_prefix_limit.expect("set"))?
                {
                    return Poll::Ready(Some(Ok(response)));
                }
                return Poll::Pending;
            }
            DecodeOutcome::Incomplete => {}
        }
        loop {
            this.buffer.ensure_capacity(this.decode_needs)?;
            let mut buf = this.buffer.free_as_mut_slice();
            if let Some(limit) = literal_prefix_limit {
                let allowed = buf.len().min(limit.get());
                buf = &mut buf[..allowed];
            }

            // The buffer should have at least one byte free
            // before we try reading into it
            // so we can treat 0 bytes read as EOF.
            // This is guaranteed by `ensure_capacity()` above
            // even if it is called with 0 as an argument.
            debug_assert!(!buf.is_empty());

            #[cfg(feature = "runtime-async-std")]
            let num_bytes_read = ready!(Pin::new(&mut this.inner).poll_read(cx, buf))?;

            #[cfg(feature = "runtime-tokio")]
            let num_bytes_read = {
                let buf = &mut tokio::io::ReadBuf::new(buf);
                let start = buf.filled().len();
                ready!(Pin::new(&mut this.inner).poll_read(cx, buf))?;
                buf.filled().len() - start
            };

            if num_bytes_read == 0 {
                if this.buffer.used() > 0 {
                    return Poll::Ready(Some(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "bytes remaining in stream",
                    ))));
                }
                return Poll::Ready(None);
            }
            this.buffer.extend_used(num_bytes_read);
            match this.decode(literal_prefix_limit)? {
                DecodeOutcome::Parsed(response) => {
                    return Poll::Ready(Some(Ok(LiteralAwareResponse::Parsed(response))));
                }
                DecodeOutcome::OversizedLiteral(boundary) => {
                    if let Some(response) =
                        this.read_literal_prefix(cx, boundary, literal_prefix_limit.expect("set"))?
                    {
                        return Poll::Ready(Some(Ok(response)));
                    }
                    return Poll::Pending;
                }
                DecodeOutcome::Incomplete => {}
            }
        }
    }

    fn read_literal_prefix(
        &mut self,
        cx: &mut Context<'_>,
        boundary: OversizedLiteralBoundary,
        literal_prefix_limit: NonZeroUsize,
    ) -> io::Result<Option<LiteralAwareResponse>> {
        let target = boundary
            .literal_start
            .checked_add(literal_prefix_limit.get())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "IMAP literal overflow"))?;
        self.buffer.ensure_capacity(target)?;
        while self.buffer.used() < target {
            let remaining = target - self.buffer.used();
            let buf = &mut self.buffer.free_as_mut_slice()[..remaining];

            #[cfg(feature = "runtime-async-std")]
            let num_bytes_read = match Pin::new(&mut self.inner).poll_read(cx, buf) {
                Poll::Pending => return Ok(None),
                Poll::Ready(result) => result?,
            };

            #[cfg(feature = "runtime-tokio")]
            let num_bytes_read = {
                let mut buf = tokio::io::ReadBuf::new(buf);
                match Pin::new(&mut self.inner).poll_read(cx, &mut buf) {
                    Poll::Pending => return Ok(None),
                    Poll::Ready(result) => result?,
                }
                buf.filled().len()
            };

            if num_bytes_read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "oversized IMAP literal ended before its retained prefix",
                ));
            }
            self.buffer.extend_used(num_bytes_read);
        }

        let block = self.buffer.take_block();
        let data = block[boundary.literal_start..target].to_vec();
        self.buffer.reset_with_data(&[]);
        self.read_closed = true;
        Ok(Some(LiteralAwareResponse::LiteralPrefix(LiteralPrefix {
            declared_size: boundary.declared_size,
            data,
        })))
    }

    pub async fn next_with_literal_prefix(
        &mut self,
        literal_prefix_limit: NonZeroUsize,
    ) -> Option<io::Result<LiteralAwareResponse>> {
        poll_fn(|cx| {
            if self.read_closed {
                return Poll::Ready(None);
            }
            let result = ready!(
                Pin::new(&mut *self)
                    .do_poll_next_with_literal_prefix(cx, Some(literal_prefix_limit))
            );
            if matches!(result, Some(Err(_))) {
                self.read_closed = true;
            }
            Poll::Ready(result)
        })
        .await
    }
}

fn oversized_literal_boundary(
    input: &[u8],
    needed: usize,
    literal_prefix_limit: NonZeroUsize,
) -> Option<OversizedLiteralBoundary> {
    for (start, byte) in input.iter().copied().enumerate() {
        if byte != b'{' {
            continue;
        }
        let digits_start = start + 1;
        let Some(relative_end) = input[digits_start..]
            .windows(3)
            .position(|window| window == b"}\r\n")
        else {
            continue;
        };
        let digits_end = digits_start + relative_end;
        let Ok(digits) = std::str::from_utf8(&input[digits_start..digits_end]) else {
            continue;
        };
        if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            continue;
        }
        let Ok(declared_size) = digits.parse::<usize>() else {
            continue;
        };
        let literal_start = digits_end + 3;
        let buffered_literal = input.len() - literal_start;
        if declared_size > literal_prefix_limit.get()
            && declared_size.checked_sub(buffered_literal) == Some(needed)
        {
            return Some(OversizedLiteralBoundary {
                literal_start,
                declared_size,
            });
        }
    }
    None
}

/// Abstraction around needed buffer management.
struct Buffer {
    /// The buffer itself.
    block: BytesMut,
    /// Offset where used bytes range ends.
    offset: usize,
    /// Largest response allocation permitted for this stream.
    max_capacity: NonZeroUsize,
}

impl Buffer {
    const BLOCK_SIZE: usize = 1024 * 4;
    const DEFAULT_MAX_CAPACITY: usize = 512 * 1024 * 1024; // 512 MiB
    #[cfg(test)]
    const MAX_CAPACITY: usize = Self::DEFAULT_MAX_CAPACITY;

    #[cfg(test)]
    fn new() -> Self {
        Self::new_with_max_response_size(
            NonZeroUsize::new(Self::DEFAULT_MAX_CAPACITY)
                .expect("default IMAP response limit is nonzero"),
        )
    }

    fn new_with_max_response_size(max_capacity: NonZeroUsize) -> Self {
        Self {
            block: BytesMut::zeroed(Self::BLOCK_SIZE.min(max_capacity.get())),
            offset: 0,
            max_capacity,
        }
    }

    /// Returns the number of bytes in the buffer containing data.
    fn used(&self) -> usize {
        self.offset
    }

    /// Returns the unused part of the buffer to which new data can be written.
    fn free_as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.block[self.offset..]
    }

    /// Indicate how many new bytes were written into the buffer.
    ///
    /// When new bytes are written into the slice returned by [`free_as_mut_slice`] this method
    /// should be called to extend the used portion of the buffer to include the new data.
    ///
    /// You can not write past the end of the buffer, so extending more then there is free
    /// space marks the entire buffer as used.
    ///
    /// [`free_as_mut_slice`]: Self::free_as_mut_slice
    // aka advance()?
    fn extend_used(&mut self, num_bytes: usize) {
        self.offset += num_bytes;
        if self.offset > self.block.len() {
            self.offset = self.block.len();
        }
    }

    /// Ensure the buffer has free capacity, optionally ensuring minimum buffer size.
    fn ensure_capacity(&mut self, required: usize) -> io::Result<()> {
        let free_bytes: usize = self.block.len() - self.offset;
        let extra_bytes_needed: usize = required.saturating_sub(self.block.len());
        if free_bytes == 0 || extra_bytes_needed > 0 {
            let increase = std::cmp::max(Buffer::BLOCK_SIZE, extra_bytes_needed);
            self.grow(increase)?;
        }

        // Assert that the buffer at least one free byte.
        debug_assert!(self.offset < self.block.len());

        // Assert that the buffer has at least the required capacity.
        debug_assert!(self.block.len() >= required);
        Ok(())
    }

    /// Grows the buffer, ensuring there are free bytes in the tail.
    ///
    /// The specified number of bytes is only a minimum.  The buffer could grow by more as
    /// it will always grow in multiples of [`BLOCK_SIZE`].
    ///
    /// If the size would be larger than the configured response limit an error is returned.
    ///
    /// [`BLOCK_SIZE`]: Self::BLOCK_SIZE
    fn grow(&mut self, num_bytes: usize) -> io::Result<()> {
        let min_size = self.block.len() + num_bytes;
        if min_size > self.max_capacity.get() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "incoming IMAP response too large",
            ));
        }
        let new_size = match min_size % Self::BLOCK_SIZE {
            0 => min_size,
            n => min_size + (Self::BLOCK_SIZE - n),
        }
        .min(self.max_capacity.get());
        self.block.resize(new_size, 0);
        Ok(())
    }

    /// Return the block backing the buffer.
    ///
    /// Next you *must* either return this block using [`return_block`] or call
    /// [`reset_with_data`].
    ///
    /// [`return_block`]: Self::return_block
    /// [`reset_with_data`]: Self::reset_with_data
    // TODO: Enforce this with typestate.
    fn take_block(&mut self) -> BytesMut {
        std::mem::replace(
            &mut self.block,
            BytesMut::zeroed(Self::BLOCK_SIZE.min(self.max_capacity.get())),
        )
    }

    /// Reset the buffer to be a new allocation with given data copied in.
    ///
    /// This allows the previously returned block from `get_block` to be used in and owned
    /// by the [ResponseData].
    ///
    /// This does not do any bounds checking to see if the new buffer would exceed the
    /// maximum size.  It will however ensure that there is at least some free space at the
    /// end of the buffer so that the next reading operation won't need to realloc right
    /// away.  This could be wasteful if the next action on the buffer is another decode
    /// rather than a read, but we don't know.
    fn reset_with_data(&mut self, data: &[u8]) {
        let min_size = data.len();
        let new_size = match min_size % Self::BLOCK_SIZE {
            0 => min_size + Self::BLOCK_SIZE,
            n => min_size + (Self::BLOCK_SIZE - n),
        }
        .min(self.max_capacity.get());
        self.block = BytesMut::zeroed(new_size);
        self.block[..data.len()].copy_from_slice(data);

        self.offset = data.len();
    }

    /// Return the block which backs this buffer.
    fn return_block(&mut self, block: BytesMut) {
        self.block = block;
    }
}

impl fmt::Debug for Buffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Buffer")
            .field("used", &self.used())
            .field("capacity", &self.block.capacity())
            .finish()
    }
}

impl<R: Read + Write + Unpin> Stream for ImapStream<R> {
    type Item = io::Result<ResponseData>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.read_closed {
            return Poll::Ready(None);
        }
        let res = match ready!(self.as_mut().do_poll_next_with_literal_prefix(cx, None)) {
            None => None,
            Some(Err(err)) => {
                self.read_closed = true;
                Some(Err(err))
            }
            Some(Ok(LiteralAwareResponse::Parsed(item))) => Some(Ok(item)),
            Some(Ok(LiteralAwareResponse::LiteralPrefix(_))) => {
                unreachable!("the Stream implementation does not cap literals")
            }
        };
        Poll::Ready(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use pin_project::pin_project;
    use std::io::Write as _;

    /// Wrapper for a stream that
    /// fails once on a first read.
    ///
    /// Writes are discarded.
    #[pin_project]
    struct FailingStream {
        #[pin]
        inner: &'static [u8],
        has_failed: bool,
    }

    impl FailingStream {
        fn new(buf: &'static [u8]) -> Self {
            Self {
                inner: buf,
                has_failed: false,
            }
        }
    }

    #[cfg(feature = "runtime-tokio")]
    impl Read for FailingStream {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<tokio::io::Result<()>> {
            let this = self.project();
            if !*this.has_failed {
                *this.has_failed = true;

                Poll::Ready(Err(std::io::Error::other("Failure")))
            } else {
                this.inner.poll_read(cx, buf)
            }
        }
    }

    #[cfg(feature = "runtime-async-std")]
    impl Read for FailingStream {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<async_std::io::Result<usize>> {
            let this = self.project();
            if !*this.has_failed {
                *this.has_failed = true;

                Poll::Ready(Err(std::io::Error::other("Failure")))
            } else {
                this.inner.poll_read(cx, buf)
            }
        }
    }

    #[cfg(feature = "runtime-tokio")]
    impl Write for FailingStream {
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
    impl Write for FailingStream {
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

        let mock_stream = FailingStream::new(b"* OK\r\n");
        let mut imap_stream = ImapStream::new(mock_stream);

        // First call is an error because underlying stream fails.
        assert!(imap_stream.next().await.unwrap().is_err());

        // IMAP stream should end even though underlying stream fails only once.
        assert!(imap_stream.next().await.is_none());
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn oversized_literal_is_rejected_before_buffer_allocation() {
        use futures_util::StreamExt;

        const RESPONSE_LIMIT: usize = 64 * 1024;
        let mock_stream =
            crate::mock_stream::MockStream::new(b"* 1 FETCH (BODY[] {33554432}\r\n".to_vec());
        let mut imap_stream = ImapStream::new_with_max_response_size(
            mock_stream,
            NonZeroUsize::new(RESPONSE_LIMIT).expect("test response limit is nonzero"),
        );

        let error = imap_stream
            .next()
            .await
            .expect("stream should return the literal-size error")
            .expect_err("literal declaration exceeds the configured limit");

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "incoming IMAP response too large");
        assert!(imap_stream.buffer.block.len() <= RESPONSE_LIMIT);
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn oversized_literal_returns_only_the_configured_prefix_without_draining() {
        const LITERAL_LIMIT: usize = 8 * 1024;
        const DECLARED_LITERAL_SIZE: usize = 10 * 1024;
        let declaration =
            format!("* 1 FETCH (UID 42 BODY[] {{{DECLARED_LITERAL_SIZE}}}\r\n").into_bytes();
        let mut response = declaration.clone();
        response.extend(std::iter::repeat_n(b'x', DECLARED_LITERAL_SIZE));
        response.extend_from_slice(b")\r\nA0001 OK done\r\n");
        let stream = crate::mock_stream::MockStream::new(response).with_max_read_size(1024);
        let mut imap_stream = ImapStream::new_with_max_response_size(
            stream,
            NonZeroUsize::new(LITERAL_LIMIT + 1024).expect("test response limit is nonzero"),
        );

        let response = imap_stream
            .next_with_literal_prefix(
                NonZeroUsize::new(LITERAL_LIMIT).expect("test literal limit is nonzero"),
            )
            .await
            .expect("literal-prefix read should succeed")
            .expect("stream should return one item");
        let LiteralAwareResponse::LiteralPrefix(literal) = response else {
            panic!("oversized declaration should return a prefix");
        };

        assert_eq!(literal.declared_size(), DECLARED_LITERAL_SIZE);
        assert_eq!(literal.data(), vec![b'x'; LITERAL_LIMIT]);
        assert!(
            imap_stream.buffer.block.len() <= LITERAL_LIMIT + 1024,
            "the stream must never allocate the declared literal size"
        );
        assert_eq!(
            imap_stream.get_ref().read_position(),
            declaration.len() + LITERAL_LIMIT,
            "the unread literal suffix and tagged completion must remain on the socket"
        );
        assert!(
            imap_stream
                .next_with_literal_prefix(
                    NonZeroUsize::new(LITERAL_LIMIT).expect("test literal limit is nonzero"),
                )
                .await
                .is_none(),
            "a capped stream must remain permanently read-closed"
        );
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn marker_like_literal_bytes_cannot_replace_the_parser_confirmed_boundary() {
        const LITERAL_LIMIT: usize = 8 * 1024;
        const DECLARED_LITERAL_SIZE: usize = 10 * 1024;
        let declaration =
            format!("* 1 FETCH (UID 42 BODY[] {{{DECLARED_LITERAL_SIZE}}}\r\n").into_bytes();
        // This fake declaration also satisfies `declared - buffered == Needed::Size`.
        // Selecting the earliest matching marker is what identifies the real parser boundary.
        let fake_marker = format!("{{{}}}\r\n", DECLARED_LITERAL_SIZE - 9).into_bytes();
        let mut body = fake_marker.clone();
        body.extend(std::iter::repeat_n(
            b'y',
            DECLARED_LITERAL_SIZE - fake_marker.len(),
        ));
        let mut response = declaration;
        response.extend_from_slice(&body);
        response.extend_from_slice(b")\r\nA0001 OK done\r\n");
        let stream = crate::mock_stream::MockStream::new(response).with_max_read_size(1024);
        let mut imap_stream = ImapStream::new_with_max_response_size(
            stream,
            NonZeroUsize::new(LITERAL_LIMIT + 1024).expect("test response limit is nonzero"),
        );

        let response = imap_stream
            .next_with_literal_prefix(
                NonZeroUsize::new(LITERAL_LIMIT).expect("test literal limit is nonzero"),
            )
            .await
            .expect("literal-prefix read should succeed")
            .expect("stream should return one item");
        let LiteralAwareResponse::LiteralPrefix(literal) = response else {
            panic!("oversized declaration should return a prefix");
        };

        assert_eq!(literal.declared_size(), DECLARED_LITERAL_SIZE);
        assert_eq!(literal.data(), &body[..LITERAL_LIMIT]);
    }

    #[test]
    fn test_buffer_empty() {
        let buf = Buffer::new();
        assert_eq!(buf.used(), 0);

        let mut buf = Buffer::new();
        let slice: &[u8] = buf.free_as_mut_slice();
        assert_eq!(slice.len(), Buffer::BLOCK_SIZE);
        assert_eq!(slice.len(), buf.block.len());
    }

    #[test]
    fn test_buffer_extend_use() {
        let mut buf = Buffer::new();
        buf.extend_used(3);
        assert_eq!(buf.used(), 3);
        let slice = buf.free_as_mut_slice();
        assert_eq!(slice.len(), Buffer::BLOCK_SIZE - 3);

        // Extend past the end of the buffer.
        buf.extend_used(Buffer::BLOCK_SIZE);
        assert_eq!(buf.used(), Buffer::BLOCK_SIZE);
        assert_eq!(buf.offset, Buffer::BLOCK_SIZE);
        assert_eq!(buf.block.len(), buf.offset);
        let slice = buf.free_as_mut_slice();
        assert_eq!(slice.len(), 0);
    }

    #[test]
    fn test_buffer_write_read() {
        let mut buf = Buffer::new();
        let mut slice = buf.free_as_mut_slice();
        slice.write_all(b"hello").unwrap();
        buf.extend_used(b"hello".len());

        let slice = &buf.block[..buf.used()];
        assert_eq!(slice, b"hello");
        assert_eq!(buf.free_as_mut_slice().len(), buf.block.len() - buf.offset);
    }

    #[test]
    fn test_buffer_grow() {
        let mut buf = Buffer::new();
        assert_eq!(buf.block.len(), Buffer::BLOCK_SIZE);
        buf.grow(1).unwrap();
        assert_eq!(buf.block.len(), 2 * Buffer::BLOCK_SIZE);

        buf.grow(Buffer::BLOCK_SIZE + 1).unwrap();
        assert_eq!(buf.block.len(), 4 * Buffer::BLOCK_SIZE);

        let ret = buf.grow(Buffer::MAX_CAPACITY);
        assert!(ret.is_err());
    }

    #[test]
    fn test_buffer_ensure_capacity() {
        // Initial state: 1 byte capacity left, initial size.
        let mut buf = Buffer::new();
        buf.extend_used(Buffer::BLOCK_SIZE - 1);
        assert_eq!(buf.free_as_mut_slice().len(), 1);
        assert_eq!(buf.block.len(), Buffer::BLOCK_SIZE);

        // Still has capacity, no size request.
        buf.ensure_capacity(0).unwrap();
        assert_eq!(buf.free_as_mut_slice().len(), 1);
        assert_eq!(buf.block.len(), Buffer::BLOCK_SIZE);

        // No more capacity, initial size.
        buf.extend_used(1);
        assert_eq!(buf.free_as_mut_slice().len(), 0);
        assert_eq!(buf.block.len(), Buffer::BLOCK_SIZE);

        // No capacity, no size request.
        buf.ensure_capacity(0).unwrap();
        assert_eq!(buf.free_as_mut_slice().len(), Buffer::BLOCK_SIZE);
        assert_eq!(buf.block.len(), 2 * Buffer::BLOCK_SIZE);

        // Some capacity, size request.
        buf.extend_used(5);
        assert_eq!(buf.offset, Buffer::BLOCK_SIZE + 5);
        buf.ensure_capacity(3 * Buffer::BLOCK_SIZE - 6).unwrap();
        assert_eq!(buf.free_as_mut_slice().len(), 2 * Buffer::BLOCK_SIZE - 5);
        assert_eq!(buf.block.len(), 3 * Buffer::BLOCK_SIZE);
    }

    /// Regression test for a bug in ensure_capacity() caused
    /// by a bug in byte-pool crate 0.2.2 dependency.
    ///
    /// ensure_capacity() sometimes did not ensure that
    /// at least one byte is available, which in turn
    /// resulted in attempt to read into a buffer of zero size.
    /// When poll_read() reads into a buffer of zero size,
    /// it can only read zero bytes, which is indistinguishable
    /// from EOF and resulted in an erroneous detection of EOF
    /// when in fact the stream was not closed.
    #[test]
    fn test_ensure_capacity_loop() {
        let mut buf = Buffer::new();

        for i in 1..500 {
            // Ask for `i` bytes.
            buf.ensure_capacity(i).unwrap();

            // Test that we can read at least 1 byte.
            let free = buf.free_as_mut_slice();
            let used = free.len();
            assert!(used > 0);

            // Use as much as allowed.
            buf.extend_used(used);

            // Test that we can read at least as much as requested.
            let block = buf.take_block();
            assert!(block.len() >= i);
            buf.return_block(block);
        }
    }

    #[test]
    fn test_buffer_take_and_return_block() {
        // This test identifies blocks by their size.
        let mut buf = Buffer::new();
        buf.grow(1).unwrap();
        let block_size = buf.block.len();

        let block = buf.take_block();
        assert_eq!(block.len(), block_size);
        assert_ne!(buf.block.len(), block_size);

        buf.return_block(block);
        assert_eq!(buf.block.len(), block_size);
    }

    #[test]
    fn test_buffer_reset_with_data() {
        // This test identifies blocks by their size.
        let data: [u8; 2 * Buffer::BLOCK_SIZE] = [b'a'; 2 * Buffer::BLOCK_SIZE];
        let mut buf = Buffer::new();
        let block_size = buf.block.len();
        assert_eq!(block_size, Buffer::BLOCK_SIZE);
        buf.reset_with_data(&data);
        assert_ne!(buf.block.len(), block_size);
        assert_eq!(buf.block.len(), 3 * Buffer::BLOCK_SIZE);
        assert!(!buf.free_as_mut_slice().is_empty());

        let data: [u8; 0] = [];
        let mut buf = Buffer::new();
        buf.reset_with_data(&data);
        assert_eq!(buf.block.len(), Buffer::BLOCK_SIZE);
    }

    #[test]
    fn test_buffer_debug() {
        assert_eq!(
            format!("{:?}", Buffer::new()),
            format!(r#"Buffer {{ used: 0, capacity: {} }}"#, Buffer::BLOCK_SIZE)
        );
    }
}
