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

/// One parsed IMAP response or the retained prefix of an oversized literal.
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

struct OversizedLiteralBoundary {
    literal_start: usize,
    declared_size: usize,
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
    /// This is set when reading from a stream returns an error
    /// or retains a prefix instead of a complete literal.
    /// Afterwards the stream returns only `None`
    /// and `poll_next()` does not read
    /// from the underlying stream.
    read_closed: bool,
    /// True after retaining a literal prefix, when further commands would be unusable.
    commands_closed: bool,
}

impl<R: Read + Write + Unpin> ImapStream<R> {
    /// Creates a new `ImapStream` based on the given `Read`er.
    pub fn new(inner: R) -> Self {
        ImapStream {
            inner,
            buffer: Buffer::new(),
            decode_needs: 0,
            read_closed: false,
            commands_closed: false,
        }
    }

    /// Creates an `ImapStream` whose response buffer cannot grow past `max_response_size`.
    pub fn new_with_max_response_size(inner: R, max_response_size: NonZeroUsize) -> Self {
        ImapStream {
            inner,
            buffer: Buffer::new_with_max_response_size(max_response_size),
            decode_needs: 0,
            read_closed: false,
            commands_closed: false,
        }
    }

    pub async fn encode(&mut self, msg: Request) -> Result<(), io::Error> {
        if self.commands_closed {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "IMAP response stream is closed; reconnect before sending commands",
            ));
        }
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

    #[cfg(feature = "compress")]
    pub(crate) fn max_response_size(&self) -> NonZeroUsize {
        self.buffer.max_capacity
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
        if self.buffer.used() < self.decode_needs {
            // We know that there is not enough data to decode anything
            // from previous attempts.
            return Ok(None);
        }

        let block = self.buffer.take_block();
        // Be aware, now self.buffer is invalid until block is returned or reset!

        let res = ResponseData::try_new_or_recover(block, |buf| {
            let buf = &buf[..self.buffer.used()];
            log::trace!("decode: input: {:?}", std::str::from_utf8(buf));
            match imap_proto::Response::parse(buf) {
                Ok((remaining, response)) => {
                    // TODO: figure out if we can use a minimum required size for a response.
                    self.decode_needs = 0;
                    self.buffer.reset_with_data(remaining).map_err(Some)?;
                    Ok(response)
                }
                Err(nom::Err::Incomplete(Needed::Size(min))) => {
                    log::trace!("decode: incomplete data, need minimum {min} bytes");
                    self.decode_needs = self
                        .buffer
                        .used()
                        .checked_add(usize::from(min))
                        .ok_or_else(|| {
                            Some(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "IMAP response size overflow",
                            ))
                        })?;
                    Err(None)
                }
                Err(nom::Err::Incomplete(_)) => {
                    log::trace!("decode: incomplete data, need unknown number of bytes");
                    self.decode_needs = 0;
                    Err(None)
                }
                Err(other) => {
                    self.decode_needs = 0;
                    Err(Some(io::Error::other(format!(
                        "{:?} during parsing of {:?}",
                        other,
                        String::from_utf8_lossy(buf)
                    ))))
                }
            }
        });
        match res {
            Ok(response) => Ok(Some(response)),
            Err((heads, err)) => {
                self.buffer.return_block(heads);
                match err {
                    Some(err) => Err(err),
                    None => Ok(None),
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
        loop {
            let prefix = literal_prefix_limit.and_then(|limit| {
                oversized_literal_boundary(&this.buffer.block[..this.buffer.used()], limit)
                    .map(|boundary| (boundary, limit))
            });
            let retaining_prefix = prefix.is_some();
            let (required, read_limit) = if let Some((boundary, limit)) = prefix {
                let target = boundary
                    .literal_start
                    .checked_add(limit.get())
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "IMAP literal overflow")
                    })?;
                if this.buffer.used() >= target {
                    let data = this.buffer.block[boundary.literal_start..target].to_vec();
                    this.buffer.reset_with_data(&[])?;
                    this.read_closed = true;
                    this.commands_closed = true;
                    return Poll::Ready(Some(Ok(LiteralAwareResponse::LiteralPrefix(
                        LiteralPrefix {
                            declared_size: boundary.declared_size,
                            data,
                        },
                    ))));
                }
                (target, Some(target - this.buffer.used()))
            } else {
                if let Some(response) = this.decode()? {
                    return Poll::Ready(Some(Ok(LiteralAwareResponse::Parsed(response))));
                }
                (
                    this.decode_needs,
                    literal_prefix_limit.map(NonZeroUsize::get),
                )
            };
            this.buffer.ensure_capacity(required)?;
            let mut buf = this.buffer.free_as_mut_slice();
            if let Some(limit) = read_limit {
                let allowed = buf.len().min(limit);
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
                        if retaining_prefix {
                            "oversized IMAP literal ended before its retained prefix"
                        } else {
                            "bytes remaining in stream"
                        },
                    ))));
                }
                return Poll::Ready(None);
            }
            this.buffer.extend_used(num_bytes_read);
        }
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

/// Finds a literal boundary confirmed by parsing the response through its declaration.
/// Skip complete literal payloads so marker-like bytes inside them cannot be mistaken
/// for a later literal declaration. This also handles responses already in the buffer.
fn oversized_literal_boundary(
    input: &[u8],
    literal_prefix_limit: NonZeroUsize,
) -> Option<OversizedLiteralBoundary> {
    let mut search_start = 0;
    while let Some(relative_start) = input[search_start..].iter().position(|byte| *byte == b'{') {
        let start = search_start + relative_start;
        search_start = start + 1;
        let Some(relative_end) = input[search_start..]
            .windows(3)
            .position(|window| window == b"}\r\n")
        else {
            continue;
        };
        let digits_end = search_start + relative_end;
        let digits = &input[search_start..digits_end];
        if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
            continue;
        }
        let declared_size = std::str::from_utf8(digits).ok()?.parse::<usize>().ok()?;
        let literal_start = digits_end + 3;
        if !matches!(imap_proto::Response::parse(&input[..literal_start]),
            Err(nom::Err::Incomplete(Needed::Size(needed))) if needed.get() == declared_size
        ) {
            continue;
        }
        if declared_size > literal_prefix_limit.get() {
            return Some(OversizedLiteralBoundary {
                literal_start,
                declared_size,
            });
        }
        search_start = literal_start.checked_add(declared_size)?;
        if search_start >= input.len() {
            return None;
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
    const MAX_CAPACITY: usize = 512 * 1024 * 1024; // 512 MiB

    fn new() -> Self {
        Self::new_with_max_response_size(
            NonZeroUsize::new(Self::MAX_CAPACITY).expect("default IMAP response limit is nonzero"),
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
            if required > self.max_capacity.get() {
                return Err(io::Error::other("incoming data too large"));
            }
            let increase = Buffer::BLOCK_SIZE
                .max(extra_bytes_needed)
                .min(self.max_capacity.get() - self.block.len());
            self.grow(increase.max(1))?;
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
        let min_size = self.block.len().checked_add(num_bytes).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "IMAP response size overflow")
        })?;
        if min_size > self.max_capacity.get() {
            return Err(io::Error::other("incoming data too large"));
        }
        let new_size = match min_size % Self::BLOCK_SIZE {
            0 => min_size,
            n => min_size.saturating_add(Self::BLOCK_SIZE - n),
        }
        .min(self.max_capacity.get());
        if new_size > self.block.capacity() {
            // Keep amortized growth without letting resize() reserve past the limit.
            let capacity = self
                .block
                .capacity()
                .saturating_mul(2)
                .max(new_size)
                .min(self.max_capacity.get());
            let mut block = BytesMut::zeroed(capacity);
            block[..self.offset].copy_from_slice(&self.block[..self.offset]);
            self.block = block;
        }
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
    /// Rejects data beyond the response limit without changing the buffer.
    /// Leaves free space for the next read when the configured limit permits it.
    fn reset_with_data(&mut self, data: &[u8]) -> io::Result<()> {
        let min_size = data.len();
        if min_size > self.max_capacity.get() {
            return Err(io::Error::other("incoming data too large"));
        }
        let padding = (Self::BLOCK_SIZE - min_size % Self::BLOCK_SIZE)
            .min(self.max_capacity.get() - min_size);
        let new_size = min_size.checked_add(padding).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "IMAP response size overflow")
        })?;
        self.block = BytesMut::zeroed(new_size);
        self.block[..data.len()].copy_from_slice(data);

        self.offset = data.len();
        Ok(())
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
    async fn bogus_literal_size_returns_a_bounded_prefix() {
        use futures_util::StreamExt;

        let mut stream = ImapStream::new_with_max_response_size(
            crate::mock_stream::MockStream::new(
                b"* 1 FETCH (BODY[] {33554432}\r\nabcdefgh".to_vec(),
            ),
            NonZeroUsize::new(64).unwrap(),
        );
        let response = stream
            .next_with_literal_prefix(NonZeroUsize::new(8).unwrap())
            .await
            .unwrap()
            .unwrap();
        let next = stream.next().await;
        let write_error = stream
            .encode(Request(None, b"NOOP".to_vec()))
            .await
            .unwrap_err();
        let LiteralAwareResponse::LiteralPrefix(prefix) = response else {
            panic!("a bogus literal size must return the retained prefix");
        };
        assert_eq!(prefix.declared_size(), 33_554_432);
        assert_eq!(prefix.data(), b"abcdefgh");
        assert!(
            next.is_none(),
            "the truncated connection must not be reused"
        );
        assert_eq!(write_error.kind(), io::ErrorKind::BrokenPipe);
        assert!(stream.get_ref().written_buf.is_empty());
    }

    #[test]
    fn reset_respects_the_configured_response_limit() {
        for len in [4999, 5000, 5001] {
            let data = vec![b'a'; len];
            let mut buf = Buffer::new_with_max_response_size(NonZeroUsize::new(5000).unwrap());
            buf.ensure_capacity(5000).unwrap();
            buf.free_as_mut_slice()[..3].copy_from_slice(b"old");
            buf.extend_used(3);
            let grown_capacity = buf.block.capacity();

            let result = buf.reset_with_data(&data);

            assert_eq!(result.is_ok(), len <= 5000);
            let expected = if len > 5000 { &b"old"[..] } else { &data };
            assert_eq!(&buf.block[..buf.used()], expected);
            assert!(grown_capacity <= 5000);
            assert!(buf.block.capacity() <= 5000);
        }
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
        buf.reset_with_data(&data).unwrap();
        assert_ne!(buf.block.len(), block_size);
        assert_eq!(buf.block.len(), 3 * Buffer::BLOCK_SIZE);
        assert!(!buf.free_as_mut_slice().is_empty());

        let data: [u8; 0] = [];
        let mut buf = Buffer::new();
        buf.reset_with_data(&data).unwrap();
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
