use std::cmp::min;
use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};

#[cfg(feature = "runtime-async-std")]
use async_std::io::{Read, Write};
#[cfg(feature = "runtime-tokio")]
use tokio::io::{AsyncRead as Read, AsyncWrite as Write};

#[derive(Default, Clone, Debug, Eq, PartialEq, Hash)]
pub struct MockStream {
    read_buf: Vec<u8>,
    read_pos: usize,
    pub written_buf: Vec<u8>,
    err_on_read: bool,
    eof_on_read: bool,
    read_delay: usize,
}

impl MockStream {
    pub fn new(read_buf: Vec<u8>) -> MockStream {
        MockStream::default().with_buf(read_buf)
    }

    pub fn with_buf(mut self, read_buf: Vec<u8>) -> MockStream {
        self.read_buf = read_buf;
        self
    }

    pub fn with_eof(mut self) -> MockStream {
        self.eof_on_read = true;
        self
    }

    pub fn with_err(mut self) -> MockStream {
        self.err_on_read = true;
        self
    }

    pub fn with_delay(mut self) -> MockStream {
        self.read_delay = 1;
        self
    }
}

#[cfg(feature = "runtime-tokio")]
impl Read for MockStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        if self.eof_on_read {
            return Poll::Ready(Ok(()));
        }
        if self.err_on_read {
            return Poll::Ready(Err(Error::new(ErrorKind::Other, "MockStream Error")));
        }
        if self.read_pos >= self.read_buf.len() {
            return Poll::Ready(Err(Error::new(ErrorKind::UnexpectedEof, "EOF")));
        }
        let mut write_len = min(buf.remaining(), self.read_buf.len() - self.read_pos);
        if self.read_delay > 0 {
            self.read_delay -= 1;
            write_len = min(write_len, 1);
        }
        let max_pos = self.read_pos + write_len;
        buf.put_slice(&self.read_buf[self.read_pos..max_pos]);
        self.read_pos += write_len;
        Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "runtime-tokio")]
impl Write for MockStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        self.written_buf.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "runtime-async-std")]
impl Read for MockStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        if self.eof_on_read {
            return Poll::Ready(Ok(0));
        }
        if self.err_on_read {
            return Poll::Ready(Err(Error::other("MockStream Error")));
        }
        if self.read_pos >= self.read_buf.len() {
            return Poll::Ready(Err(Error::new(ErrorKind::UnexpectedEof, "EOF")));
        }
        let mut write_len = min(buf.len(), self.read_buf.len() - self.read_pos);
        if self.read_delay > 0 {
            self.read_delay -= 1;
            write_len = min(write_len, 1);
        }
        let max_pos = self.read_pos + write_len;
        for x in self.read_pos..max_pos {
            buf[x - self.read_pos] = self.read_buf[x];
        }
        self.read_pos += write_len;
        Poll::Ready(Ok(write_len))
    }
}

#[cfg(feature = "runtime-async-std")]
impl Write for MockStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        self.written_buf.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
}
