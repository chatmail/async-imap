//! Adds support for the IMAP IDLE command specificed in [RFC2177](https://tools.ietf.org/html/rfc2177).

use std::fmt;
use std::pin::Pin;
use std::time::Duration;

#[cfg(feature = "runtime-async-std")]
use async_std::{
    future::timeout,
    io::{Read, Write},
};
use futures::prelude::*;
use futures::task::{Context, Poll};
use imap_proto::{RequestId, Response, Status};
use stop_token::prelude::*;
#[cfg(feature = "runtime-tokio")]
use tokio::{
    io::{AsyncRead as Read, AsyncWrite as Write},
    time::timeout,
};

use crate::client::Session;
use crate::error::Result;
use crate::parse::handle_unilateral;
use crate::types::ResponseData;

/// `Handle` allows a client to block waiting for changes to the remote mailbox.
///
/// The handle blocks using the [`IDLE` command](https://tools.ietf.org/html/rfc2177#section-3)
/// specificed in [RFC 2177](https://tools.ietf.org/html/rfc2177) until the underlying server state
/// changes in some way. While idling does inform the client what changes happened on the server,
/// this implementation will currently just block until _anything_ changes, and then notify the
///
/// Note that the server MAY consider a client inactive if it has an IDLE command running, and if
/// such a server has an inactivity timeout it MAY log the client off implicitly at the end of its
/// timeout period.  Because of that, clients using IDLE are advised to terminate the IDLE and
/// re-issue it at least every 29 minutes to avoid being logged off. [`Handle::wait`]
/// does this. This still allows a client to receive immediate mailbox updates even though it need
/// only "poll" at half hour intervals.
///
/// As long as a [`Handle`] is active, the mailbox cannot be otherwise accessed.
#[derive(Debug)]
pub struct Handle<T: Read + Write + Unpin + fmt::Debug> {
    session: Session<T>,
    id: Option<RequestId>,
}

impl<T: Read + Write + Unpin + fmt::Debug> Unpin for Handle<T> {}

impl<T: Read + Write + Unpin + fmt::Debug + Send> Stream for Handle<T> {
    type Item = std::io::Result<ResponseData>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.as_mut().session().get_stream().poll_next(cx)
    }
}

/// A stream of server responses after sending `IDLE`.
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
pub struct IdleStream<'a, St> {
    stream: &'a mut St,
}

impl<St: Unpin> Unpin for IdleStream<'_, St> {}

impl<'a, St: Stream + Unpin> IdleStream<'a, St> {
    unsafe_pinned!(stream: &'a mut St);

    pub(crate) fn new(stream: &'a mut St) -> Self {
        IdleStream { stream }
    }
}

impl<St: futures::stream::FusedStream + Unpin> futures::stream::FusedStream for IdleStream<'_, St> {
    fn is_terminated(&self) -> bool {
        self.stream.is_terminated()
    }
}

impl<St: Stream + Unpin> Stream for IdleStream<'_, St> {
    type Item = St::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream().poll_next(cx)
    }
}

/// Possible responses that happen on an open idle connection.
#[derive(Debug, PartialEq, Eq)]
pub enum IdleResponse {
    /// The manual interrupt was used to interrupt the idle connection..
    ManualInterrupt,
    /// The idle connection timed out, because of the user set timeout.
    Timeout,
    /// The server has indicated that some new action has happened.
    NewData(ResponseData),
}

// Make it possible to access the inner connection and modify its settings, such as read/write
// timeouts.
impl<T: Read + Write + Unpin + fmt::Debug> AsMut<T> for Handle<T> {
    fn as_mut(&mut self) -> &mut T {
        self.session.conn.stream.as_mut()
    }
}

impl<T: Read + Write + Unpin + fmt::Debug + Send> Handle<T> {
    unsafe_pinned!(session: Session<T>);

    pub(crate) fn new(session: Session<T>) -> Handle<T> {
        Handle { session, id: None }
    }

    /// Start listening to the server side responses.
    /// Must be called after [`Handle::init`].
    pub fn wait(
        &mut self,
    ) -> (
        impl Future<Output = Result<IdleResponse>> + '_,
        stop_token::StopSource,
    ) {
        self.wait_with_timeout(Duration::from_secs(24 * 60 * 60))
    }

    /// Start listening to the server side responses.
    ///
    /// Stops after the passed in `timeout` without any response from the server.
    /// Timeout is reset by any response, including `* OK Still here` keepalives.
    ///
    /// Must be called after [Handle::init].
    pub fn wait_with_timeout(
        &mut self,
        dur: Duration,
    ) -> (
        impl Future<Output = Result<IdleResponse>> + '_,
        stop_token::StopSource,
    ) {
        assert!(
            self.id.is_some(),
            "Cannot listen to response without starting IDLE"
        );

        let sender = self.session.unsolicited_responses_tx.clone();

        let interrupt = stop_token::StopSource::new();
        let raw_stream = IdleStream::new(self);
        let mut interruptible_stream = raw_stream.timeout_at(interrupt.token());

        let fut = async move {
            loop {
                let Ok(res) = timeout(dur, interruptible_stream.next()).await else {
                    return Ok(IdleResponse::Timeout);
                };

                let Some(Ok(resp)) = res else {
                    return Ok(IdleResponse::ManualInterrupt);
                };

                let resp = resp?;
                match resp.parsed() {
                    Response::Data {
                        status: Status::Ok, ..
                    } => {
                        // all good continue
                    }
                    Response::Continue { .. } => {
                        // continuation, wait for it
                    }
                    Response::Done { .. } => {
                        handle_unilateral(resp, sender.clone());
                    }
                    _ => return Ok(IdleResponse::NewData(resp)),
                }
            }
        };

        (fut, interrupt)
    }

    /// Initialise the idle connection by sending the `IDLE` command to the server.
    pub async fn init(&mut self) -> Result<()> {
        let id = self.session.run_command("IDLE").await?;
        self.id = Some(id);
        while let Some(res) = self.session.stream.try_next().await? {
            match res.parsed() {
                Response::Continue { .. } => {
                    return Ok(());
                }
                Response::Done {
                    tag,
                    status,
                    information,
                    ..
                } => {
                    if tag == self.id.as_ref().unwrap() {
                        if let Status::Bad = status {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::ConnectionRefused,
                                information.as_ref().unwrap().to_string(),
                            )
                            .into());
                        }
                    }
                    handle_unilateral(res, self.session.unsolicited_responses_tx.clone());
                }
                _ => {
                    handle_unilateral(res, self.session.unsolicited_responses_tx.clone());
                }
            }
        }

        Err(std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "").into())
    }

    /// Signal that we want to exit the idle connection, by sending the `DONE`
    /// command to the server.
    pub async fn done(mut self) -> Result<Session<T>> {
        assert!(
            self.id.is_some(),
            "Cannot call DONE on a non initialized idle connection"
        );
        self.session.run_command_untagged("DONE").await?;
        let sender = self.session.unsolicited_responses_tx.clone();
        self.session
            .check_done_ok(&self.id.expect("invalid setup"), Some(sender))
            .await?;

        Ok(self.session)
    }
}
