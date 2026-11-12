//! Adds support for the QRESYNC command specificed in [RFC2087](https://tools.ietf.org/html/rfc7162).

use async_channel as channel;
#[cfg(feature = "runtime-async-std")]
use async_std::io::{Read, Write};
use futures_util::{Stream, StreamExt as _, TryStreamExt as _, io};
use imap_proto::{self, MailboxDatum, RequestId, Response, Status};
#[cfg(feature = "runtime-tokio")]
use tokio::io::{AsyncRead as Read, AsyncWrite as Write};

use crate::Session;
use crate::client::validate_str;
use crate::error::{Error, Result, StatusResponse};
use crate::parse::{filter, handle_unilateral};
use crate::types::*;

/// Meta-informations about an IMAP mailbox when QRESYNC has been enabled.
/// This allows to retrieve the uids of modified and deleted emails since last `QRESYNC` command.
#[derive(Debug, Default)]
pub struct Qresync {
    /// Meta-information about an IMAP mailbox
    pub mailbox: Mailbox,

    /// A listing of UIDs that have been expunged since last `QRESYNC` command.
    pub vanished: Vec<Uid>,

    /// A listing of messages that have been added since last `QRESYNC` command.
    pub fetch: Vec<Fetch>,
}

impl Qresync {
    /// Selects a mailbox with `(QRESYNC)` parameter as defined in
    /// [RFC 7162](https://www.rfc-editor.org/rfc/rfc7162.html#section-3.2.5).
    pub async fn select<
        T: Read + Write + Unpin + std::fmt::Debug + Send,
        S1: AsRef<str>,
        S2: AsRef<str>,
        S3: AsRef<str>,
    >(
        session: &mut Session<T>,
        mailbox_name: S1,
        uid_validity: u32,
        highest_modseq: u64,
        known_uid_set_opt: Option<S2>,
        known_sequence_ranges_opt: Option<S3>,
    ) -> Result<Qresync> {
        Qresync::enable(session).await?;

        let mut query = format!(
            "SELECT {} (QRESYNC ({uid_validity} {highest_modseq}",
            validate_str(mailbox_name.as_ref())?
        );

        if let Some(known_uid_set) = known_uid_set_opt {
            query.push_str(&format!(" {}", validate_str(known_uid_set.as_ref())?));
        }

        if let Some(known_sequence_ranges) = known_sequence_ranges_opt {
            query.push_str(&format!(
                " ({})",
                validate_str(known_sequence_ranges.as_ref())?
            ));
        }

        query.push_str("))");

        let id = session.run_command(&query).await?;
        parse_mailbox_qresync(
            &mut session.conn.stream,
            session.unsolicited_responses_tx.clone(),
            id,
        )
        .await
    }

    /// Enable `(QRESYNC)` as defined in
    /// [RFC 7162](https://www.rfc-editor.org/rfc/rfc7162.html#section-3.2.3).
    async fn enable<T: Read + Write + Unpin + std::fmt::Debug + Send>(
        session: &mut Session<T>,
    ) -> Result<()> {
        let id = session.run_command("ENABLE QRESYNC").await?;
        parse_enable_qresync(
            &mut session.conn.stream,
            session.unsolicited_responses_tx.clone(),
            id,
        )
        .await
    }
}

pub(crate) async fn parse_enable_qresync<T: Stream<Item = io::Result<ResponseData>> + Unpin>(
    stream: &mut T,
    unsolicited: channel::Sender<UnsolicitedResponse>,
    command_tag: RequestId,
) -> Result<()> {
    while let Some(resp) = stream
        .take_while(|res| filter(res, &command_tag))
        .try_next()
        .await?
    {
        match resp.parsed() {
            Response::Capabilities(cs) => {
                cs.iter()
                    .find(|c| Capability::from(*c).eq(&Capability::Atom(String::from("QRESYNC"))))
                    .ok_or(Error::Io(io::Error::other(
                        "QRESYNC is not available on the server".to_string(),
                    )))?;
            }
            Response::Done {
                tag,
                status,
                outcome,
                ..
            } if tag == &command_tag => {
                use imap_proto::Status;
                match status {
                    Status::Ok => {
                        break;
                    }
                    Status::Bad => {
                        return Err(Error::Bad(StatusResponse::new(resp)));
                    }
                    Status::No => {
                        return Err(Error::No(StatusResponse::new(resp)));
                    }
                    _ => {
                        return Err(Error::Io(io::Error::other(format!(
                            "status: {status:?}, outcome: {outcome:?}"
                        ))));
                    }
                }
            }
            _ => {
                handle_unilateral(resp, unsolicited.clone());
            }
        }
    }

    Ok(())
}

pub(crate) async fn parse_mailbox_qresync<T: Stream<Item = io::Result<ResponseData>> + Unpin>(
    stream: &mut T,
    unsolicited: channel::Sender<UnsolicitedResponse>,
    command_tag: RequestId,
) -> Result<Qresync> {
    let mut mailbox = Mailbox::default();
    let mut qresync = Qresync::default();

    while let Some(resp) = stream.try_next().await? {
        match resp.parsed() {
            Response::Done {
                tag,
                status,
                outcome,
                ..
            } if tag == &command_tag => match status {
                Status::Ok => {
                    break;
                }
                Status::Bad => {
                    return Err(Error::Bad(StatusResponse::new(resp)));
                }
                Status::No => {
                    return Err(Error::No(StatusResponse::new(resp)));
                }
                _ => {
                    return Err(Error::Io(io::Error::other(format!(
                        "status: {status:?}, outcome: {outcome:?}"
                    ))));
                }
            },
            Response::Data { status, outcome } => match status {
                Status::Ok => {
                    use imap_proto::ResponseCode;
                    match &outcome.code {
                        Some(ResponseCode::UidValidity(uid)) => {
                            mailbox.uid_validity = Some(*uid);
                        }
                        Some(ResponseCode::UidNext(unext)) => {
                            mailbox.uid_next = Some(*unext);
                        }
                        Some(ResponseCode::HighestModSeq(highest_modseq)) => {
                            mailbox.highest_modseq = Some(*highest_modseq);
                        }
                        Some(ResponseCode::Unseen(n)) => {
                            mailbox.unseen = Some(*n);
                        }
                        Some(ResponseCode::PermanentFlags(flags)) => {
                            mailbox
                                .permanent_flags
                                .extend(flags.iter().map(|s| (*s).to_string()).map(Flag::from));
                        }
                        _ => {}
                    }
                }
                Status::Bad => {
                    return Err(Error::Bad(StatusResponse::new(resp)));
                }
                Status::No => {
                    return Err(Error::No(StatusResponse::new(resp)));
                }
                _ => {
                    return Err(Error::Io(io::Error::other(format!(
                        "status: {status:?}, outcome: {outcome:?}"
                    ))));
                }
            },
            Response::MailboxData(m) => match m {
                MailboxDatum::Status { .. } => handle_unilateral(resp, unsolicited.clone()),
                MailboxDatum::Exists(e) => {
                    mailbox.exists = *e;
                }
                MailboxDatum::Recent(r) => {
                    mailbox.recent = *r;
                }
                MailboxDatum::Flags(flags) => {
                    mailbox
                        .flags
                        .extend(flags.iter().map(|s| (*s).to_string()).map(Flag::from));
                }
                _ => {}
            },
            Response::Vanished { uids, .. } => {
                let mut uids = uids
                    .iter()
                    .flat_map(|r| r.clone().collect::<Vec<_>>())
                    .collect::<Vec<_>>();
                uids.sort();
                qresync.vanished = uids;
            }
            Response::Fetch(..) => qresync.fetch.push(Fetch::new(resp)),
            _ => {
                handle_unilateral(resp, unsolicited.clone());
            }
        }
    }

    qresync.mailbox = mailbox;

    Ok(qresync)
}
