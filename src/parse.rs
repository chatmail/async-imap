use std::collections::HashSet;

use async_channel as channel;
use futures::io;
use futures::prelude::*;
use futures::stream::Stream;
use imap_proto::{self, MailboxDatum, Metadata, RequestId, Response};

use crate::error::{Error, Result};
use crate::types::ResponseData;
use crate::types::*;

pub(crate) fn parse_names<T: Stream<Item = io::Result<ResponseData>> + Unpin + Send>(
    stream: &mut T,
    unsolicited: channel::Sender<UnsolicitedResponse>,
    command_tag: RequestId,
) -> impl Stream<Item = Result<Name>> + '_ + Send + Unpin {
    use futures::{FutureExt, StreamExt};

    StreamExt::filter_map(
        StreamExt::take_while(stream, move |res| filter(res, &command_tag)),
        move |resp| {
            let unsolicited = unsolicited.clone();
            async move {
                match resp {
                    Ok(resp) => match resp.parsed() {
                        Response::MailboxData(MailboxDatum::List { .. }) => {
                            let name = Name::from_mailbox_data(resp);
                            Some(Ok(name))
                        }
                        _ => {
                            handle_unilateral(resp, unsolicited);
                            None
                        }
                    },
                    Err(err) => Some(Err(err.into())),
                }
            }
            .boxed()
        },
    )
}

pub(crate) fn filter(
    res: &io::Result<ResponseData>,
    command_tag: &RequestId,
) -> impl Future<Output = bool> {
    let val = filter_sync(res, command_tag);
    futures::future::ready(val)
}

pub(crate) fn filter_sync(res: &io::Result<ResponseData>, command_tag: &RequestId) -> bool {
    match res {
        Ok(res) => match res.parsed() {
            Response::Done { tag, .. } => tag != command_tag,
            _ => true,
        },
        Err(_err) => {
            // Do not filter out the errors such as unexpected EOF.
            true
        }
    }
}

pub(crate) fn parse_fetches<T: Stream<Item = io::Result<ResponseData>> + Unpin + Send>(
    stream: &mut T,
    unsolicited: channel::Sender<UnsolicitedResponse>,
    command_tag: RequestId,
) -> impl Stream<Item = Result<Fetch>> + '_ + Send + Unpin {
    use futures::{FutureExt, StreamExt};

    StreamExt::filter_map(
        StreamExt::take_while(stream, move |res| filter(res, &command_tag)),
        move |resp| {
            let unsolicited = unsolicited.clone();

            async move {
                match resp {
                    Ok(resp) => match resp.parsed() {
                        Response::Fetch(..) => Some(Ok(Fetch::new(resp))),
                        _ => {
                            handle_unilateral(resp, unsolicited);
                            None
                        }
                    },
                    Err(err) => Some(Err(err.into())),
                }
            }
            .boxed()
        },
    )
}

pub(crate) async fn parse_status<T: Stream<Item = io::Result<ResponseData>> + Unpin + Send>(
    stream: &mut T,
    expected_mailbox: &str,
    unsolicited: channel::Sender<UnsolicitedResponse>,
    command_tag: RequestId,
) -> Result<Mailbox> {
    let mut mbox = Mailbox::default();

    while let Some(resp) = stream.try_next().await? {
        match resp.parsed() {
            Response::Done {
                tag,
                status,
                code,
                information,
                ..
            } if tag == &command_tag => {
                use imap_proto::Status;
                match status {
                    Status::Ok => {
                        break;
                    }
                    Status::Bad => {
                        return Err(Error::Bad(format!("code: {code:?}, info: {information:?}")))
                    }
                    Status::No => {
                        return Err(Error::No(format!("code: {code:?}, info: {information:?}")))
                    }
                    _ => {
                        return Err(Error::Io(io::Error::other(format!(
                            "status: {status:?}, code: {code:?}, information: {information:?}"
                        ))));
                    }
                }
            }
            Response::MailboxData(MailboxDatum::Status { mailbox, status })
                if mailbox == expected_mailbox =>
            {
                for attribute in status {
                    match attribute {
                        StatusAttribute::HighestModSeq(highest_modseq) => {
                            mbox.highest_modseq = Some(*highest_modseq)
                        }
                        StatusAttribute::Messages(exists) => mbox.exists = *exists,
                        StatusAttribute::Recent(recent) => mbox.recent = *recent,
                        StatusAttribute::UidNext(uid_next) => mbox.uid_next = Some(*uid_next),
                        StatusAttribute::UidValidity(uid_validity) => {
                            mbox.uid_validity = Some(*uid_validity)
                        }
                        StatusAttribute::Unseen(unseen) => mbox.unseen = Some(*unseen),
                        _ => {}
                    }
                }
            }
            _ => {
                handle_unilateral(resp, unsolicited.clone());
            }
        }
    }

    Ok(mbox)
}

pub(crate) fn parse_expunge<T: Stream<Item = io::Result<ResponseData>> + Unpin + Send>(
    stream: &mut T,
    unsolicited: channel::Sender<UnsolicitedResponse>,
    command_tag: RequestId,
) -> impl Stream<Item = Result<u32>> + '_ + Send {
    use futures::StreamExt;

    StreamExt::filter_map(
        StreamExt::take_while(stream, move |res| filter(res, &command_tag)),
        move |resp| {
            let unsolicited = unsolicited.clone();

            async move {
                match resp {
                    Ok(resp) => match resp.parsed() {
                        Response::Expunge(id) => Some(Ok(*id)),
                        _ => {
                            handle_unilateral(resp, unsolicited);
                            None
                        }
                    },
                    Err(err) => Some(Err(err.into())),
                }
            }
        },
    )
}

pub(crate) async fn parse_capabilities<T: Stream<Item = io::Result<ResponseData>> + Unpin>(
    stream: &mut T,
    unsolicited: channel::Sender<UnsolicitedResponse>,
    command_tag: RequestId,
) -> Result<Capabilities> {
    let mut caps: HashSet<Capability> = HashSet::new();

    while let Some(resp) = stream
        .take_while(|res| filter(res, &command_tag))
        .try_next()
        .await?
    {
        match resp.parsed() {
            Response::Capabilities(cs) => {
                for c in cs {
                    caps.insert(Capability::from(c)); // TODO: avoid clone
                }
            }
            _ => {
                handle_unilateral(resp, unsolicited.clone());
            }
        }
    }

    Ok(Capabilities(caps))
}

pub(crate) async fn parse_noop<T: Stream<Item = io::Result<ResponseData>> + Unpin>(
    stream: &mut T,
    unsolicited: channel::Sender<UnsolicitedResponse>,
    command_tag: RequestId,
) -> Result<()> {
    while let Some(resp) = stream
        .take_while(|res| filter(res, &command_tag))
        .try_next()
        .await?
    {
        handle_unilateral(resp, unsolicited.clone());
    }

    Ok(())
}

pub(crate) async fn parse_mailbox<T: Stream<Item = io::Result<ResponseData>> + Unpin>(
    stream: &mut T,
    unsolicited: channel::Sender<UnsolicitedResponse>,
    command_tag: RequestId,
) -> Result<Mailbox> {
    let mut mailbox = Mailbox::default();

    while let Some(resp) = stream.try_next().await? {
        match resp.parsed() {
            Response::Done {
                tag,
                status,
                code,
                information,
                ..
            } if tag == &command_tag => {
                use imap_proto::Status;
                match status {
                    Status::Ok => {
                        break;
                    }
                    Status::Bad => {
                        return Err(Error::Bad(format!("code: {code:?}, info: {information:?}")))
                    }
                    Status::No => {
                        return Err(Error::No(format!("code: {code:?}, info: {information:?}")))
                    }
                    _ => {
                        return Err(Error::Io(io::Error::other(format!(
                            "status: {status:?}, code: {code:?}, information: {information:?}"
                        ))));
                    }
                }
            }
            Response::Data {
                status,
                code,
                information,
            } => {
                use imap_proto::Status;

                match status {
                    Status::Ok => {
                        use imap_proto::ResponseCode;
                        match code {
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
                        return Err(Error::Bad(format!("code: {code:?}, info: {information:?}")))
                    }
                    Status::No => {
                        return Err(Error::No(format!("code: {code:?}, info: {information:?}")))
                    }
                    _ => {
                        return Err(Error::Io(io::Error::other(format!(
                            "status: {status:?}, code: {code:?}, information: {information:?}"
                        ))));
                    }
                }
            }
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
                MailboxDatum::List { .. } => {}
                MailboxDatum::MetadataSolicited { .. } => {}
                MailboxDatum::MetadataUnsolicited { .. } => {}
                MailboxDatum::Search { .. } => {}
                MailboxDatum::Sort { .. } => {}
                _ => {}
            },
            _ => {
                handle_unilateral(resp, unsolicited.clone());
            }
        }
    }

    Ok(mailbox)
}

pub(crate) async fn parse_ids<T: Stream<Item = io::Result<ResponseData>> + Unpin>(
    stream: &mut T,
    unsolicited: channel::Sender<UnsolicitedResponse>,
    command_tag: RequestId,
) -> Result<HashSet<u32>> {
    let mut ids: HashSet<u32> = HashSet::new();

    while let Some(resp) = stream
        .take_while(|res| filter(res, &command_tag))
        .try_next()
        .await?
    {
        match resp.parsed() {
            Response::MailboxData(MailboxDatum::Search(cs)) => {
                for c in cs {
                    ids.insert(*c);
                }
            }
            _ => {
                handle_unilateral(resp, unsolicited.clone());
            }
        }
    }

    Ok(ids)
}

/// Parses [GETMETADATA](https://www.rfc-editor.org/info/rfc5464) response.
pub(crate) async fn parse_metadata<T: Stream<Item = io::Result<ResponseData>> + Unpin>(
    stream: &mut T,
    mailbox_name: &str,
    unsolicited: channel::Sender<UnsolicitedResponse>,
    command_tag: RequestId,
) -> Result<Vec<Metadata>> {
    let mut res_values = Vec::new();
    while let Some(resp) = stream
        .take_while(|res| filter(res, &command_tag))
        .try_next()
        .await?
    {
        match resp.parsed() {
            // METADATA Response with Values
            // <https://datatracker.ietf.org/doc/html/rfc5464.html#section-4.4.1>
            Response::MailboxData(MailboxDatum::MetadataSolicited { mailbox, values })
                if mailbox == mailbox_name =>
            {
                res_values.extend_from_slice(values.as_slice());
            }

            // We are not interested in
            // [Unsolicited METADATA Response without Values](https://datatracker.ietf.org/doc/html/rfc5464.html#section-4.4.2),
            // they go to unsolicited channel with other unsolicited responses.
            _ => {
                handle_unilateral(resp, unsolicited.clone());
            }
        }
    }
    Ok(res_values)
}

/// Sends unilateral server response
/// (see Section 7 of RFC 3501)
/// into the channel.
///
/// If the channel is full or closed,
/// i.e. the responses are not being consumed,
/// ignores new responses.
pub(crate) fn handle_unilateral(
    res: ResponseData,
    unsolicited: channel::Sender<UnsolicitedResponse>,
) {
    match res.parsed() {
        Response::MailboxData(MailboxDatum::Status { mailbox, status }) => {
            unsolicited
                .try_send(UnsolicitedResponse::Status {
                    mailbox: (mailbox.as_ref()).into(),
                    attributes: status.to_vec(),
                })
                .ok();
        }
        Response::MailboxData(MailboxDatum::Recent(n)) => {
            unsolicited.try_send(UnsolicitedResponse::Recent(*n)).ok();
        }
        Response::MailboxData(MailboxDatum::Exists(n)) => {
            unsolicited.try_send(UnsolicitedResponse::Exists(*n)).ok();
        }
        Response::Expunge(n) => {
            unsolicited.try_send(UnsolicitedResponse::Expunge(*n)).ok();
        }
        _ => {
            unsolicited.try_send(UnsolicitedResponse::Other(res)).ok();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_channel::bounded;
    use bytes::BytesMut;

    fn input_stream(data: &[&str]) -> Vec<io::Result<ResponseData>> {
        data.iter()
            .map(|line| {
                let block = BytesMut::from(line.as_bytes());
                ResponseData::try_new(block, |bytes| -> io::Result<_> {
                    let (remaining, response) = imap_proto::parser::parse_response(bytes).unwrap();
                    assert_eq!(remaining.len(), 0);
                    Ok(response)
                })
            })
            .collect()
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn parse_capability_test() {
        let expected_capabilities = &["IMAP4rev1", "STARTTLS", "AUTH=GSSAPI", "LOGINDISABLED"];
        let responses =
            input_stream(&["* CAPABILITY IMAP4rev1 STARTTLS AUTH=GSSAPI LOGINDISABLED\r\n"]);

        let mut stream = async_std::stream::from_iter(responses);
        let (send, recv) = bounded(10);
        let id = RequestId("A0001".into());
        let capabilities = parse_capabilities(&mut stream, send, id).await.unwrap();
        // shouldn't be any unexpected responses parsed
        assert!(recv.is_empty());
        assert_eq!(capabilities.len(), 4);
        for e in expected_capabilities {
            assert!(capabilities.has_str(e));
        }
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn parse_capability_case_insensitive_test() {
        // Test that "IMAP4REV1" (instead of "IMAP4rev1") is accepted
        let expected_capabilities = &["IMAP4rev1", "STARTTLS"];
        let responses = input_stream(&["* CAPABILITY IMAP4REV1 STARTTLS\r\n"]);
        let mut stream = async_std::stream::from_iter(responses);

        let (send, recv) = bounded(10);
        let id = RequestId("A0001".into());
        let capabilities = parse_capabilities(&mut stream, send, id).await.unwrap();

        // shouldn't be any unexpected responses parsed
        assert!(recv.is_empty());
        assert_eq!(capabilities.len(), 2);
        for e in expected_capabilities {
            assert!(capabilities.has_str(e));
        }
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    #[should_panic]
    async fn parse_capability_invalid_test() {
        let (send, recv) = bounded(10);
        let responses = input_stream(&["* JUNK IMAP4rev1 STARTTLS AUTH=GSSAPI LOGINDISABLED\r\n"]);
        let mut stream = async_std::stream::from_iter(responses);

        let id = RequestId("A0001".into());
        parse_capabilities(&mut stream, send.clone(), id)
            .await
            .unwrap();
        assert!(recv.is_empty());
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn parse_names_test() {
        let (send, recv) = bounded(10);
        let responses = input_stream(&["* LIST (\\HasNoChildren) \".\" \"INBOX\"\r\n"]);
        let mut stream = async_std::stream::from_iter(responses);

        let id = RequestId("A0001".into());
        let names: Vec<_> = parse_names(&mut stream, send, id)
            .try_collect::<Vec<Name>>()
            .await
            .unwrap();
        assert!(recv.is_empty());
        assert_eq!(names.len(), 1);
        assert_eq!(
            names[0].attributes(),
            &[NameAttribute::Extension("\\HasNoChildren".into())]
        );
        assert_eq!(names[0].delimiter(), Some("."));
        assert_eq!(names[0].name(), "INBOX");
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn parse_fetches_empty() {
        let (send, recv) = bounded(10);
        let responses = input_stream(&[]);
        let mut stream = async_std::stream::from_iter(responses);
        let id = RequestId("a".into());

        let fetches = parse_fetches(&mut stream, send, id)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(recv.is_empty());
        assert!(fetches.is_empty());
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn parse_fetches_test() {
        let (send, recv) = bounded(10);
        let responses = input_stream(&[
            "* 24 FETCH (FLAGS (\\Seen) UID 4827943)\r\n",
            "* 25 FETCH (FLAGS (\\Seen))\r\n",
        ]);
        let mut stream = async_std::stream::from_iter(responses);
        let id = RequestId("a".into());

        let fetches = parse_fetches(&mut stream, send, id)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(recv.is_empty());

        assert_eq!(fetches.len(), 2);
        assert_eq!(fetches[0].message, 24);
        assert_eq!(fetches[0].flags().collect::<Vec<_>>(), vec![Flag::Seen]);
        assert_eq!(fetches[0].uid, Some(4827943));
        assert_eq!(fetches[0].body(), None);
        assert_eq!(fetches[0].header(), None);
        assert_eq!(fetches[1].message, 25);
        assert_eq!(fetches[1].flags().collect::<Vec<_>>(), vec![Flag::Seen]);
        assert_eq!(fetches[1].uid, None);
        assert_eq!(fetches[1].body(), None);
        assert_eq!(fetches[1].header(), None);
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn parse_fetches_w_unilateral() {
        // https://github.com/mattnenterprise/rust-imap/issues/81
        let (send, recv) = bounded(10);
        let responses = input_stream(&["* 37 FETCH (UID 74)\r\n", "* 1 RECENT\r\n"]);
        let mut stream = async_std::stream::from_iter(responses);
        let id = RequestId("a".into());

        let fetches = parse_fetches(&mut stream, send, id)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(recv.recv().await.unwrap(), UnsolicitedResponse::Recent(1));

        assert_eq!(fetches.len(), 1);
        assert_eq!(fetches[0].message, 37);
        assert_eq!(fetches[0].uid, Some(74));
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn parse_names_w_unilateral() {
        let (send, recv) = bounded(10);
        let responses = input_stream(&[
            "* LIST (\\HasNoChildren) \".\" \"INBOX\"\r\n",
            "* 4 EXPUNGE\r\n",
        ]);
        let mut stream = async_std::stream::from_iter(responses);

        let id = RequestId("A0001".into());
        let names = parse_names(&mut stream, send, id)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(recv.recv().await.unwrap(), UnsolicitedResponse::Expunge(4));

        assert_eq!(names.len(), 1);
        assert_eq!(
            names[0].attributes(),
            &[NameAttribute::Extension("\\HasNoChildren".into())]
        );
        assert_eq!(names[0].delimiter(), Some("."));
        assert_eq!(names[0].name(), "INBOX");
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn parse_capabilities_w_unilateral() {
        let (send, recv) = bounded(10);
        let responses = input_stream(&[
            "* CAPABILITY IMAP4rev1 STARTTLS AUTH=GSSAPI LOGINDISABLED\r\n",
            "* STATUS dev.github (MESSAGES 10 UIDNEXT 11 UIDVALIDITY 1408806928 UNSEEN 0)\r\n",
            "* 4 EXISTS\r\n",
        ]);
        let mut stream = async_std::stream::from_iter(responses);

        let expected_capabilities = &["IMAP4rev1", "STARTTLS", "AUTH=GSSAPI", "LOGINDISABLED"];

        let id = RequestId("A0001".into());
        let capabilities = parse_capabilities(&mut stream, send, id).await.unwrap();

        assert_eq!(capabilities.len(), 4);
        for e in expected_capabilities {
            assert!(capabilities.has_str(e));
        }

        assert_eq!(
            recv.recv().await.unwrap(),
            UnsolicitedResponse::Status {
                mailbox: "dev.github".to_string(),
                attributes: vec![
                    StatusAttribute::Messages(10),
                    StatusAttribute::UidNext(11),
                    StatusAttribute::UidValidity(1408806928),
                    StatusAttribute::Unseen(0)
                ]
            }
        );
        assert_eq!(recv.recv().await.unwrap(), UnsolicitedResponse::Exists(4));
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn parse_ids_w_unilateral() {
        let (send, recv) = bounded(10);
        let responses = input_stream(&[
            "* SEARCH 23 42 4711\r\n",
            "* 1 RECENT\r\n",
            "* STATUS INBOX (MESSAGES 10 UIDNEXT 11 UIDVALIDITY 1408806928 UNSEEN 0)\r\n",
        ]);
        let mut stream = async_std::stream::from_iter(responses);

        let id = RequestId("A0001".into());
        let ids = parse_ids(&mut stream, send, id).await.unwrap();

        assert_eq!(ids, [23, 42, 4711].iter().cloned().collect());

        assert_eq!(recv.recv().await.unwrap(), UnsolicitedResponse::Recent(1));
        assert_eq!(
            recv.recv().await.unwrap(),
            UnsolicitedResponse::Status {
                mailbox: "INBOX".to_string(),
                attributes: vec![
                    StatusAttribute::Messages(10),
                    StatusAttribute::UidNext(11),
                    StatusAttribute::UidValidity(1408806928),
                    StatusAttribute::Unseen(0)
                ]
            }
        );
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn parse_ids_test() {
        let (send, recv) = bounded(10);
        let responses = input_stream(&[
                "* SEARCH 1600 1698 1739 1781 1795 1885 1891 1892 1893 1898 1899 1901 1911 1926 1932 1933 1993 1994 2007 2032 2033 2041 2053 2062 2063 2065 2066 2072 2078 2079 2082 2084 2095 2100 2101 2102 2103 2104 2107 2116 2120 2135 2138 2154 2163 2168 2172 2189 2193 2198 2199 2205 2212 2213 2221 2227 2267 2275 2276 2295 2300 2328 2330 2332 2333 2334\r\n",
                "* SEARCH 2335 2336 2337 2338 2339 2341 2342 2347 2349 2350 2358 2359 2362 2369 2371 2372 2373 2374 2375 2376 2377 2378 2379 2380 2381 2382 2383 2384 2385 2386 2390 2392 2397 2400 2401 2403 2405 2409 2411 2414 2417 2419 2420 2424 2426 2428 2439 2454 2456 2467 2468 2469 2490 2515 2519 2520 2521\r\n",
            ]);
        let mut stream = async_std::stream::from_iter(responses);

        let id = RequestId("A0001".into());
        let ids = parse_ids(&mut stream, send, id).await.unwrap();

        assert!(recv.is_empty());
        let ids: HashSet<u32> = ids.iter().cloned().collect();
        assert_eq!(
            ids,
            [
                1600, 1698, 1739, 1781, 1795, 1885, 1891, 1892, 1893, 1898, 1899, 1901, 1911, 1926,
                1932, 1933, 1993, 1994, 2007, 2032, 2033, 2041, 2053, 2062, 2063, 2065, 2066, 2072,
                2078, 2079, 2082, 2084, 2095, 2100, 2101, 2102, 2103, 2104, 2107, 2116, 2120, 2135,
                2138, 2154, 2163, 2168, 2172, 2189, 2193, 2198, 2199, 2205, 2212, 2213, 2221, 2227,
                2267, 2275, 2276, 2295, 2300, 2328, 2330, 2332, 2333, 2334, 2335, 2336, 2337, 2338,
                2339, 2341, 2342, 2347, 2349, 2350, 2358, 2359, 2362, 2369, 2371, 2372, 2373, 2374,
                2375, 2376, 2377, 2378, 2379, 2380, 2381, 2382, 2383, 2384, 2385, 2386, 2390, 2392,
                2397, 2400, 2401, 2403, 2405, 2409, 2411, 2414, 2417, 2419, 2420, 2424, 2426, 2428,
                2439, 2454, 2456, 2467, 2468, 2469, 2490, 2515, 2519, 2520, 2521
            ]
            .iter()
            .cloned()
            .collect()
        );
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn parse_ids_search() {
        let (send, recv) = bounded(10);
        let responses = input_stream(&["* SEARCH\r\n"]);
        let mut stream = async_std::stream::from_iter(responses);

        let id = RequestId("A0001".into());
        let ids = parse_ids(&mut stream, send, id).await.unwrap();

        assert!(recv.is_empty());
        let ids: HashSet<u32> = ids.iter().cloned().collect();
        assert_eq!(ids, HashSet::<u32>::new());
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn parse_mailbox_does_not_exist_error() {
        let (send, recv) = bounded(10);
        let responses = input_stream(&[
            "A0003 NO Mailbox doesn't exist: DeltaChat (0.001 + 0.140 + 0.139 secs).\r\n",
        ]);
        let mut stream = async_std::stream::from_iter(responses);

        let id = RequestId("A0003".into());
        let mailbox = parse_mailbox(&mut stream, send, id).await;
        assert!(recv.is_empty());

        assert!(matches!(mailbox, Err(Error::No(_))));
    }
}
