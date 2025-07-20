use std::borrow::Cow;
use std::time::Duration;

use anyhow::{Context as _, Result};
use async_imap::Session;
use async_native_tls::TlsConnector;
use async_smtp::{SendableEmail, SmtpClient, SmtpTransport};
#[cfg(feature = "runtime-async-std")]
use async_std::{net::TcpStream, task, task::sleep};
use futures::{StreamExt, TryStreamExt};
#[cfg(feature = "runtime-tokio")]
use tokio::{net::TcpStream, task, time::sleep};

fn tls() -> TlsConnector {
    TlsConnector::new()
        .danger_accept_invalid_hostnames(true)
        .danger_accept_invalid_certs(true)
}

fn test_host() -> String {
    std::env::var("TEST_HOST").unwrap_or_else(|_| "127.0.0.1".into())
}

async fn session(user: &str) -> Result<Session<async_native_tls::TlsStream<TcpStream>>> {
    let host = test_host();
    let addr = (host.as_str(), 3993);
    let tcp_stream = TcpStream::connect(addr).await?;
    let tls = tls();
    let tls_stream = tls.connect("imap.example.com", tcp_stream).await?;

    let mut client = async_imap::Client::new(tls_stream);
    let _greeting = client
        .read_response()
        .await?
        .context("unexpected end of stream, expected greeting")?;

    let session = client
        .login(user, user)
        .await
        .map_err(|(err, _client)| err)?;
    Ok(session)
}

// ignored because of https://github.com/greenmail-mail-test/greenmail/issues/135
async fn _connect_insecure_then_secure() -> Result<()> {
    let tcp_stream = TcpStream::connect((test_host().as_ref(), 3143)).await?;
    let tls = tls();
    let mut client = async_imap::Client::new(tcp_stream);
    let _greeting = client
        .read_response()
        .await?
        .context("unexpected end of stream, expected greeting")?;
    client.run_command_and_check_ok("STARTTLS", None).await?;
    let stream = client.into_inner();
    let tls_stream = tls.connect("imap.example.com", stream).await?;
    let _client = async_imap::Client::new(tls_stream);
    Ok(())
}

async fn smtp(user: &str) -> Result<SmtpTransport<async_native_tls::TlsStream<TcpStream>>> {
    let host = test_host();
    let tcp_stream = TcpStream::connect((host.as_str(), 3465)).await?;

    let tls = tls();
    let tls_stream = tls.connect("localhost", tcp_stream).await?;
    let client = SmtpClient::new().smtp_utf8(true);
    let mut transport = SmtpTransport::new(client, tls_stream).await?;
    let credentials =
        async_smtp::authentication::Credentials::new(user.to_string(), user.to_string());
    let mechanism = vec![
        async_smtp::authentication::Mechanism::Plain,
        async_smtp::authentication::Mechanism::Login,
    ];
    transport.try_login(&credentials, &mechanism).await?;
    Ok(transport)
}

async fn login() -> Result<()> {
    session("readonly-test@localhost").await?;
    Ok(())
}

async fn logout() -> Result<()> {
    let mut s = session("readonly-test@localhost").await?;
    s.logout().await?;
    Ok(())
}

async fn inbox_zero() -> Result<()> {
    // https://github.com/greenmail-mail-test/greenmail/issues/265
    let mut s = session("readonly-test@localhost").await?;
    s.select("INBOX").await?;
    let inbox = s.search("ALL").await?;
    assert_eq!(inbox.len(), 0);
    Ok(())
}

fn make_email(to: &str) -> SendableEmail {
    let message_id = "abc";
    async_smtp::SendableEmail::new(
        async_smtp::Envelope::new(
            Some("sender@localhost".parse().unwrap()),
            vec![to.parse().unwrap()],
        )
        .unwrap(),
        format!("To: <{}>\r\nFrom: <sender@localhost>\r\nMessage-ID: <{}.msg@localhost>\r\nSubject: My first e-mail\r\n\r\nHello world from SMTP", to, message_id),
    )
}

async fn inbox() -> Result<()> {
    let to = "inbox@localhost";

    // first log in so we'll see the unsolicited e-mails
    let mut c = session(to).await?;
    c.select("INBOX").await?;

    println!("sending");
    let mut s = smtp(to).await?;

    // then send the e-mail
    let mail = make_email(to);
    s.send(mail).await?;

    println!("searching");

    // now we should see the e-mail!
    let inbox = c.search("ALL").await?;
    // and the one message should have the first message sequence number
    assert_eq!(inbox.len(), 1);
    assert!(inbox.contains(&1));

    // we should also get two unsolicited responses: Exists and Recent
    c.noop().await.unwrap();
    println!("noop done");
    let mut unsolicited = Vec::new();
    while !c.unsolicited_responses.is_empty() {
        unsolicited.push(c.unsolicited_responses.recv().await.unwrap());
    }

    assert_eq!(unsolicited.len(), 2);
    assert!(unsolicited
        .iter()
        .any(|m| m == &async_imap::types::UnsolicitedResponse::Exists(1)));
    assert!(unsolicited
        .iter()
        .any(|m| m == &async_imap::types::UnsolicitedResponse::Recent(1)));

    println!("fetching");

    // let's see that we can also fetch the e-mail
    let fetch: Vec<_> = c
        .fetch("1", "(ALL UID)")
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(fetch.len(), 1);
    let fetch = &fetch[0];
    assert_eq!(fetch.message, 1);
    assert_ne!(fetch.uid, None);
    assert_eq!(fetch.size, Some(21));
    let e = fetch.envelope().unwrap();
    assert_eq!(e.subject, Some(Cow::Borrowed(&b"My first e-mail"[..])));
    assert_ne!(e.from, None);
    assert_eq!(e.from.as_ref().unwrap().len(), 1);
    let from = &e.from.as_ref().unwrap()[0];
    assert_eq!(from.mailbox, Some(Cow::Borrowed(&b"sender"[..])));
    assert_eq!(from.host, Some(Cow::Borrowed(&b"localhost"[..])));
    assert_ne!(e.to, None);
    assert_eq!(e.to.as_ref().unwrap().len(), 1);
    let to = &e.to.as_ref().unwrap()[0];
    assert_eq!(to.mailbox, Some(Cow::Borrowed(&b"inbox"[..])));
    assert_eq!(to.host, Some(Cow::Borrowed(&b"localhost"[..])));
    let date_opt = fetch.internal_date();
    assert!(date_opt.is_some());

    // and let's delete it to clean up
    c.store("1", "+FLAGS (\\Deleted)")
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    c.expunge().await.unwrap().collect::<Vec<_>>().await;

    // the e-mail should be gone now
    let inbox = c.search("ALL").await?;
    assert_eq!(inbox.len(), 0);
    Ok(())
}

async fn inbox_uid() -> Result<()> {
    let to = "inbox-uid@localhost";

    // first log in so we'll see the unsolicited e-mails
    let mut c = session(to).await?;
    c.select("INBOX").await?;

    // then send the e-mail
    let mut s = smtp(to).await?;
    let e = make_email(to);
    s.send(e).await?;

    // now we should see the e-mail!
    let inbox = c.uid_search("ALL").await?;
    // and the one message should have the first message sequence number
    assert_eq!(inbox.len(), 1);
    let uid = inbox.into_iter().next().unwrap();

    // we should also get two unsolicited responses: Exists and Recent
    c.noop().await?;
    let mut unsolicited = Vec::new();
    while !c.unsolicited_responses.is_empty() {
        unsolicited.push(c.unsolicited_responses.recv().await?);
    }

    assert_eq!(unsolicited.len(), 2);
    assert!(unsolicited
        .iter()
        .any(|m| m == &async_imap::types::UnsolicitedResponse::Exists(1)));
    assert!(unsolicited
        .iter()
        .any(|m| m == &async_imap::types::UnsolicitedResponse::Recent(1)));

    // let's see that we can also fetch the e-mail
    let fetch: Vec<_> = c
        .uid_fetch(format!("{}", uid), "(ALL UID)")
        .await?
        .try_collect()
        .await?;
    assert_eq!(fetch.len(), 1);
    let fetch = &fetch[0];
    assert_eq!(fetch.uid, Some(uid));
    let e = fetch.envelope().unwrap();
    assert_eq!(e.subject, Some(Cow::Borrowed(&b"My first e-mail"[..])));
    let date_opt = fetch.internal_date();
    assert!(date_opt.is_some());

    // and let's delete it to clean up
    c.uid_store(format!("{}", uid), "+FLAGS (\\Deleted)")
        .await?
        .collect::<Vec<_>>()
        .await;
    c.expunge().await?.collect::<Vec<_>>().await;

    // the e-mail should be gone now
    let inbox = c.search("ALL").await?;
    assert_eq!(inbox.len(), 0);
    Ok(())
}

async fn _list() -> Result<()> {
    let mut s = session("readonly-test@localhost").await?;
    s.select("INBOX").await?;
    let subdirs: Vec<_> = s.list(None, Some("%")).await?.collect().await;
    assert_eq!(subdirs.len(), 0);

    // TODO: make a subdir
    Ok(())
}

// Greenmail does not support IDLE :(
async fn _idle() -> Result<()> {
    let mut session = session("idle-test@localhost").await?;

    // get that inbox
    let res = session.select("INBOX").await?;
    println!("selected: {:#?}", res);

    // fetchy fetch
    let msg_stream = session.fetch("1:3", "(FLAGS BODY.PEEK[])").await?;
    let msgs = msg_stream.collect::<Vec<_>>().await;
    println!("msgs: {:?}", msgs.len());

    // Idle session
    println!("starting idle");
    let mut idle = session.idle();
    idle.init().await?;

    let (idle_wait, interrupt) = idle.wait_with_timeout(std::time::Duration::from_secs(30));
    println!("idle wait");

    task::spawn(async move {
        println!("waiting for 1s");
        sleep(Duration::from_secs(2)).await;
        println!("interrupting idle");
        drop(interrupt);
    });

    let idle_result = idle_wait.await;
    println!("idle result: {:#?}", &idle_result);

    // return the session after we are done with it
    let mut session = idle.done().await?;

    println!("logging out");
    session.logout().await?;

    Ok(())
}

#[cfg_attr(feature = "runtime-tokio", tokio::main)]
#[cfg_attr(feature = "runtime-async-std", async_std::main)]
async fn main() -> Result<()> {
    login().await?;
    logout().await?;
    inbox_zero().await?;
    inbox().await?;
    inbox_uid().await?;
    Ok(())
}
