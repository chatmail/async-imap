use std::collections::{HashMap, HashSet};
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::str;

use async_channel::{self as channel, bounded};
#[cfg(feature = "runtime-async-std")]
use async_std::io::{Read, Write, WriteExt};
use base64::Engine as _;
use extensions::id::{format_identification, parse_id};
use extensions::quota::parse_get_quota_root;
use futures::{io, Stream, TryStreamExt};
use imap_proto::{Metadata, RequestId, Response};
#[cfg(feature = "runtime-tokio")]
use tokio::io::{AsyncRead as Read, AsyncWrite as Write, AsyncWriteExt};

use super::authenticator::Authenticator;
use super::error::{Error, ParseError, Result, ValidateError};
use super::parse::*;
use super::types::*;
use crate::extensions::{self, quota::parse_get_quota};
use crate::imap_stream::ImapStream;

macro_rules! quote {
    ($x:expr) => {
        format!("\"{}\"", $x.replace(r"\", r"\\").replace("\"", "\\\""))
    };
}

/// An authenticated IMAP session providing the usual IMAP commands. This type is what you get from
/// a succesful login attempt.
///
/// Note that the server *is* allowed to unilaterally send things to the client for messages in
/// a selected mailbox whose status has changed. See the note on [unilateral server responses
/// in RFC 3501](https://tools.ietf.org/html/rfc3501#section-7). Any such messages are parsed out
/// and sent on `Session::unsolicited_responses`.
// Both `Client` and `Session` deref to [`Connection`](struct.Connection.html), the underlying
// primitives type.
#[derive(Debug)]
pub struct Session<T: Read + Write + Unpin + fmt::Debug> {
    pub(crate) conn: Connection<T>,
    pub(crate) unsolicited_responses_tx: channel::Sender<UnsolicitedResponse>,

    /// Server responses that are not related to the current command. See also the note on
    /// [unilateral server responses in RFC 3501](https://tools.ietf.org/html/rfc3501#section-7).
    pub unsolicited_responses: channel::Receiver<UnsolicitedResponse>,
}

impl<T: Read + Write + Unpin + fmt::Debug> Unpin for Session<T> {}
impl<T: Read + Write + Unpin + fmt::Debug> Unpin for Client<T> {}
impl<T: Read + Write + Unpin + fmt::Debug> Unpin for Connection<T> {}

// Make it possible to access the inner connection and modify its settings, such as read/write
// timeouts.
impl<T: Read + Write + Unpin + fmt::Debug> AsMut<T> for Session<T> {
    fn as_mut(&mut self) -> &mut T {
        self.conn.stream.as_mut()
    }
}

/// An (unauthenticated) handle to talk to an IMAP server.
///
/// This is what you get when first
/// connecting. A succesfull call to [`Client::login`] or [`Client::authenticate`] will return a
/// [`Session`] instance that provides the usual IMAP methods.
// Both `Client` and `Session` deref to [`Connection`](struct.Connection.html), the underlying
// primitives type.
#[derive(Debug)]
pub struct Client<T: Read + Write + Unpin + fmt::Debug> {
    conn: Connection<T>,
}

/// The underlying primitives type. Both `Client`(unauthenticated) and `Session`(after succesful
/// login) use a `Connection` internally for the TCP stream primitives.
#[derive(Debug)]
pub struct Connection<T: Read + Write + Unpin + fmt::Debug> {
    pub(crate) stream: ImapStream<T>,

    /// Manages the request ids.
    pub(crate) request_ids: IdGenerator,
}

// `Deref` instances are so we can make use of the same underlying primitives in `Client` and
// `Session`
impl<T: Read + Write + Unpin + fmt::Debug> Deref for Client<T> {
    type Target = Connection<T>;

    fn deref(&self) -> &Connection<T> {
        &self.conn
    }
}

impl<T: Read + Write + Unpin + fmt::Debug> DerefMut for Client<T> {
    fn deref_mut(&mut self) -> &mut Connection<T> {
        &mut self.conn
    }
}

impl<T: Read + Write + Unpin + fmt::Debug> Deref for Session<T> {
    type Target = Connection<T>;

    fn deref(&self) -> &Connection<T> {
        &self.conn
    }
}

impl<T: Read + Write + Unpin + fmt::Debug> DerefMut for Session<T> {
    fn deref_mut(&mut self) -> &mut Connection<T> {
        &mut self.conn
    }
}

// As the pattern of returning the unauthenticated `Client` (a.k.a. `self`) back with a login error
// is relatively common, it's abstacted away into a macro here.
//
// Note: 1) using `.map_err(|e| (e, self))` or similar here makes the closure own self, so we can't
//          do that.
//       2) in theory we wouldn't need the second parameter, and could just use the identifier
//          `self` from the surrounding function, but being explicit here seems a lot cleaner.
macro_rules! ok_or_unauth_client_err {
    ($r:expr, $self:expr) => {
        match $r {
            Ok(o) => o,
            Err(e) => return Err((e.into(), $self)),
        }
    };
}

impl<T: Read + Write + Unpin + fmt::Debug + Send> Client<T> {
    /// Creates a new client over the given stream.
    ///
    /// This method primarily exists for writing tests that mock the underlying transport, but can
    /// also be used to support IMAP over custom tunnels.
    pub fn new(stream: T) -> Client<T> {
        let stream = ImapStream::new(stream);

        Client {
            conn: Connection {
                stream,
                request_ids: IdGenerator::new(),
            },
        }
    }

    /// Convert this Client into the raw underlying stream.
    pub fn into_inner(self) -> T {
        let Self { conn, .. } = self;
        conn.into_inner()
    }

    /// Log in to the IMAP server. Upon success a [`Session`](struct.Session.html) instance is
    /// returned; on error the original `Client` instance is returned in addition to the error.
    /// This is because `login` takes ownership of `self`, so in order to try again (e.g. after
    /// prompting the user for credetials), ownership of the original `Client` needs to be
    /// transferred back to the caller.
    ///
    /// ```ignore
    /// # fn main() -> async_imap::error::Result<()> {
    /// # async_std::task::block_on(async {
    ///
    /// let tls = async_native_tls::TlsConnector::new();
    /// let client = async_imap::connect(
    ///     ("imap.example.org", 993),
    ///     "imap.example.org",
    ///     tls
    /// ).await?;
    ///
    /// match client.login("user", "pass").await {
    ///     Ok(s) => {
    ///         // you are successfully authenticated!
    ///     },
    ///     Err((e, orig_client)) => {
    ///         eprintln!("error logging in: {}", e);
    ///         // prompt user and try again with orig_client here
    ///         return Err(e);
    ///     }
    /// }
    ///
    /// # Ok(())
    /// # }) }
    /// ```
    pub async fn login<U: AsRef<str>, P: AsRef<str>>(
        mut self,
        username: U,
        password: P,
    ) -> ::std::result::Result<Session<T>, (Error, Client<T>)> {
        let u = ok_or_unauth_client_err!(validate_str(username.as_ref()), self);
        let p = ok_or_unauth_client_err!(validate_str(password.as_ref()), self);
        ok_or_unauth_client_err!(
            self.run_command_and_check_ok(&format!("LOGIN {u} {p}"), None)
                .await,
            self
        );

        Ok(Session::new(self.conn))
    }

    /// Authenticate with the server using the given custom `authenticator` to handle the server's
    /// challenge.
    ///
    /// ```ignore
    /// struct OAuth2 {
    ///     user: String,
    ///     access_token: String,
    /// }
    ///
    /// impl async_imap::Authenticator for &OAuth2 {
    ///     type Response = String;
    ///     fn process(&mut self, _: &[u8]) -> Self::Response {
    ///         format!(
    ///             "user={}\x01auth=Bearer {}\x01\x01",
    ///             self.user, self.access_token
    ///         )
    ///     }
    /// }
    ///
    /// # fn main() -> async_imap::error::Result<()> {
    /// # async_std::task::block_on(async {
    ///
    ///     let auth = OAuth2 {
    ///         user: String::from("me@example.com"),
    ///         access_token: String::from("<access_token>"),
    ///     };
    ///
    ///     let domain = "imap.example.com";
    ///     let tls = async_native_tls::TlsConnector::new();
    ///     let client = async_imap::connect((domain, 993), domain, tls).await?;
    ///     match client.authenticate("XOAUTH2", &auth).await {
    ///         Ok(session) => {
    ///             // you are successfully authenticated!
    ///         },
    ///         Err((err, orig_client)) => {
    ///             eprintln!("error authenticating: {}", err);
    ///             // prompt user and try again with orig_client here
    ///             return Err(err);
    ///         }
    ///     };
    /// # Ok(())
    /// # }) }
    /// ```
    pub async fn authenticate<A: Authenticator, S: AsRef<str>>(
        mut self,
        auth_type: S,
        authenticator: A,
    ) -> ::std::result::Result<Session<T>, (Error, Client<T>)> {
        let id = ok_or_unauth_client_err!(
            self.run_command(&format!("AUTHENTICATE {}", auth_type.as_ref()))
                .await,
            self
        );
        let session = self.do_auth_handshake(id, authenticator).await?;
        Ok(session)
    }

    /// This func does the handshake process once the authenticate command is made.
    async fn do_auth_handshake<A: Authenticator>(
        mut self,
        id: RequestId,
        mut authenticator: A,
    ) -> ::std::result::Result<Session<T>, (Error, Client<T>)> {
        // explicit match blocks neccessary to convert error to tuple and not bind self too
        // early (see also comment on `login`)
        loop {
            let Some(res) = ok_or_unauth_client_err!(self.read_response().await, self) else {
                return Err((Error::ConnectionLost, self));
            };
            match res.parsed() {
                Response::Continue { information, .. } => {
                    let challenge = if let Some(text) = information {
                        ok_or_unauth_client_err!(
                            base64::engine::general_purpose::STANDARD
                                .decode(text.as_ref())
                                .map_err(|e| Error::Parse(ParseError::Authentication(
                                    (*text).to_string(),
                                    Some(e)
                                ))),
                            self
                        )
                    } else {
                        Vec::new()
                    };
                    let raw_response = &mut authenticator.process(&challenge);
                    let auth_response =
                        base64::engine::general_purpose::STANDARD.encode(raw_response);

                    ok_or_unauth_client_err!(
                        self.conn.run_command_untagged(&auth_response).await,
                        self
                    );
                }
                _ => {
                    ok_or_unauth_client_err!(self.check_done_ok_from(&id, None, res).await, self);
                    return Ok(Session::new(self.conn));
                }
            }
        }
    }
}

impl<T: Read + Write + Unpin + fmt::Debug + Send> Session<T> {
    unsafe_pinned!(conn: Connection<T>);

    pub(crate) fn get_stream(self: Pin<&mut Self>) -> Pin<&mut ImapStream<T>> {
        self.conn().stream()
    }

    // not public, just to avoid duplicating the channel creation code
    fn new(conn: Connection<T>) -> Self {
        let (tx, rx) = bounded(100);
        Session {
            conn,
            unsolicited_responses: rx,
            unsolicited_responses_tx: tx,
        }
    }

    /// Selects a mailbox.
    ///
    /// The `SELECT` command selects a mailbox so that messages in the mailbox can be accessed.
    /// Note that earlier versions of this protocol only required the FLAGS, EXISTS, and RECENT
    /// untagged data; consequently, client implementations SHOULD implement default behavior for
    /// missing data as discussed with the individual item.
    ///
    /// Only one mailbox can be selected at a time in a connection; simultaneous access to multiple
    /// mailboxes requires multiple connections.  The `SELECT` command automatically deselects any
    /// currently selected mailbox before attempting the new selection. Consequently, if a mailbox
    /// is selected and a `SELECT` command that fails is attempted, no mailbox is selected.
    ///
    /// Note that the server *is* allowed to unilaterally send things to the client for messages in
    /// a selected mailbox whose status has changed. See the note on [unilateral server responses
    /// in RFC 3501](https://tools.ietf.org/html/rfc3501#section-7). This means that if run commands,
    /// you *may* see additional untagged `RECENT`, `EXISTS`, `FETCH`, and `EXPUNGE` responses.
    /// You can get them from the `unsolicited_responses` channel of the [`Session`](struct.Session.html).
    pub async fn select<S: AsRef<str>>(&mut self, mailbox_name: S) -> Result<Mailbox> {
        // TODO: also note READ/WRITE vs READ-only mode!
        let id = self
            .run_command(&format!("SELECT {}", validate_str(mailbox_name.as_ref())?))
            .await?;
        let mbox = parse_mailbox(
            &mut self.conn.stream,
            self.unsolicited_responses_tx.clone(),
            id,
        )
        .await?;

        Ok(mbox)
    }

    /// Selects a mailbox with `(CONDSTORE)` parameter as defined in
    /// [RFC 7162](https://www.rfc-editor.org/rfc/rfc7162.html#section-3.1.8).
    pub async fn select_condstore<S: AsRef<str>>(&mut self, mailbox_name: S) -> Result<Mailbox> {
        let id = self
            .run_command(&format!(
                "SELECT {} (CONDSTORE)",
                validate_str(mailbox_name.as_ref())?
            ))
            .await?;
        let mbox = parse_mailbox(
            &mut self.conn.stream,
            self.unsolicited_responses_tx.clone(),
            id,
        )
        .await?;

        Ok(mbox)
    }

    /// The `EXAMINE` command is identical to [`Session::select`] and returns the same output;
    /// however, the selected mailbox is identified as read-only. No changes to the permanent state
    /// of the mailbox, including per-user state, will happen in a mailbox opened with `examine`;
    /// in particular, messagess cannot lose [`Flag::Recent`] in an examined mailbox.
    pub async fn examine<S: AsRef<str>>(&mut self, mailbox_name: S) -> Result<Mailbox> {
        let id = self
            .run_command(&format!("EXAMINE {}", validate_str(mailbox_name.as_ref())?))
            .await?;
        let mbox = parse_mailbox(
            &mut self.conn.stream,
            self.unsolicited_responses_tx.clone(),
            id,
        )
        .await?;

        Ok(mbox)
    }

    /// Fetch retreives data associated with a set of messages in the mailbox.
    ///
    /// Note that the server *is* allowed to unilaterally include `FETCH` responses for other
    /// messages in the selected mailbox whose status has changed. See the note on [unilateral
    /// server responses in RFC 3501](https://tools.ietf.org/html/rfc3501#section-7).
    ///
    /// `query` is a list of "data items" (space-separated in parentheses if `>1`). There are three
    /// "macro items" which specify commonly-used sets of data items, and can be used instead of
    /// data items.  A macro must be used by itself, and not in conjunction with other macros or
    /// data items. They are:
    ///
    ///  - `ALL`: equivalent to: `(FLAGS INTERNALDATE RFC822.SIZE ENVELOPE)`
    ///  - `FAST`: equivalent to: `(FLAGS INTERNALDATE RFC822.SIZE)`
    ///
    /// The currently defined data items that can be fetched are listen [in the
    /// RFC](https://tools.ietf.org/html/rfc3501#section-6.4.5), but here are some common ones:
    ///
    ///  - `FLAGS`: The flags that are set for this message.
    ///  - `INTERNALDATE`: The internal date of the message.
    ///  - `BODY[<section>]`:
    ///
    ///    The text of a particular body section.  The section specification is a set of zero or
    ///    more part specifiers delimited by periods.  A part specifier is either a part number
    ///    (see RFC) or one of the following: `HEADER`, `HEADER.FIELDS`, `HEADER.FIELDS.NOT`,
    ///    `MIME`, and `TEXT`.  An empty section specification (i.e., `BODY[]`) refers to the
    ///    entire message, including the header.
    ///
    ///    The `HEADER`, `HEADER.FIELDS`, and `HEADER.FIELDS.NOT` part specifiers refer to the
    ///    [RFC-2822](https://tools.ietf.org/html/rfc2822) header of the message or of an
    ///    encapsulated [MIME-IMT](https://tools.ietf.org/html/rfc2046)
    ///    MESSAGE/[RFC822](https://tools.ietf.org/html/rfc822) message. `HEADER.FIELDS` and
    ///    `HEADER.FIELDS.NOT` are followed by a list of field-name (as defined in
    ///    [RFC-2822](https://tools.ietf.org/html/rfc2822)) names, and return a subset of the
    ///    header.  The subset returned by `HEADER.FIELDS` contains only those header fields with
    ///    a field-name that matches one of the names in the list; similarly, the subset returned
    ///    by `HEADER.FIELDS.NOT` contains only the header fields with a non-matching field-name.
    ///    The field-matching is case-insensitive but otherwise exact.  Subsetting does not
    ///    exclude the [RFC-2822](https://tools.ietf.org/html/rfc2822) delimiting blank line
    ///    between the header and the body; the blank line is included in all header fetches,
    ///    except in the case of a message which has no body and no blank line.
    ///
    ///    The `MIME` part specifier refers to the [MIME-IMB](https://tools.ietf.org/html/rfc2045)
    ///    header for this part.
    ///
    ///    The `TEXT` part specifier refers to the text body of the message,
    ///    omitting the [RFC-2822](https://tools.ietf.org/html/rfc2822) header.
    ///
    ///    [`Flag::Seen`] is implicitly set when `BODY` is fetched; if this causes the flags to
    ///    change, they will generally be included as part of the `FETCH` responses.
    ///  - `BODY.PEEK[<section>]`: An alternate form of `BODY[<section>]` that does not implicitly
    ///    set [`Flag::Seen`].
    ///  - `ENVELOPE`: The envelope structure of the message.  This is computed by the server by
    ///    parsing the [RFC-2822](https://tools.ietf.org/html/rfc2822) header into the component
    ///    parts, defaulting various fields as necessary.
    ///  - `RFC822`: Functionally equivalent to `BODY[]`.
    ///  - `RFC822.HEADER`: Functionally equivalent to `BODY.PEEK[HEADER]`.
    ///  - `RFC822.SIZE`: The [RFC-2822](https://tools.ietf.org/html/rfc2822) size of the message.
    ///  - `UID`: The unique identifier for the message.
    pub async fn fetch<S1, S2>(
        &mut self,
        sequence_set: S1,
        query: S2,
    ) -> Result<impl Stream<Item = Result<Fetch>> + '_ + Send>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        let id = self
            .run_command(&format!(
                "FETCH {} {}",
                sequence_set.as_ref(),
                query.as_ref()
            ))
            .await?;
        let res = parse_fetches(
            &mut self.conn.stream,
            self.unsolicited_responses_tx.clone(),
            id,
        );

        Ok(res)
    }

    /// Equivalent to [`Session::fetch`], except that all identifiers in `uid_set` are
    /// [`Uid`]s. See also the [`UID` command](https://tools.ietf.org/html/rfc3501#section-6.4.8).
    pub async fn uid_fetch<S1, S2>(
        &mut self,
        uid_set: S1,
        query: S2,
    ) -> Result<impl Stream<Item = Result<Fetch>> + '_ + Send + Unpin>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        let id = self
            .run_command(&format!(
                "UID FETCH {} {}",
                uid_set.as_ref(),
                query.as_ref()
            ))
            .await?;
        let res = parse_fetches(
            &mut self.conn.stream,
            self.unsolicited_responses_tx.clone(),
            id,
        );
        Ok(res)
    }

    /// Noop always succeeds, and it does nothing.
    pub async fn noop(&mut self) -> Result<()> {
        let id = self.run_command("NOOP").await?;
        parse_noop(
            &mut self.conn.stream,
            self.unsolicited_responses_tx.clone(),
            id,
        )
        .await?;
        Ok(())
    }

    /// Logout informs the server that the client is done with the connection.
    pub async fn logout(&mut self) -> Result<()> {
        self.run_command_and_check_ok("LOGOUT").await?;
        Ok(())
    }

    /// The [`CREATE` command](https://tools.ietf.org/html/rfc3501#section-6.3.3) creates a mailbox
    /// with the given name.  `Ok` is returned only if a new mailbox with that name has been
    /// created.  It is an error to attempt to create `INBOX` or a mailbox with a name that
    /// refers to an extant mailbox.  Any error in creation will return [`Error::No`].
    ///
    /// If the mailbox name is suffixed with the server's hierarchy separator character (as
    /// returned from the server by [`Session::list`]), this is a declaration that the client
    /// intends to create mailbox names under this name in the hierarchy.  Servers that do not
    /// require this declaration will ignore the declaration.  In any case, the name created is
    /// without the trailing hierarchy delimiter.
    ///
    /// If the server's hierarchy separator character appears elsewhere in the name, the server
    /// will generally create any superior hierarchical names that are needed for the `CREATE`
    /// command to be successfully completed.  In other words, an attempt to create `foo/bar/zap`
    /// on a server in which `/` is the hierarchy separator character will usually create `foo/`
    /// and `foo/bar/` if they do not already exist.
    ///
    /// If a new mailbox is created with the same name as a mailbox which was deleted, its unique
    /// identifiers will be greater than any unique identifiers used in the previous incarnation of
    /// the mailbox UNLESS the new incarnation has a different unique identifier validity value.
    /// See the description of the [`UID`
    /// command](https://tools.ietf.org/html/rfc3501#section-6.4.8) for more detail.
    pub async fn create<S: AsRef<str>>(&mut self, mailbox_name: S) -> Result<()> {
        self.run_command_and_check_ok(&format!("CREATE {}", validate_str(mailbox_name.as_ref())?))
            .await?;

        Ok(())
    }

    /// The [`DELETE` command](https://tools.ietf.org/html/rfc3501#section-6.3.4) permanently
    /// removes the mailbox with the given name.  `Ok` is returned only if the mailbox has been
    /// deleted.  It is an error to attempt to delete `INBOX` or a mailbox name that does not
    /// exist.
    ///
    /// The `DELETE` command will not remove inferior hierarchical names. For example, if a mailbox
    /// `foo` has an inferior `foo.bar` (assuming `.` is the hierarchy delimiter character),
    /// removing `foo` will not remove `foo.bar`.  It is an error to attempt to delete a name that
    /// has inferior hierarchical names and also has [`NameAttribute::NoSelect`].
    ///
    /// It is permitted to delete a name that has inferior hierarchical names and does not have
    /// [`NameAttribute::NoSelect`].  In this case, all messages in that mailbox are removed, and
    /// the name will acquire [`NameAttribute::NoSelect`].
    ///
    /// The value of the highest-used unique identifier of the deleted mailbox will be preserved so
    /// that a new mailbox created with the same name will not reuse the identifiers of the former
    /// incarnation, UNLESS the new incarnation has a different unique identifier validity value.
    /// See the description of the [`UID`
    /// command](https://tools.ietf.org/html/rfc3501#section-6.4.8) for more detail.
    pub async fn delete<S: AsRef<str>>(&mut self, mailbox_name: S) -> Result<()> {
        self.run_command_and_check_ok(&format!("DELETE {}", validate_str(mailbox_name.as_ref())?))
            .await?;

        Ok(())
    }

    /// The [`RENAME` command](https://tools.ietf.org/html/rfc3501#section-6.3.5) changes the name
    /// of a mailbox.  `Ok` is returned only if the mailbox has been renamed.  It is an error to
    /// attempt to rename from a mailbox name that does not exist or to a mailbox name that already
    /// exists.  Any error in renaming will return [`Error::No`].
    ///
    /// If the name has inferior hierarchical names, then the inferior hierarchical names will also
    /// be renamed.  For example, a rename of `foo` to `zap` will rename `foo/bar` (assuming `/` is
    /// the hierarchy delimiter character) to `zap/bar`.
    ///
    /// If the server's hierarchy separator character appears in the name, the server will
    /// generally create any superior hierarchical names that are needed for the `RENAME` command
    /// to complete successfully.  In other words, an attempt to rename `foo/bar/zap` to
    /// `baz/rag/zowie` on a server in which `/` is the hierarchy separator character will
    /// generally create `baz/` and `baz/rag/` if they do not already exist.
    ///
    /// The value of the highest-used unique identifier of the old mailbox name will be preserved
    /// so that a new mailbox created with the same name will not reuse the identifiers of the
    /// former incarnation, UNLESS the new incarnation has a different unique identifier validity
    /// value. See the description of the [`UID`
    /// command](https://tools.ietf.org/html/rfc3501#section-6.4.8) for more detail.
    ///
    /// Renaming `INBOX` is permitted, and has special behavior.  It moves all messages in `INBOX`
    /// to a new mailbox with the given name, leaving `INBOX` empty.  If the server implementation
    /// supports inferior hierarchical names of `INBOX`, these are unaffected by a rename of
    /// `INBOX`.
    pub async fn rename<S1: AsRef<str>, S2: AsRef<str>>(&mut self, from: S1, to: S2) -> Result<()> {
        self.run_command_and_check_ok(&format!(
            "RENAME {} {}",
            quote!(from.as_ref()),
            quote!(to.as_ref())
        ))
        .await?;

        Ok(())
    }

    /// The [`SUBSCRIBE` command](https://tools.ietf.org/html/rfc3501#section-6.3.6) adds the
    /// specified mailbox name to the server's set of "active" or "subscribed" mailboxes as
    /// returned by [`Session::lsub`].  This command returns `Ok` only if the subscription is
    /// successful.
    ///
    /// The server may validate the mailbox argument to `SUBSCRIBE` to verify that it exists.
    /// However, it will not unilaterally remove an existing mailbox name from the subscription
    /// list even if a mailbox by that name no longer exists.
    pub async fn subscribe<S: AsRef<str>>(&mut self, mailbox: S) -> Result<()> {
        self.run_command_and_check_ok(&format!("SUBSCRIBE {}", quote!(mailbox.as_ref())))
            .await?;
        Ok(())
    }

    /// The [`UNSUBSCRIBE` command](https://tools.ietf.org/html/rfc3501#section-6.3.7) removes the
    /// specified mailbox name from the server's set of "active" or "subscribed" mailboxes as
    /// returned by [`Session::lsub`].  This command returns `Ok` only if the unsubscription is
    /// successful.
    pub async fn unsubscribe<S: AsRef<str>>(&mut self, mailbox: S) -> Result<()> {
        self.run_command_and_check_ok(&format!("UNSUBSCRIBE {}", quote!(mailbox.as_ref())))
            .await?;
        Ok(())
    }

    /// The [`CAPABILITY` command](https://tools.ietf.org/html/rfc3501#section-6.1.1) requests a
    /// listing of capabilities that the server supports.  The server will include "IMAP4rev1" as
    /// one of the listed capabilities. See [`Capabilities`] for further details.
    pub async fn capabilities(&mut self) -> Result<Capabilities> {
        let id = self.run_command("CAPABILITY").await?;
        let c = parse_capabilities(
            &mut self.conn.stream,
            self.unsolicited_responses_tx.clone(),
            id,
        )
        .await?;
        Ok(c)
    }

    /// The [`EXPUNGE` command](https://tools.ietf.org/html/rfc3501#section-6.4.3) permanently
    /// removes all messages that have [`Flag::Deleted`] set from the currently selected mailbox.
    /// The message sequence number of each message that is removed is returned.
    pub async fn expunge(&mut self) -> Result<impl Stream<Item = Result<Seq>> + '_ + Send> {
        let id = self.run_command("EXPUNGE").await?;
        let res = parse_expunge(
            &mut self.conn.stream,
            self.unsolicited_responses_tx.clone(),
            id,
        );
        Ok(res)
    }

    /// The [`UID EXPUNGE` command](https://tools.ietf.org/html/rfc4315#section-2.1) permanently
    /// removes all messages that both have [`Flag::Deleted`] set and have a [`Uid`] that is
    /// included in the specified sequence set from the currently selected mailbox.  If a message
    /// either does not have [`Flag::Deleted`] set or has a [`Uid`] that is not included in the
    /// specified sequence set, it is not affected.
    ///
    /// This command is particularly useful for disconnected use clients. By using `uid_expunge`
    /// instead of [`Self::expunge`] when resynchronizing with the server, the client can ensure that it
    /// does not inadvertantly remove any messages that have been marked as [`Flag::Deleted`] by
    /// other clients between the time that the client was last connected and the time the client
    /// resynchronizes.
    ///
    /// This command requires that the server supports [RFC
    /// 4315](https://tools.ietf.org/html/rfc4315) as indicated by the `UIDPLUS` capability (see
    /// [`Session::capabilities`]). If the server does not support the `UIDPLUS` capability, the
    /// client should fall back to using [`Session::store`] to temporarily remove [`Flag::Deleted`]
    /// from messages it does not want to remove, then invoking [`Session::expunge`].  Finally, the
    /// client should use [`Session::store`] to restore [`Flag::Deleted`] on the messages in which
    /// it was temporarily removed.
    ///
    /// Alternatively, the client may fall back to using just [`Session::expunge`], risking the
    /// unintended removal of some messages.
    pub async fn uid_expunge<S: AsRef<str>>(
        &mut self,
        uid_set: S,
    ) -> Result<impl Stream<Item = Result<Uid>> + '_ + Send> {
        let id = self
            .run_command(&format!("UID EXPUNGE {}", uid_set.as_ref()))
            .await?;
        let res = parse_expunge(
            &mut self.conn.stream,
            self.unsolicited_responses_tx.clone(),
            id,
        );
        Ok(res)
    }

    /// The [`CHECK` command](https://tools.ietf.org/html/rfc3501#section-6.4.1) requests a
    /// checkpoint of the currently selected mailbox.  A checkpoint refers to any
    /// implementation-dependent housekeeping associated with the mailbox (e.g., resolving the
    /// server's in-memory state of the mailbox with the state on its disk) that is not normally
    /// executed as part of each command.  A checkpoint MAY take a non-instantaneous amount of real
    /// time to complete.  If a server implementation has no such housekeeping considerations,
    /// [`Session::check`] is equivalent to [`Session::noop`].
    ///
    /// There is no guarantee that an `EXISTS` untagged response will happen as a result of
    /// `CHECK`.  [`Session::noop`] SHOULD be used for new message polling.
    pub async fn check(&mut self) -> Result<()> {
        self.run_command_and_check_ok("CHECK").await?;
        Ok(())
    }

    /// The [`CLOSE` command](https://tools.ietf.org/html/rfc3501#section-6.4.2) permanently
    /// removes all messages that have [`Flag::Deleted`] set from the currently selected mailbox,
    /// and returns to the authenticated state from the selected state.  No `EXPUNGE` responses are
    /// sent.
    ///
    /// No messages are removed, and no error is given, if the mailbox is selected by
    /// [`Session::examine`] or is otherwise selected read-only.
    ///
    /// Even if a mailbox is selected, [`Session::select`], [`Session::examine`], or
    /// [`Session::logout`] command MAY be issued without previously invoking [`Session::close`].
    /// [`Session::select`], [`Session::examine`], and [`Session::logout`] implicitly close the
    /// currently selected mailbox without doing an expunge.  However, when many messages are
    /// deleted, a `CLOSE-LOGOUT` or `CLOSE-SELECT` sequence is considerably faster than an
    /// `EXPUNGE-LOGOUT` or `EXPUNGE-SELECT` because no `EXPUNGE` responses (which the client would
    /// probably ignore) are sent.
    pub async fn close(&mut self) -> Result<()> {
        self.run_command_and_check_ok("CLOSE").await?;
        Ok(())
    }

    /// The [`STORE` command](https://tools.ietf.org/html/rfc3501#section-6.4.6) alters data
    /// associated with a message in the mailbox.  Normally, `STORE` will return the updated value
    /// of the data with an untagged FETCH response.  A suffix of `.SILENT` in `query` prevents the
    /// untagged `FETCH`, and the server assumes that the client has determined the updated value
    /// itself or does not care about the updated value.
    ///
    /// The currently defined data items that can be stored are:
    ///
    ///  - `FLAGS <flag list>`:
    ///
    ///    Replace the flags for the message (other than [`Flag::Recent`]) with the argument.  The
    ///    new value of the flags is returned as if a `FETCH` of those flags was done.
    ///
    ///  - `FLAGS.SILENT <flag list>`: Equivalent to `FLAGS`, but without returning a new value.
    ///
    ///  - `+FLAGS <flag list>`
    ///
    ///    Add the argument to the flags for the message.  The new value of the flags is returned
    ///    as if a `FETCH` of those flags was done.
    ///  - `+FLAGS.SILENT <flag list>`: Equivalent to `+FLAGS`, but without returning a new value.
    ///
    ///  - `-FLAGS <flag list>`
    ///
    ///    Remove the argument from the flags for the message.  The new value of the flags is
    ///    returned as if a `FETCH` of those flags was done.
    ///
    ///  - `-FLAGS.SILENT <flag list>`: Equivalent to `-FLAGS`, but without returning a new value.
    ///
    /// In all cases, `<flag list>` is a space-separated list enclosed in parentheses.
    ///
    /// # Examples
    ///
    /// Delete a message:
    ///
    /// ```no_run
    /// use async_imap::{types::Seq, Session, error::Result};
    /// #[cfg(feature = "runtime-async-std")]
    /// use async_std::net::TcpStream;
    /// #[cfg(feature = "runtime-tokio")]
    /// use tokio::net::TcpStream;
    /// use futures::TryStreamExt;
    ///
    /// async fn delete(seq: Seq, s: &mut Session<TcpStream>) -> Result<()> {
    ///     let updates_stream = s.store(format!("{}", seq), "+FLAGS (\\Deleted)").await?;
    ///     let _updates: Vec<_> = updates_stream.try_collect().await?;
    ///     s.expunge().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn store<S1, S2>(
        &mut self,
        sequence_set: S1,
        query: S2,
    ) -> Result<impl Stream<Item = Result<Fetch>> + '_ + Send>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        let id = self
            .run_command(&format!(
                "STORE {} {}",
                sequence_set.as_ref(),
                query.as_ref()
            ))
            .await?;
        let res = parse_fetches(
            &mut self.conn.stream,
            self.unsolicited_responses_tx.clone(),
            id,
        );
        Ok(res)
    }

    /// Equivalent to [`Session::store`], except that all identifiers in `sequence_set` are
    /// [`Uid`]s. See also the [`UID` command](https://tools.ietf.org/html/rfc3501#section-6.4.8).
    pub async fn uid_store<S1, S2>(
        &mut self,
        uid_set: S1,
        query: S2,
    ) -> Result<impl Stream<Item = Result<Fetch>> + '_ + Send>
    where
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        let id = self
            .run_command(&format!(
                "UID STORE {} {}",
                uid_set.as_ref(),
                query.as_ref()
            ))
            .await?;
        let res = parse_fetches(
            &mut self.conn.stream,
            self.unsolicited_responses_tx.clone(),
            id,
        );
        Ok(res)
    }

    /// The [`COPY` command](https://tools.ietf.org/html/rfc3501#section-6.4.7) copies the
    /// specified message(s) to the end of the specified destination mailbox.  The flags and
    /// internal date of the message(s) will generally be preserved, and [`Flag::Recent`] will
    /// generally be set, in the copy.
    ///
    /// If the `COPY` command is unsuccessful for any reason, the server restores the destination
    /// mailbox to its state before the `COPY` attempt.
    pub async fn copy<S1: AsRef<str>, S2: AsRef<str>>(
        &mut self,
        sequence_set: S1,
        mailbox_name: S2,
    ) -> Result<()> {
        self.run_command_and_check_ok(&format!(
            "COPY {} {}",
            sequence_set.as_ref(),
            mailbox_name.as_ref()
        ))
        .await?;

        Ok(())
    }

    /// Equivalent to [`Session::copy`], except that all identifiers in `sequence_set` are
    /// [`Uid`]s. See also the [`UID` command](https://tools.ietf.org/html/rfc3501#section-6.4.8).
    pub async fn uid_copy<S1: AsRef<str>, S2: AsRef<str>>(
        &mut self,
        uid_set: S1,
        mailbox_name: S2,
    ) -> Result<()> {
        self.run_command_and_check_ok(&format!(
            "UID COPY {} {}",
            uid_set.as_ref(),
            mailbox_name.as_ref()
        ))
        .await?;

        Ok(())
    }

    /// The [`MOVE` command](https://tools.ietf.org/html/rfc6851#section-3.1) takes two
    /// arguments: a sequence set and a named mailbox. Each message included in the set is moved,
    /// rather than copied, from the selected (source) mailbox to the named (target) mailbox.
    ///
    /// This means that a new message is created in the target mailbox with a
    /// new [`Uid`], the original message is removed from the source mailbox, and
    /// it appears to the client as a single action.  This has the same
    /// effect for each message as this sequence:
    ///
    ///   1. COPY
    ///   2. STORE +FLAGS.SILENT \DELETED
    ///   3. EXPUNGE
    ///
    /// This command requires that the server supports [RFC
    /// 6851](https://tools.ietf.org/html/rfc6851) as indicated by the `MOVE` capability (see
    /// [`Session::capabilities`]).
    ///
    /// Although the effect of the `MOVE` is the same as the preceding steps, the semantics are not
    /// identical: The intermediate states produced by those steps do not occur, and the response
    /// codes are different.  In particular, though the `COPY` and `EXPUNGE` response codes will be
    /// returned, response codes for a `store` will not be generated and [`Flag::Deleted`] will not
    /// be set for any message.
    ///
    /// Because a `MOVE` applies to a set of messages, it might fail partway through the set.
    /// Regardless of whether the command is successful in moving the entire set, each individual
    /// message will either be moved or unaffected.  The server will leave each message in a state
    /// where it is in at least one of the source or target mailboxes (no message can be lost or
    /// orphaned).  The server will generally not leave any message in both mailboxes (it would be
    /// bad for a partial failure to result in a bunch of duplicate messages).  This is true even
    /// if the server returns with [`Error::No`].
    pub async fn mv<S1: AsRef<str>, S2: AsRef<str>>(
        &mut self,
        sequence_set: S1,
        mailbox_name: S2,
    ) -> Result<()> {
        self.run_command_and_check_ok(&format!(
            "MOVE {} {}",
            sequence_set.as_ref(),
            validate_str(mailbox_name.as_ref())?
        ))
        .await?;

        Ok(())
    }

    /// Equivalent to [`Session::copy`], except that all identifiers in `sequence_set` are
    /// [`Uid`]s. See also the [`UID` command](https://tools.ietf.org/html/rfc3501#section-6.4.8)
    /// and the [semantics of `MOVE` and `UID
    /// MOVE`](https://tools.ietf.org/html/rfc6851#section-3.3).
    pub async fn uid_mv<S1: AsRef<str>, S2: AsRef<str>>(
        &mut self,
        uid_set: S1,
        mailbox_name: S2,
    ) -> Result<()> {
        self.run_command_and_check_ok(&format!(
            "UID MOVE {} {}",
            uid_set.as_ref(),
            validate_str(mailbox_name.as_ref())?
        ))
        .await?;

        Ok(())
    }

    /// The [`LIST` command](https://tools.ietf.org/html/rfc3501#section-6.3.8) returns a subset of
    /// names from the complete set of all names available to the client.  It returns the name
    /// attributes, hierarchy delimiter, and name of each such name; see [`Name`] for more detail.
    ///
    /// If `reference_name` is `None` (or `""`), the currently selected mailbox is used.
    /// The returned mailbox names must match the supplied `mailbox_pattern`.  A non-empty
    /// reference name argument is the name of a mailbox or a level of mailbox hierarchy, and
    /// indicates the context in which the mailbox name is interpreted.
    ///
    /// If `mailbox_pattern` is `None` (or `""`), it is a special request to return the hierarchy
    /// delimiter and the root name of the name given in the reference.  The value returned as the
    /// root MAY be the empty string if the reference is non-rooted or is an empty string.  In all
    /// cases, a hierarchy delimiter (or `NIL` if there is no hierarchy) is returned.  This permits
    /// a client to get the hierarchy delimiter (or find out that the mailbox names are flat) even
    /// when no mailboxes by that name currently exist.
    ///
    /// The reference and mailbox name arguments are interpreted into a canonical form that
    /// represents an unambiguous left-to-right hierarchy.  The returned mailbox names will be in
    /// the interpreted form.
    ///
    /// The character `*` is a wildcard, and matches zero or more characters at this position.  The
    /// character `%` is similar to `*`, but it does not match a hierarchy delimiter.  If the `%`
    /// wildcard is the last character of a mailbox name argument, matching levels of hierarchy are
    /// also returned.  If these levels of hierarchy are not also selectable mailboxes, they are
    /// returned with [`NameAttribute::NoSelect`].
    ///
    /// The special name `INBOX` is included if `INBOX` is supported by this server for this user
    /// and if the uppercase string `INBOX` matches the interpreted reference and mailbox name
    /// arguments with wildcards.  The criteria for omitting `INBOX` is whether `SELECT INBOX` will
    /// return failure; it is not relevant whether the user's real `INBOX` resides on this or some
    /// other server.
    pub async fn list(
        &mut self,
        reference_name: Option<&str>,
        mailbox_pattern: Option<&str>,
    ) -> Result<impl Stream<Item = Result<Name>> + '_ + Send> {
        let id = self
            .run_command(&format!(
                "LIST {} {}",
                quote!(reference_name.unwrap_or("")),
                mailbox_pattern.unwrap_or("\"\"")
            ))
            .await?;
        let names = parse_names(
            &mut self.conn.stream,
            self.unsolicited_responses_tx.clone(),
            id,
        );

        Ok(names)
    }

    /// The [`LSUB` command](https://tools.ietf.org/html/rfc3501#section-6.3.9) returns a subset of
    /// names from the set of names that the user has declared as being "active" or "subscribed".
    /// The arguments to this method the same as for [`Session::list`].
    ///
    /// The returned [`Name`]s MAY contain different mailbox flags from response to
    /// [`Session::list`].  If this should happen, the flags returned by [`Session::list`] are
    /// considered more authoritative.
    ///
    /// A special situation occurs when invoking `lsub` with the `%` wildcard. Consider what
    /// happens if `foo/bar` (with a hierarchy delimiter of `/`) is subscribed but `foo` is not.  A
    /// `%` wildcard to `lsub` must return `foo`, not `foo/bar`, and it will be flagged with
    /// [`NameAttribute::NoSelect`].
    ///
    /// The server will not unilaterally remove an existing mailbox name from the subscription list
    /// even if a mailbox by that name no longer exists.
    pub async fn lsub(
        &mut self,
        reference_name: Option<&str>,
        mailbox_pattern: Option<&str>,
    ) -> Result<impl Stream<Item = Result<Name>> + '_ + Send> {
        let id = self
            .run_command(&format!(
                "LSUB {} {}",
                quote!(reference_name.unwrap_or("")),
                mailbox_pattern.unwrap_or("")
            ))
            .await?;
        let names = parse_names(
            &mut self.conn.stream,
            self.unsolicited_responses_tx.clone(),
            id,
        );

        Ok(names)
    }

    /// The [`STATUS` command](https://tools.ietf.org/html/rfc3501#section-6.3.10) requests the
    /// status of the indicated mailbox. It does not change the currently selected mailbox, nor
    /// does it affect the state of any messages in the queried mailbox (in particular, `status`
    /// will not cause messages to lose [`Flag::Recent`]).
    ///
    /// `status` provides an alternative to opening a second [`Session`] and using
    /// [`Session::examine`] on a mailbox to query that mailbox's status without deselecting the
    /// current mailbox in the first `Session`.
    ///
    /// Unlike [`Session::list`], `status` is not guaranteed to be fast in its response.  Under
    /// certain circumstances, it can be quite slow.  In some implementations, the server is
    /// obliged to open the mailbox read-only internally to obtain certain status information.
    /// Also unlike [`Session::list`], `status` does not accept wildcards.
    ///
    /// > Note: `status` is intended to access the status of mailboxes other than the currently
    /// > selected mailbox.  Because `status` can cause the mailbox to be opened internally, and
    /// > because this information is available by other means on the selected mailbox, `status`
    /// > SHOULD NOT be used on the currently selected mailbox.
    ///
    /// The STATUS command MUST NOT be used as a "check for new messages in the selected mailbox"
    /// operation (refer to sections [7](https://tools.ietf.org/html/rfc3501#section-7),
    /// [7.3.1](https://tools.ietf.org/html/rfc3501#section-7.3.1), and
    /// [7.3.2](https://tools.ietf.org/html/rfc3501#section-7.3.2) for more information about the
    /// proper method for new message checking).
    ///
    /// The currently defined status data items that can be requested are:
    ///
    ///  - `MESSAGES`: The number of messages in the mailbox.
    ///  - `RECENT`: The number of messages with [`Flag::Recent`] set.
    ///  - `UIDNEXT`: The next [`Uid`] of the mailbox.
    ///  - `UIDVALIDITY`: The unique identifier validity value of the mailbox (see [`Uid`]).
    ///  - `UNSEEN`: The number of messages which do not have [`Flag::Seen`] set.
    ///
    /// `data_items` is a space-separated list enclosed in parentheses.
    pub async fn status<S1: AsRef<str>, S2: AsRef<str>>(
        &mut self,
        mailbox_name: S1,
        data_items: S2,
    ) -> Result<Mailbox> {
        let id = self
            .run_command(&format!(
                "STATUS {} {}",
                validate_str(mailbox_name.as_ref())?,
                data_items.as_ref()
            ))
            .await?;
        let mbox = parse_status(
            &mut self.conn.stream,
            mailbox_name.as_ref(),
            self.unsolicited_responses_tx.clone(),
            id,
        )
        .await?;
        Ok(mbox)
    }

    /// This method returns a handle that lets you use the [`IDLE`
    /// command](https://tools.ietf.org/html/rfc2177#section-3) to listen for changes to the
    /// currently selected mailbox.
    ///
    /// It's often more desirable to have the server transmit updates to the client in real time.
    /// This allows a user to see new mail immediately.  It also helps some real-time applications
    /// based on IMAP, which might otherwise need to poll extremely often (such as every few
    /// seconds).  While the spec actually does allow a server to push `EXISTS` responses
    /// aysynchronously, a client can't expect this behaviour and must poll.  This method provides
    /// you with such a mechanism.
    ///
    /// `idle` may be used with any server that returns `IDLE` as one of the supported capabilities
    /// (see [`Session::capabilities`]). If the server does not advertise the `IDLE` capability,
    /// the client MUST NOT use `idle` and must instead poll for mailbox updates.  In particular,
    /// the client MUST continue to be able to accept unsolicited untagged responses to ANY
    /// command, as specified in the base IMAP specification.
    ///
    /// See [`extensions::idle::Handle`] for details.
    pub fn idle(self) -> extensions::idle::Handle<T> {
        extensions::idle::Handle::new(self)
    }

    /// The [`APPEND` command](https://tools.ietf.org/html/rfc3501#section-6.3.11) appends
    /// `content` as a new message to the end of the specified destination `mailbox`.  This
    /// argument SHOULD be in the format of an [RFC-2822](https://tools.ietf.org/html/rfc2822)
    /// message.
    ///
    /// > Note: There MAY be exceptions, e.g., draft messages, in which required RFC-2822 header
    /// > lines are omitted in the message literal argument to `append`.  The full implications of
    /// > doing so MUST be understood and carefully weighed.
    ///
    /// If the append is unsuccessful for any reason, the mailbox is restored to its state before
    /// the append attempt; no partial appending will happen.
    ///
    /// If the destination `mailbox` does not exist, the server returns an error, and does not
    /// automatically create the mailbox.
    ///
    /// If the mailbox is currently selected, the normal new message actions will generally occur.
    /// Specifically, the server will generally notify the client immediately via an untagged
    /// `EXISTS` response.  If the server does not do so, the client MAY issue a `NOOP` command (or
    /// failing that, a `CHECK` command) after one or more `APPEND` commands.
    pub async fn append(
        &mut self,
        mailbox: impl AsRef<str>,
        flags: Option<&str>,
        internaldate: Option<&str>,
        content: impl AsRef<[u8]>,
    ) -> Result<()> {
        let content = content.as_ref();
        let id = self
            .run_command(&format!(
                "APPEND \"{}\"{}{}{}{} {{{}}}",
                mailbox.as_ref(),
                if flags.is_some() { " " } else { "" },
                flags.unwrap_or(""),
                if internaldate.is_some() { " " } else { "" },
                internaldate.unwrap_or(""),
                content.len()
            ))
            .await?;

        let Some(res) = self.read_response().await? else {
            return Err(Error::Append);
        };
        let Response::Continue { .. } = res.parsed() else {
            return Err(Error::Append);
        };

        self.stream.as_mut().write_all(content).await?;
        self.stream.as_mut().write_all(b"\r\n").await?;
        self.stream.flush().await?;
        self.conn
            .check_done_ok(&id, Some(self.unsolicited_responses_tx.clone()))
            .await?;
        Ok(())
    }

    /// The [`SEARCH` command](https://tools.ietf.org/html/rfc3501#section-6.4.4) searches the
    /// mailbox for messages that match the given `query`.  `query` consist of one or more search
    /// keys separated by spaces.  The response from the server contains a listing of [`Seq`]s
    /// corresponding to those messages that match the searching criteria.
    ///
    /// When multiple search keys are specified, the result is the intersection of all the messages
    /// that match those keys.  Or, in other words, only messages that match *all* the keys. For
    /// example, the criteria
    ///
    /// ```text
    /// DELETED FROM "SMITH" SINCE 1-Feb-1994
    /// ```
    ///
    /// refers to all deleted messages from Smith that were placed in the mailbox since February 1,
    /// 1994.  A search key can also be a parenthesized list of one or more search keys (e.g., for
    /// use with the `OR` and `NOT` keys).
    ///
    /// In all search keys that use strings, a message matches the key if the string is a substring
    /// of the field.  The matching is case-insensitive.
    ///
    /// Below is a selection of common search keys.  The full list can be found in the
    /// specification of the [`SEARCH command`](https://tools.ietf.org/html/rfc3501#section-6.4.4).
    ///
    ///  - `NEW`: Messages that have [`Flag::Recent`] set but not [`Flag::Seen`]. This is functionally equivalent to `(RECENT UNSEEN)`.
    ///  - `OLD`: Messages that do not have [`Flag::Recent`] set.  This is functionally equivalent to `NOT RECENT` (as opposed to `NOT NEW`).
    ///  - `RECENT`: Messages that have [`Flag::Recent`] set.
    ///  - `ANSWERED`: Messages with [`Flag::Answered`] set.
    ///  - `DELETED`: Messages with [`Flag::Deleted`] set.
    ///  - `DRAFT`: Messages with [`Flag::Draft`] set.
    ///  - `FLAGGED`: Messages with [`Flag::Flagged`] set.
    ///  - `SEEN`: Messages that have [`Flag::Seen`] set.
    ///  - `<sequence set>`: Messages with message sequence numbers corresponding to the specified message sequence number set.
    ///  - `UID <sequence set>`: Messages with [`Uid`] corresponding to the specified unique identifier set.  Sequence set ranges are permitted.
    ///
    ///  - `SUBJECT <string>`: Messages that contain the specified string in the envelope structure's `SUBJECT` field.
    ///  - `BODY <string>`: Messages that contain the specified string in the body of the message.
    ///  - `FROM <string>`: Messages that contain the specified string in the envelope structure's `FROM` field.
    ///  - `TO <string>`: Messages that contain the specified string in the envelope structure's `TO` field.
    ///
    ///  - `NOT <search-key>`: Messages that do not match the specified search key.
    ///  - `OR <search-key1> <search-key2>`: Messages that match either search key.
    ///
    ///  - `BEFORE <date>`: Messages whose internal date (disregarding time and timezone) is earlier than the specified date.
    ///  - `SINCE <date>`: Messages whose internal date (disregarding time and timezone) is within or later than the specified date.
    pub async fn search<S: AsRef<str>>(&mut self, query: S) -> Result<HashSet<Seq>> {
        let id = self
            .run_command(&format!("SEARCH {}", query.as_ref()))
            .await?;
        let seqs = parse_ids(
            &mut self.conn.stream,
            self.unsolicited_responses_tx.clone(),
            id,
        )
        .await?;

        Ok(seqs)
    }

    /// Equivalent to [`Session::search`], except that the returned identifiers
    /// are [`Uid`] instead of [`Seq`]. See also the [`UID`
    /// command](https://tools.ietf.org/html/rfc3501#section-6.4.8).
    pub async fn uid_search<S: AsRef<str>>(&mut self, query: S) -> Result<HashSet<Uid>> {
        let id = self
            .run_command(&format!("UID SEARCH {}", query.as_ref()))
            .await?;
        let uids = parse_ids(
            &mut self.conn.stream,
            self.unsolicited_responses_tx.clone(),
            id,
        )
        .await?;

        Ok(uids)
    }

    /// The [`GETQUOTA` command](https://tools.ietf.org/html/rfc2087#section-4.2)
    pub async fn get_quota(&mut self, quota_root: &str) -> Result<Quota> {
        let id = self
            .run_command(format!("GETQUOTA {}", quote!(quota_root)))
            .await?;
        let c = parse_get_quota(
            &mut self.conn.stream,
            self.unsolicited_responses_tx.clone(),
            id,
        )
        .await?;
        Ok(c)
    }

    /// The [`GETQUOTAROOT` command](https://tools.ietf.org/html/rfc2087#section-4.3)
    pub async fn get_quota_root(
        &mut self,
        mailbox_name: &str,
    ) -> Result<(Vec<QuotaRoot>, Vec<Quota>)> {
        let id = self
            .run_command(format!("GETQUOTAROOT {}", quote!(mailbox_name)))
            .await?;
        let c = parse_get_quota_root(
            &mut self.conn.stream,
            self.unsolicited_responses_tx.clone(),
            id,
        )
        .await?;
        Ok(c)
    }

    /// The [`GETMETADATA` command](https://datatracker.ietf.org/doc/html/rfc5464.html#section-4.2)
    pub async fn get_metadata(
        &mut self,
        mailbox_name: &str,
        options: &str,
        entry_specifier: &str,
    ) -> Result<Vec<Metadata>> {
        let options = if options.is_empty() {
            String::new()
        } else {
            format!(" {options}")
        };
        let id = self
            .run_command(format!(
                "GETMETADATA {} {}{}",
                quote!(mailbox_name),
                options,
                entry_specifier
            ))
            .await?;
        let metadata = parse_metadata(
            &mut self.conn.stream,
            mailbox_name,
            self.unsolicited_responses_tx.clone(),
            id,
        )
        .await?;
        Ok(metadata)
    }

    /// The [`ID` command](https://datatracker.ietf.org/doc/html/rfc2971)
    ///
    /// `identification` is an iterable sequence of pairs such as `("name", Some("MyMailClient"))`.
    pub async fn id(
        &mut self,
        identification: impl IntoIterator<Item = (&str, Option<&str>)>,
    ) -> Result<Option<HashMap<String, String>>> {
        let id = self
            .run_command(format!("ID ({})", format_identification(identification)))
            .await?;
        let server_identification = parse_id(
            &mut self.conn.stream,
            self.unsolicited_responses_tx.clone(),
            id,
        )
        .await?;
        Ok(server_identification)
    }

    /// Similar to `id`, but don't identify ourselves.
    ///
    /// Sends `ID NIL` command and returns server response.
    pub async fn id_nil(&mut self) -> Result<Option<HashMap<String, String>>> {
        let id = self.run_command("ID NIL").await?;
        let server_identification = parse_id(
            &mut self.conn.stream,
            self.unsolicited_responses_tx.clone(),
            id,
        )
        .await?;
        Ok(server_identification)
    }

    // these are only here because they are public interface, the rest is in `Connection`
    /// Runs a command and checks if it returns OK.
    pub async fn run_command_and_check_ok<S: AsRef<str>>(&mut self, command: S) -> Result<()> {
        self.conn
            .run_command_and_check_ok(
                command.as_ref(),
                Some(self.unsolicited_responses_tx.clone()),
            )
            .await?;

        Ok(())
    }

    /// Runs any command passed to it.
    pub async fn run_command<S: AsRef<str>>(&mut self, command: S) -> Result<RequestId> {
        let id = self.conn.run_command(command.as_ref()).await?;

        Ok(id)
    }

    /// Runs an arbitrary command, without adding a tag to it.
    pub async fn run_command_untagged<S: AsRef<str>>(&mut self, command: S) -> Result<()> {
        self.conn.run_command_untagged(command.as_ref()).await?;

        Ok(())
    }

    /// Read the next response on the connection.
    pub async fn read_response(&mut self) -> io::Result<Option<ResponseData>> {
        self.conn.read_response().await
    }
}

impl<T: Read + Write + Unpin + fmt::Debug> Connection<T> {
    unsafe_pinned!(stream: ImapStream<T>);

    /// Gets a reference to the underlying stream.
    pub fn get_ref(&self) -> &T {
        self.stream.get_ref()
    }

    /// Gets a mutable reference to the underlying stream.
    pub fn get_mut(&mut self) -> &mut T {
        self.stream.get_mut()
    }

    /// Convert this connection into the raw underlying stream.
    pub fn into_inner(self) -> T {
        let Self { stream, .. } = self;
        stream.into_inner()
    }

    /// Read the next response on the connection.
    pub async fn read_response(&mut self) -> io::Result<Option<ResponseData>> {
        self.stream.try_next().await
    }

    pub(crate) async fn run_command_untagged(&mut self, command: &str) -> Result<()> {
        self.stream
            .encode(Request(None, command.as_bytes().into()))
            .await?;
        self.stream.flush().await?;
        Ok(())
    }

    pub(crate) async fn run_command(&mut self, command: &str) -> Result<RequestId> {
        let request_id = self.request_ids.next().unwrap(); // safe: never returns Err
        self.stream
            .encode(Request(Some(request_id.clone()), command.as_bytes().into()))
            .await?;
        self.stream.flush().await?;
        Ok(request_id)
    }

    /// Execute a command and check that the next response is a matching done.
    pub async fn run_command_and_check_ok(
        &mut self,
        command: &str,
        unsolicited: Option<channel::Sender<UnsolicitedResponse>>,
    ) -> Result<()> {
        let id = self.run_command(command).await?;
        self.check_done_ok(&id, unsolicited).await?;

        Ok(())
    }

    pub(crate) async fn check_done_ok(
        &mut self,
        id: &RequestId,
        unsolicited: Option<channel::Sender<UnsolicitedResponse>>,
    ) -> Result<()> {
        if let Some(first_res) = self.stream.try_next().await? {
            self.check_done_ok_from(id, unsolicited, first_res).await
        } else {
            Err(Error::ConnectionLost)
        }
    }

    pub(crate) async fn check_done_ok_from(
        &mut self,
        id: &RequestId,
        unsolicited: Option<channel::Sender<UnsolicitedResponse>>,
        mut response: ResponseData,
    ) -> Result<()> {
        loop {
            if let Response::Done {
                status,
                code,
                information,
                tag,
            } = response.parsed()
            {
                self.check_status_ok(status, code.as_ref(), information.as_deref())?;

                if tag == id {
                    return Ok(());
                }
            }

            if let Some(unsolicited) = unsolicited.clone() {
                handle_unilateral(response, unsolicited);
            }

            let Some(res) = self.stream.try_next().await? else {
                return Err(Error::ConnectionLost);
            };
            response = res;
        }
    }

    pub(crate) fn check_status_ok(
        &self,
        status: &imap_proto::Status,
        code: Option<&imap_proto::ResponseCode<'_>>,
        information: Option<&str>,
    ) -> Result<()> {
        use imap_proto::Status;
        match status {
            Status::Ok => Ok(()),
            Status::Bad => Err(Error::Bad(format!("code: {code:?}, info: {information:?}"))),
            Status::No => Err(Error::No(format!("code: {code:?}, info: {information:?}"))),
            _ => Err(Error::Io(io::Error::other(format!(
                "status: {status:?}, code: {code:?}, information: {information:?}"
            )))),
        }
    }
}

fn validate_str(value: &str) -> Result<String> {
    let quoted = quote!(value);
    if quoted.find('\n').is_some() {
        return Err(Error::Validate(ValidateError('\n')));
    }
    if quoted.find('\r').is_some() {
        return Err(Error::Validate(ValidateError('\r')));
    }
    Ok(quoted)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::super::error::Result;
    use super::super::mock_stream::MockStream;
    use super::*;
    use std::borrow::Cow;
    use std::future::Future;

    use async_std::sync::{Arc, Mutex};
    use futures::StreamExt;
    use imap_proto::Status;

    macro_rules! mock_client {
        ($s:expr) => {
            Client::new($s)
        };
    }

    macro_rules! mock_session {
        ($s:expr) => {
            Session::new(mock_client!($s).conn)
        };
    }

    macro_rules! assert_eq_bytes {
        ($a:expr, $b:expr, $c:expr) => {
            assert_eq!(
                std::str::from_utf8($a).unwrap(),
                std::str::from_utf8($b).unwrap(),
                $c
            )
        };
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn fetch_body() {
        let response = "a0 OK Logged in.\r\n\
                        * 2 FETCH (BODY[TEXT] {3}\r\nfoo)\r\n\
                        a0 OK FETCH completed\r\n";
        let mut session = mock_session!(MockStream::new(response.as_bytes().to_vec()));
        session.read_response().await.unwrap().unwrap();
        session.read_response().await.unwrap().unwrap();
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn readline_delay_read() {
        let greeting = "* OK Dovecot ready.\r\n";
        let mock_stream = MockStream::default()
            .with_buf(greeting.as_bytes().to_vec())
            .with_delay();

        let mut client = mock_client!(mock_stream);
        let actual_response = client.read_response().await.unwrap().unwrap();
        assert_eq!(
            actual_response.parsed(),
            &Response::Data {
                status: Status::Ok,
                code: None,
                information: Some(Cow::Borrowed("Dovecot ready.")),
            }
        );
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn readline_eof() {
        let mock_stream = MockStream::default().with_eof();
        let mut client = mock_client!(mock_stream);
        let res = client.read_response().await.unwrap();
        assert!(res.is_none());
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    #[should_panic]
    async fn readline_err() {
        // TODO Check the error test
        let mock_stream = MockStream::default().with_err();
        let mut client = mock_client!(mock_stream);
        client.read_response().await.unwrap().unwrap();
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn authenticate() {
        let response = b"+ YmFy\r\n\
                         A0001 OK Logged in\r\n"
            .to_vec();
        let command = "A0001 AUTHENTICATE PLAIN\r\n\
                       Zm9v\r\n";
        let mock_stream = MockStream::new(response);
        let client = mock_client!(mock_stream);
        enum Authenticate {
            Auth,
        }
        impl Authenticator for &Authenticate {
            type Response = Vec<u8>;
            fn process(&mut self, challenge: &[u8]) -> Self::Response {
                assert!(challenge == b"bar", "Invalid authenticate challenge");
                b"foo".to_vec()
            }
        }
        let session = client
            .authenticate("PLAIN", &Authenticate::Auth)
            .await
            .ok()
            .unwrap();
        assert_eq_bytes!(
            &session.stream.inner.written_buf,
            command.as_bytes(),
            "Invalid authenticate command"
        );
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn login() {
        let response = b"A0001 OK Logged in\r\n".to_vec();
        let username = "username";
        let password = "password";
        let command = format!("A0001 LOGIN {} {}\r\n", quote!(username), quote!(password));
        let mock_stream = MockStream::new(response);
        let client = mock_client!(mock_stream);
        if let Ok(session) = client.login(username, password).await {
            assert_eq!(
                session.stream.inner.written_buf,
                command.as_bytes().to_vec(),
                "Invalid login command"
            );
        } else {
            unreachable!("invalid login");
        }
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn logout() {
        let response = b"A0001 OK Logout completed.\r\n".to_vec();
        let command = "A0001 LOGOUT\r\n";
        let mock_stream = MockStream::new(response);
        let mut session = mock_session!(mock_stream);
        session.logout().await.unwrap();
        assert!(
            session.stream.inner.written_buf == command.as_bytes().to_vec(),
            "Invalid logout command"
        );
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn rename() {
        let response = b"A0001 OK RENAME completed\r\n".to_vec();
        let current_mailbox_name = "INBOX";
        let new_mailbox_name = "NEWINBOX";
        let command = format!(
            "A0001 RENAME {} {}\r\n",
            quote!(current_mailbox_name),
            quote!(new_mailbox_name)
        );
        let mock_stream = MockStream::new(response);
        let mut session = mock_session!(mock_stream);
        session
            .rename(current_mailbox_name, new_mailbox_name)
            .await
            .unwrap();
        assert!(
            session.stream.inner.written_buf == command.as_bytes().to_vec(),
            "Invalid rename command"
        );
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn subscribe() {
        let response = b"A0001 OK SUBSCRIBE completed\r\n".to_vec();
        let mailbox = "INBOX";
        let command = format!("A0001 SUBSCRIBE {}\r\n", quote!(mailbox));
        let mock_stream = MockStream::new(response);
        let mut session = mock_session!(mock_stream);
        session.subscribe(mailbox).await.unwrap();
        assert!(
            session.stream.inner.written_buf == command.as_bytes().to_vec(),
            "Invalid subscribe command"
        );
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn unsubscribe() {
        let response = b"A0001 OK UNSUBSCRIBE completed\r\n".to_vec();
        let mailbox = "INBOX";
        let command = format!("A0001 UNSUBSCRIBE {}\r\n", quote!(mailbox));
        let mock_stream = MockStream::new(response);
        let mut session = mock_session!(mock_stream);
        session.unsubscribe(mailbox).await.unwrap();
        assert!(
            session.stream.inner.written_buf == command.as_bytes().to_vec(),
            "Invalid unsubscribe command"
        );
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn expunge() {
        let response = b"A0001 OK EXPUNGE completed\r\n".to_vec();
        let mock_stream = MockStream::new(response);
        let mut session = mock_session!(mock_stream);
        session.expunge().await.unwrap().collect::<Vec<_>>().await;
        assert!(
            session.stream.inner.written_buf == b"A0001 EXPUNGE\r\n".to_vec(),
            "Invalid expunge command"
        );
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn uid_expunge() {
        let response = b"* 2 EXPUNGE\r\n\
            * 3 EXPUNGE\r\n\
            * 4 EXPUNGE\r\n\
            A0001 OK UID EXPUNGE completed\r\n"
            .to_vec();
        let mock_stream = MockStream::new(response);
        let mut session = mock_session!(mock_stream);
        session
            .uid_expunge("2:4")
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert!(
            session.stream.inner.written_buf == b"A0001 UID EXPUNGE 2:4\r\n".to_vec(),
            "Invalid expunge command"
        );
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn check() {
        let response = b"A0001 OK CHECK completed\r\n".to_vec();
        let mock_stream = MockStream::new(response);
        let mut session = mock_session!(mock_stream);
        session.check().await.unwrap();
        assert!(
            session.stream.inner.written_buf == b"A0001 CHECK\r\n".to_vec(),
            "Invalid check command"
        );
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn examine() {
        let response = b"* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n\
            * OK [PERMANENTFLAGS ()] Read-only mailbox.\r\n\
            * 1 EXISTS\r\n\
            * 1 RECENT\r\n\
            * OK [UNSEEN 1] First unseen.\r\n\
            * OK [UIDVALIDITY 1257842737] UIDs valid\r\n\
            * OK [UIDNEXT 2] Predicted next UID\r\n\
            A0001 OK [READ-ONLY] Select completed.\r\n"
            .to_vec();
        let expected_mailbox = Mailbox {
            flags: vec![
                Flag::Answered,
                Flag::Flagged,
                Flag::Deleted,
                Flag::Seen,
                Flag::Draft,
            ],
            exists: 1,
            recent: 1,
            unseen: Some(1),
            permanent_flags: vec![],
            uid_next: Some(2),
            uid_validity: Some(1257842737),
            highest_modseq: None,
        };
        let mailbox_name = "INBOX";
        let command = format!("A0001 EXAMINE {}\r\n", quote!(mailbox_name));
        let mock_stream = MockStream::new(response);
        let mut session = mock_session!(mock_stream);
        let mailbox = session.examine(mailbox_name).await.unwrap();
        assert!(
            session.stream.inner.written_buf == command.as_bytes().to_vec(),
            "Invalid examine command"
        );
        assert_eq!(mailbox, expected_mailbox);
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn select() {
        let response = b"* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n\
            * OK [PERMANENTFLAGS (\\* \\Answered \\Flagged \\Deleted \\Draft \\Seen)] \
              Read-only mailbox.\r\n\
            * 1 EXISTS\r\n\
            * 1 RECENT\r\n\
            * OK [UNSEEN 1] First unseen.\r\n\
            * OK [UIDVALIDITY 1257842737] UIDs valid\r\n\
            * OK [UIDNEXT 2] Predicted next UID\r\n\
            * OK [HIGHESTMODSEQ 90060115205545359] Highest mailbox modsequence\r\n\
            A0001 OK [READ-ONLY] Select completed.\r\n"
            .to_vec();
        let expected_mailbox = Mailbox {
            flags: vec![
                Flag::Answered,
                Flag::Flagged,
                Flag::Deleted,
                Flag::Seen,
                Flag::Draft,
            ],
            exists: 1,
            recent: 1,
            unseen: Some(1),
            permanent_flags: vec![
                Flag::MayCreate,
                Flag::Answered,
                Flag::Flagged,
                Flag::Deleted,
                Flag::Draft,
                Flag::Seen,
            ],
            uid_next: Some(2),
            uid_validity: Some(1257842737),
            highest_modseq: Some(90060115205545359),
        };
        let mailbox_name = "INBOX";
        let command = format!("A0001 SELECT {}\r\n", quote!(mailbox_name));
        let mock_stream = MockStream::new(response);
        let mut session = mock_session!(mock_stream);
        let mailbox = session.select(mailbox_name).await.unwrap();
        assert!(
            session.stream.inner.written_buf == command.as_bytes().to_vec(),
            "Invalid select command"
        );
        assert_eq!(mailbox, expected_mailbox);
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn search() {
        let response = b"* SEARCH 1 2 3 4 5\r\n\
            A0001 OK Search completed\r\n"
            .to_vec();
        let mock_stream = MockStream::new(response);
        let mut session = mock_session!(mock_stream);
        let ids = session.search("Unseen").await.unwrap();
        let ids: HashSet<u32> = ids.iter().cloned().collect();
        assert!(
            session.stream.inner.written_buf == b"A0001 SEARCH Unseen\r\n".to_vec(),
            "Invalid search command"
        );
        assert_eq!(ids, [1, 2, 3, 4, 5].iter().cloned().collect());
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn uid_search() {
        let response = b"* SEARCH 1 2 3 4 5\r\n\
            A0001 OK Search completed\r\n"
            .to_vec();
        let mock_stream = MockStream::new(response);
        let mut session = mock_session!(mock_stream);
        let ids = session.uid_search("Unseen").await.unwrap();
        let ids: HashSet<Uid> = ids.iter().cloned().collect();
        assert!(
            session.stream.inner.written_buf == b"A0001 UID SEARCH Unseen\r\n".to_vec(),
            "Invalid search command"
        );
        assert_eq!(ids, [1, 2, 3, 4, 5].iter().cloned().collect());
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn uid_search_unordered() {
        let response = b"* SEARCH 1 2 3 4 5\r\n\
            A0002 OK CAPABILITY completed\r\n\
            A0001 OK Search completed\r\n"
            .to_vec();
        let mock_stream = MockStream::new(response);
        let mut session = mock_session!(mock_stream);
        let ids = session.uid_search("Unseen").await.unwrap();
        let ids: HashSet<Uid> = ids.iter().cloned().collect();
        assert!(
            session.stream.inner.written_buf == b"A0001 UID SEARCH Unseen\r\n".to_vec(),
            "Invalid search command"
        );
        assert_eq!(ids, [1, 2, 3, 4, 5].iter().cloned().collect());
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn capability() {
        let response = b"* CAPABILITY IMAP4rev1 STARTTLS AUTH=GSSAPI LOGINDISABLED\r\n\
            A0001 OK CAPABILITY completed\r\n"
            .to_vec();
        let expected_capabilities = vec!["IMAP4rev1", "STARTTLS", "AUTH=GSSAPI", "LOGINDISABLED"];
        let mock_stream = MockStream::new(response);
        let mut session = mock_session!(mock_stream);
        let capabilities = session.capabilities().await.unwrap();
        assert!(
            session.stream.inner.written_buf == b"A0001 CAPABILITY\r\n".to_vec(),
            "Invalid capability command"
        );
        assert_eq!(capabilities.len(), 4);
        for e in expected_capabilities {
            assert!(capabilities.has_str(e));
        }
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn create() {
        let response = b"A0001 OK CREATE completed\r\n".to_vec();
        let mailbox_name = "INBOX";
        let command = format!("A0001 CREATE {}\r\n", quote!(mailbox_name));
        let mock_stream = MockStream::new(response);
        let mut session = mock_session!(mock_stream);
        session.create(mailbox_name).await.unwrap();
        assert!(
            session.stream.inner.written_buf == command.as_bytes().to_vec(),
            "Invalid create command"
        );
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn delete() {
        let response = b"A0001 OK DELETE completed\r\n".to_vec();
        let mailbox_name = "INBOX";
        let command = format!("A0001 DELETE {}\r\n", quote!(mailbox_name));
        let mock_stream = MockStream::new(response);
        let mut session = mock_session!(mock_stream);
        session.delete(mailbox_name).await.unwrap();
        assert!(
            session.stream.inner.written_buf == command.as_bytes().to_vec(),
            "Invalid delete command"
        );
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn noop() {
        let response = b"A0001 OK NOOP completed\r\n".to_vec();
        let mock_stream = MockStream::new(response);
        let mut session = mock_session!(mock_stream);
        session.noop().await.unwrap();
        assert!(
            session.stream.inner.written_buf == b"A0001 NOOP\r\n".to_vec(),
            "Invalid noop command"
        );
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn close() {
        let response = b"A0001 OK CLOSE completed\r\n".to_vec();
        let mock_stream = MockStream::new(response);
        let mut session = mock_session!(mock_stream);
        session.close().await.unwrap();
        assert!(
            session.stream.inner.written_buf == b"A0001 CLOSE\r\n".to_vec(),
            "Invalid close command"
        );
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn store() {
        generic_store(" ", |c, set, query| async move {
            c.lock()
                .await
                .store(set, query)
                .await?
                .collect::<Vec<_>>()
                .await;
            Ok(())
        })
        .await;
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn uid_store() {
        generic_store(" UID ", |c, set, query| async move {
            c.lock()
                .await
                .uid_store(set, query)
                .await?
                .collect::<Vec<_>>()
                .await;
            Ok(())
        })
        .await;
    }

    async fn generic_store<'a, F, T, K>(prefix: &'a str, op: F)
    where
        F: 'a + FnOnce(Arc<Mutex<Session<MockStream>>>, &'a str, &'a str) -> K,
        K: 'a + Future<Output = Result<T>>,
    {
        let res = "* 2 FETCH (FLAGS (\\Deleted \\Seen))\r\n\
                   * 3 FETCH (FLAGS (\\Deleted))\r\n\
                   * 4 FETCH (FLAGS (\\Deleted \\Flagged \\Seen))\r\n\
                   A0001 OK STORE completed\r\n";

        generic_with_uid(res, "STORE", "2.4", "+FLAGS (\\Deleted)", prefix, op).await;
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn copy() {
        generic_copy(" ", |c, set, query| async move {
            c.lock().await.copy(set, query).await?;
            Ok(())
        })
        .await;
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn uid_copy() {
        generic_copy(" UID ", |c, set, query| async move {
            c.lock().await.uid_copy(set, query).await?;
            Ok(())
        })
        .await;
    }

    async fn generic_copy<'a, F, T, K>(prefix: &'a str, op: F)
    where
        F: 'a + FnOnce(Arc<Mutex<Session<MockStream>>>, &'a str, &'a str) -> K,
        K: 'a + Future<Output = Result<T>>,
    {
        generic_with_uid(
            "A0001 OK COPY completed\r\n",
            "COPY",
            "2:4",
            "MEETING",
            prefix,
            op,
        )
        .await;
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn mv() {
        let response = b"* OK [COPYUID 1511554416 142,399 41:42] Moved UIDs.\r\n\
            * 2 EXPUNGE\r\n\
            * 1 EXPUNGE\r\n\
            A0001 OK Move completed\r\n"
            .to_vec();
        let mailbox_name = "MEETING";
        let command = format!("A0001 MOVE 1:2 {}\r\n", quote!(mailbox_name));
        let mock_stream = MockStream::new(response);
        let mut session = mock_session!(mock_stream);
        session.mv("1:2", mailbox_name).await.unwrap();
        assert!(
            session.stream.inner.written_buf == command.as_bytes().to_vec(),
            "Invalid move command"
        );
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn uid_mv() {
        let response = b"* OK [COPYUID 1511554416 142,399 41:42] Moved UIDs.\r\n\
            * 2 EXPUNGE\r\n\
            * 1 EXPUNGE\r\n\
            A0001 OK Move completed\r\n"
            .to_vec();
        let mailbox_name = "MEETING";
        let command = format!("A0001 UID MOVE 41:42 {}\r\n", quote!(mailbox_name));
        let mock_stream = MockStream::new(response);
        let mut session = mock_session!(mock_stream);
        session.uid_mv("41:42", mailbox_name).await.unwrap();
        assert!(
            session.stream.inner.written_buf == command.as_bytes().to_vec(),
            "Invalid uid move command"
        );
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn fetch() {
        generic_fetch(" ", |c, seq, query| async move {
            c.lock()
                .await
                .fetch(seq, query)
                .await?
                .collect::<Vec<_>>()
                .await;

            Ok(())
        })
        .await;
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn uid_fetch() {
        generic_fetch(" UID ", |c, seq, query| async move {
            c.lock()
                .await
                .uid_fetch(seq, query)
                .await?
                .collect::<Vec<_>>()
                .await;
            Ok(())
        })
        .await;
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn fetch_unexpected_eof() {
        // Connection is lost, there will never be any response.
        let response = b"".to_vec();

        let mock_stream = MockStream::new(response);
        let mut session = mock_session!(mock_stream);

        {
            let mut fetch_result = session
                .uid_fetch("1:*", "(FLAGS BODY.PEEK[])")
                .await
                .unwrap();

            // Unexpected EOF.
            let err = fetch_result.try_next().await.unwrap_err();
            let Error::Io(io_err) = err else {
                panic!("Unexpected error type: {err}")
            };
            assert_eq!(io_err.kind(), io::ErrorKind::UnexpectedEof);
        }

        assert_eq!(
            session.stream.inner.written_buf,
            b"A0001 UID FETCH 1:* (FLAGS BODY.PEEK[])\r\n".to_vec()
        );
    }

    async fn generic_fetch<'a, F, T, K>(prefix: &'a str, op: F)
    where
        F: 'a + FnOnce(Arc<Mutex<Session<MockStream>>>, &'a str, &'a str) -> K,
        K: 'a + Future<Output = Result<T>>,
    {
        generic_with_uid(
            "A0001 OK FETCH completed\r\n",
            "FETCH",
            "1",
            "BODY[]",
            prefix,
            op,
        )
        .await;
    }

    async fn generic_with_uid<'a, F, T, K>(
        res: &'a str,
        cmd: &'a str,
        seq: &'a str,
        query: &'a str,
        prefix: &'a str,
        op: F,
    ) where
        F: 'a + FnOnce(Arc<Mutex<Session<MockStream>>>, &'a str, &'a str) -> K,
        K: 'a + Future<Output = Result<T>>,
    {
        let resp = res.as_bytes().to_vec();
        let line = format!("A0001{prefix}{cmd} {seq} {query}\r\n");
        let session = Arc::new(Mutex::new(mock_session!(MockStream::new(resp))));

        {
            let _ = op(session.clone(), seq, query).await.unwrap();
        }
        assert!(
            session.lock().await.stream.inner.written_buf == line.as_bytes().to_vec(),
            "Invalid command"
        );
    }

    #[test]
    fn quote_backslash() {
        assert_eq!("\"test\\\\text\"", quote!(r"test\text"));
    }

    #[test]
    fn quote_dquote() {
        assert_eq!("\"test\\\"text\"", quote!("test\"text"));
    }

    #[test]
    fn validate_random() {
        assert_eq!(
            "\"~iCQ_k;>[&\\\"sVCvUW`e<<P!wJ\"",
            &validate_str("~iCQ_k;>[&\"sVCvUW`e<<P!wJ").unwrap()
        );
    }

    #[test]
    fn validate_newline() {
        if let Err(ref e) = validate_str("test\nstring") {
            if let Error::Validate(ref ve) = e {
                if ve.0 == '\n' {
                    return;
                }
            }
            panic!("Wrong error: {e:?}");
        }
        panic!("No error");
    }

    #[test]
    #[allow(unreachable_patterns)]
    fn validate_carriage_return() {
        if let Err(ref e) = validate_str("test\rstring") {
            if let Error::Validate(ref ve) = e {
                if ve.0 == '\r' {
                    return;
                }
            }
            panic!("Wrong error: {e:?}");
        }
        panic!("No error");
    }

    /// Emulates a server responding to `FETCH` requests
    /// with a body of 76 bytes of headers and N 74-byte lines,
    /// where N is the requested message sequence number.
    #[cfg(feature = "runtime-tokio")]
    async fn handle_client(stream: tokio::io::DuplexStream) -> Result<()> {
        use tokio::io::AsyncBufReadExt;

        let (reader, mut writer) = tokio::io::split(stream);
        let reader = tokio::io::BufReader::new(reader);

        let mut lines = reader.lines();
        while let Some(line) = lines.next_line().await? {
            let (request_id, request) = line.split_once(' ').unwrap();
            eprintln!("Received request {request_id}.");

            let (id, _) = request
                .strip_prefix("FETCH ")
                .unwrap()
                .split_once(' ')
                .unwrap();
            let id = id.parse().unwrap();

            let mut body = concat!(
                "From: Bob <bob@example.com>\r\n",
                "To: Alice <alice@example.org>\r\n",
                "Subject: Test\r\n",
                "Message-Id: <foobar@example.com>\r\n",
                "Date: Sun, 22 Mar 2020 00:00:00 +0100\r\n",
                "\r\n",
            )
            .to_string();
            for _ in 1..id {
                body +=
                    "012345678901234567890123456789012345678901234567890123456789012345678901\r\n";
            }
            let body_len = body.len();

            let response = format!("* {id} FETCH (RFC822.SIZE {body_len} BODY[] {{{body_len}}}\r\n{body} FLAGS (\\Seen))\r\n");
            writer.write_all(response.as_bytes()).await?;
            writer
                .write_all(format!("{request_id} OK FETCH completed\r\n").as_bytes())
                .await?;
            writer.flush().await?;
        }

        Ok(())
    }

    /// Test requestng 1000 messages each larger than a previous one.
    ///
    /// This is a regression test for v0.6.0 async-imap,
    /// which sometimes failed to allocate free buffer space,
    /// read into a buffer of zero size and erroneously detected it
    /// as the end of stream.
    #[cfg(feature = "runtime-tokio")]
    #[cfg_attr(
        feature = "runtime-tokio",
        tokio::test(flavor = "multi_thread", worker_threads = 2)
    )]
    async fn large_fetch() -> Result<()> {
        use futures::TryStreamExt;

        let (client, server) = tokio::io::duplex(4096);
        tokio::spawn(handle_client(server));

        let client = crate::Client::new(client);
        let mut imap_session = Session::new(client.conn);

        for i in 200..300 {
            eprintln!("Fetching {i}.");
            let mut messages_stream = imap_session
                .fetch(format!("{i}"), "(RFC822.SIZE BODY.PEEK[] FLAGS)")
                .await?;
            let fetch = messages_stream
                .try_next()
                .await?
                .expect("no FETCH returned");
            let body = fetch.body().expect("message did not have a body!");
            assert_eq!(body.len(), 76 + 74 * i);

            let no_fetch = messages_stream.try_next().await?;
            assert!(no_fetch.is_none());
            drop(messages_stream);
        }

        Ok(())
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn status() {
        {
            let response = b"* STATUS INBOX (UIDNEXT 25)\r\n\
                A0001 OK [CLIENTBUG] Status on selected mailbox completed (0.001 + 0.000 secs).\r\n"
                .to_vec();

            let mock_stream = MockStream::new(response);
            let mut session = mock_session!(mock_stream);
            let status = session.status("INBOX", "(UIDNEXT)").await.unwrap();
            assert_eq!(
                session.stream.inner.written_buf,
                b"A0001 STATUS \"INBOX\" (UIDNEXT)\r\n".to_vec()
            );
            assert_eq!(status.uid_next, Some(25));
        }

        {
            let response = b"* STATUS INBOX (RECENT 15)\r\n\
                A0001 OK STATUS completed\r\n"
                .to_vec();

            let mock_stream = MockStream::new(response);
            let mut session = mock_session!(mock_stream);
            let status = session.status("INBOX", "(RECENT)").await.unwrap();
            assert_eq!(
                session.stream.inner.written_buf,
                b"A0001 STATUS \"INBOX\" (RECENT)\r\n".to_vec()
            );
            assert_eq!(status.recent, 15);
        }

        {
            // Example from RFC 3501.
            let response = b"* STATUS blurdybloop (MESSAGES 231 UIDNEXT 44292)\r\n\
                A0001 OK STATUS completed\r\n"
                .to_vec();

            let mock_stream = MockStream::new(response);
            let mut session = mock_session!(mock_stream);
            let status = session
                .status("blurdybloop", "(UIDNEXT MESSAGES)")
                .await
                .unwrap();
            assert_eq!(
                session.stream.inner.written_buf,
                b"A0001 STATUS \"blurdybloop\" (UIDNEXT MESSAGES)\r\n".to_vec()
            );
            assert_eq!(status.uid_next, Some(44292));
            assert_eq!(status.exists, 231);
        }
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn append() {
        {
            // APPEND command when INBOX is *not* selected.
            //
            // Only APPENDUID response is returned.
            let response = b"+ OK\r\nA0001 OK [APPENDUID 1725735035 2] Append completed (0.052 + 12.097 + 0.049 secs).\r\n".to_vec();

            let mock_stream = MockStream::new(response);
            let mut session = mock_session!(mock_stream);
            session
                .append("INBOX", Some(r"(\Seen)"), None, "foobarbaz")
                .await
                .unwrap();
            assert_eq!(
                session.stream.inner.written_buf,
                b"A0001 APPEND \"INBOX\" (\\Seen) {9}\r\nfoobarbaz\r\n".to_vec()
            );
        }

        {
            // APPEND command when INBOX is selected.
            //
            // EXISTS response is returned before APPENDUID response is returned.
            let response = b"+ OK\r\n* 3 EXISTS\r\n* 2 RECENT\r\nA0001 OK [APPENDUID 1725735035 2] Append completed (0.052 + 12.097 + 0.049 secs).\r\n".to_vec();

            let mock_stream = MockStream::new(response);
            let mut session = mock_session!(mock_stream);
            session
                .append("INBOX", Some(r"(\Seen)"), None, "foobarbaz")
                .await
                .unwrap();
            assert_eq!(
                session.stream.inner.written_buf,
                b"A0001 APPEND \"INBOX\" (\\Seen) {9}\r\nfoobarbaz\r\n".to_vec()
            );
            let exists_response = session.unsolicited_responses.recv().await.unwrap();
            assert_eq!(exists_response, UnsolicitedResponse::Exists(3));
            let recent_response = session.unsolicited_responses.recv().await.unwrap();
            assert_eq!(recent_response, UnsolicitedResponse::Recent(2));
        }

        {
            // APPEND to nonexisting folder fails.
            let response =
                b"A0001 NO [TRYCREATE] Mailbox doesn't exist: foobar (0.001 + 0.000 secs)."
                    .to_vec();
            let mock_stream = MockStream::new(response);
            let mut session = mock_session!(mock_stream);
            session
                .append("foobar", None, None, "foobarbaz")
                .await
                .unwrap_err();
            assert_eq!(
                session.stream.inner.written_buf,
                b"A0001 APPEND \"foobar\" {9}\r\n".to_vec()
            );
        }
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn get_metadata() {
        {
            let response = b"* METADATA \"INBOX\" (/private/comment \"My own comment\")\r\n\
                A0001 OK GETMETADATA complete\r\n"
                .to_vec();

            let mock_stream = MockStream::new(response);
            let mut session = mock_session!(mock_stream);
            let metadata = session
                .get_metadata("INBOX", "", "/private/comment")
                .await
                .unwrap();
            assert_eq!(
                session.stream.inner.written_buf,
                b"A0001 GETMETADATA \"INBOX\" /private/comment\r\n".to_vec()
            );
            assert_eq!(metadata.len(), 1);
            assert_eq!(metadata[0].entry, "/private/comment");
            assert_eq!(metadata[0].value.as_ref().unwrap(), "My own comment");
        }

        {
            let response = b"* METADATA \"INBOX\" (/shared/comment \"Shared comment\" /private/comment \"My own comment\")\r\n\
                A0001 OK GETMETADATA complete\r\n"
                .to_vec();

            let mock_stream = MockStream::new(response);
            let mut session = mock_session!(mock_stream);
            let metadata = session
                .get_metadata("INBOX", "", "(/shared/comment /private/comment)")
                .await
                .unwrap();
            assert_eq!(
                session.stream.inner.written_buf,
                b"A0001 GETMETADATA \"INBOX\" (/shared/comment /private/comment)\r\n".to_vec()
            );
            assert_eq!(metadata.len(), 2);
            assert_eq!(metadata[0].entry, "/shared/comment");
            assert_eq!(metadata[0].value.as_ref().unwrap(), "Shared comment");
            assert_eq!(metadata[1].entry, "/private/comment");
            assert_eq!(metadata[1].value.as_ref().unwrap(), "My own comment");
        }

        {
            let response = b"* METADATA \"\" (/shared/comment {15}\r\nChatmail server /shared/admin {28}\r\nmailto:root@nine.testrun.org)\r\n\
                A0001 OK OK Getmetadata completed (0.001 + 0.000 secs).\r\n"
                .to_vec();

            let mock_stream = MockStream::new(response);
            let mut session = mock_session!(mock_stream);
            let metadata = session
                .get_metadata("", "", "(/shared/comment /shared/admin)")
                .await
                .unwrap();
            assert_eq!(
                session.stream.inner.written_buf,
                b"A0001 GETMETADATA \"\" (/shared/comment /shared/admin)\r\n".to_vec()
            );
            assert_eq!(metadata.len(), 2);
            assert_eq!(metadata[0].entry, "/shared/comment");
            assert_eq!(metadata[0].value.as_ref().unwrap(), "Chatmail server");
            assert_eq!(metadata[1].entry, "/shared/admin");
            assert_eq!(
                metadata[1].value.as_ref().unwrap(),
                "mailto:root@nine.testrun.org"
            );
        }

        {
            let response = b"* METADATA \"\" (/shared/comment \"Chatmail server\")\r\n\
                * METADATA \"\" (/shared/admin \"mailto:root@nine.testrun.org\")\r\n\
                A0001 OK OK Getmetadata completed (0.001 + 0.000 secs).\r\n"
                .to_vec();

            let mock_stream = MockStream::new(response);
            let mut session = mock_session!(mock_stream);
            let metadata = session
                .get_metadata("", "", "(/shared/comment /shared/admin)")
                .await
                .unwrap();
            assert_eq!(
                session.stream.inner.written_buf,
                b"A0001 GETMETADATA \"\" (/shared/comment /shared/admin)\r\n".to_vec()
            );
            assert_eq!(metadata.len(), 2);
            assert_eq!(metadata[0].entry, "/shared/comment");
            assert_eq!(metadata[0].value.as_ref().unwrap(), "Chatmail server");
            assert_eq!(metadata[1].entry, "/shared/admin");
            assert_eq!(
                metadata[1].value.as_ref().unwrap(),
                "mailto:root@nine.testrun.org"
            );
        }

        {
            let response = b"* METADATA \"\" (/shared/comment NIL /shared/admin NIL)\r\n\
                A0001 OK OK Getmetadata completed (0.001 + 0.000 secs).\r\n"
                .to_vec();

            let mock_stream = MockStream::new(response);
            let mut session = mock_session!(mock_stream);
            let metadata = session
                .get_metadata("", "", "(/shared/comment /shared/admin)")
                .await
                .unwrap();
            assert_eq!(
                session.stream.inner.written_buf,
                b"A0001 GETMETADATA \"\" (/shared/comment /shared/admin)\r\n".to_vec()
            );
            assert_eq!(metadata.len(), 2);
            assert_eq!(metadata[0].entry, "/shared/comment");
            assert_eq!(metadata[0].value, None);
            assert_eq!(metadata[1].entry, "/shared/admin");
            assert_eq!(metadata[1].value, None);
        }
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn test_get_quota_root() {
        {
            let response = b"* QUOTAROOT Sent Userquota\r\n\
                    * QUOTA Userquota (STORAGE 4855 48576)\r\n\
                    A0001 OK Getquotaroot completed (0.004 + 0.000 + 0.004 secs).\r\n"
                .to_vec();

            let mock_stream = MockStream::new(response);
            let mut session = mock_session!(mock_stream);
            let (quotaroots, quota) = dbg!(session.get_quota_root("Sent").await.unwrap());
            assert_eq!(
                str::from_utf8(&session.stream.inner.written_buf).unwrap(),
                "A0001 GETQUOTAROOT \"Sent\"\r\n"
            );
            assert_eq!(
                quotaroots,
                vec![QuotaRoot {
                    mailbox_name: "Sent".to_string(),
                    quota_root_names: vec!["Userquota".to_string(),],
                },],
            );
            assert_eq!(
                quota,
                vec![Quota {
                    root_name: "Userquota".to_string(),
                    resources: vec![QuotaResource {
                        name: QuotaResourceName::Storage,
                        usage: 4855,
                        limit: 48576,
                    }],
                }]
            );
            assert_eq!(quota[0].resources[0].get_usage_percentage(), 9);
        }

        {
            let response = b"* QUOTAROOT \"INBOX\" \"#19\"\r\n\
                    * QUOTA \"#19\" (STORAGE 0 0)\r\n\
                    A0001 OK GETQUOTAROOT successful.\r\n"
                .to_vec();

            let mock_stream = MockStream::new(response);
            let mut session = mock_session!(mock_stream);
            let (quotaroots, quota) = session.get_quota_root("INBOX").await.unwrap();
            assert_eq!(
                str::from_utf8(&session.stream.inner.written_buf).unwrap(),
                "A0001 GETQUOTAROOT \"INBOX\"\r\n"
            );
            assert_eq!(
                quotaroots,
                vec![QuotaRoot {
                    mailbox_name: "INBOX".to_string(),
                    quota_root_names: vec!["#19".to_string(),],
                },],
            );
            assert_eq!(
                quota,
                vec![Quota {
                    root_name: "#19".to_string(),
                    resources: vec![QuotaResource {
                        name: QuotaResourceName::Storage,
                        usage: 0,
                        limit: 0,
                    }],
                }]
            );
            assert_eq!(quota[0].resources[0].get_usage_percentage(), 0);
        }
    }

    #[cfg_attr(feature = "runtime-tokio", tokio::test)]
    #[cfg_attr(feature = "runtime-async-std", async_std::test)]
    async fn test_parsing_error() {
        // Simulate someone connecting to SMTP server with IMAP client.
        let response = b"220 mail.example.org ESMTP Postcow\r\n".to_vec();
        let command = "A0001 NOOP\r\n";
        let mock_stream = MockStream::new(response);
        let mut session = mock_session!(mock_stream);
        assert!(session
            .noop()
            .await
            .unwrap_err()
            .to_string()
            .contains("220 mail.example.org ESMTP Postcow"));
        assert!(
            session.stream.inner.written_buf == command.as_bytes().to_vec(),
            "Invalid NOOP command"
        );
    }
}
