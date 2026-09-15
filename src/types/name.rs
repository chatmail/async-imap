pub use imap_proto::types::NameAttribute;
use imap_proto::{MailboxDatum, MailboxListData, Response};
use self_cell::self_cell;

use crate::types::ResponseData;

self_cell!(
    /// A name that matches a `LIST` or `LSUB` command.
    pub struct Name {
        owner: Box<ResponseData>,

        #[covariant]
        dependent: InnerName,
    }

    impl { Debug }
);

#[derive(PartialEq, Eq, Debug)]
pub struct InnerName<'a> {
    attributes: Vec<NameAttribute<'a>>,
    delimiter: Option<&'a str>,
    name: &'a str,
}

impl Name {
    pub(crate) fn from_mailbox_data(resp: ResponseData) -> Self {
        Name::new(Box::new(resp), |response| match response.parsed() {
            Response::MailboxData(MailboxDatum::List(MailboxListData {
                name_attributes,
                delimiter,
                name,
            })) => InnerName {
                attributes: name_attributes.to_owned(),
                delimiter: delimiter.as_deref(),
                name,
            },
            _ => panic!("cannot construct from non mailbox data"),
        })
    }

    /// Attributes of this name.
    pub fn attributes(&self) -> &[NameAttribute<'_>] {
        &self.borrow_dependent().attributes[..]
    }

    /// The hierarchy delimiter is a character used to delimit levels of hierarchy in a mailbox
    /// name.  A client can use it to create child mailboxes, and to search higher or lower levels
    /// of naming hierarchy.  All children of a top-level hierarchy node use the same
    /// separator character.  `None` means that no hierarchy exists; the name is a "flat" name.
    pub fn delimiter(&self) -> Option<&str> {
        self.borrow_dependent().delimiter
    }

    /// The name represents an unambiguous left-to-right hierarchy, and are valid for use as a
    /// reference in `LIST` and `LSUB` commands. Unless [`NameAttribute::NoSelect`] is indicated,
    /// the name is also valid as an argument for commands, such as `SELECT`, that accept mailbox
    /// names.
    pub fn name(&self) -> &str {
        self.borrow_dependent().name
    }
}
