use std::fmt;

use bytes::BytesMut;
use imap_proto::{RequestId, Response};
use self_cell::self_cell;

self_cell!(
    pub struct ResponseData {
        owner: BytesMut,

        #[covariant]
        dependent: Response,
    }
);

impl std::cmp::PartialEq for ResponseData {
    fn eq(&self, other: &Self) -> bool {
        self.parsed() == other.parsed()
    }
}

impl std::cmp::Eq for ResponseData {}

impl fmt::Debug for ResponseData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponseData")
            .field("raw", &self.borrow_owner().len())
            .field("response", self.borrow_dependent())
            .finish()
    }
}

impl ResponseData {
    pub fn request_id(&self) -> Option<&RequestId> {
        match self.borrow_dependent() {
            Response::Done { tag, .. } => Some(tag),
            _ => None,
        }
    }

    pub fn parsed(&self) -> &Response<'_> {
        self.borrow_dependent()
    }
}
