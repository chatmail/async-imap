use imap_proto::RequestId;

/// Request ID generator.
#[derive(Debug)]
pub struct IdGenerator {
    /// Last returned ID.
    next: u64,
}

impl IdGenerator {
    /// Creates a new request ID generator.
    pub fn new() -> Self {
        Self { next: 0 }
    }

    pub fn next(&mut self) -> RequestId {
        self.next += 1;
        RequestId(format!("A{:04}", self.next % 10_000))
    }
}

impl Default for IdGenerator {
    fn default() -> Self {
        Self::new()
    }
}
