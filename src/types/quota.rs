use imap_proto::types::Quota as QuotaRef;
use imap_proto::types::QuotaResource as QuotaResourceRef;
use imap_proto::types::QuotaResourceName as QuotaResourceNameRef;
use imap_proto::types::QuotaRoot as QuotaRootRef;

/// <https://tools.ietf.org/html/rfc2087#section-3>
#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub enum QuotaResourceName {
    /// Sum of messages' RFC822.SIZE, in units of 1024 octets
    Storage,
    /// Number of messages
    Message,
    /// A different/custom resource
    Atom(String),
}

impl From<QuotaResourceNameRef<'_>> for QuotaResourceName {
    fn from(name: QuotaResourceNameRef<'_>) -> Self {
        match name {
            QuotaResourceNameRef::Message => QuotaResourceName::Message,
            QuotaResourceNameRef::Storage => QuotaResourceName::Storage,
            QuotaResourceNameRef::Atom(v) => QuotaResourceName::Atom(v.to_string()),
        }
    }
}

/// 5.1. QUOTA Response (<https://tools.ietf.org/html/rfc2087#section-5.1>)
#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct QuotaResource {
    /// name of the resource
    pub name: QuotaResourceName,
    /// current usage of the resource
    pub usage: u64,
    /// resource limit
    pub limit: u64,
}

impl From<QuotaResourceRef<'_>> for QuotaResource {
    fn from(resource: QuotaResourceRef<'_>) -> Self {
        Self {
            name: resource.name.into(),
            usage: resource.usage,
            limit: resource.limit,
        }
    }
}

impl QuotaResource {
    /// Returns the usage percentage of a QuotaResource.
    pub fn get_usage_percentage(&self) -> u64 {
        self.usage
            .saturating_mul(100)
            .checked_div(self.limit)
            // Assume that if `limit` is 0, this means that storage is unlimited:
            .unwrap_or(0)
    }
}

/// 5.1. QUOTA Response (<https://tools.ietf.org/html/rfc2087#section-5.1>)
#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct Quota {
    /// quota root name
    pub root_name: String,
    /// quota resources for this quota
    pub resources: Vec<QuotaResource>,
}

impl From<QuotaRef<'_>> for Quota {
    fn from(quota: QuotaRef<'_>) -> Self {
        Self {
            root_name: quota.root_name.to_string(),
            resources: quota.resources.iter().map(|r| r.clone().into()).collect(),
        }
    }
}

/// 5.2. QUOTAROOT Response (<https://tools.ietf.org/html/rfc2087#section-5.2>)
#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct QuotaRoot {
    /// mailbox name
    pub mailbox_name: String,
    /// zero or more quota root names
    pub quota_root_names: Vec<String>,
}

impl From<QuotaRootRef<'_>> for QuotaRoot {
    fn from(root: QuotaRootRef<'_>) -> Self {
        Self {
            mailbox_name: root.mailbox_name.to_string(),
            quota_root_names: root
                .quota_root_names
                .iter()
                .map(|n| n.to_string())
                .collect(),
        }
    }
}
