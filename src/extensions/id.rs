//! IMAP ID extension specified in [RFC2971](https://datatracker.ietf.org/doc/html/rfc2971)

use async_channel as channel;
use futures::io;
use futures::prelude::*;
use imap_proto::{self, RequestId, Response};
use std::collections::HashMap;

use crate::types::ResponseData;
use crate::types::*;
use crate::{
    error::Result,
    parse::{filter, handle_unilateral},
};

fn escape(s: &str) -> String {
    s.replace('\\', r"\\").replace('\"', "\\\"")
}

/// Formats list of key-value pairs for ID command.
///
/// Returned list is not wrapped in parenthesis, the caller should do it.
pub(crate) fn format_identification<'a, 'b>(
    id: impl IntoIterator<Item = (&'a str, Option<&'b str>)>,
) -> String {
    id.into_iter()
        .map(|(k, v)| {
            format!(
                "\"{}\" {}",
                escape(k),
                v.map_or("NIL".to_string(), |v| format!("\"{}\"", escape(v)))
            )
        })
        .collect::<Vec<String>>()
        .join(" ")
}

pub(crate) async fn parse_id<T: Stream<Item = io::Result<ResponseData>> + Unpin>(
    stream: &mut T,
    unsolicited: channel::Sender<UnsolicitedResponse>,
    command_tag: RequestId,
) -> Result<Option<HashMap<String, String>>> {
    let mut id = None;
    while let Some(resp) = stream
        .take_while(|res| filter(res, &command_tag))
        .try_next()
        .await?
    {
        match resp.parsed() {
            Response::Id(res) => {
                id = res.as_ref().map(|m| {
                    m.iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect()
                })
            }
            _ => {
                handle_unilateral(resp, unsolicited.clone());
            }
        }
    }

    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_identification() {
        assert_eq!(
            format_identification([("name", Some("MyClient"))]),
            r#""name" "MyClient""#
        );

        assert_eq!(
            format_identification([("name", Some(r#""MyClient"\"#))]),
            r#""name" "\"MyClient\"\\""#
        );

        assert_eq!(
            format_identification([("name", Some("MyClient")), ("version", Some("2.0"))]),
            r#""name" "MyClient" "version" "2.0""#
        );

        assert_eq!(
            format_identification([("name", None), ("version", Some("2.0"))]),
            r#""name" NIL "version" "2.0""#
        );
    }
}
