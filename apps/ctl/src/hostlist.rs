//! Hostlist expansion: `node-[1-4,7]` → `node-1 node-2 node-3 node-4 node-7`.
//!
//! The notation Slurm uses, because an operator of this framework already types it
//! all day. Supported: comma-separated items at the top level, bracketed ranges and
//! singletons inside an item, more than one bracket per item, and **zero-padded
//! ranges that keep their width** — `node-[08-11]` is `node-08 … node-11`, not
//! `node-8`, because a node's name is a string and `node-8` is a different machine
//! or no machine at all.
//!
//! Everything it rejects, it rejects loudly. A silently mis-expanded hostlist is a
//! command sent to the wrong nodes, which is the one failure an operator tool must
//! not have.

use cs_util::{Error, ErrorKind, Result};

/// Expand a hostlist into node names, in the order written, without duplicates.
pub fn expand(list: &str) -> Result<Vec<String>> {
    let mut nodes = Vec::new();
    for item in split_items(list)? {
        expand_item(&item, &mut nodes)?;
    }
    // Duplicates are not an error — two `--nodes` may overlap, and an operator who
    // wrote a node twice meant it once — but sending the same command twice to one
    // node would be.
    let mut seen = std::collections::HashSet::new();
    nodes.retain(|node| seen.insert(node.clone()));
    if nodes.is_empty() {
        return Err(Error::new(
            ErrorKind::Config,
            format!("{list:?} names no nodes"),
        ));
    }
    Ok(nodes)
}

/// Split on commas that are not inside brackets.
fn split_items(list: &str) -> Result<Vec<String>> {
    let mut items = Vec::new();
    let mut current = String::new();
    let mut depth = 0usize;

    for ch in list.chars() {
        match ch {
            '[' => {
                depth += 1;
                current.push(ch);
            }
            ']' => {
                depth = depth.checked_sub(1).ok_or_else(|| {
                    Error::new(ErrorKind::Config, format!("unmatched ']' in {list:?}"))
                })?;
                current.push(ch);
            }
            // The comma that separates items, rather than range members.
            ',' if depth == 0 => {
                items.push(std::mem::take(&mut current));
            }
            _ => current.push(ch),
        }
    }
    if depth > 0 {
        return Err(Error::new(
            ErrorKind::Config,
            format!("unclosed '[' in {list:?}"),
        ));
    }
    items.push(current);

    items.retain(|item| !item.trim().is_empty());
    Ok(items)
}

/// Expand one item, which may hold several brackets.
fn expand_item(item: &str, into: &mut Vec<String>) -> Result<()> {
    let item = item.trim();
    let Some(open) = item.find('[') else {
        into.push(item.to_owned());
        return Ok(());
    };
    let close = item[open..]
        .find(']')
        .ok_or_else(|| Error::new(ErrorKind::Config, format!("unclosed '[' in {item:?}")))?
        + open;

    let prefix = &item[..open];
    let suffix = &item[close + 1..];
    for member in item[open + 1..close].split(',') {
        for value in members(member.trim(), item)? {
            // Recurse, so a second bracket in the suffix is expanded too.
            expand_item(&format!("{prefix}{value}{suffix}"), into)?;
        }
    }
    Ok(())
}

/// One bracket member: `7`, or `1-4`.
fn members(member: &str, item: &str) -> Result<Vec<String>> {
    if member.is_empty() {
        return Err(Error::new(
            ErrorKind::Config,
            format!("empty range member in {item:?}"),
        ));
    }

    let Some((from, to)) = member.split_once('-') else {
        number(member, item)?;
        return Ok(vec![member.to_owned()]);
    };

    let start = number(from, item)?;
    let end = number(to, item)?;
    if start > end {
        return Err(Error::new(
            ErrorKind::Config,
            format!("range {member:?} in {item:?} counts backwards"),
        ));
    }
    const LIMIT: u64 = 100_000;
    if end - start >= LIMIT {
        return Err(Error::new(
            ErrorKind::Config,
            format!("range {member:?} in {item:?} covers more than {LIMIT} nodes"),
        ));
    }

    // The width of the *written* bound, so `[08-11]` stays two digits and `[8-11]`
    // does not become one.
    let width = if from.starts_with('0') || to.starts_with('0') {
        from.len().max(to.len())
    } else {
        0
    };
    Ok((start..=end)
        .map(|value| format!("{value:0width$}"))
        .collect())
}

/// One bound of a range, which has to be a number.
fn number(raw: &str, item: &str) -> Result<u64> {
    raw.parse().map_err(|_| {
        Error::new(
            ErrorKind::Config,
            format!("{raw:?} in {item:?} is not a number"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nodes(list: &str) -> Vec<String> {
        expand(list).unwrap_or_else(|err| panic!("{list:?} should expand: {err:?}"))
    }

    #[test]
    fn a_plain_name_is_itself() {
        assert_eq!(nodes("node-1"), vec!["node-1"]);
    }

    #[test]
    fn a_comma_separated_list_keeps_its_order() {
        assert_eq!(nodes("c,a,b"), vec!["c", "a", "b"]);
    }

    #[test]
    fn a_range_expands() {
        assert_eq!(
            nodes("node-[1-4]"),
            vec!["node-1", "node-2", "node-3", "node-4"]
        );
    }

    #[test]
    fn a_range_list_mixes_singletons_and_ranges() {
        assert_eq!(nodes("n[1-2,7,9-10]"), vec!["n1", "n2", "n7", "n9", "n10"]);
    }

    /// The one that matters: `node-08` and `node-8` are different machines.
    #[test]
    fn zero_padding_is_preserved_at_its_written_width() {
        assert_eq!(
            nodes("node-[08-11]"),
            vec!["node-08", "node-09", "node-10", "node-11"]
        );
        assert_eq!(nodes("node-[001-002]"), vec!["node-001", "node-002"]);
        assert_eq!(
            nodes("node-[8-11]"),
            vec!["node-8", "node-9", "node-10", "node-11"],
            "no leading zero means no padding"
        );
    }

    #[test]
    fn a_suffix_after_the_bracket_is_kept() {
        assert_eq!(nodes("rack[1-2]-head"), vec!["rack1-head", "rack2-head"]);
    }

    #[test]
    fn two_brackets_multiply() {
        assert_eq!(nodes("r[1-2]n[1-2]"), vec!["r1n1", "r1n2", "r2n1", "r2n2"]);
    }

    #[test]
    fn a_comma_inside_brackets_is_not_an_item_separator() {
        assert_eq!(nodes("a[1,2],b"), vec!["a1", "a2", "b"]);
    }

    #[test]
    fn duplicates_are_collapsed_because_a_command_must_not_be_sent_twice() {
        assert_eq!(nodes("node-1,node-[1-2],node-1"), vec!["node-1", "node-2"]);
    }

    #[test]
    fn whitespace_and_empty_items_are_tolerated() {
        assert_eq!(nodes(" node-1 , node-2 ,"), vec!["node-1", "node-2"]);
    }

    #[test]
    fn a_backwards_range_is_rejected_rather_than_silently_empty() {
        let err = expand("node-[5-1]").expect_err("backwards");
        assert_eq!(err.kind(), ErrorKind::Config);
        assert!(err.to_string().contains("backwards"), "{err}");
    }

    #[test]
    fn an_unclosed_bracket_is_rejected() {
        assert!(expand("node-[1-4").is_err());
        assert!(expand("node-1]").is_err());
    }

    #[test]
    fn a_non_numeric_range_is_rejected() {
        let err = expand("node-[a-b]").expect_err("not numbers");
        assert!(err.to_string().contains("not a number"), "{err}");
    }

    #[test]
    fn an_empty_list_names_no_nodes_and_says_so() {
        assert!(expand("").is_err());
        assert!(expand(" , ").is_err());
    }

    #[test]
    fn an_absurd_range_is_refused_before_it_allocates() {
        let err = expand("n[1-999999]").expect_err("too many");
        assert!(err.to_string().contains("more than"), "{err}");
    }
}
