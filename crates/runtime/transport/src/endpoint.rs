use std::fmt;
use std::str::FromStr;

use cs_util::{Error, ErrorKind, Result};

/// Where to dial, or where to listen.
///
/// A `scheme://authority` string rather than an enum, because the set of
/// transports is open: `tcp://head01:7777`, `mock://server`,
/// `ucx://10.0.0.1:18515`. The engine passes one through without interpreting it;
/// each transport checks the scheme it answers to and parses the authority
/// however it likes.
///
/// ```
/// use cs_transport::Endpoint;
///
/// let ep: Endpoint = "tcp://head01:7777".parse()?;
/// assert_eq!(ep.scheme(), "tcp");
/// assert_eq!(ep.authority(), "head01:7777");
/// assert!(ep.has_scheme("tcp"));
/// # Ok::<(), cs_util::Error>(())
/// ```
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Endpoint {
    raw: String,
    /// Index of the first byte after `://`.
    authority_at: usize,
}

impl Endpoint {
    /// Parse an endpoint.
    ///
    /// Requires a `scheme://authority` shape, a scheme of `[a-z0-9+.-]`, and a
    /// non-empty authority. Anything else is [`ErrorKind::Config`] — a bad
    /// endpoint is a configuration mistake, caught at startup rather than on the
    /// first reconnect.
    pub fn parse(raw: impl Into<String>) -> Result<Self> {
        let raw = raw.into();
        let Some(split) = raw.find("://") else {
            return Err(config_error(format!(
                "endpoint {raw:?} is not scheme://authority"
            )));
        };
        let (scheme, authority_at) = (&raw[..split], split + 3);

        if scheme.is_empty() {
            return Err(config_error(format!("endpoint {raw:?} has no scheme")));
        }
        if let Some(bad) = scheme
            .chars()
            .find(|c| !matches!(c, 'a'..='z' | '0'..='9' | '+' | '-' | '.'))
        {
            return Err(config_error(format!(
                "endpoint {raw:?} has {bad:?} in its scheme; use lowercase [a-z0-9+.-]"
            )));
        }
        if raw[authority_at..].is_empty() {
            return Err(config_error(format!("endpoint {raw:?} has no authority")));
        }

        Ok(Self { raw, authority_at })
    }

    /// The scheme, without `://`.
    #[must_use]
    pub fn scheme(&self) -> &str {
        &self.raw[..self.authority_at - 3]
    }

    /// Everything after `://`. Never empty.
    #[must_use]
    pub fn authority(&self) -> &str {
        &self.raw[self.authority_at..]
    }

    /// The whole endpoint as written.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.raw
    }

    /// Whether this endpoint is for `scheme`.
    #[must_use]
    pub fn has_scheme(&self, scheme: &str) -> bool {
        self.scheme() == scheme
    }

    /// The authority, if this endpoint is for `scheme`.
    ///
    /// What a transport calls first: it either gets the part it knows how to
    /// parse, or an error naming the mismatch.
    ///
    /// ```
    /// use cs_transport::Endpoint;
    ///
    /// let ep: Endpoint = "tcp://head01:7777".parse()?;
    /// assert_eq!(ep.require_scheme("tcp")?, "head01:7777");
    /// assert!(ep.require_scheme("ucx").is_err());
    /// # Ok::<(), cs_util::Error>(())
    /// ```
    pub fn require_scheme(&self, scheme: &str) -> Result<&str> {
        if self.has_scheme(scheme) {
            Ok(self.authority())
        } else {
            Err(config_error(format!(
                "endpoint {} is not a {scheme} endpoint",
                self.raw
            )))
        }
    }

    /// Build an endpoint from its parts, without parsing.
    ///
    /// # Panics
    ///
    /// Never — but it does not validate either, so prefer
    /// [`parse`](Endpoint::parse) for anything from configuration. Intended for a
    /// transport reporting an endpoint it just bound or accepted.
    #[must_use]
    pub fn from_parts(scheme: &str, authority: &str) -> Self {
        Self {
            raw: format!("{scheme}://{authority}"),
            authority_at: scheme.len() + 3,
        }
    }
}

impl FromStr for Endpoint {
    type Err = Error;

    fn from_str(raw: &str) -> Result<Self> {
        Self::parse(raw)
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.raw)
    }
}

impl fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.raw)
    }
}

#[track_caller]
fn config_error(msg: String) -> Error {
    Error::new(ErrorKind::Config, msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_scheme_and_authority() {
        for (raw, scheme, authority) in [
            ("tcp://head01:7777", "tcp", "head01:7777"),
            ("mock://server", "mock", "server"),
            ("ucx://10.0.0.1:18515", "ucx", "10.0.0.1:18515"),
            (
                "grpc+tls://head01:443/session",
                "grpc+tls",
                "head01:443/session",
            ),
            ("tcp://[::1]:7777", "tcp", "[::1]:7777"),
            // `://` inside the authority belongs to the authority.
            ("mock://a://b", "mock", "a://b"),
        ] {
            let ep = Endpoint::parse(raw).expect(raw);
            assert_eq!(ep.scheme(), scheme, "{raw}");
            assert_eq!(ep.authority(), authority, "{raw}");
            assert_eq!(ep.as_str(), raw);
            assert_eq!(ep.to_string(), raw);
        }
    }

    #[test]
    fn rejects_malformed_endpoints_as_configuration_errors() {
        for bad in [
            "head01:7777",
            "://head01",
            "tcp:/head01",
            "tcp://",
            "TCP://head01:7777",
            "tcp x://head01",
            "",
        ] {
            let err = Endpoint::parse(bad).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Config, "{bad:?}");
            assert!(!err.is_retryable(), "{bad:?} must not be retried");
        }
    }

    #[test]
    fn require_scheme_is_how_a_transport_rejects_a_foreign_endpoint() {
        let ep = Endpoint::parse("tcp://head01:7777").unwrap();
        assert_eq!(ep.require_scheme("tcp").unwrap(), "head01:7777");

        let err = ep.require_scheme("mock").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Config);
        assert!(err.to_string().contains("not a mock endpoint"));
    }

    #[test]
    fn from_parts_round_trips_through_parse() {
        let ep = Endpoint::from_parts("tcp", "10.0.0.5:34521");
        assert_eq!(ep.scheme(), "tcp");
        assert_eq!(ep.authority(), "10.0.0.5:34521");
        assert_eq!(Endpoint::parse(ep.as_str()).unwrap(), ep);
    }

    #[test]
    fn endpoints_are_usable_as_map_keys_and_debug_readably() {
        let a: Endpoint = "tcp://a:1".parse().unwrap();
        let b: Endpoint = "tcp://a:1".parse().unwrap();
        let c: Endpoint = "tcp://a:2".parse().unwrap();
        assert_eq!(a, b);
        assert_ne!(a, c);

        let set: std::collections::HashSet<_> = [a.clone(), b, c].into_iter().collect();
        assert_eq!(set.len(), 2);
        assert_eq!(format!("{a:?}"), "\"tcp://a:1\"");
    }
}
