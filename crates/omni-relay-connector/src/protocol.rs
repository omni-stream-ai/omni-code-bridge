use serde::{Deserialize, Serialize};

pub type Headers = Vec<(String, String)>;
pub const MAX_CHUNK: usize = 64 * 1024;
pub const MAX_HEAD: usize = 32 * 1024;

#[derive(Debug, Serialize, Deserialize)]
pub struct Open {
    pub id: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Frame {
    Request {
        method: String,
        path: String,
        headers: Headers,
        websocket: bool,
    },
    Response {
        status: u16,
        headers: Headers,
    },
    End,
    /// Release one bounded HTTP chunk in the opposite direction.
    Ack,
    Error,
}

// Both endpoints strip hop-by-hop fields. Connector supplies upstream credentials
// from its local configuration, never from an untrusted incoming request.
pub fn forward_headers(headers: &http::HeaderMap, request: bool) -> Headers {
    let connection: Vec<String> = headers
        .get_all("connection")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(',').map(|s| s.trim().to_ascii_lowercase()))
        .collect();
    headers
        .iter()
        .filter_map(|(name, value)| {
            let name = name.as_str();
            let excluded = matches!(
                name,
                "connection"
                    | "keep-alive"
                    | "proxy-authenticate"
                    | "proxy-authorization"
                    | "te"
                    | "trailer"
                    | "transfer-encoding"
                    | "upgrade"
                    | "host"
                    | "content-length"
                    | "sec-websocket-key"
                    | "sec-websocket-version"
                    | "sec-websocket-accept"
                    | "sec-websocket-extensions"
            ) || connection.iter().any(|n| n == name)
                || (request
                    && (matches!(
                        name,
                        "authorization"
                            | "cookie"
                            | "x-omni-code-client-id"
                            | "forwarded"
                            | "x-real-ip"
                    ) || name.starts_with("x-forwarded-")
                        || name.starts_with("x-relay-")))
                || (request && name.starts_with("cf-"))
                || (!request && (name == "set-cookie" || name.starts_with("access-control-")));
            (!excluded)
                .then(|| value.to_str().ok().map(|v| (name.to_owned(), v.to_owned())))
                .flatten()
        })
        .collect()
}
