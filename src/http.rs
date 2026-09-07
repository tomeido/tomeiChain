//! Bounded HTTP/1 request framing for the connection-close JSON API.

use crate::{ChainError, Result, MAX_HTTP_REQUEST_BYTES};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Semaphore, SemaphorePermit};

const MAX_HEADER_BYTES: usize = 64 * 1024;
static BODY_BUDGET: Semaphore = Semaphore::const_new(64 * 1024 * 1024);

pub(crate) struct Request {
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub body: Vec<u8>,
    pub authorization: Option<String>,
    // Keep the reservation through routing, where JSON decoding also needs the body.
    _body_permit: SemaphorePermit<'static>,
}

fn bad_request(message: &str) -> ChainError {
    ChainError::InvalidTx(message.to_string())
}

fn is_token(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte)
}

pub(crate) async fn read_request(stream: &mut TcpStream) -> Result<Request> {
    let mut buffered = Vec::with_capacity(1024);
    let mut chunk = [0u8; 8192];
    let header_end = loop {
        if let Some(end) = buffered.windows(4).position(|part| part == b"\r\n\r\n") {
            break end;
        }
        if buffered.len() == MAX_HEADER_BYTES {
            return Err(ChainError::TooLarge("request headers too large".into()));
        }
        let room = chunk.len().min(MAX_HEADER_BYTES - buffered.len());
        let read = stream.read(&mut chunk[..room]).await?;
        if read == 0 {
            return Err(bad_request("incomplete request headers"));
        }
        buffered.extend_from_slice(&chunk[..read]);
    };

    let header_text = std::str::from_utf8(&buffered[..header_end])
        .map_err(|_| bad_request("request headers must be UTF-8"))?;
    let mut lines = header_text.split("\r\n");
    let first = lines.next().unwrap_or_default();
    let parts: Vec<&str> = first.split(' ').collect();
    if parts.len() != 3
        || parts[0].is_empty()
        || !parts[0].bytes().all(is_token)
        || !parts[1].starts_with('/')
        || parts[1].contains('#')
        || parts[1].bytes().any(|b| b <= 0x20 || b == 0x7f)
        || !matches!(parts[2], "HTTP/1.0" | "HTTP/1.1")
    {
        return Err(bad_request("invalid HTTP request line"));
    }
    let method = parts[0].to_string();
    let (path, query) = match parts[1].split_once('?') {
        Some((path, query)) => (path.to_string(), Some(query.to_string())),
        None => (parts[1].to_string(), None),
    };

    let mut content_length = None;
    let mut authorization = None;
    let mut expect_continue = false;
    let mut has_transfer_encoding = false;
    for line in lines {
        let (name, raw_value) = line
            .split_once(':')
            .ok_or_else(|| bad_request("invalid HTTP header"))?;
        if name.is_empty()
            || !name.bytes().all(is_token)
            || raw_value
                .bytes()
                .any(|b| (b < 0x20 && b != b'\t') || b == 0x7f)
        {
            return Err(bad_request("invalid HTTP header"));
        }
        let value = raw_value.trim_matches([' ', '\t']);
        if name.eq_ignore_ascii_case("content-length") {
            if content_length.is_some() {
                return Err(bad_request("duplicate Content-Length"));
            }
            if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
                return Err(bad_request("invalid Content-Length"));
            }
            let length = value
                .parse::<usize>()
                .map_err(|_| ChainError::TooLarge("body too large".into()))?;
            if length > MAX_HTTP_REQUEST_BYTES {
                return Err(ChainError::TooLarge("body too large".into()));
            }
            content_length = Some(length);
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            has_transfer_encoding = true;
        } else if name.eq_ignore_ascii_case("authorization") {
            if authorization.is_some() {
                return Err(bad_request("duplicate Authorization"));
            }
            authorization = Some(value.to_string());
        } else if name.eq_ignore_ascii_case("expect") {
            if !value.eq_ignore_ascii_case("100-continue") || expect_continue {
                return Err(bad_request("unsupported or duplicate Expect"));
            }
            expect_continue = true;
        }
    }
    if has_transfer_encoding {
        return Err(ChainError::Unsupported(
            "Transfer-Encoding is unsupported; use Content-Length".into(),
        ));
    }
    if content_length.is_none() && matches!(method.as_str(), "POST" | "PUT" | "PATCH") {
        return Err(ChainError::Unsupported("Content-Length is required".into()));
    }
    let content_length = content_length.unwrap_or(0);
    let body_permit = BODY_BUDGET
        .try_acquire_many(content_length as u32)
        .map_err(|_| ChainError::TooLarge("concurrent request body budget exhausted".into()))?;
    if expect_continue {
        stream.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").await?;
    }

    let already_read = &buffered[header_end + 4..];
    let mut body = Vec::with_capacity(content_length);
    body.extend_from_slice(&already_read[..already_read.len().min(content_length)]);
    while body.len() < content_length {
        let needed = chunk.len().min(content_length - body.len());
        let read = stream.read(&mut chunk[..needed]).await?;
        if read == 0 {
            return Err(bad_request("request body shorter than Content-Length"));
        }
        body.extend_from_slice(&chunk[..read]);
    }
    Ok(Request {
        method,
        path,
        query,
        body,
        authorization,
        _body_permit: body_permit,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio::time::{timeout, Duration};

    async fn parse(raw: Vec<u8>) -> Result<Request> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (mut server, _) = listener.accept().await.unwrap();
        let writer = tokio::spawn(async move {
            let _ = client.write_all(&raw).await;
            let _ = client.shutdown().await;
        });
        let result = timeout(Duration::from_secs(5), read_request(&mut server))
            .await
            .expect("parser did not terminate");
        writer.abort();
        result
    }

    #[tokio::test]
    async fn preserves_auth_query_and_exact_body() {
        let request = parse(
            b"POST /tx?from=1 HTTP/1.1\r\nHost: localhost\r\naUtHoRiZaTiOn: Bearer secret\r\nContent-Length: 2\r\n\r\n{}extra"
                .to_vec(),
        )
        .await
        .unwrap();
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/tx");
        assert_eq!(request.query.as_deref(), Some("from=1"));
        assert_eq!(request.authorization.as_deref(), Some("Bearer secret"));
        assert_eq!(request.body, b"{}");
    }

    #[tokio::test]
    async fn rejects_ambiguous_lengths_and_truncated_json() {
        for headers in [
            "Content-Length: 2\r\nContent-Length: 2",
            "Content-Length: 2\r\ncontent-length: 9",
            "Content-Length: -1",
            "Content-Length: +2",
            "Content-Length: 2, 2",
            "Content-Length: nope",
            "Content-Length:",
            "Content-Length : 2",
            "Content-Length: 3",
        ] {
            let raw = format!("POST /tx HTTP/1.1\r\n{headers}\r\n\r\n{{}}");
            assert!(
                parse(raw.into_bytes()).await.is_err(),
                "accepted {headers:?}"
            );
        }
    }

    #[tokio::test]
    async fn rejects_all_transfer_encodings_and_missing_post_length() {
        for encoding in ["chunked", "identity", "gzip", "", "gzip, chunked"] {
            let raw = format!(
                "POST /tx HTTP/1.1\r\nTransfer-Encoding: {encoding}\r\nContent-Length: 2\r\n\r\n{{}}"
            );
            assert!(matches!(
                parse(raw.into_bytes()).await,
                Err(ChainError::Unsupported(_))
            ));
        }
        assert!(matches!(
            parse(b"POST /tx HTTP/1.1\r\nHost: localhost\r\n\r\n".to_vec()).await,
            Err(ChainError::Unsupported(_))
        ));
    }

    #[tokio::test]
    async fn rejects_oversized_headers_and_body_before_reading_body() {
        let raw = format!(
            "GET /health HTTP/1.1\r\nX-Pad: {}\r\n\r\n",
            "x".repeat(MAX_HEADER_BYTES)
        );
        assert!(matches!(
            parse(raw.into_bytes()).await,
            Err(ChainError::TooLarge(_))
        ));
        let raw = format!(
            "POST /tx HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
            MAX_HTTP_REQUEST_BYTES + 1
        );
        assert!(matches!(
            parse(raw.into_bytes()).await,
            Err(ChainError::TooLarge(_))
        ));
    }

    #[tokio::test]
    async fn rejects_invalid_request_lines_headers_and_duplicate_credentials() {
        for raw in [
            "GET /health\r\n\r\n",
            "GET /health HTTP/2\r\n\r\n",
            "GET http://localhost/health HTTP/1.1\r\n\r\n",
            "GET /health HTTP/1.1 extra\r\n\r\n",
            "GET /health HTTP/1.1\r\nBroken-header\r\n\r\n",
            "GET /health HTTP/1.1\r\n Folded: value\r\n\r\n",
            "GET /health HTTP/1.1\r\nX-Header: value\nInjected: value\r\n\r\n",
            "POST /tx HTTP/1.1\r\nAuthorization: Bearer one\r\nauthorization: Bearer two\r\nContent-Length: 0\r\n\r\n",
        ] {
            assert!(parse(raw.as_bytes().to_vec()).await.is_err(), "accepted {raw:?}");
        }
    }

    #[tokio::test]
    async fn sends_continue_before_waiting_for_body() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (mut server, _) = listener.accept().await.unwrap();
        let parser = tokio::spawn(async move { read_request(&mut server).await });
        client
            .write_all(b"POST /tx HTTP/1.1\r\nContent-Length: 2\r\nExpect: 100-continue\r\n\r\n")
            .await
            .unwrap();
        let mut interim = [0; 25];
        timeout(Duration::from_secs(5), client.read_exact(&mut interim))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&interim, b"HTTP/1.1 100 Continue\r\n\r\n");
        client.write_all(b"{}").await.unwrap();
        let request = timeout(Duration::from_secs(5), parser)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(request.body, b"{}");
    }
}
