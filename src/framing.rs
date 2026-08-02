//! Shared wire framing primitives for libp2p tunnel substreams and RPC.
//!
//! Two protocols use these primitives:
//!
//! ## Tagged framing (tunnel data)
//! Each message is: `[1-byte tag] [4-byte BE length] [payload]`
//! - `TAG_JSON` (0x01): JSON text payload
//! - `TAG_BINARY` (0x02): raw binary payload
//! - `TAG_END` (0x03): end-of-stream marker (no length/payload)
//!
//! ## Length-prefixed framing (RPC)
//! Each message is: `[4-byte BE length] [JSON payload]`
//! Used by the persistent RPC stream between daemon and relay.

use futures_util::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use std::io;

// ── Constants ────────────────────────────────────────────────────────

pub const TAG_JSON: u8 = 0x01;
pub const TAG_BINARY: u8 = 0x02;
pub const TAG_END: u8 = 0x03;

/// Maximum frame payload: 16 MiB.
pub const MAX_FRAME_SIZE: u32 = 16 * 1024 * 1024;

/// Payload size a large binary body is split into before writing.
///
/// A peer that reads a frame larger than [`MAX_FRAME_SIZE`] tears down the
/// whole stream, so writers must never emit one. 1 MiB leaves plenty of
/// headroom and matches the chunk size the file tunnels already use.
pub const CHUNK_SIZE: usize = 1024 * 1024;

// ── Tagged framing (tunnel data streams) ─────────────────────────────

/// A tagged frame read from a stream.
#[derive(Debug)]
pub enum TaggedFrame {
    Json(serde_json::Value),
    Binary(Vec<u8>),
    End,
}

/// Read one tagged frame. Returns `None` on EOF.
pub async fn read_tagged_frame<T>(io: &mut T) -> io::Result<Option<TaggedFrame>>
where
    T: AsyncRead + Unpin + Send,
{
    let mut tag_buf = [0u8; 1];
    match io.read_exact(&mut tag_buf).await {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }

    match tag_buf[0] {
        TAG_END => Ok(Some(TaggedFrame::End)),
        TAG_JSON => {
            let payload = read_payload(io).await?;
            let val = serde_json::from_slice(&payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Some(TaggedFrame::Json(val)))
        }
        TAG_BINARY => {
            let payload = read_payload(io).await?;
            Ok(Some(TaggedFrame::Binary(payload)))
        }
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown frame tag: 0x{other:02x}"),
        )),
    }
}

/// Write a JSON tagged frame.
pub async fn write_json<T>(io: &mut T, val: &serde_json::Value) -> io::Result<()>
where
    T: AsyncWrite + Unpin + Send,
{
    let data =
        serde_json::to_vec(val).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    write_tagged(io, TAG_JSON, &data).await
}

/// Write binary data as one or more tagged frames.
///
/// Payloads over [`CHUNK_SIZE`] are split across frames. Readers concatenate
/// binary frames until the end marker, so splitting is invisible to them —
/// whereas a single oversized frame would make the peer drop the stream.
pub async fn write_binary<T>(io: &mut T, data: &[u8]) -> io::Result<()>
where
    T: AsyncWrite + Unpin + Send,
{
    if data.is_empty() {
        return write_tagged(io, TAG_BINARY, data).await;
    }
    for chunk in data.chunks(CHUNK_SIZE) {
        write_tagged(io, TAG_BINARY, chunk).await?;
    }
    Ok(())
}

/// Read binary frames until the end marker or EOF, concatenating them.
///
/// A JSON frame during the data phase also ends the body (legacy end signal).
/// Errors with `InvalidData` once more than `limit` bytes have arrived.
pub async fn read_binary_body<T>(io: &mut T, limit: usize) -> io::Result<Vec<u8>>
where
    T: AsyncRead + Unpin + Send,
{
    let mut body = Vec::new();
    loop {
        match read_tagged_frame(io).await? {
            Some(TaggedFrame::Binary(data)) => {
                body.extend_from_slice(&data);
                if body.len() > limit {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("body too large: over {limit} bytes"),
                    ));
                }
            }
            Some(TaggedFrame::End) | Some(TaggedFrame::Json(_)) | None => return Ok(body),
        }
    }
}

/// Write the end-of-stream marker.
pub async fn write_end<T>(io: &mut T) -> io::Result<()>
where
    T: AsyncWrite + Unpin + Send,
{
    io.write_all(&[TAG_END]).await?;
    io.flush().await
}

// ── Length-prefixed framing (RPC) ────────────────────────────────────

/// Read a length-prefixed JSON value.
pub async fn read_lp_json<T>(io: &mut T) -> io::Result<serde_json::Value>
where
    T: AsyncRead + Unpin + Send,
{
    let payload = read_payload(io).await?;
    serde_json::from_slice(&payload).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Write a length-prefixed JSON value.
pub async fn write_lp_json<T>(io: &mut T, val: &serde_json::Value) -> io::Result<()>
where
    T: AsyncWrite + Unpin + Send,
{
    let data =
        serde_json::to_vec(val).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if data.len() > MAX_FRAME_SIZE as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame too large: {} bytes", data.len()),
        ));
    }
    io.write_all(&(data.len() as u32).to_be_bytes()).await?;
    io.write_all(&data).await?;
    io.flush().await?;
    Ok(())
}

// ── Internal ─────────────────────────────────────────────────────────

async fn read_payload<T>(io: &mut T) -> io::Result<Vec<u8>>
where
    T: AsyncRead + Unpin + Send,
{
    let mut len_buf = [0u8; 4];
    io.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame too large: {len} bytes"),
        ));
    }
    let mut buf = vec![0u8; len as usize];
    io.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn write_tagged<T>(io: &mut T, tag: u8, data: &[u8]) -> io::Result<()>
where
    T: AsyncWrite + Unpin + Send,
{
    // Binary payloads are pre-split by write_binary; a JSON frame this big is
    // a bug — fail the write instead of emitting a frame the peer will die on.
    if data.len() > MAX_FRAME_SIZE as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame too large: {} bytes", data.len()),
        ));
    }
    io.write_all(&[tag]).await?;
    io.write_all(&(data.len() as u32).to_be_bytes()).await?;
    io.write_all(data).await?;
    // A frame is a complete message — flush so streamed consumers (SSE
    // chunks relayed frame-by-frame) see it immediately.
    io.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Count the tagged frames in a written buffer.
    fn frame_count(mut buf: &[u8]) -> usize {
        let mut n = 0;
        while !buf.is_empty() {
            n += 1;
            if buf[0] == TAG_END {
                buf = &buf[1..];
                continue;
            }
            let len = u32::from_be_bytes(buf[1..5].try_into().unwrap()) as usize;
            buf = &buf[5 + len..];
        }
        n
    }

    #[tokio::test]
    async fn large_binary_is_split_into_chunk_sized_frames() {
        let payload = vec![7u8; CHUNK_SIZE * 2 + 512];
        let mut buf = Vec::new();
        write_binary(&mut buf, &payload).await.unwrap();
        write_end(&mut buf).await.unwrap();

        // 3 binary frames + end marker, none over the wire limit.
        assert_eq!(frame_count(&buf), 4);
        assert_eq!(
            u32::from_be_bytes(buf[1..5].try_into().unwrap()) as usize,
            CHUNK_SIZE
        );

        // Reassembles to exactly the original bytes.
        let mut cursor = futures_util::io::Cursor::new(buf);
        let body = read_binary_body(&mut cursor, usize::MAX).await.unwrap();
        assert_eq!(body, payload);
    }

    #[tokio::test]
    async fn empty_binary_still_writes_one_frame() {
        let mut buf = Vec::new();
        write_binary(&mut buf, &[]).await.unwrap();
        assert_eq!(frame_count(&buf), 1);
    }

    #[tokio::test]
    async fn read_binary_body_rejects_over_limit() {
        let mut buf = Vec::new();
        write_binary(&mut buf, &vec![0u8; 4096]).await.unwrap();
        write_end(&mut buf).await.unwrap();

        let mut cursor = futures_util::io::Cursor::new(buf);
        let err = read_binary_body(&mut cursor, 1024).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn oversized_json_frame_errors_instead_of_writing() {
        // A string long enough that the serialized JSON exceeds the limit.
        let huge = serde_json::json!("x".repeat(MAX_FRAME_SIZE as usize + 1));
        let mut buf = Vec::new();
        assert!(write_json(&mut buf, &huge).await.is_err());
        assert!(write_lp_json(&mut buf, &huge).await.is_err());
        assert!(buf.is_empty());
    }
}
