//! The layout of the fragmented MP4 files of `VideoEncoder`, read without FFmpeg: the header
//! (`ftyp` and `moov`), then a fragment (`moof` and `mdat`) per frame, in order. The header
//! followed by any of the fragments, in order, is a video of those frames: an MPEG-DASH
//! initialization segment and media segments.

use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;

use super::Error;

/// Where the parts of a video are in its file, as byte ranges.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mp4Layout {
    /// The header: `ftyp` and `moov`, the DASH initialization segment.
    pub header: Range<u64>,
    /// The fragment of each frame, in order: its `moof` and `mdat`.
    pub fragments: Vec<Range<u64>>,
}

/// The `styp` box that starts a DASH media segment of fragments of these files (ISO/IEC
/// 23009-1, 6.3.4.2): brand `msdh`, compatible with `msdh` and `msix`.
pub const SEGMENT_TYPE_BOX: [u8; 24] = [
    0, 0, 0, 24, b's', b't', b'y', b'p', b'm', b's', b'd', b'h', 0, 0, 0, 0, b'm', b's', b'd',
    b'h', b'm', b's', b'i', b'x',
];

fn invalid(message: impl Into<String>) -> Error {
    Error::Frame(format!("not a video of fragments: {}", message.into()))
}

/// Reads the layout of the video `file`, of `len` bytes: only the headers of its top-level
/// boxes are read.
pub fn mp4_layout(mut file: impl Read + Seek, len: u64) -> Result<Mp4Layout, Error> {
    let mut header_end: Option<u64> = None;
    let mut fragments = Vec::new();
    // The start of the fragment whose `moof` was read, waiting for its `mdat`.
    let mut moof: Option<u64> = None;
    let mut offset = 0;
    while offset < len {
        file.seek(SeekFrom::Start(offset))?;
        let mut head = [0; 8];
        file.read_exact(&mut head)?;
        let mut size = u64::from(u32::from_be_bytes([head[0], head[1], head[2], head[3]]));
        let kind = [head[4], head[5], head[6], head[7]];
        if size == 1 {
            let mut large = [0; 8];
            file.read_exact(&mut large)?;
            size = u64::from_be_bytes(large);
        } else if size == 0 {
            size = len - offset;
        }
        if size < 8 || offset + size > len {
            return Err(invalid(format!("a box of {size} bytes at {offset}")));
        }
        match &kind {
            b"ftyp" | b"moov" if header_end.is_none() => {}
            b"moof" if moof.is_none() => {
                header_end.get_or_insert(offset);
                moof = Some(offset);
            }
            b"mdat" => {
                let start = moof
                    .take()
                    .ok_or_else(|| invalid(format!("data with no fragment at {offset}")))?;
                fragments.push(start..offset + size);
            }
            other => {
                return Err(invalid(format!(
                    "a `{}` box at {offset}",
                    String::from_utf8_lossy(other)
                )));
            }
        }
        offset += size;
    }
    if moof.is_some() {
        return Err(invalid("a fragment with no data"));
    }
    let header_end = header_end.ok_or_else(|| invalid("no fragments"))?;
    Ok(Mp4Layout {
        header: 0..header_end,
        fragments,
    })
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    fn mp4_box(kind: &[u8; 4], body: usize) -> Vec<u8> {
        let mut bytes = ((8 + body) as u32).to_be_bytes().to_vec();
        bytes.extend_from_slice(kind);
        bytes.extend(std::iter::repeat_n(0, body));
        bytes
    }

    fn layout(boxes: &[Vec<u8>], extra_len: u64) -> Result<Mp4Layout, Error> {
        let file = boxes.concat();
        mp4_layout(Cursor::new(&file), file.len() as u64 + extra_len)
    }

    #[test]
    fn fragments_are_found() {
        let layout = layout(
            &[
                mp4_box(b"ftyp", 12),
                mp4_box(b"moov", 30),
                mp4_box(b"moof", 10),
                mp4_box(b"mdat", 100),
                mp4_box(b"moof", 10),
                mp4_box(b"mdat", 5),
            ],
            0,
        )
        .unwrap();
        assert_eq!(layout.header, 0..58);
        assert_eq!(layout.fragments, [58..184, 184..215]);
    }

    #[test]
    fn other_files_are_rejected() {
        for (boxes, extra_len) in [
            (vec![mp4_box(b"ftyp", 12), mp4_box(b"moov", 30)], 0),
            (vec![mp4_box(b"ftyp", 12), mp4_box(b"mdat", 30)], 0),
            (vec![mp4_box(b"ftyp", 12), mp4_box(b"moof", 10)], 0),
            (
                vec![
                    mp4_box(b"moof", 10),
                    mp4_box(b"mdat", 10),
                    mp4_box(b"moov", 10),
                ],
                0,
            ),
            (vec![mp4_box(b"ftyp", 12), mp4_box(b"free", 10)], 0),
            // Truncated.
            (vec![mp4_box(b"ftyp", 12), mp4_box(b"moof", 10)], 3),
        ] {
            assert!(layout(&boxes, extra_len).is_err(), "{boxes:?}");
        }
    }
}
