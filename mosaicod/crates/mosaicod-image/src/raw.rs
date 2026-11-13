//! Raw images (the `ImageSequenceRaw` serialization format): the pixels as a camera driver writes
//! them, described as in a ROS `sensor_msgs/Image` by their size, the bytes of each row (`stride`,
//! padding included), a pixel `encoding` and their byte order.
//!
//! [`decode_raw`] turns them into an [`Image`], what a video is encoded from, and [`encode_raw`]
//! back into exactly the same bytes: the channels of BGR(A) pixels are swapped into RGB(A) and back,
//! 16-bit samples read in their byte order and written back in it, and the row padding, which
//! must be zero, written back as zero.

use crate::ColorType::{Gray, Rgb, Rgba};
use crate::{ColorType, Image, ImageInfo, Samples};

/// An error turning a raw image into an [`Image`], or back.
#[derive(Debug, thiserror::Error)]
pub enum RawError {
    /// The bytes don't match their description (size, stride).
    #[error("invalid raw image: {0}")]
    Invalid(String),
    /// The image can't be a video without losing information: its encoding, or its padding.
    #[error("unsupported raw image: {0}")]
    Unsupported(String),
}

/// What a raw image is, but its bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawLayout<'a> {
    pub width: u32,
    pub height: u32,
    /// Bytes per row, its padding included.
    pub stride: u32,
    /// The pixel encoding, as in ROS: `mono8`, `bgr8`, `16UC1`...
    pub encoding: &'a str,
    /// Whether the samples of 16 bits are big-endian.
    pub big_endian: bool,
}

/// What the samples of an encoding are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Encoding {
    color: ColorType,
    bit_depth: u8,
    /// Its channels are blue, green, red (then alpha): swapped into RGB(A).
    bgr: bool,
}

impl Encoding {
    fn parse(name: &str) -> Result<Self, RawError> {
        let (color, bit_depth, bgr) = match name {
            // Bayer images are one sample per pixel: kept as they are, as grayscale.
            "mono8" | "8UC1" | "bayer_rggb8" | "bayer_bggr8" | "bayer_gbrg8" | "bayer_grbg8" => {
                (Gray, 8, false)
            }
            // Signed samples are kept bit for bit.
            "mono16" | "16UC1" | "16SC1" | "bayer_rggb16" | "bayer_bggr16" | "bayer_gbrg16"
            | "bayer_grbg16" => (Gray, 16, false),
            "rgb8" | "8UC3" => (Rgb, 8, false),
            "bgr8" => (Rgb, 8, true),
            "rgba8" | "8UC4" => (Rgba, 8, false),
            "bgra8" => (Rgba, 8, true),
            "rgb16" | "16UC3" => (Rgb, 16, false),
            "bgr16" => (Rgb, 16, true),
            "rgba16" | "16UC4" => (Rgba, 16, false),
            "bgra16" => (Rgba, 16, true),
            // FIXME: `yuv422`, `uyvy`, `yuyv` (`yuv422_yuy2`) can be lossless videos too, their
            // samples rearranged into planes (`yuv422p`), but `Image` has no YUV type yet.
            // FIXME: `32FC1` (depth images, in meters) and the other floating point encodings:
            // FFV1 has no single channel of 32-bit floats; they could be split into two planes of
            // 16 bits, or written as three channels.
            _ => {
                return Err(RawError::Unsupported(format!(
                    "encoding `{name}`, which can't be a lossless video"
                )));
            }
        };
        Ok(Self {
            color,
            bit_depth,
            bgr,
        })
    }

    /// Bytes per pixel.
    fn pixel_bytes(&self) -> usize {
        self.color.channels() * usize::from(self.bit_depth / 8)
    }
}

/// The encoding of `layout`, and the bytes of a row of pixels, its padding excluded.
fn check(layout: &RawLayout) -> Result<(Encoding, usize), RawError> {
    let encoding = Encoding::parse(layout.encoding)?;
    if layout.width == 0 || layout.height == 0 {
        return Err(RawError::Invalid(format!(
            "a {}x{} image",
            layout.width, layout.height
        )));
    }
    let row = layout.width as usize * encoding.pixel_bytes();
    if (layout.stride as usize) < row {
        return Err(RawError::Invalid(format!(
            "a stride of {} bytes, but a row of {} {} pixels is {row} bytes",
            layout.stride, layout.width, layout.encoding
        )));
    }
    Ok((encoding, row))
}

/// Swaps the first and the third channel of each pixel of `samples`, of `channels` channels.
fn swap_red_blue<T>(samples: &mut [T], channels: usize) {
    for pixel in samples.chunks_exact_mut(channels) {
        pixel.swap(0, 2);
    }
}

/// The image of the raw `data` of `layout`. Fails if `data` is not `stride * height` bytes, or a
/// stride shorter than a row ([`RawError::Invalid`]); if the encoding can't be a lossless video,
/// or the row padding is not zero, which a video does not keep ([`RawError::Unsupported`]).
pub fn decode_raw(layout: &RawLayout, data: &[u8]) -> Result<Image, RawError> {
    let (encoding, row) = check(layout)?;
    let stride = layout.stride as usize;
    let expected = stride * layout.height as usize;
    if data.len() != expected {
        return Err(RawError::Invalid(format!(
            "{} bytes, but {} rows of {stride} bytes are {expected}",
            data.len(),
            layout.height
        )));
    }
    let rows = data.chunks_exact(stride);
    // FIXME: row padding of bytes other than zero is lost in a video: it could be kept apart
    // (e.g. in the frame index) and written back by `encode_raw`, so that such images can be
    // videos too.
    if rows.clone().any(|r| r[row..].iter().any(|&b| b != 0)) {
        return Err(RawError::Unsupported(
            "row padding of bytes other than zero, which a video does not keep".to_owned(),
        ));
    }

    let channels = encoding.color.channels();
    let samples = if encoding.bit_depth == 8 {
        let mut samples = Vec::with_capacity(row * layout.height as usize);
        for r in rows {
            samples.extend_from_slice(&r[..row]);
        }
        if encoding.bgr {
            swap_red_blue(&mut samples, channels);
        }
        Samples::Eight(samples)
    } else {
        let read = if layout.big_endian {
            u16::from_be_bytes
        } else {
            u16::from_le_bytes
        };
        let mut samples = Vec::with_capacity(row / 2 * layout.height as usize);
        for r in rows {
            samples.extend(r[..row].as_chunks::<2>().0.iter().map(|&b| read(b)));
        }
        if encoding.bgr {
            swap_red_blue(&mut samples, channels);
        }
        Samples::Sixteen(samples)
    };
    let info = ImageInfo {
        width: layout.width,
        height: layout.height,
        color: encoding.color,
        bit_depth: encoding.bit_depth,
    };
    Image::new(info, samples).map_err(|e| RawError::Invalid(e.to_string()))
}

/// The raw bytes of `image` as `layout` describes them: those [`decode_raw`] decoded it from.
/// Fails if `image` is not an image of `layout` ([`RawError::Invalid`]).
pub fn encode_raw(layout: &RawLayout, image: &Image) -> Result<Vec<u8>, RawError> {
    let (encoding, row) = check(layout)?;
    let info = image.info();
    if (info.width, info.height, info.color, info.bit_depth)
        != (
            layout.width,
            layout.height,
            encoding.color,
            encoding.bit_depth,
        )
    {
        return Err(RawError::Invalid(format!(
            "a {}x{} {} image of {} bits is not a {}x{} {} image",
            info.width,
            info.height,
            info.color,
            info.bit_depth,
            layout.width,
            layout.height,
            layout.encoding
        )));
    }

    let channels = encoding.color.channels();
    let stride = layout.stride as usize;
    let mut data = vec![0; stride * layout.height as usize];
    match image.samples() {
        Samples::Eight(samples) => {
            for (to, from) in data.chunks_exact_mut(stride).zip(samples.chunks_exact(row)) {
                let pixels = &mut to[..row];
                pixels.copy_from_slice(from);
                if encoding.bgr {
                    swap_red_blue(pixels, channels);
                }
            }
        }
        Samples::Sixteen(samples) => {
            let write = if layout.big_endian {
                u16::to_be_bytes
            } else {
                u16::to_le_bytes
            };
            for (to, from) in data
                .chunks_exact_mut(stride)
                .zip(samples.chunks_exact(row / 2))
            {
                let pixels = to[..row].as_chunks_mut::<2>().0;
                for (bytes, &sample) in pixels.iter_mut().zip(from) {
                    *bytes = write(sample);
                }
                if encoding.bgr {
                    swap_red_blue(pixels, channels);
                }
            }
        }
    }
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layout(encoding: &str, stride: u32, big_endian: bool) -> RawLayout<'_> {
        RawLayout {
            width: 3,
            height: 2,
            stride,
            encoding,
            big_endian,
        }
    }

    /// Raw bytes of `layout`, with zero padding: each pixel byte its index in the row and the
    /// row's number, so that a swapped or misplaced byte shows.
    fn raw_bytes(layout: &RawLayout, row: usize) -> Vec<u8> {
        let mut data = vec![0; layout.stride as usize * layout.height as usize];
        for (y, bytes) in data.chunks_exact_mut(layout.stride as usize).enumerate() {
            for (x, byte) in bytes[..row].iter_mut().enumerate() {
                *byte = (16 * y + x + 1) as u8;
            }
        }
        data
    }

    #[test]
    fn every_supported_encoding_comes_back_byte_for_byte() {
        let cases = [
            ("mono8", 3, ColorType::Gray, 8),
            ("8UC1", 3, ColorType::Gray, 8),
            ("bayer_rggb8", 3, ColorType::Gray, 8),
            ("mono16", 6, ColorType::Gray, 16),
            ("16SC1", 6, ColorType::Gray, 16),
            ("bayer_grbg16", 6, ColorType::Gray, 16),
            ("rgb8", 9, ColorType::Rgb, 8),
            ("bgr8", 9, ColorType::Rgb, 8),
            ("8UC4", 12, ColorType::Rgba, 8),
            ("bgra8", 12, ColorType::Rgba, 8),
            ("rgb16", 18, ColorType::Rgb, 16),
            ("bgr16", 18, ColorType::Rgb, 16),
            ("16UC4", 24, ColorType::Rgba, 16),
            ("bgra16", 24, ColorType::Rgba, 16),
        ];
        for (encoding, row, color, bit_depth) in cases {
            for (padding, big_endian) in [(0, false), (5, false), (0, true), (3, true)] {
                let layout = layout(encoding, row as u32 + padding, big_endian);
                let data = raw_bytes(&layout, row);
                let image = decode_raw(&layout, &data).unwrap();
                let case = format!("{encoding}, padding {padding}, big endian {big_endian}");
                assert_eq!(image.info().color, color, "{case}");
                assert_eq!(image.info().bit_depth, bit_depth, "{case}");
                assert_eq!(encode_raw(&layout, &image).unwrap(), data, "{case}");
            }
        }
    }

    #[test]
    fn channels_and_byte_order_are_those_of_the_image() {
        // A BGR pixel of 8 bits is red 3, green 2, blue 1.
        let bgr = RawLayout {
            width: 1,
            height: 1,
            stride: 3,
            encoding: "bgr8",
            big_endian: false,
        };
        let image = decode_raw(&bgr, &[1, 2, 3]).unwrap();
        assert_eq!(image.samples(), &Samples::Eight(vec![3, 2, 1]));

        // The sample 0x0102, big-endian, then little-endian.
        let mut mono = RawLayout {
            width: 1,
            height: 1,
            stride: 2,
            encoding: "mono16",
            big_endian: true,
        };
        let image = decode_raw(&mono, &[1, 2]).unwrap();
        assert_eq!(image.samples(), &Samples::Sixteen(vec![0x0102]));
        mono.big_endian = false;
        assert_eq!(encode_raw(&mono, &image).unwrap(), [2, 1]);
    }

    #[test]
    fn wrong_or_lossy_images_are_rejected() {
        let mono = layout("mono8", 4, false);
        let invalid = |layout: &RawLayout, data: &[u8]| {
            matches!(decode_raw(layout, data), Err(RawError::Invalid(_)))
        };
        let unsupported = |layout: &RawLayout, data: &[u8]| {
            matches!(decode_raw(layout, data), Err(RawError::Unsupported(_)))
        };
        // Not `stride * height` bytes, a stride shorter than a row, no pixels.
        assert!(invalid(&mono, &[0; 7]));
        assert!(invalid(&mono, &[0; 9]));
        assert!(invalid(&layout("rgb8", 8, false), &[0; 16]));
        assert!(invalid(&RawLayout { width: 0, ..mono }, &[]));
        // Padding a video would not keep.
        let mut padded = raw_bytes(&mono, 3);
        padded[3] = 7;
        assert!(unsupported(&mono, &padded));
        // Encodings that can't be a lossless video yet, and unknown ones.
        for encoding in ["32FC1", "yuv422", "uyvy", "yuyv", "64FC1", "MONO8", "jpeg"] {
            assert!(
                unsupported(&layout(encoding, 12, false), &[0; 24]),
                "{encoding}"
            );
        }

        // Back into a layout of another image.
        let image = decode_raw(&mono, &raw_bytes(&mono, 3)).unwrap();
        assert!(matches!(
            encode_raw(&layout("mono16", 6, false), &image),
            Err(RawError::Invalid(_))
        ));
    }
}
