//! Decoding and validation of the images of an image sequence: encoded
//! images, all of the same [`ImageFormat`] (the `ImageSequence` serialization
//! format), or raw ones (the `ImageSequenceRaw` serialization format).
//!
//!
//! - [`ImageFormat`]: parsing a format name checks that it is supported;
//! - [`format_of`]: the format of some bytes, if they are an image, checked
//!   without decoding its pixels;
//! - [`decode`]: decodes an image into its samples, an [`Image`], what a
//!   video is encoded from;
//! - [`encode`]: encodes an [`Image`], what the images of a video are read
//!   back as;
//! - [`decode_raw`] and [`encode_raw`]: the same for raw images (the
//!   `ImageSequenceRaw` serialization format), byte for byte.
//!
//! The supported layouts are grayscale, grayscale with alpha, RGB and RGBA,
//! with 8 or 16 bits per sample. PNGs of 1, 2 or 4 bits and palette PNGs are
//! expanded to 8 bits.

mod png;
mod raw;

pub use raw::{RawError, RawLayout, decode_raw, encode_raw};

/// An error validating or decoding an image.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The image format is not one of [`ImageFormat::ALL`].
    #[error(
        "unsupported image format `{0}`, expected one of: {supported}",
        supported = ImageFormat::supported()
    )]
    UnknownFormat(String),
    /// The bytes are not a valid image of the declared format.
    #[error("invalid {format} image: {reason}")]
    Invalid { format: ImageFormat, reason: String },
    /// The image is valid, but its layout is not supported.
    #[error("unsupported {format} image: {reason}")]
    Unsupported { format: ImageFormat, reason: String },
    /// Encoding the image in the format failed.
    #[error("unable to encode a {format} image: {reason}")]
    Encoding { format: ImageFormat, reason: String },
    /// An image does not hold the samples its size requires, or they are not
    /// of its bit depth.
    #[error("a {width}x{height} image needs {expected} samples, not {found}")]
    FrameSize {
        width: u32,
        height: u32,
        expected: usize,
        found: usize,
    },
}

impl Error {
    pub(crate) fn invalid(format: ImageFormat, reason: impl ToString) -> Self {
        Self::Invalid {
            format,
            reason: reason.to_string(),
        }
    }

    pub(crate) fn encoding(format: ImageFormat, reason: impl ToString) -> Self {
        Self::Encoding {
            format,
            reason: reason.to_string(),
        }
    }

    pub(crate) fn unsupported(format: ImageFormat, reason: impl ToString) -> Self {
        Self::Unsupported {
            format,
            reason: reason.to_string(),
        }
    }
}

/// The encoding of the images of an image sequence, as written in its
/// `format` column: lossless only, so that the images are stored exactly as
/// the camera produced them.
///
/// To support a new lossless format: add a variant, add it to [`Self::ALL`],
/// name it in [`Self::as_str`], then complete every `match` the compiler
/// points at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ImageFormat {
    /// Portable Network Graphics: always lossless.
    Png,
}

impl ImageFormat {
    /// Every supported format.
    pub const ALL: &[Self] = &[Self::Png];

    /// The name of the format, as written in the `format` column.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Png => "png",
        }
    }

    /// The names of the supported formats, comma-separated.
    fn supported() -> String {
        Self::ALL
            .iter()
            .map(Self::as_str)
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Checks that `bytes` are an image of this format (see [`format_of`]).
    fn validate(&self, bytes: &[u8]) -> Result<ImageInfo, Error> {
        match self {
            Self::Png => png::validate(bytes),
        }
    }
}

impl std::str::FromStr for ImageFormat {
    type Err = Error;

    /// Parses the exact name of a supported format (`png`, not `PNG`).
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .iter()
            .find(|format| format.as_str() == value)
            .copied()
            .ok_or_else(|| Error::UnknownFormat(value.to_owned()))
    }
}

impl std::fmt::Display for ImageFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The channels of a pixel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColorType {
    Gray,
    GrayAlpha,
    Rgb,
    Rgba,
}

impl ColorType {
    /// Samples per pixel.
    pub fn channels(&self) -> usize {
        match self {
            Self::Gray => 1,
            Self::GrayAlpha => 2,
            Self::Rgb => 3,
            Self::Rgba => 4,
        }
    }

    pub fn has_alpha(&self) -> bool {
        matches!(self, Self::GrayAlpha | Self::Rgba)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Gray => "gray",
            Self::GrayAlpha => "gray_alpha",
            Self::Rgb => "rgb",
            Self::Rgba => "rgba",
        }
    }
}

impl std::fmt::Display for ColorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What an image holds, as [`decode`] gives it back.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ImageInfo {
    pub width: u32,
    pub height: u32,
    pub color: ColorType,
    /// Bits per sample: 8 or 16.
    pub bit_depth: u8,
}

/// The samples of an image, row after row with no padding, the channels of
/// each pixel one after the other.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Samples {
    Eight(Vec<u8>),
    Sixteen(Vec<u16>),
}

/// A decoded image.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Image {
    info: ImageInfo,
    samples: Samples,
}

impl Image {
    /// An image from its samples: fails unless they are as many as `info`
    /// requires, of its bit depth.
    pub fn new(info: ImageInfo, samples: Samples) -> Result<Self, Error> {
        let expected = info.width as usize * info.height as usize * info.color.channels();
        let (found, depth) = match &samples {
            Samples::Eight(s) => (s.len(), 8),
            Samples::Sixteen(s) => (s.len(), 16),
        };
        if found != expected || depth != info.bit_depth {
            return Err(Error::FrameSize {
                width: info.width,
                height: info.height,
                expected,
                found,
            });
        }
        Ok(Self { info, samples })
    }

    pub fn info(&self) -> &ImageInfo {
        &self.info
    }

    pub fn samples(&self) -> &Samples {
        &self.samples
    }
}

/// The supported format the image `bytes` are of, `None` if they are not an
/// image of any of them (e.g. garbage, truncated).
///
/// Only the image's structure is read, not its pixels. For a PNG: the
/// signature, the chunks before the image data (each with its CRC), and that
/// it ends with the `IEND` chunk. A valid image whose pixel data is corrupted
/// fails only in [`decode`], and so does one of a layout that is not
/// supported.
pub fn format_of(bytes: &[u8]) -> Option<ImageFormat> {
    ImageFormat::ALL
        .iter()
        .copied()
        .find(|format| !matches!(format.validate(bytes), Err(Error::Invalid { .. })))
}

/// Decodes the image `bytes` of `format`. A PNG of fewer than 8 bits, or with
/// a palette, is expanded to 8 bits (and a palette to RGB or RGBA).
pub fn decode(format: ImageFormat, bytes: &[u8]) -> Result<Image, Error> {
    match format {
        ImageFormat::Png => png::decode(bytes),
    }
}

/// Encodes `image` in `format`, losslessly: [`decode`] gives back exactly
/// `image`, but not necessarily the bytes it was decoded from.
pub fn encode(format: ImageFormat, image: &Image) -> Result<Vec<u8>, Error> {
    match format {
        ImageFormat::Png => png::encode(image),
    }
}

/// Test images.
#[cfg(any(test, feature = "testing"))]
pub mod testing {
    use super::{ColorType, Image, ImageInfo, Samples};

    /// A `width`x`height` image of `color` and `bit_depth` (8 or 16), with a
    /// smooth gradient in each channel, alpha fully opaque.
    pub fn gradient(width: u32, height: u32, color: ColorType, bit_depth: u8) -> Image {
        let channels = color.channels();
        let max = if bit_depth == 8 { 255.0 } else { 65535.0 };
        let mut values = Vec::with_capacity(width as usize * height as usize * channels);
        for y in 0..height {
            for x in 0..width {
                for c in 0..channels {
                    let value = if color.has_alpha() && c == channels - 1 {
                        1.0
                    } else {
                        let t = (x + 2 * y + 7 * c as u32) as f64;
                        0.5 + 0.4 * (t / 9.0).sin()
                    };
                    values.push((value * max).round() as u16);
                }
            }
        }
        let samples = if bit_depth == 8 {
            Samples::Eight(values.into_iter().map(|v| v as u8).collect())
        } else {
            Samples::Sixteen(values)
        };
        let info = ImageInfo {
            width,
            height,
            color,
            bit_depth,
        };
        Image::new(info, samples).expect("samples of the right size")
    }
}

#[cfg(test)]
mod tests {
    use super::testing::gradient;
    use super::*;

    fn png(image: &Image) -> Vec<u8> {
        encode(ImageFormat::Png, image).unwrap()
    }

    #[test]
    fn formats_are_parsed() {
        assert_eq!("png".parse::<ImageFormat>().unwrap(), ImageFormat::Png);
        for name in ["jpeg", "tiff", "PNG", "", "png "] {
            assert!(
                matches!(name.parse::<ImageFormat>(), Err(Error::UnknownFormat(ref f)) if f == name),
                "{name:?}"
            );
        }
        assert_eq!(
            Error::UnknownFormat("tiff".to_owned()).to_string(),
            "unsupported image format `tiff`, expected one of: png"
        );
    }

    #[test]
    fn every_layout_is_encoded_and_decoded_exactly() {
        for color in [
            ColorType::Gray,
            ColorType::GrayAlpha,
            ColorType::Rgb,
            ColorType::Rgba,
        ] {
            for depth in [8, 16] {
                let image = gradient(9, 4, color, depth);
                let bytes = png(&image);
                assert_eq!(format_of(&bytes), Some(ImageFormat::Png), "{color} {depth}");
                assert_eq!(
                    decode(ImageFormat::Png, &bytes).unwrap(),
                    image,
                    "{color} {depth}"
                );
            }
        }
    }

    #[test]
    fn what_is_not_a_valid_image_has_no_format() {
        let bytes = png(&gradient(32, 32, ColorType::Rgb, 8));
        let mut corrupted_header = bytes.clone();
        corrupted_header[18] ^= 0xff; // inside IHDR: its CRC no longer matches
        for bytes in [
            &b""[..],
            b"\x89PNG\r\n\x1a\n",
            b"not an image at all, just some bytes",
            // The start of a TIFF: not a supported format.
            b"II*\0\x08\0\0\0",
            &bytes[..bytes.len() - 20],
            &corrupted_header,
        ] {
            assert_eq!(format_of(bytes), None, "{bytes:?}");
        }
    }
}
