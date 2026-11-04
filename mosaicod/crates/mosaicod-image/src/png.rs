//! PNG, with the `png` crate.

use std::io::Cursor;

use png::{BitDepth, Decoder, Limits, Reader, Transformations};

use super::{ColorType, Error, Image, ImageFormat, ImageInfo, Samples};

/// The last chunk of every PNG: `IEND`, empty, with its CRC.
const IEND: [u8; 12] = [0, 0, 0, 0, b'I', b'E', b'N', b'D', 0xAE, 0x42, 0x60, 0x82];

fn invalid(reason: impl ToString) -> Error {
    Error::invalid(ImageFormat::Png, reason)
}

/// Reads the chunks before the image data, checking their CRCs, and
/// returns the reader positioned on the image data.
fn reader(bytes: &[u8]) -> Result<Reader<Cursor<&[u8]>>, Error> {
    if !bytes.ends_with(&IEND) {
        return Err(invalid("it does not end with the IEND chunk: truncated?"));
    }
    // The images of a sequence can be large (an 8K RGBA image of 16 bits is
    // ~265 MB, above the crate's default 64 MiB): the size is not limited.
    let mut decoder = Decoder::new_with_limits(Cursor::new(bytes), Limits { bytes: usize::MAX });
    // Fewer than 8 bits and palettes expanded, 16 bits kept.
    decoder.set_transformations(Transformations::EXPAND);
    decoder.read_info().map_err(invalid)
}

fn info(reader: &Reader<Cursor<&[u8]>>) -> Result<ImageInfo, Error> {
    let (width, height) = reader.info().size();
    let (color, depth) = reader.output_color_type();
    let color = match color {
        png::ColorType::Grayscale => ColorType::Gray,
        png::ColorType::GrayscaleAlpha => ColorType::GrayAlpha,
        png::ColorType::Rgb => ColorType::Rgb,
        png::ColorType::Rgba => ColorType::Rgba,
        // Expanded to RGB or RGBA by `Transformations::EXPAND`.
        png::ColorType::Indexed => {
            return Err(Error::unsupported(ImageFormat::Png, "an indexed image"));
        }
    };
    let bit_depth = match depth {
        BitDepth::Eight => 8,
        BitDepth::Sixteen => 16,
        // Expanded to 8 bits by `Transformations::EXPAND`.
        other => {
            return Err(Error::unsupported(
                ImageFormat::Png,
                format!("{other:?} bits per sample"),
            ));
        }
    };
    Ok(ImageInfo {
        width,
        height,
        color,
        bit_depth,
    })
}

pub(crate) fn validate(bytes: &[u8]) -> Result<ImageInfo, Error> {
    info(&reader(bytes)?)
}

pub(crate) fn decode(bytes: &[u8]) -> Result<Image, Error> {
    let mut reader = reader(bytes)?;
    let info = info(&reader)?;
    let size = reader
        .output_buffer_size()
        .ok_or_else(|| invalid("the image is too large"))?;
    let mut buffer = vec![0; size];
    let frame = reader.next_frame(&mut buffer).map_err(invalid)?;
    buffer.truncate(frame.buffer_size());

    let row = info.width as usize * info.color.channels() * usize::from(info.bit_depth / 8);
    if frame.line_size != row {
        return Err(invalid(format!(
            "rows of {} bytes, expected {row}",
            frame.line_size
        )));
    }
    let samples = match info.bit_depth {
        8 => Samples::Eight(buffer),
        // PNG stores 16-bit samples big-endian.
        _ => Samples::Sixteen(
            buffer
                .as_chunks::<2>()
                .0
                .iter()
                .map(|&sample| u16::from_be_bytes(sample))
                .collect(),
        ),
    };
    Image::new(info, samples)
}

pub(crate) fn encode(image: &Image) -> Result<Vec<u8>, Error> {
    let failed = |e: png::EncodingError| Error::encoding(ImageFormat::Png, e);
    let info = image.info();
    let mut bytes = Vec::new();
    let mut encoder = png::Encoder::new(&mut bytes, info.width, info.height);
    encoder.set_color(match info.color {
        ColorType::Gray => png::ColorType::Grayscale,
        ColorType::GrayAlpha => png::ColorType::GrayscaleAlpha,
        ColorType::Rgb => png::ColorType::Rgb,
        ColorType::Rgba => png::ColorType::Rgba,
    });
    let writer = match image.samples() {
        Samples::Eight(samples) => {
            encoder.set_depth(BitDepth::Eight);
            let mut writer = encoder.write_header().map_err(failed)?;
            writer.write_image_data(samples).map_err(failed)?;
            writer
        }
        Samples::Sixteen(samples) => {
            encoder.set_depth(BitDepth::Sixteen);
            let mut writer = encoder.write_header().map_err(failed)?;
            // PNG stores 16-bit samples big-endian.
            let big_endian: Vec<u8> = samples.iter().flat_map(|s| s.to_be_bytes()).collect();
            writer.write_image_data(&big_endian).map_err(failed)?;
            writer
        }
    };
    writer.finish().map_err(failed)?;
    Ok(bytes)
}
