/// Used to write asyncrously data to a path (tipically over network)
pub trait AsyncWriteToPath {
    /// Writes the provided buffer to the specified path.
    ///
    /// Returns the number of bytes written on success.
    fn write_to_path(
        &self,
        path: impl AsRef<std::path::Path>,
        buf: impl Into<bytes::Bytes>,
    ) -> impl Future<Output = std::io::Result<()>>;
}

/// A trait for converting a type into its **file extension** representation.
///
/// This is typically implemented for structs that represent file formats or
/// other entities that are conventionally identified by a file extension string.
///
/// Implementors should ensure the returned string is a valid file extension,
/// usually without the leading dot (`.`).
///
/// # Example
///
/// ```
/// use mosaicod_core::traits::AsExtension;
///
/// // A hypothetical struct representing a JPEG file
/// struct JpegFile;
///
/// impl AsExtension for JpegFile {
///     fn as_extension(&self) -> String {
///         "jpg".to_string()
///     }
/// }
///
/// assert_eq!(JpegFile.as_extension(), "jpg");
/// ```
pub trait AsExtension {
    /// Returns the file extension string associated with this type.
    ///
    /// The returned string should **not** include the leading dot (`.`).
    ///
    /// # Returns
    ///
    /// A `String` representing the file extension (e.g., `"png"`, `"tar"`, `"json"`).
    fn as_extension(&self) -> String;
}
