/// A marker type representing a scope where data loss is explicitly acknowledged.
///
/// Use [`allow_data_loss`] to build this token.
pub struct DataLossToken {
    _private: (),
}

/// Creates a new `DataLossToken`.
///
/// This serves as an explicit opt-in mechanism for operations that may
/// result in irreversible data loss.
///
/// # Example
/// An example of the typical use of this construct:
///
/// ```
/// use mosaicod_core::types::{DataLossToken, allow_data_loss};
///
/// fn dangerous_function(_: DataLossToken) {
///     // destroy the universe
/// }
///
/// dangerous_function(allow_data_loss());
/// ```
pub fn allow_data_loss() -> DataLossToken {
    DataLossToken { _private: () }
}
