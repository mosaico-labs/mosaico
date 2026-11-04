pub mod cleanup;
pub use cleanup::*;

pub mod store_optimizer;
pub use store_optimizer::*;

// The test images of the image sequence tests, which need the `video` feature: dev-dependencies
// can't be optional.
#[cfg(all(test, not(feature = "video")))]
use mosaicod_image as _;
