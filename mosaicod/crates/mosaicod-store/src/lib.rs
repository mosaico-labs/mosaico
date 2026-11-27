// Only enables TLS in `object_store`'s HTTP client, see `reqwest` in the workspace manifest.
use reqwest as _;

mod store;
pub use store::*;
