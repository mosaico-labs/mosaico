//! Rust types generated from the `.proto` schemas in the repo-root `proto/` directory.
//!
//! Wire format is raw protobuf binary (`prost::Message::encode`/`decode`), not JSON.
//! Only the generated structs live here; conversions to/from domain types
//! belong to `mosaicod-marshal` crate.

pub mod v1 {
    #[allow(clippy::all)]
    pub mod time {
        include!(concat!(env!("OUT_DIR"), "/mosaicod.v1.time.rs"));
    }

    #[allow(clippy::all)]
    pub mod core {
        include!(concat!(env!("OUT_DIR"), "/mosaicod.v1.core.rs"));
    }

    #[allow(clippy::all)]
    pub mod requests {
        include!(concat!(env!("OUT_DIR"), "/mosaicod.v1.requests.rs"));
    }

    #[allow(clippy::all)]
    pub mod responses {
        include!(concat!(env!("OUT_DIR"), "/mosaicod.v1.responses.rs"));
    }

    #[allow(clippy::all)]
    pub mod flight {
        include!(concat!(env!("OUT_DIR"), "/mosaicod.v1.flight.rs"));
    }
}
