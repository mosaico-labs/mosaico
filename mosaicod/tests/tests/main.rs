//! Single entry point of the integration tests.
//!
//! Every file in this folder is a module of one test binary, so the tests are linked
//! only once. Filter by module with e.g. `cargo test -p tests auth::`.

#![allow(unused_crate_dependencies)]

mod auth;
mod cleanup;
mod concurrent;
mod do_get;
mod do_put;
mod hook;
mod list_flights;
mod ontology_query;
mod query;
mod resources;
mod schema;
mod sequence;
mod session;
mod store_optimizer;
mod tls;
mod topic;
mod unsorted_data;
