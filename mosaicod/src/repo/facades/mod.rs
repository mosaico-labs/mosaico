//! # Facade Module
//!
//! This module implements the **Facade** pattern, serving as an intermediate logic layer
//! between the application's public interface and the underlying persistence providers.
//!
//! The primary objective of these facades is to centralize business logic and data
//! operations while abstracting the complexities of the underlying systems.
//!
//! * **Database Abstraction:** Facades allow the system to interact with resources without
//!   exposing whether the underlying provider is a SQL, NoSQL, or other database type.
//! * **Coordinated Logic:** They manage multi-step operations—such as transactions that
//!   span both the metadata repository and the physical object store—ensuring state
//!   consistency.
//! * **Encapsulation:** By focusing database operations within this layer, the rest of
//!   the system interacts with high-level entities like [`FacadeTopic`] rather than
//!   manipulating raw database models.

mod facade_sequence;
pub use facade_sequence::*;

mod facade_topic;
pub use facade_topic::*;

mod facade_layer;
pub use facade_layer::*;

mod facade_error;
pub use facade_error::*;

mod facade_chunk;
pub use facade_chunk::*;

mod facade_query;
pub use facade_query::*;
