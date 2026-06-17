//! Event broker gRPC service for mosaicod.
//!
//! Provides the infrastructure for building event-driven pipelines and
//! automations directly on top of a mosaicod cluster, without requiring
//! an external message broker. Clients subscribe to typed event queues
//! that are populated whenever data operations occur in the cluster
//! (sequence and topic lifecycle, uploads, notifications). Workers consume
//! events through a blocking long-poll interface with at-least-once delivery
//! guarantees, and a coordinator component handles worker provisioning and
//! scaling decisions based on live queue depth.
#![allow(unused_crate_dependencies)]

pub mod broker;
