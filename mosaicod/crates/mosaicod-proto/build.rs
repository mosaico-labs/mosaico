use std::path::PathBuf;

/// `.proto` schemas live at the repo root (`proto/`), shared with `mosaico-sdk-py`.
/// `protox` is a pure-Rust protobuf compiler: it removes the need for a system
/// `protoc` binary (and thus for `protoc` in the docker build image).
///
/// Wire format is raw protobuf binary (`prost::Message`), not JSON.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../proto");
    let proto_files = [
        proto_root.join("mosaicod/v1/time.proto"),
        proto_root.join("mosaicod/v1/core.proto"),
        proto_root.join("mosaicod/v1/requests.proto"),
        proto_root.join("mosaicod/v1/responses.proto"),
        proto_root.join("mosaicod/v1/flight.proto"),
    ];

    for proto_file in &proto_files {
        println!("cargo:rerun-if-changed={}", proto_file.display());
    }

    let file_descriptor_set = protox::compile(&proto_files, [&proto_root])?;

    prost_build::Config::new()
        // `Format` is also used by `mosaicod-marshal`'s on-disk JSON metadata
        // format (see `metadata.rs`'s `JsonTopicOntologyMetadata`),
        // not just the binary do_action wire protocol: a plain hand-derived
        // serde impl is enough since it's a unit-variant-only enum.
        // `rename_all = "snake_case"` keeps the JSON representation ("default"/"ragged"/"image") unchanged.
        .type_attribute(
            ".mosaicod.v1.core.Format",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute(
            ".mosaicod.v1.core.Format",
            "#[serde(rename_all = \"snake_case\")]",
        )
        .compile_fds(file_descriptor_set)?;

    Ok(())
}
