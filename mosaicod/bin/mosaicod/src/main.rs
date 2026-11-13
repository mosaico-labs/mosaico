// Setup default global allocator
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use mosaicod_config::{ConfigBuilder, params::DefaultConfig};

fn main() {
    mosaicod_commands::run(ConfigBuilder::new().add_conf(DefaultConfig::new()));
}
