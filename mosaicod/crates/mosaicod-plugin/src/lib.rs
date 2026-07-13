//! Generic plugin SDK: the FFI infrastructure shared by the host loader and
//! every plugin `.so`.
//!
//! This file knows nothing about what a plugin actually does. Individual
//! extension points ("hooks") live in submodules (e.g. [`auth`]). Adding a new
//! hook is a fixed recipe:
//! 1. define the hook trait in a new submodule;
//! 2. add a `register_*` method to [`PluginRegistrar`];
//! 3. add a matching slot in the host registry (`mosaicod-plugin-loader`);
//! 4. plant a call site in the open-source code where the hook is used.

pub mod auth;

// Contract version, checked during the load handshake.
pub const CONTRACT_VERSION: &str = env!("CARGO_PKG_VERSION");

// Rustc version this crate was built with, checked during the load handshake.
pub const RUSTC_VERSION: &str = env!("RUSTC_VERSION");

/// Static declaration exported by every plugin as its ONLY C symbol.
///
/// `register` passes a fat pointer (`&mut dyn PluginRegistrar`) across the FFI
/// boundary: this is NOT C-stable and is sound only because host and plugin are
/// built with the same toolchain and the same versions of the shared crates,
/// an assumption enforced by the handshake fields in this struct.
#[repr(C)]
#[allow(improper_ctypes_definitions)]
pub struct PluginDeclaration {
    // Handshake fields: read first, before running any plugin code.
    pub rustc_version: &'static str,
    pub contract_version: &'static str,
    // Called after the handshake succeeds to let the plugin register its hooks.
    pub register: unsafe extern "C" fn(&mut dyn PluginRegistrar),
}

/// Host-provided sink: a plugin registers each hook it implements through here.
///
/// One method per hook kind; a single `.so` may register several. The concrete
/// implementation (where the hooks are stored) lives host-side in the loader.
pub trait PluginRegistrar {
    /// Register an authentication provider (the [`auth`] hook).
    fn register_auth(&mut self, plugin: Box<dyn auth::AuthPlugin>);
}

/// Exports a plugin: generates the `#[no_mangle]` declaration and the C shim,
/// filling in the version handshake automatically.
///
/// Pass a `fn(&mut dyn PluginRegistrar)` that registers whichever hooks this
/// plugin implements:
///
/// ```ignore
/// fn register(reg: &mut dyn mosaicod_plugin::PluginRegistrar) {
///     reg.register_auth(Box::new(MyAuth::default()));
/// }
/// mosaicod_plugin::export_plugin!(register);
/// ```
#[macro_export]
macro_rules! export_plugin {
    ($register:path) => {
        #[unsafe(no_mangle)]
        pub static PLUGIN_DECLARATION: $crate::PluginDeclaration = $crate::PluginDeclaration {
            rustc_version: $crate::RUSTC_VERSION,
            contract_version: $crate::CONTRACT_VERSION,
            register: __plugin_register,
        };

        unsafe extern "C" fn __plugin_register(registrar: &mut dyn $crate::PluginRegistrar) {
            $register(registrar);
        }
    };
}
