use std::path::Path;
use std::sync::Arc;

use libloading::Library;
use mosaicod_facade as facade;
use mosaicod_plugin::auth::AuthPlugin;
use mosaicod_plugin::{PluginDeclaration, PluginRegistrar};
use tracing::{info, warn};

/// Symbol every plugin exports (see `mosaicod_plugin::export_plugin!`).
const DECLARATION_SYMBOL: &[u8] = b"PLUGIN_DECLARATION\0";

#[derive(thiserror::Error, Debug)]
pub enum LoaderError {
    #[error("cannot read plugins directory '{0}'")]
    ReadDir(String),
    #[error("cannot open plugin '{0}': {1}")]
    Open(String, String),
    #[error("plugin '{0}' is missing the declaration symbol: {1}")]
    MissingSymbol(String, String),
    #[error("plugin '{path}' is incompatible: {field} mismatch (host='{host}', plugin='{plugin}')")]
    Handshake {
        path: String,
        field: &'static str,
        host: String,
        plugin: String,
    },
    #[error("auth plugin init failed: {0}")]
    Init(String),
}

/// Host-side sink: collects each hook a plugin registers.
///
/// One optional slot per hook kind. Supporting a new extension point means
/// adding a field here and the matching `PluginRegistrar` method.
#[derive(Default)]
struct Registrar {
    auth: Option<Box<dyn AuthPlugin>>,
}

impl PluginRegistrar for Registrar {
    fn register_auth(&mut self, plugin: Box<dyn AuthPlugin>) {
        if self.auth.is_some() {
            warn!("an auth plugin was already registered; overriding it");
        }
        self.auth = Some(plugin);
    }
}

/// Registry of loaded plugins. MUST stay alive as long as the plugins are used:
/// dropping it unloads the `.so` files and any surviving `Arc` clone dangles.
pub struct LoadedPlugins {
    // Declared BEFORE `_libraries` on purpose: struct fields drop top-to-bottom,
    // so the plugin instances are dropped before their libraries are unloaded.
    auth: Option<Arc<dyn AuthPlugin>>,
    _libraries: Vec<Library>,
}

impl LoadedPlugins {
    /// An empty registry: no plugins loaded. Used when no plugins path is
    /// configured.
    pub fn empty() -> Self {
        Self {
            auth: None,
            _libraries: Vec::new(),
        }
    }

    /// Cloneable handle to the auth plugin, if one was loaded.
    pub fn auth(&self) -> Option<Arc<dyn AuthPlugin>> {
        self.auth.clone()
    }
}

/// Opens one `.so`, runs the handshake, and lets it register its hooks into
/// `registrar`. Returns the [`Library`], which the caller must keep alive.
///
/// # Safety
/// Loads and executes arbitrary native code; the caller must trust the file.
unsafe fn load_into(path: &Path, registrar: &mut Registrar) -> Result<Library, LoaderError> {
    let name = path.display().to_string();

    // dlopen: map the shared object into the process.
    let library = unsafe { Library::new(path) }
        .map_err(|e| LoaderError::Open(name.clone(), e.to_string()))?;

    // dlsym the declaration. For a `static`, the symbol address IS
    // `&PluginDeclaration`, so we read it as `*const PluginDeclaration`.
    let decl = unsafe {
        library
            .get::<*const PluginDeclaration>(DECLARATION_SYMBOL)
            .map_err(|e| LoaderError::MissingSymbol(name.clone(), e.to_string()))?
    };
    let decl: &PluginDeclaration = unsafe { &**decl };

    // Handshake: reject BEFORE running any plugin code.
    if decl.rustc_version != mosaicod_plugin::RUSTC_VERSION {
        return Err(LoaderError::Handshake {
            path: name,
            field: "rustc_version",
            host: mosaicod_plugin::RUSTC_VERSION.to_string(),
            plugin: decl.rustc_version.to_string(),
        });
    }
    if decl.contract_version != mosaicod_plugin::CONTRACT_VERSION {
        return Err(LoaderError::Handshake {
            path: name,
            field: "contract_version",
            host: mosaicod_plugin::CONTRACT_VERSION.to_string(),
            plugin: decl.contract_version.to_string(),
        });
    }

    // Let the plugin register whatever hooks it implements.
    unsafe { (decl.register)(registrar) };

    Ok(library)
}

/// Scans `plugins_path`, loads every `.so`, and initializes the hooks found.
pub async fn load(plugins_path: &Path, ctx: facade::Context) -> Result<LoadedPlugins, LoaderError> {
    let dir = std::fs::read_dir(plugins_path)
        .map_err(|_| LoaderError::ReadDir(plugins_path.display().to_string()))?;

    let mut registrar = Registrar::default();
    let mut libraries = Vec::new();

    for entry in dir.flatten() {
        let path = entry.path();
        // Only consider shared objects for the current platform.
        if path.extension().and_then(|e| e.to_str()) != Some(std::env::consts::DLL_EXTENSION) {
            continue;
        }

        let library = unsafe { load_into(&path, &mut registrar)? };
        libraries.push(library);
        info!("loaded plugin '{}'", path.display());
    }

    // init() needs &mut, so run it while still a Box, then freeze into an Arc.
    let auth = match registrar.auth {
        Some(mut plugin) => {
            plugin.init(ctx).await.map_err(LoaderError::Init)?;
            Some(Arc::from(plugin))
        }
        None => None,
    };

    Ok(LoadedPlugins {
        auth,
        _libraries: libraries,
    })
}
