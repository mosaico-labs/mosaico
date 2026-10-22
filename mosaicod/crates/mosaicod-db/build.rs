use std::{env, fs, path::Path};

fn main() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is not set");

    // Locate the `mosaicod` directory among the ancestors of this crate.
    let mosaicod_dir = Path::new(&manifest_dir)
        .ancestors()
        .find(|dir| dir.ends_with("mosaicod"))
        .unwrap_or_else(|| {
            panic!("Build failed: mosaicod directory not found above {manifest_dir}")
        });

    let env_file = mosaicod_dir.join(".env");

    println!("cargo:rerun-if-changed={}", env_file.display());

    let contents = fs::read_to_string(&env_file).unwrap_or_else(|_| {
        panic!(
            "Build failed: .env file not found at {}",
            env_file.display()
        )
    });

    // Look for a non-empty `DATABASE_URL=<value>` entry, skipping comments.
    let database_url_defined = contents.lines().any(|line| {
        let line = line.trim();
        if line.starts_with('#') {
            return false;
        }
        let line = line.strip_prefix("export ").unwrap_or(line);
        match line.split_once('=') {
            Some((key, value)) => {
                key.trim() == "DATABASE_URL"
                    && !value
                        .trim()
                        .trim_matches(|c| c == '"' || c == '\'')
                        .is_empty()
            }
            None => false,
        }
    });

    if !database_url_defined {
        panic!(
            "Build failed: DATABASE_URL is missing or empty in {}",
            env_file.display()
        );
    }
}
