use crate::common;
use clap::Subcommand;
use colored::Colorize;
use mosaicod_core::{params, types};
use mosaicod_db as db;
use mosaicod_facade as facade;

#[derive(Subcommand, Debug)]
pub enum Auth {
    /// Create a new API key with custom parameters
    Create {
        /// Specifies permissions for the API key. Allowed values are: read, write, delete, manage
        #[arg(num_args=1.., value_delimiter=' ', required=true)]
        permissions: Vec<String>,

        /// Define a description for the new API key
        #[arg(short, long)]
        description: Option<String>,

        /// Define a time duration (using the ISO8601 format) after which the API key in no longer valid.
        #[arg(short, long)]
        expires: Option<String>,
    },

    /// Revoke an API key permissions
    Revoke {
        /// API key fingerprint of the key to revoke. The fingerprint are the last 8 digits of
        /// the API key.
        fingerprint: String,
    },

    /// Return the status of an API key.
    Status {
        /// API key fingerprint of the key. The fingerprint are the last 8 digits of
        /// the API key.
        fingerprint: String,
    },

    /// List all API keys
    List,
}

/// Convert the strings obtained from the CLI into a [`types::Permissions`]
fn cast_to_permissions(permissions: Vec<String>) -> Result<types::Permissions, String> {
    let mut perm = types::Permissions::default();
    for p in permissions {
        match p.as_str() {
            "read" => perm = perm.add(types::Permissions::READ),
            "write" => perm = perm.add(types::Permissions::WRITE),
            "delete" => perm = perm.add(types::Permissions::DELETE),
            "manage" => perm = perm.add(types::Permissions::MANAGE),
            _ => return Err("Permission not allowed".to_string()),
        };
    }

    Ok(perm)
}

pub fn auth(auth: Auth) -> Result<(), common::Error> {
    common::load_env_variables()?;

    let rt = common::init_runtime()?;

    let db = common::init_db(
        &rt,
        db::Config {
            db_url: params::params().db_url.parse()?,
        },
    )?;

    match auth {
        Auth::Create {
            permissions,
            description,
            expires,
        } => {
            let permissions = cast_to_permissions(permissions)?;

            let expires: Option<std::time::Duration> = if let Some(expires) = expires {
                Some(expires.parse::<iso8601::Duration>()?.into())
            } else {
                None
            };

            // If no description is provided use the empty string
            let description = description.unwrap_or_default();

            let scope: Result<types::AuthScope, facade::Error> = rt.block_on(async {
                let fauth = facade::Auth::create(permissions, description, expires, db).await?;
                Ok(fauth.into_scope())
            });

            let scope = scope?;

            println!("{}", scope.key);
        }

        Auth::Revoke { fingerprint } => {
            let res: Result<(), facade::Error> = rt.block_on(async {
                let fauth = facade::Auth::try_from_fingerprint(fingerprint, db).await?;

                fauth.delete().await?;

                Ok(())
            });

            res?;
        }

        Auth::Status { fingerprint } => {
            let res: Result<(), facade::Error> = rt.block_on(async {
                let fauth = facade::Auth::try_from_fingerprint(fingerprint, db).await?;

                let scope = fauth.into_scope();

                print_auth_scope_details(scope);

                Ok(())
            });

            res?;
        }

        Auth::List => {
            let res: Result<(), facade::Error> = rt.block_on(async {
                let scopes = facade::Auth::all_scopes(db).await?;

                print_auth_scope_list(scopes);

                Ok(())
            });

            res?;
        }
    };

    Ok(())
}

fn print_auth_scope_details(scope: types::AuthScope) {
    println!(
        "{:>13} {}",
        "Expired:".bold(),
        if scope.is_expired() { "true" } else { "false" }
    );
    let datetime: types::DateTime = scope.creation_timestamp.into();
    println!("{:>13} {}", "Created:".bold(), datetime);
    println!("{:>13} {}", "Description:".bold(), scope.description);

    let perms: Vec<String> = scope.permissions.into();
    println!("{:>13} {}", "Permissions:".bold(), perms.join(", "));
}

fn print_auth_scope_list(scopes: Vec<types::AuthScope>) {
    // Header
    println!(
        "{:>12} {:>24} {:>10} {:>30}    {}",
        "Fingerprint".bold(),
        "Created".bold(),
        "Expired".bold(),
        "Permissions".bold(),
        "Description".bold()
    );
    for scope in scopes {
        let datetime: types::DateTime = scope.creation_timestamp.into();
        let permissions: Vec<String> = scope.permissions.into();

        println!(
            "{:>12} {:>24} {:>10} {:>30}    {}",
            scope.key().fingerprint(),
            datetime.to_string(),
            if scope.is_expired() { "true" } else { "false" },
            permissions.join(", "),
            scope.description
        );
    }
}
