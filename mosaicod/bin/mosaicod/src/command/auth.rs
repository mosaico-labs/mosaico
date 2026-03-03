use crate::common;
use clap::Subcommand;
use colored::Colorize;
use mosaicod_core::{params, types};
use mosaicod_db as db;
use mosaicod_facade as facade;

#[derive(Subcommand, Debug)]
pub enum Auth {
    /// Create a new authorization policy with custom parameters
    Create {
        /// Specifies permissions for the policy. Allowed values are: read, write, delete, manage
        #[arg(num_args=1.., value_delimiter=' ', required=true)]
        permissions: Vec<String>,

        /// Define a description for the policy
        #[arg(short, long)]
        description: Option<String>,

        /// Define a time duration (using the ISO8601 format) after which the policy in no longer valid.
        #[arg(short, long)]
        expires: Option<String>,
    },

    /// Revoke a policy
    Revoke {
        /// API key fingerprint of the policy to revoke. The fingerprint are the last 8 digits of
        /// the API key.
        fingerprint: String,
    },

    /// Return the status of a policy
    Status {
        /// API key fingerprint of the key. The fingerprint are the last 8 digits of
        /// the API key.
        fingerprint: String,
    },

    /// List all policies
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

            let policy: Result<types::AuthorizationPolicy, facade::Error> = rt.block_on(async {
                let fauth = facade::Auth::create(permissions, description, expires, db).await?;
                Ok(fauth.into_policy())
            });

            let policy = policy?;

            println!("{}", policy.key);
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

                let policy = fauth.into_policy();

                print_authz_policy_details(policy);

                Ok(())
            });

            res?;
        }

        Auth::List => {
            let res: Result<(), facade::Error> = rt.block_on(async {
                let policies = facade::Auth::all_policies(db).await?;

                print_authz_policy_list(policies);

                Ok(())
            });

            res?;
        }
    };

    Ok(())
}

fn print_authz_policy_details(policy: types::AuthorizationPolicy) {
    println!(
        "{:>13} {}",
        "Expired:".bold(),
        if policy.is_expired() { "true" } else { "false" }
    );
    let created_datetime: types::DateTime = policy.creation_timestamp.into();
    let expired_datetime: Option<types::DateTime> = policy.expiration_timestamp.map(|t| t.into());

    println!("{:>13} {}", "Created:".bold(), created_datetime);
    println!(
        "{:>13} {}",
        "Expires:".bold(),
        if let Some(ts) = expired_datetime {
            ts.to_string()
        } else {
            "never".to_owned()
        }
    );
    println!("{:>13} {}", "Description:".bold(), policy.description);

    let perms: Vec<String> = policy.permissions.into();
    println!("{:>13} {}", "Permissions:".bold(), perms.join(", "));
}

fn print_authz_policy_list(policies: Vec<types::AuthorizationPolicy>) {
    // Header
    println!(
        "{:>12} {:>24} {:>10} {:>30}    {}",
        "Fingerprint".bold(),
        "Created".bold(),
        "Expired".bold(),
        "Permissions".bold(),
        "Description".bold()
    );
    for policy in policies {
        let datetime: types::DateTime = policy.creation_timestamp.into();
        let permissions: Vec<String> = policy.permissions.into();
        let expired = if policy.is_expired() {
            "expired".red()
        } else {
            "valid".green()
        };

        println!(
            "{:>12} {:>24} {:>10} {:>30}    {}",
            policy.key().fingerprint(),
            datetime.to_string(),
            expired,
            permissions.join(", "),
            policy.description
        );
    }
}
