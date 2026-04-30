use crate::common;
use clap::{ArgGroup, Subcommand};
use colored::Colorize;
use mosaicod_core::{self as core, error::PublicResult as Result, params, types};
use mosaicod_db as db;
use mosaicod_facade as facade;
use mosaicod_query as query;
use std::sync::Arc;
use tracing::error;

#[derive(Subcommand, Debug)]
pub enum ApiKey {
    /// Create a new API key with custom parameters
    #[clap(group(
    ArgGroup::new("expiration")
        .required(false)
        .args(["expires_in", "expires_at"]),
    ))]
    Create {
        /// Specifies permissions for the key. Allowed values are: read, write, delete, manage
        #[arg(short, long, required = true)]
        permissions: String,

        /// Define a description for the key
        #[arg(short, long)]
        description: Option<String>,

        /// Define a time duration, using the ISO8601 format, after which the key in no longer valid (e.g. `P1Y2M3D` 1 year 2 months and 3 days).
        #[arg(long)]
        expires_in: Option<String>,

        /// Define a datetime, using the rfc3339 format, after which the key in no longer valid (e.g 2026-03-27T12:20:00Z).
        #[arg(long)]
        expires_at: Option<String>,
    },

    /// Revoke a key
    Revoke {
        /// Fingerprints of the keys to revoke.
        /// The fingerprint is the last 8 digits of the API key.
        #[arg(required = true, num_args = 1..)]
        fingerprints: Vec<String>,
    },

    /// Return the status of a key
    Status {
        /// Fingerprint of the key. The fingerprint are the last 8 digits of
        /// the API key.
        fingerprint: String,
    },

    /// List all keys
    List,

    /// Purge keys
    Purge {
        /// If no option is provided, all expired keys are removed.
        /// With --all  or -A, all keys are removed.
        #[arg(short = 'A', long = "all")]
        all: bool,
    },
}

pub fn auth(auth: ApiKey) -> Result<()> {
    common::load_env_variables()?;

    let rt = common::init_runtime()?;

    let store = common::init_store()?;

    let ts_gw = Arc::new(query::TimeseriesEngine::try_new(
        store.clone(),
        params::params().query_engine_memory_pool_size.value,
    )?);

    let db = common::init_db(
        &rt,
        &db::Config {
            db_url: params::params().db_url.value.parse().map_err(|_| {
                core::Error::invalid_configuration(
                    params::params().db_url.env.clone(),
                    "unable to parse".to_string(),
                )
            })?,
            // Here we are using only one connection since it's a CLI command
            max_connections: 1,
        },
    )?;

    let context = facade::Context {
        store: store.clone(),
        db: db.clone(),
        timeseries_querier: ts_gw.clone(),
    };

    match auth {
        ApiKey::Create {
            permissions,
            description,
            expires_in,
            expires_at,
        } => {
            let permissions = permissions.parse()?;

            // Only one at a time between expires_at and expires_in can be set.
            let expiration_datetime: Option<types::Timestamp> = if let Some(expires_in) = expires_in
            {
                Some(
                    types::Timestamp::now()
                        + expires_in
                            .parse::<iso8601::Duration>()
                            .map_err(|e| core::Error::unsupported_time(e.to_string()))?
                            .into(),
                )
            } else if let Some(expires_at) = expires_at {
                let parsed_datetime: types::Timestamp =
                    chrono::DateTime::parse_from_rfc3339(&expires_at)
                        .map_err(|e| core::Error::unsupported_time(e.to_string()))?
                        .with_timezone(&chrono::Utc)
                        .into();

                if parsed_datetime < types::Timestamp::now() {
                    Err(core::Error::unsupported_time(
                        "invalid (past date)".to_owned(),
                    ))?;
                }

                Some(parsed_datetime)
            } else {
                None
            };

            // If no description is provided use the empty string
            let description = description.unwrap_or_default();

            let policy: core::error::PublicResult<types::ApiKey> = rt.block_on(async {
                let handle =
                    facade::auth::create(&context, permissions, description, expiration_datetime)
                        .await?;
                Ok(handle.into())
            });

            let policy = policy?;

            println!("{}", policy.key);
        }

        ApiKey::Revoke { fingerprints } => {
            let res: core::error::PublicResult<()> = rt.block_on(async {
                for fingerprint in fingerprints {
                    let handle = facade::auth::Handle::try_from_fingerprint(&context, &fingerprint)
                        .await
                        .map_err(|_| core::Error::invalid_fingerprint(fingerprint.clone()))?;

                    facade::auth::delete(&context, handle).await?;
                }

                Ok(())
            });

            res?;
        }

        ApiKey::Status { fingerprint } => {
            let res: Result<()> = rt.block_on(async {
                let handle =
                    facade::auth::Handle::try_from_fingerprint(&context, &fingerprint).await?;

                print_authz_policy_details(handle.into());

                Ok(())
            });

            res?;
        }

        ApiKey::List => {
            let res: Result<()> = rt.block_on(async {
                let policies = facade::auth::all_keys(&context).await?;

                print_authz_policy_list(policies);

                Ok(())
            });

            res?;
        }

        ApiKey::Purge { all } => {
            let res: Result<()> = rt.block_on(async {
                let mut errors = Vec::new();
                let keys = facade::auth::all_keys(&context).await?;
                for key in keys.iter().filter(|k| all || k.is_expired()) {
                    let fingerprint = key.token().fingerprint();

                    let result: Result<()> = async {
                        let handle =
                            facade::auth::Handle::try_from_fingerprint(&context, fingerprint)
                                .await?;
                        facade::auth::delete(&context, handle).await?;
                        Ok(())
                    }
                    .await;

                    if let Err(e) = result {
                        errors.push((fingerprint, e));
                    }
                }

                if !errors.is_empty() {
                    for (fingerprint, err) in &errors {
                        error!(fingerprint, ?err);
                    }

                    return Err(core::Error::internal(Some(format!(
                        "failed to purge {} keys",
                        errors.len(),
                    )))
                    .to_public_error());
                }

                Ok(())
            });

            res?
        }
    };

    Ok(())
}

fn print_authz_policy_details(policy: types::ApiKey) {
    let created_datetime: types::DateTime = policy.created_at.into();
    let expired_datetime: Option<types::DateTime> = policy.expires_at.map(|t| t.into());

    println!("{:>13} {}", "CREATED:".bold(), created_datetime);

    println!(
        "{:>13} {}",
        "PERMISSIONS:".bold(),
        String::from(policy.permission)
    );

    println!(
        "{:>13} {}",
        "EXPIRES:".bold(),
        if let Some(ts) = expired_datetime {
            ts.to_string()
        } else {
            "never".yellow().to_string()
        }
    );

    println!(
        "{:>13} {}",
        "EXPIRED:".bold(),
        if policy.is_expired() {
            "expired".red()
        } else {
            "valid".green()
        }
    );

    println!("{:>13} {}", "DESCRIPTION:".bold(), policy.description);
}

fn print_authz_policy_list(policies: Vec<types::ApiKey>) {
    // Header
    println!(
        "{:>12} {:>24} {:>24} {:>10} {:>14}    {}",
        "FINGERPRINT".bold(),
        "CREATED".bold(),
        "EXPIRES".bold(),
        "STATUS".bold(),
        "PERMISSIONS".bold(),
        "DESCRIPTION".bold()
    );
    for policy in policies {
        let datetime: types::DateTime = policy.created_at.into();
        let expired_datetime: Option<types::DateTime> = policy.expires_at.map(|t| t.into());

        let expired = if policy.is_expired() {
            "expired".red()
        } else {
            "valid".green()
        };

        println!(
            "{:>12} {:>24} {:>24} {:>10} {:>14}    {}",
            policy.token().fingerprint(),
            datetime.to_string(),
            expired_datetime.map_or("never".yellow(), |ts| { ts.to_string().white() }),
            expired,
            String::from(policy.permission),
            policy.description
        );
    }
}
