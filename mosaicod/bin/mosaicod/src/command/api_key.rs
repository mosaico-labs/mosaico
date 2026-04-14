use crate::common;
use clap::{ArgGroup, Subcommand};
use colored::Colorize;
use mosaicod_core::{self as core, error::PublicResult as Result, params, types};
use mosaicod_db as db;
use mosaicod_facade as facade;

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
        /// Fingerprint of the key to revoke. The fingerprint are the last 8 digits of
        /// the API key.
        fingerprint: String,
    },

    /// Return the status of a key
    Status {
        /// Fingerprint of the key. The fingerprint are the last 8 digits of
        /// the API key.
        fingerprint: String,
    },

    /// List all keys
    List,
}

pub fn auth(auth: ApiKey) -> Result<()> {
    common::load_env_variables()?;

    let rt = common::init_runtime()?;

    let db = common::init_db(
        &rt,
        &db::Config {
            db_url: params::params().db_url.value.parse().map_err(|_| {
                core::Error::invalid_configuration(
                    params::params().db_url.env.clone(),
                    "unable to parse".to_string(),
                )
            })?,
        },
    )?;

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
                let fauth =
                    facade::Auth::create(permissions, description, expiration_datetime, db).await?;
                Ok(fauth.into_api_key())
            });

            let policy = policy?;

            println!("{}", policy.key);
        }

        ApiKey::Revoke { fingerprint } => {
            let res: core::error::PublicResult<()> = rt.block_on(async {
                let fauth = facade::Auth::try_from_fingerprint(&fingerprint, db).await?;

                fauth.delete().await?;

                Ok(())
            });

            res?;
        }

        ApiKey::Status { fingerprint } => {
            let res: Result<()> = rt.block_on(async {
                let fauth = facade::Auth::try_from_fingerprint(&fingerprint, db).await?;

                let policy = fauth.into_api_key();

                print_authz_policy_details(policy);

                Ok(())
            });

            res?;
        }

        ApiKey::List => {
            let res: Result<()> = rt.block_on(async {
                let policies = facade::Auth::all_keys(db).await?;

                print_authz_policy_list(policies);

                Ok(())
            });

            res?;
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
