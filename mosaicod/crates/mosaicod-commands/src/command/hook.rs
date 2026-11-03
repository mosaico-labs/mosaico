use crate::common;
use clap::Subcommand;
use colored::Colorize;
use mosaicod_config::params;
use mosaicod_core::{self as core, error::PublicResult as Result, types};
use mosaicod_db as db;
use mosaicod_facade as facade;
use mosaicod_query as query;
use std::sync::Arc;

#[derive(Subcommand, Debug)]
pub enum Hook {
    /// Register a hook and print its id.
    Create {
        /// The `http` or `https` URL that events are sent to.
        url: String,
    },

    /// List all hooks.
    #[command(visible_alias = "list")]
    Ls,

    /// Delete hooks. If any id is unknown, nothing is deleted.
    #[command(visible_alias = "remove")]
    Rm {
        /// Ids of the hooks to delete, as shown by `hook ls`.
        #[arg(required = true, num_args = 1..)]
        ids: Vec<i32>,
    },
}

pub fn hook(cmd: Hook) -> Result<()> {
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
            user: params::params().db_user.value.clone(),
            password: params::params().db_password.value.clone(),
            // Here we are using only one connection since it's a CLI command
            max_connections: 1,
        },
    )?;

    let context = facade::Context::new(store, db, ts_gw);

    rt.block_on(async {
        match cmd {
            Hook::Create { url } => {
                let hook = facade::hook::create(&context, &url).await?;
                println!("{}", hook.id);
            }

            Hook::Ls => {
                let hooks = facade::hook::list(&context).await?;
                print_hook_list(&hooks);
            }

            Hook::Rm { mut ids } => {
                ids.sort_unstable();
                ids.dedup();

                // Check every id first, so a typo doesn't leave the other hooks half deleted.
                let hooks = facade::hook::list(&context).await?;
                let unknown: Vec<String> = ids
                    .iter()
                    .filter(|id| !hooks.iter().any(|hook| hook.id == **id))
                    .map(i32::to_string)
                    .collect();

                if !unknown.is_empty() {
                    let noun = if unknown.len() == 1 { "hook" } else { "hooks" };
                    Err(core::Error::not_found(format!(
                        "{noun} {}",
                        unknown.join(", ")
                    )))?;
                }

                for id in ids {
                    facade::hook::delete(&context, id).await?;
                }
            }
        }

        Ok(())
    })
}

fn print_hook_list(hooks: &[types::Hook]) {
    const W_ID: usize = 6;
    const W_CREATED: usize = 22;

    println!(
        "{} {} {}",
        format!("{:<W_ID$}", "ID").bold(),
        format!("{:<W_CREATED$}", "CREATED").bold(),
        "URL".bold(),
    );

    for hook in hooks {
        let created: types::DateTime = hook.created_at.into();

        println!(
            "{:<W_ID$} {:<W_CREATED$} {}",
            hook.id,
            created.to_string(),
            hook.url,
        );
    }
}
