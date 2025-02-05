use std::{env, process};
use std::sync::Arc;
use std::collections::HashMap;

use anyhow::anyhow;
use futures::stream::FuturesUnordered;
use futures::TryStreamExt;
use structopt::StructOpt;
use tokio::{signal, task};

use crate::db::connection;
use crate::util::devices;

mod db;
mod util;

#[derive(Debug, Clone, StructOpt)]
pub struct Opt {
    /// read ratio
    #[structopt(default_value = "80")]
    read_ratio: u8,
    /// write ratio
    #[structopt(default_value = "20")]
    write_ratio: u8,
    /// simulators
    #[structopt(default_value = "30")]
    simulators: u16,
    /// DB host
    #[structopt(default_value = "0.0.0.0")]
    db_host: String,
    /// DB port
    #[structopt(default_value = "9042")]
    db_port: String,
    /// DB user
    #[structopt(default_value = "scylla")]
    db_user: String,
    /// DB pass
    #[structopt(default_value = "asd")]
    db_password: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // dotenv::dotenv().ok();
    util::logging::init();
    let opt = Opt::from_args();

    let db_config: HashMap<&str, String> = HashMap::from([
        ("host", opt.db_host),
        ("port", opt.db_port),
        ("user", opt.db_user),
        ("password", opt.db_password),
    ]);

    if opt.read_ratio + opt.write_ratio != 100 {
        return Err(anyhow!(
            "Invalid ratio configuration. Sum of read_ratio and write_ratio must be 100."
        ));
    }

    let migrate = match env::var("MIGRATE")
        .unwrap_or_default()
        .to_uppercase().as_str() {
        "TRUE" => true,
        "FALSE" => false,
        _ => true,
    };

    let db_session = Arc::new(
        connection::builder(migrate, db_config)
            .await
            .expect("Failed to connect to database"),
    );

    let ctrl_c_handler = tokio::spawn(async {
        signal::ctrl_c().await.expect("Failed to listen for Ctrl-C");
        process::exit(0);
    });

    let devices_tasks: FuturesUnordered<_> = (0..opt.simulators).map(|_| {
        task::spawn(devices::simulator(
            db_session.clone(),
            opt.read_ratio,
            opt.write_ratio,
        ))
    }).collect();
    let devices_result = devices_tasks.try_collect::<Vec<_>>().await?;

    devices_result.into_iter().collect::<Result<Vec<_>, _>>()?;

    tokio::select! {
        _ = ctrl_c_handler => (),
    }

    Ok(())
}
