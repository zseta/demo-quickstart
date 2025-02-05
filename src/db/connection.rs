use std::collections::HashMap;
use std::env;
use std::time::Duration;

use anyhow::{anyhow, Result};
use scylla::{Session, SessionBuilder};
use scylla::load_balancing::DefaultPolicy;
use scylla::statement::Consistency;
use scylla::transport::ExecutionProfile;
use tokio_retry::{Retry, strategy::ExponentialBackoff};
use tracing::{debug, info};

use crate::db::ddl::DDL;

pub async fn builder(migrate: bool, db_config: HashMap<&str, String>) -> Result<Session> {
    let hostnames: Vec<String> = (db_config.get("host").unwrap_or(&"".to_string()).to_owned() + ":9042").split(',').map(String::from).collect();
    let user= db_config.get("user").map(|s| s.as_str()).unwrap_or("scylla");
    let passwd= db_config.get("user").map(|s| s.as_str()).unwrap_or("scylla");

    let consistency = match env::var("CL")
        .unwrap_or_default()
        .to_uppercase().as_str() {
        "ONE" => Consistency::One,
        "TWO" => Consistency::Two,
        "THREE" => Consistency::Three,
        "QUORUM" => Consistency::Quorum,
        "ALL" => Consistency::All,
        "LOCAL_QUORUM" => Consistency::LocalQuorum,
        "EACH_QUORUM" => Consistency::EachQuorum,
        "SERIAL" => Consistency::Serial,
        "LOCAL_SERIAL" => Consistency::LocalSerial,
        "LOCAL_ONE" => Consistency::LocalOne,
        _ => Consistency::LocalQuorum,
    };

    info!("Connecting to ScyllaDB at: {}  CL: {}", hostnames[0], consistency);

    let strategy = ExponentialBackoff::from_millis(500).max_delay(Duration::from_secs(20));

    let session = Retry::spawn(strategy, || async {
        let datacenter = env::var("DATACENTER").unwrap_or("datacenter1".to_string());

        let default_policy = DefaultPolicy::builder()
            .prefer_datacenter(datacenter)
            .token_aware(true)
            .permit_dc_failover(false)
            .build();


        let profile = ExecutionProfile::builder()
            .load_balancing_policy(default_policy)
            .consistency(consistency)
            .build();

        let handle = profile.into_handle();

        SessionBuilder::new()
            .known_nodes(&hostnames)
            .user(user, passwd)
            .default_execution_profile_handle(handle)
            .build()
            .await
    })
        .await
        .map_err(|e| anyhow!("Error connecting to the database: {}", e))?;

    if migrate {
        let replication_factor = env::var("RF").unwrap_or("1".to_string());
        let schema_query = DDL.trim()
            .replace('\n', " ")
            .replace("<RF>", &replication_factor);
        for q in schema_query.split(';') {
            let query = q.to_owned() + ";";
            if !query.starts_with("--") && query.len() > 1 {
                debug!("Running Migration {}", query);
                session
                    .query(query, &[])
                    .await
                    .map_err(|e| anyhow!("Error executing migration query: {}", e))?;
            }
        }
    }

    Ok(session)
}
