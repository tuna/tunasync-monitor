use colored::*;
use reqwest;
use serde::Deserialize;
use std::time::SystemTime;
use structopt::StructOpt;

#[derive(Deserialize, Debug)]
struct TunasyncStatus {
    name: String,
    is_master: bool,
    status: String,
    last_update: String,
    last_update_ts: i64,
    last_ended: String,
    last_ended_ts: i64,
    upstream: String,
    size: String,
}

#[derive(StructOpt)]
struct Args {
    #[structopt(short, long, default_value = "7")]
    expire_days: i64,
}

#[paw::main]
fn main(args: Args) {
    for server in ["neomirrors", "nanomirrors"].iter() {
        let client = reqwest::Client::new();
        let mut res = client
            .get(&format!(
                "https://{}.tuna.tsinghua.edu.cn/static/tunasync.json",
                server
            ))
            .header(reqwest::header::USER_AGENT, "tunasync-monitor")
            .send()
            .unwrap();
        let mut status: Vec<TunasyncStatus> = res.json().unwrap();
        status.sort_by_key(|status| -status.last_update_ts);
        let time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let mut fail = false;
        for entry in status {
            if entry.status == "failed" {
                let expired = time - entry.last_update_ts;
                if expired > 60 * 60 * 24 * args.expire_days && entry.last_update_ts > 0 {
                    // one week
                    println!(
                        "{} {}: {}, {} days ago",
                        server.blue(),
                        "failed".red(),
                        entry.name,
                        (time - entry.last_update_ts) / (60 * 60 * 24),
                    );
                    fail = true;
                }
            }
        }

        if !fail {
            println!("{} {}: no out of sync mirrors", server.blue(), "success".green());
        }
    }
}
