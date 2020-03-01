use colored::*;
use elasticsearch::{http::transport::Transport, Elasticsearch, Error, SearchParts};
use reqwest;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::collections::HashSet;
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

    #[structopt(short = "E", long, default_value = "http://localhost:9200")]
    elasticsearch: String,

    #[structopt(short, long, default_value = "2020.01.*")]
    pattern: String,

    #[structopt(short, long)]
    query: bool,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::from_args();
    let mut repos = vec![];
    let mut repo_sizes: HashMap<String, Vec<String>> = HashMap::new();
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
            repos.push(entry.name.clone());
            repo_sizes
                .entry(entry.name.clone())
                .or_insert(vec![])
                .push(entry.size);
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
            println!(
                "{} {}: no out of sync mirrors",
                server.blue(),
                "success".green()
            );
        }
    }

    if args.query {
        let transport = Transport::single_node(&args.elasticsearch)?;
        let client = Elasticsearch::new(transport);
        println!(
            "{} {}: using {}",
            "elasticsearch".blue(),
            "success".green(),
            args.elasticsearch.blue()
        );

        repos.sort();
        repos.dedup();
        let mut repo_count: HashSet<String> = HashSet::new();
        for repo in &repos {
            repo_count.insert(repo.clone());
        }

        let wildcard = format!("filebeat-{}", args.pattern);
        let response = client
            .search(SearchParts::Index(&[&wildcard]))
            .size(repos.len() as i64)
            .body(json!({
                "aggs": {
                    "repo_count": {
                        "terms": {
                            "field": "nginx.access.first_level",
                            "include": repos,
                            "order": {
                                "_count": "asc"
                            },
                            "size": repos.len()
                        }
                    }
                }
            }))
            .allow_no_indices(true)
            .send()
            .await?;
        let response_body = response.read_body::<Value>().await?;
        println!(
            "{}: showing {}",
            "elasticsearch".blue(),
            args.pattern,
        );
        for item in response_body["aggregations"]["repo_count"]["buckets"]
            .as_array()
            .unwrap()
        {
            let count = item["doc_count"].as_i64().unwrap();
            let repo = item["key"].as_str().unwrap();
            repo_count.remove(repo);
            println!(
                "{} {}: {} size={:?}",
                "requests to".blue(),
                repo,
                count,
                repo_sizes[repo]
            );
        }
        for repo in repo_count {
            println!(
                "{}: {} size={:?}",
                "unused repo".blue(),
                repo,
                repo_sizes[&repo]
            );
        }
    }
    Ok(())
}
