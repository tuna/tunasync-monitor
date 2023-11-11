use colored::*;
use elasticsearch::{http::transport::Transport, Elasticsearch, Error, SearchParts};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::collections::HashSet;
use clap::Parser;
use tunasync_monitor::*;

#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value = "7")]
    expire_days: i64,

    #[arg(short = 'E', long, default_value = "http://localhost:9200")]
    elasticsearch: String,

    #[arg(short, long, default_value = "2020.01.*")]
    pattern: String,

    #[arg(short, long)]
    query: bool,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    let mut repos = vec![];
    let mut repo_sizes: HashMap<String, Vec<String>> = HashMap::new();
    for server in [
        "neomirrors.tuna.tsinghua.edu.cn",
        "nanomirrors.tuna.tsinghua.edu.cn",
    ]
    .iter()
    {
        let status = get_server_status(server)
            .await
            .expect("Get server status should not fail");
        let mut fail = false;
        let expired_repos = get_expired_repos(&status, args.expire_days);
        for entry in status {
            repos.push(entry.name.clone());
            repo_sizes
                .entry(entry.name.clone())
                .or_insert(vec![])
                .push(entry.size);
        }
        for (repo, days) in expired_repos {
            println!(
                "{} {}: {}, {} days ago",
                server.blue(),
                "failed".red(),
                repo,
                days,
            );
            fail = true;
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
        let response_body = response.json::<Value>().await?;
        println!("{}: showing {}", "elasticsearch".blue(), args.pattern,);
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
