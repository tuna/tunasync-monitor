use serde::Deserialize;
use std::time::SystemTime;

#[derive(Deserialize, Debug)]
pub struct TunasyncStatus {
    pub name: String,
    pub is_master: bool,
    pub status: String,
    pub last_update: String,
    pub last_update_ts: i64,
    pub last_ended: String,
    pub last_ended_ts: i64,
    pub upstream: String,
    pub size: String,
}

pub fn get_server_status(server: &str) -> Vec<TunasyncStatus> {
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
    status
}

pub fn get_expire_days(ts: i64) -> i64 {
    let time = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    (time - ts) / (60 * 60 * 24)
}

pub fn get_expired_repos(status: &Vec<TunasyncStatus>, expire_days: i64) -> Vec<(String, i64)> {
    let mut res = vec![];
    let time = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    for entry in status {
        let expired = time - entry.last_update_ts;
        if expired > 60 * 60 * 24 * expire_days && entry.last_update_ts > 0 {
            res.push((
                entry.name.clone(),
                (time - entry.last_update_ts) / (60 * 60 * 24),
            ));
        }
    }
    res
}
