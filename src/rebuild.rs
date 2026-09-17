use std::sync::Arc;
use std::collections::{HashSet, VecDeque};
use std::time::Duration;

use tracing::info;
use serde::{Deserialize, Serialize};
use base64::{Engine as _, engine::general_purpose::STANDARD};

use crate::core::{App, Record, SysError, Deleted};
use crate::core::{select_volumes_by_key};

#[derive(Debug, Serialize, Deserialize)]
pub struct File {
  pub name: String,
  #[serde(rename = "type")]
  pub ftype: String,
  pub mtime: String
}

async fn get_unique_keys(
  app:&App,
  volume:&str
) -> Result<HashSet<String>, SysError> {
  let mut uniq_keys = HashSet::new();
  let mut queue = VecDeque::from([String::from("/")]);
  while let Some(path) = queue.pop_front() {
    let url = format!("http://{}{}", volume, path);
    let files: Vec<File> = app.client
      .get(&url)
      .timeout(Duration::from_secs(app.voltimeout.try_into().unwrap()))
      .send()
      .await?
      .json()
      .await?;
    for file in files {
      let child = format!("{}/{}", path.trim_end_matches('/'), file.name);
      if file.name == "body_temp" {
        continue;
      }
      if file.ftype == "directory" {
        queue.push_back(child);
        continue;
      }
      if file.name.ends_with(".pid") {
        continue;
      }
      let decoded = STANDARD.decode(file.name.as_bytes())?;
      let key = String::from_utf8(decoded)
        .map_err(|_| SysError::Internal)?;
      uniq_keys.insert(key);
    }
  }
  Ok(uniq_keys)
}

pub async fn rebuild(app:Arc<App>) -> Result<(), SysError> {
  let mut tasks = Vec::with_capacity(app.volumes.len());
  for volume in &app.volumes {
    let app = Arc::clone(&app);
    let volume = volume.clone();
    tasks.push(tokio::spawn(async move {
      get_unique_keys(&app, &volume).await
    }));
  }
  let mut uniq_keys = HashSet::<String>::new();
  for task in tasks {
    let keys = task.await.unwrap().unwrap();
    uniq_keys.extend(keys);
  }
  info!("rebuild: uniq_keys = {:?}", uniq_keys);
  for key in &uniq_keys {
    let kvolumes = select_volumes_by_key(
      key,
      &app.volumes,
      app.nreplicas,
      app.nsub
    );
    app.put_record(
      &key.to_string(), &Record {
        replica_volumes: kvolumes.clone(),
        deleted: Deleted::NO,
        content_hash: "".to_string()
      }
    )?;
  }
  Ok(())
}

