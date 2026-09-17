use std::sync::Arc;
use std::ops::Bound;
use std::collections::HashSet;
use std::time::Duration;

use tracing::info;
use redb::{ReadableDatabase};
use axum::body::Body;

use crate::core::{TABLE, DEFAULT_LIST_LIMIT, Deleted, Record, App, SysError};
use crate::core::{
  to_record, select_volumes_by_key, hash_key_into_path, stream_to_replicas
};

fn needs_rebalance(kvolumes:&Vec<String>, rvolumes:&Vec<String>) -> bool {
  info!("needs_rebalance: kvolumes = {:?}, rvolumes = {:?}", kvolumes, rvolumes);
  if kvolumes.len() != rvolumes.len() {
    return true;
  }
  for i in 0..kvolumes.len() {
    if kvolumes[i] != rvolumes[i] {
      return true;
    }
  }
  return false;
}

async fn balance_item(app:&App, key:String, rec:Record) -> Result<(),SysError> {
  info!("balance_item: key = {key}");
  let kvolumes = select_volumes_by_key(
    key.as_str(),
    &app.volumes,
    app.nreplicas,
    app.nsub
  );
  let rvolumes:Vec<String> = rec.replica_volumes;
  if !needs_rebalance(&kvolumes, &rvolumes) {
    info!("balance_item: no rebalance required");
    return Ok(());
  }
  let aset:HashSet<&String> = kvolumes.iter().collect();
  let bset:HashSet<&String> = rvolumes.iter().collect();
  let add_set:Vec<String> = kvolumes
    .iter()
    .filter(|v| !bset.contains(v))
    .map(|volume| {
      format!(
        "http://{}/{}",
        volume,
        hash_key_into_path(key.as_bytes())
      )
    })
    .collect();
  let remove_set:Vec<String> = rvolumes
    .iter()
    .filter(|v| !aset.contains(v))
    .map(|volume| {
      format!(
        "http://{}/{}",
        volume,
        hash_key_into_path(key.as_bytes())
      )
    })
    .collect();
  let mut remote_from:Option<String> = None;
  for volume in rvolumes {
    let rpath = format!(
      "http://{}/{}", volume, hash_key_into_path(key.as_bytes())
    );
    let resp = app.client
      .head(&rpath)
      .timeout(Duration::from_secs(app.voltimeout.try_into().unwrap()))
      .send()
      .await;
    if let Ok(resp) = resp {
      if resp.status().is_success() {
        remote_from = Some(rpath);
        break;
      }
    }
  }
  let Some(remote_from) = remote_from else {
    return Err(SysError::Internal);
  };
  let resp = app.client
    .get(&remote_from)
    .send()
    .await?
    .error_for_status()?;
  let body = Body::from_stream(resp.bytes_stream());
  stream_to_replicas(&app.client, body, add_set).await?;
  for rpath in remove_set {
    app.client
      .delete(rpath)
      .timeout(Duration::from_secs(app.voltimeout.try_into().unwrap()))
      .send()
      .await?
      .error_for_status()?;
  }
  app.put_record(
    &key.to_string(), &Record {
      replica_volumes: kvolumes,
      deleted: Deleted::NO,
      content_hash: rec.content_hash
    }
  )?;
  Ok(())
}

pub async fn rebalance(app:Arc<App>) -> Result<(),SysError> {
  let table = app.db.begin_read()?.open_table(TABLE)?;
  let range = table.range((
    Bound::<String>::Unbounded,
    Bound::<String>::Unbounded
  ))?;
  let mut count = 0;
  for item in range {
    let (key, rec) = item?;
    let _ = balance_item(
      &app,
      key.value().to_string(),
      to_record(rec.value().as_str()).unwrap()
    ).await;
    count += 1;
    if count > DEFAULT_LIST_LIMIT {
      break;
    }
  }
  Ok(())
}


