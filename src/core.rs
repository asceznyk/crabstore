use std::collections::HashSet;
use std::ops::Bound;

use thiserror;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::{error, info};
use redb::{Database, ReadableDatabase, TableDefinition};
use axum::{
  Json,
  http::StatusCode,
  response::{IntoResponse, Response},
  extract::{Request}
};
use axum::body::Body;
use reqwest::Client;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use futures_util::StreamExt;
use md5::{Digest, Md5};
use base64::{Engine as _, engine::general_purpose::STANDARD};

pub const DEFAULT_LIST_LIMIT:usize = 100;
pub const MAX_LIST_LIMIT:usize = 1000;

pub const TABLE:TableDefinition<String,String> = TableDefinition::new("path_map");

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Deleted(pub i32);

impl Deleted {
  pub const NO:Deleted = Deleted(0);
  pub const SOFT:Deleted = Deleted(1);
  pub const HARD:Deleted = Deleted(2);
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Record {
  pub replica_volumes: Vec<String>,
  pub deleted: Deleted,
  pub content_hash: String
}

#[derive(Debug, thiserror::Error)]
pub enum SysError {
  #[error("transaction error: {0}")]
  Transaction(#[from] redb::TransactionError),
  #[error("table error: {0}")]
  Table(#[from] redb::TableError),
  #[error("storage error: {0}")]
  Storage(#[from] redb::StorageError),
  #[error("commit error: {0}")]
  Commit(#[from] redb::CommitError),
  #[error("JSON error: {0}")]
  Json(#[from] serde_json::Error),
  #[error("HTTP error: {0}")]
  Reqwest(#[from] reqwest::Error),
  #[error("Axum error: {0}")]
  Axum(#[from] axum::Error),
  #[error("base64 error: {0}")]
  Base64Decode(#[from] base64::DecodeError),
  #[error("not found")]
  InvalidCursor,
  #[error("record not found")]
  RecordNotFound,
  #[error("internal server error")]
  Internal,
}

impl IntoResponse for SysError {
  fn into_response(self) -> Response {
    match self {
      SysError::Internal => (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({
          "error": "Internal server error"
        })),
      ).into_response(),
      SysError::RecordNotFound => (
        StatusCode::NOT_FOUND,
        Json(json!({
          "error": "Record not found"
        })),
      ).into_response(),
      _ => (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({
          "error": "Internal server error"
        })),
      ).into_response(),
    }
  }
}

pub fn to_record(json_str:&str) -> Result<Record,SysError> {
  let rec:Record = match serde_json::from_str(json_str) {
    Ok(rec) => rec,
    Err(err) => {
      error!("to_record: error in from_str serde_json!");
      return Err(err.into());
    }
  };
  Ok(rec)
}

pub fn from_record(rec:&Record) -> Result<String,SysError> {
  let json:String = match serde_json::to_string(rec) {
    Ok(json) => json,
    Err(err) => {
      error!("from_record: error in to_string serde_json!");
      return Err(err.into());
    }
  };
  Ok(json)
}

fn hash_key_volume(key:&[u8], volume:&str) -> [u8;16] {
  let mut hasher = Md5::new();
  hasher.update(key);
  hasher.update(volume.as_bytes());
  hasher.finalize().into()
}

pub fn hash_key_into_path(key:&[u8]) -> String {
  let mut hasher = Md5::new();
  hasher.update(key);
  let mkey = hasher.finalize();
  let b64key = STANDARD.encode(key);
  format!("{:02X}/{:02X}/{}", mkey[0], mkey[1], b64key)
}

#[derive(Debug)]
struct SortVol<'a> {
  volume: &'a str,
  hash: [u8; 16],
}

pub fn select_volumes_by_key(
  key:&str,
  volumes:&[String],
  nreplicas:usize,
  nsub:usize
) -> Vec<String> {
  assert!(nreplicas <= volumes.len());
  assert!(nsub > 0);
  let mut sortvols = Vec::<SortVol>::new();
  for volume in volumes {
    let hash = hash_key_volume(key.as_bytes(), volume);
    sortvols.push(SortVol { volume, hash });
  }
  sortvols.sort_by(|a, b| a.hash.cmp(&b.hash));
  let mut kvolumes = Vec::<String>::new();
  for i in 0..nreplicas {
    let sv = &sortvols[i];
    let volname = if nsub == 1 {
      sv.volume.to_string()
    } else {
      let svhash = (sv.hash[12] as u32) << 24
        | (sv.hash[13] as u32) << 16
        | (sv.hash[14] as u32) << 8
        | sv.hash[15] as u32;
      format!("{}/sv{:02X}", sv.volume, svhash % nsub as u32)
    };
    kvolumes.push(volname);
  }
  kvolumes
}

pub async fn stream_to_replicas(
  client:&Client,
  body:Body,
  remote_paths:Vec<String>
) -> Result<(),SysError> {
  info!("stream_to_replicas: remote_paths = {:?}", remote_paths);
  let mut senders = Vec::new();
  let mut uploads = Vec::new();
  for rpath in remote_paths {
    let (tx, rx) = mpsc::channel::<Result<bytes::Bytes,std::io::Error>>(256);
    senders.push(tx);
    let req_body = reqwest::Body::wrap_stream(ReceiverStream::new(rx));
    let client = client.clone();
    let upload = tokio::spawn(async move {
      let result = async {
        let resp = client
          .put(&rpath)
          .body(req_body)
          .send()
          .await?;
        info!("stream_to_replicas: upload to {} received response: {}", rpath, resp.status());
        resp.error_for_status()?;
        Ok::<(),reqwest::Error>(())
      }.await;
      if let Err(ref e) = result {
        error!("stream_to_replicas: upload to {} failed: {}", rpath, e);
      }
      result
    });
    uploads.push(upload);
  }
  let mut body_stream = body.into_data_stream();
  loop {
    let chunk = body_stream.next().await;
    let Some(chunk) = chunk else {
      break;
    };
    let chunk = chunk?;
    for tx in &senders {
      tx.send(Ok(chunk.clone()))
        .await
        .map_err(|_| SysError::Internal)?;
    }
  }
  drop(senders);
  for upload in uploads {
    upload.await .map_err(|_| SysError::Internal)??;
  }
  info!("stream_to_replicas: completed all writes!");
  Ok(())
}

#[derive(Debug, Deserialize)]
pub struct ListQuery {
  pub list: Option<String>,
  pub limit: Option<usize>,
  pub cursor: Option<String>
}

#[derive(Debug)]
pub struct App {
  pub uindex: Mutex<HashSet<String>>,
  pub volumes: Vec<String>,
  pub nsub: usize,
  pub nreplicas: usize,
  pub voltimeout: usize,
  pub db: Database,
  pub client: reqwest::Client,
}

fn prefix_upper_bound(prefix:&str) -> Option<String> {
  let mut bytes = prefix.as_bytes().to_vec();
  for i in (0..bytes.len()).rev() {
    if bytes[i] < 0xff {
      bytes[i] += 1;
      bytes.truncate(i + 1);
      return String::from_utf8(bytes).ok();
    }
  }
  None
}

impl App {
  pub fn ensure_table(&self) -> Result<&Database,SysError> {
    info!("app.ensure_table: called!");
    let db = &self.db;
    {
      let write_txn = db.begin_write()?;
      let table = write_txn.open_table(TABLE)?;
      drop(table);
      write_txn.commit()?;
    }
    Ok(db)
  }
  pub fn get_record(&self, key:&String) -> Result<Record,SysError> {
    let db = &self.db;
    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(TABLE)?;
    let record = table.get(key)?.ok_or(SysError::RecordNotFound)?;
    let json = record.value().to_string();
    to_record(&json)
  }
  pub fn put_record(&self, key:&String, rec:&Record) -> Result<(),SysError> {
    let db = &self.db;
    let write_txn = db.begin_write()?;
    {
      let mut table = write_txn.open_table(TABLE)?;
      let json:String = from_record(rec)?;
      table.insert(key, json)?;
    }
    write_txn.commit()?;
    Ok(())
  }
  pub fn delete_record(&self, key:&String) -> Result<(),SysError> {
    let db = &self.db;
    let write_txn = db.begin_write()?;
    {
      let mut table = write_txn.open_table(TABLE)?;
      table.remove(key)?.ok_or(SysError::RecordNotFound)?;
    }
    write_txn.commit()?;
    Ok(())
  }
  pub async fn write_to_replicas(
    &self,
    key:&String,
    req:Request
  ) -> Result<(),SysError> {
    let kvolumes:Vec<String> = select_volumes_by_key(
      key, &self.volumes, self.nreplicas, self.nsub
    );
    self.put_record(
      &key.to_string(), &Record {
        replica_volumes: kvolumes.clone(),
        deleted: Deleted::SOFT,
        content_hash: "".to_string()
      }
    )?;
    let mut remote_paths = Vec::<String>::new();
    for i in 0..kvolumes.len() {
      let rpath = format!(
        "http://{}/{}",
        kvolumes[i].to_string(),
        hash_key_into_path(key.as_bytes())
      );
      remote_paths.push(rpath);
    }
    stream_to_replicas(&self.client, req.into_body(), remote_paths).await?;
    self.put_record(
      &key.to_string(), &Record {
        replica_volumes: kvolumes.clone(),
        deleted: Deleted::NO,
        content_hash: "".to_string(),
      }
    )?;
    Ok(())
  }
  pub async fn list_keys(
    &self, prefix:&String, list_query:ListQuery
  ) -> Result<Json<Value>,SysError> {
    let limit = list_query.limit.unwrap_or(DEFAULT_LIST_LIMIT).min(MAX_LIST_LIMIT);
    let cursor:Option<String> = list_query.cursor;
    let read_txn = self.db.begin_read()?;
    let table = read_txn.open_table(TABLE)?;
    let lower = prefix.clone();
    let upper:Option<String> = prefix_upper_bound(prefix.as_str());
    if let Some(cursor) = &cursor {
      if !cursor.starts_with(&lower) {
        return Err(SysError::InvalidCursor);
      }
    }
    let range = match (cursor, upper) {
      (Some(cursor), Some(upper)) => table.range((
        Bound::Excluded(cursor),
        Bound::Excluded(upper),
      ))?,
      (None, Some(upper)) => table.range(
        lower..upper
      )?,
      (Some(cursor), None) => table.range((
        Bound::Excluded(cursor),
        Bound::<String>::Unbounded,
      ))?,
      (None, None) => table.range((
        Bound::<String>::Unbounded,
        Bound::<String>::Unbounded,
      ))?,
    };
    let mut res:Vec<String> = Vec::with_capacity(limit+1);
    for item in range {
      let (key, _) = item?;
      res.push(key.value().to_string());
      if res.len() > limit {
        break;
      }
    }
    let has_more = res.len() > limit;
    if has_more { res.pop(); };
    let next = if has_more { res.last().clone() } else { None };
    Ok(
      Json(json!({
        "keys": res,
        "next": next,
        "has_more": has_more
      }))
    )
  }
}

