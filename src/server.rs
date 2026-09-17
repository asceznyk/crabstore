use std::sync::Arc;
use std::time::Duration;

use axum::{
  extract::{State, Request},
  http::{StatusCode},
  response::{IntoResponse, Response},
  Json,
  Router,
  routing::any
};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue};
use tracing::{error,info};
use serde_json::{json, Value};
use rand::seq::SliceRandom;
use rand::rng;

use crate::core::{App, Deleted, SysError, ListQuery};
use crate::core::{hash_key_into_path};

async fn handle_get(
  app:&App,
  key:&str
) -> Result<(StatusCode, HeaderMap, Json<Value>), SysError> {
  let not_found = || {
    (
      StatusCode::NOT_FOUND,
      HeaderMap::new(),
      Json(json!({
        "status": "Not found",
        "message": "No such record in DB"
      }))
    )
  };
  let rec = match app.get_record(&key.to_string()) {
    Ok(rec) => rec,
    Err(err) => {
      return Err(err);
    }
  };
  if rec.deleted == Deleted::SOFT || rec.deleted == Deleted::HARD {
    return Ok(not_found());
  }
  let mut rvolumes = rec.replica_volumes.clone();
  rvolumes.shuffle(&mut rng());
  let mut mpath:Option<String> = None;
  for rvolume in rvolumes {
    let rpath = format!(
      "http://{}/{}",
      rvolume,
      hash_key_into_path(key.as_bytes())
    );
    let resp = app.client
      .head(&rpath)
      .timeout(Duration::from_secs(app.voltimeout.try_into().unwrap()))
      .send()
      .await;
    if let Ok(resp) = resp {
      if resp.status().is_success() {
        mpath = Some(rpath);
        break;
      }
    }
  }
  match mpath {
    Some(mpath) => {
      let mut headers = HeaderMap::new();
      headers.insert(
        header::LOCATION,
        mpath.parse().unwrap()
      );
      headers.insert(
        HeaderName::from_static("key-volumes"),
        HeaderValue::from_str(&rec.replica_volumes.join(",")).unwrap()
      );
      headers.insert(
        HeaderName::from_static("key-balance"),
        HeaderValue::from_str("balanced").unwrap()
      );
      Ok((
        StatusCode::FOUND,
        headers,
        Json(json!({}))
      ))
    }
    None => {
      Ok(not_found())
    }
  }
}

async fn handle_put(
  app:&App,
  key:&str,
  req:Request,
) -> Result<(StatusCode, Json<Value>), SysError> {
  let content_length = req
    .headers()
    .get(axum::http::header::CONTENT_LENGTH)
    .and_then(|v| v.to_str().ok())
    .and_then(|v| v.parse::<usize>().ok());
  if content_length == Some(0) {
    return Ok((
      StatusCode::LENGTH_REQUIRED,
      Json(json!({
        "error": "Content-Length is required"
      })),
    ));
  }
  let rec = match app.get_record(&key.to_string()) {
    Ok(rec) => Some(rec),
    Err(SysError::RecordNotFound) => None,
    Err(err) => {
      error!("handle_put: Err(err) = {:?}!", err);
      return Err(err);
    },
  };
  if let Some(rec) = rec {
    if rec.deleted == Deleted::NO {
      return Ok((
        StatusCode::FORBIDDEN,
        Json(json!({
          "error": "PUTting into an existing key"
        })),
      ));
    }
  }
  let _ = app.write_to_replicas(&key.to_string(), req).await?;
  Ok((
    StatusCode::CREATED,
    Json(json!({"status": "Success"})),
  ))
}

async fn handle_delete(app:&App, key:&str) -> Result<StatusCode,SysError> {
  let rec = match app.get_record(&key.to_string()) {
    Ok(rec) => rec,
    Err(SysError::RecordNotFound) => {
      error!("handle_delete: record is not found!");
      return Err(SysError::RecordNotFound);
    }
    Err(err) => {
      error!("handle_delete: internal server error?!");
      return Err(err);
    }
  };
  if rec.deleted == Deleted::SOFT || rec.deleted == Deleted::HARD {
    return Ok(StatusCode::NOT_FOUND);
  }
  for rvolume in rec.replica_volumes {
    let rpath = format!(
      "http://{}/{}",
      rvolume,
      hash_key_into_path(key.as_bytes())
    );
    app.client
      .delete(&rpath)
      .timeout(Duration::from_secs(app.voltimeout.try_into().unwrap()))
      .send()
      .await?
      .error_for_status()?;
  }
  app.delete_record(&key.to_string())?;
  Ok(StatusCode::NO_CONTENT)
}

async fn dispatch_key(app:Arc<App>, key:String, req:Request) -> Response {
  info!("dispatch_key: key request");
  let rmethod = req.method().as_str();
  if rmethod == "PUT" || rmethod == "DELETE" {
    let mut uindex = app.uindex.lock().await;
    if !uindex.insert(key.clone()) {
      return (
        StatusCode::CONFLICT,
        Json(json!({
          "error": "The key is being used"
        })),
      ).into_response();
    }
    drop(uindex);
  }
  match rmethod {
    "GET" => {
      handle_get(&app, &key).await.into_response()
    },
    "PUT" => {
      let resp = handle_put(&app, &key, req).await;
      app.uindex.lock().await.remove(&key);
      resp.into_response()
    },
    "DELETE" => {
      let resp = handle_delete(&app, &key).await;
      app.uindex.lock().await.remove(&key);
      resp.into_response()
    },
    _ => {
      (
        StatusCode::METHOD_NOT_ALLOWED,
        Json(json!({
          "error": "Method not allowed"
        })),
      ).into_response()
    },
  }
}

async fn dispatch_query(
  app:Arc<App>,
  prefix:String,
  req:Request
) -> Response {
  info!("dispatch_query: query request..");
  let raw_query = req.uri().query().unwrap_or("");
  let list_query: ListQuery = match serde_urlencoded::from_str(raw_query) {
    Ok(list_query) => list_query,
    Err(err) => {
      error!("dispatch_query: invalid query: {err}");
      return StatusCode::BAD_REQUEST.into_response();
    }
  };
  if list_query.list.is_none() {
    return StatusCode::BAD_REQUEST.into_response();
  }
  info!("dispatch_query: list_query = {:?}, prefix = {}", list_query, prefix);
  let res = match app.list_keys(&prefix, list_query).await {
    Ok(res) => res,
    Err(err) => {
      return err.into_response();
    }
  };
  res.into_response()
}

pub async fn dispatch(
  State(app):State<Arc<App>>,
  req:Request,
) -> Response {
  info!("dispatch: routing request...");
  let path = req.uri().path();
  let key = path.strip_prefix('/').unwrap_or(path);
  if req.uri().query().is_some() {
    return dispatch_query(app, key.to_string(), req).await;
  }
  if key.is_empty() {
    info!("dispatch: empty key!");
    return StatusCode::BAD_REQUEST.into_response();
  }
  dispatch_key(app, key.to_string(), req).await
}

pub async fn serve(app:Arc<App>, port:usize) -> Result<(),SysError> {
  let _ = app.ensure_table()?;
  let aroute = Router::new()
    .route("/", any(dispatch))
    .route("/{*key}", any(dispatch))
    .with_state(app);
  let listener = tokio::net::TcpListener::bind(format!("localhost:{port}"))
    .await
    .unwrap();
  info!("serve: listening on http://localhost:{port}");
  axum::serve(listener, aroute).await.unwrap();
  Ok(())
}

