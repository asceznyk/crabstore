use rand::Rng;
use reqwest::Client;
use std::env;
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinSet;

async fn remote_delete(
  client:&Client, remote:&str
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
  let response = client.delete(remote).send().await?;
  if response.status() != reqwest::StatusCode::NO_CONTENT {
    return Err(format!("remote_delete: wrong status code {}", response.status()).into());
  }
  Ok(())
}

async fn remote_put(
  client:&Client, remote:&str, body:&str
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
  let response = client.put(remote).body(body.to_owned()).send().await?;
  if response.status() != reqwest::StatusCode::CREATED && response.status() != reqwest::StatusCode::NO_CONTENT {
    return Err(format!("remote_put: wrong status code {}", response.status()).into());
  }
  Ok(())
}

async fn remote_get(
  client:&Client, remote:&str
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
  let response = client.get(remote).send().await?;
  if response.status() != reqwest::StatusCode::OK {
    return Err(format!("remote_get: wrong status code {}", response.status()).into());
  }
  Ok(response.text().await?)
}

#[tokio::main]
async fn main() {
  let args:Vec<String> = env::args().collect();
  if args.len() != 2 {
    eprintln!("usage: {} <port>", args[0]);
    std::process::exit(1);
  }
  let port = &args[1];
  let base_url = format!("http://localhost:{}", port);
  let client = Arc::new(
    reqwest::Client::builder()
      .pool_max_idle_per_host(1000)
      .build()
      .expect("failed to create HTTP client")
  );
  let count = 10000;
  let workers = 16;
  println!("starting thrasher");
  let start = Instant::now();
  let mut tasks = JoinSet::new();
  for worker in 0..workers {
    let client = Arc::clone(&client);
    let base_url = base_url.clone();
    let n_ops = count / workers + usize::from(worker < count % workers);
    tasks.spawn(async move {
      for _ in 0..n_ops {
        let key = format!("benchmark-{}", rand::rng().random::<u64>());
        let value = format!("value-{}", rand::rng().random::<u64>());
        let url = format!("{}/{}", base_url, key);
        let opstart = Instant::now();
        if let Err(err) = remote_put(&client, &url, &value).await {
          eprintln!("PUT FAILED: {}", err);
          return false;
        }
        //println!("[worker {}] PUT {} in {:?}", worker, key, opstart.elapsed());
        let opstart = Instant::now();
        match remote_get(&client, &url).await {
          Ok(body) if body == value => {}
          Ok(body) => {
            eprintln!("GET FAILED: value mismatch: got {:?}, expected {:?}", body, value);
            return false;
          }
          Err(err) => {
            eprintln!("GET FAILED: {}", err);
            return false;
          }
        }
        //println!("[worker {}] GET {} in {:?}", worker, key, opstart.elapsed());
        let opstart = Instant::now();
        if let Err(err) = remote_delete(&client, &url).await {
          eprintln!("DELETE FAILED: {}", err);
          return false;
        }
        //println!("[worker {}] DELETE {} in {:?}", worker, key, opstart.elapsed());
      }
      true
    });
  }
  while let Some(result) = tasks.join_next().await {
    match result {
      Ok(true) => {}
      Ok(false) | Err(_) => {
        eprintln!("ERROR");
        std::process::exit(1);
      }
    }
  }
  let elapsed = start.elapsed();
  println!("{} write/read/delete in {:?}", count, elapsed);
  println!("thats {:.2}/sec", count as f64 / elapsed.as_secs_f64());
}

