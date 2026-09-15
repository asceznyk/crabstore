use std::path::PathBuf;
use std::sync::Arc;
use std::collections::HashSet;

use tokio::sync::Mutex;
use tracing::{error,info};
use clap::{Parser, Subcommand};
use redb::{Database};

mod core;
use core::{
  App, Record, Deleted, SysError
};
mod server;
use server::{serve};
mod rebuild;
use rebuild::{rebuild};

#[derive(Subcommand)]
enum Command {
  Run,
  Rebuild,
  //Rebalance
}

const DEFAULT_PORT:usize = 4000;
const DEFAULT_NREPLICAS:usize = 3;
const DEFAULT_NSUB:usize = 5;
const DEFAULT_VOLTIMEOUT:usize = 5;

#[derive(Parser)]
struct Args {
  #[arg(long, default_value_t = DEFAULT_PORT, help = "port to listen on")]
  port:usize,
  #[arg(long, default_value = "/tmp/index.db", help = "path to the db file")]
  dbfile:PathBuf,
  #[arg(long, default_value_t = DEFAULT_NREPLICAS, help = "num of replicas")]
  nreplicas:usize,
  #[arg(long, default_value_t = DEFAULT_NSUB, help = "num of subvolumes/drives")]
  nsub:usize,
  #[arg(long, default_value = "", help = "list of volume servers comma separated")]
  pvolumes:String,
  #[arg(long, default_value_t = DEFAULT_VOLTIMEOUT, help = "request timeout for volume servers - in seconds")]
  voltimeout:usize,
  #[command(subcommand)]
  command:Option<Command>,
}

#[tokio::main]
async fn main() {
  tracing_subscriber::fmt()
    .with_writer(std::io::stdout)
    .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
    .init();
  let args = Args::parse();
  let port = args.port;
  let dbfile = args.dbfile;
  let nreplicas = args.nreplicas;
  let nsub = args.nsub;
  let pvolumes = args.pvolumes;
  let voltimeout = args.voltimeout;
  info!("main: dbfile = {}", dbfile.display());
  info!("main: nreplicas = {nreplicas}, nsub = {nsub}, voltimeout = {voltimeout}");
  let volumes:Vec<String> = pvolumes
    .split(",")
    .map(String::from)
    .collect();
  let vlen:usize = volumes.len();
  if vlen <= 0 {
    error!("main: no. of volumes <= 0, you must have atleast one volume server");
    return;
  }
  if nreplicas <= 0 {
    error!("main: you need to have atleast one replica");
    return;
  }
  if nreplicas > vlen {
    error!("main: error - you need to have atleast as many volume servers as replicas");
    error!("main: {} > {}", nreplicas, vlen);
    return;
  }
  let app = Arc::new(App {
    volumes,
    nreplicas,
    nsub,
    voltimeout,
    db: Database::create(dbfile.clone()).unwrap(),
    uindex: Mutex::new(HashSet::new()),
  });
  match args.command {
    Some(Command::Run) => {
      serve(app, port).await;
    },
    Some (Command::Rebuild) => {
      rebuild(app).await;
    }
    None => {
      error!("main: no command provided! available: `run, rebuild, rebalance`");
    }
  }
}

