# crabstore
A simple distributed key value store written in Rust. Inspired by [minikeyvalue](https://github.com/geohot/minikeyvalue). It uses stock NGINX as a volume server and [REDB](https://www.redb.org/) for indexing.

### API
- GET `/key`
  - Redirects to the volume containing the key.
- PUT `/key`
  - Blocks. `201` = written, `403` = key already exists.
- DELETE `/key`
  - Blocks. `204` = deleted.
- GET `/key?list`
  - Lists keys matching the prefix.

### Start Volume Servers
```bash
sudo ./scripts/bringup.sh
```
This starts the nginx volume servers used by crabstore.
To stop them:

```bash
sudo kill $(pgrep -f nginx)
```

### Start Master Server
Built if first with `cargo build --release`. Then:
```bash
./target/release/crabstore \
  --pvolumes localhost:4001,localhost:4002,localhost:4003,localhost:4004,localhost:4005 \
  --dbfile /tmp/crabstore.db
```

### Usage

```bash
# put "bigswag" in key "wehave"
curl -v -X PUT -d bigswag localhost:4000/wehave

# get key "wehave"
curl -v localhost:4000/wehave

# delete key "wehave"
curl -v -X DELETE localhost:4000/wehave

# list keys starting with "we"
curl -v localhost:4000/we?list

# put a file
curl -v -X PUT -T /path/to/local/file.txt localhost:4000/file.txt

# get a file
curl -v -o /path/to/local/file.txt localhost:4000/file.txt
```

### Replication
Set the number of replicas with:
```bash
--nreplicas 3
```
You can set it upto 5, for now.

### Rebuilding
The index can be reconstructed from the files on the volume servers.
```bash
./target/release/crabstore \
  --pvolumes localhost:4001,localhost:4002,localhost:4003,localhost:4004,localhost:4005 \
  --dbfile /tmp/rebuild.db \
  rebuild
```

### Rebalancing
Rebalance data after changing the volume servers.
```bash
./target/release/crabstore \
  --pvolumes localhost:4001,localhost:4002,localhost:4003,localhost:4004,localhost:4005 \
  --dbfile /tmp/index.db \
  rebalance
```
The master should be stopped before running `rebuild` or `rebalance`.

### Storage
Values are stored as regular files on the volume servers.
Keys are hashed into paths:
```text
key -> hash -> volume/path -> nginx -> file
```
The master only stores the index mapping keys to volumes.

### Testing
`scripts/thrasher` can be used for load testing.
```bash
cargo run --release --bin thrasher -- 4000
```
Example:

```text
10000 write/read/delete in 3.377056185s
thats 2961.16/sec
```
The benchmark uses small keys and values. It is mainly a test of request, replication, and index overhead. Use a release build when benchmarking.

