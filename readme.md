# NodeNsync — Distributed Key-Value Store

![Go](https://img.shields.io/badge/Go-1.21+-blue)
[![License: MIT](https://img.shields.io/badge/License-MIT-green)](LICENSE)

NodeNsync is a high-performance, distributed key-value store designed for scalability and fault tolerance. It supports clustering, replication, pub/sub messaging, and snapshot-based persistence, making it ideal for applications requiring fast data access with resilience.

---

## Features

- **Distributed Clustering**: Automatic leader election and slot-based sharding.
- **Replication**: Async data synchronization across nodes.
- **Persistence**: Periodic snapshots to disk for recovery.
- **Pub/Sub Messaging**: Real-time message broadcasting.
- **Role-Based Access**: Admin/user roles with session tokens.
- **RESP Protocol Support**: Compatible with standard CLI tools.
- **Rate Limiting**: Protect against abusive clients.
- **Health Monitoring**: Track connections, latency, and memory usage.

---

## Installation

### Prerequisites
- **Go 1.21+**
- **Redis CLI** (for testing commands)

### Steps
1. Clone the repository:
   ```bash
   git clone https://github.com/XIN2025/NodeNsync.git
   cd NodeNsync
   ```

2. Build the binary:
   ```bash
   go build -o NodeNsync cmd/server/main.go
   ```

3. Start the server (default: :6379):
   ```bash
   ./NodeNsync --listenAddr :6379
   ```

## Configuration

| Flag | Description | Default |
|------|-------------|---------|
| `--listenAddr` | Address to listen on | `:6379` |
| `--dataDir` | Directory for snapshots | `./data` |
| `--snapshotInterval` | Auto-snapshot interval | `1m` |
| `--rateLimit` | Max commands/sec per client | `3` |

Example:
```bash
./NodeNsync --listenAddr :6380 --dataDir /mnt/data --rateLimit 10
```

## Usage

### Connect with Redis CLI
```bash
redis-cli -p 6379
```

### Basic Commands
Set/Get Keys:
```redis
127.0.0.1:6379> SET user:1 "Alice"
OK
127.0.0.1:6379> GET user:1
"Alice"
```

### Pub/Sub Messaging
```redis
# Terminal 1 (Subscriber):
127.0.0.1:6379> SUBSCRIBE updates

# Terminal 2 (Publisher):
127.0.0.1:6379> PUBLISH updates "New data!"
```

## Advanced Features

### Clustering
Start additional nodes:
```bash
./NodeNsync --listenAddr :6381
```

Join nodes via CLI:
```redis
REPLICAOF host port  # Promote to leader
```

### Replication
Configure a node as a replica:
```redis
REPLICAOF 127.0.0.1 6379
```

### Persistence
Manual snapshot:
```redis
SAVE
```

## Monitoring
```redis
127.0.0.1:6379> INFO
# Server
version:1.0.0
connected_clients:5
used_memory:128MB

# Replication
role:master
connected_replicas:2
```

## Building from Source
Install dependencies:
```bash
go get github.com/tidwall/resp
```

Run tests:
```bash
go test ./...
```

## Contributing
1. Fork the repository
2. Create a feature branch: `git checkout -b feat/new-feature`
3. Commit changes: `git commit -am 'Add feature'`
4. Push: `git push origin feat/new-feature`
5. Submit a pull request



## FAQ

**Q: Can I use Redis clients with NodeNsync?**  
A: Yes! NodeNsync uses the RESP protocol for compatibility.

**Q: Where is data stored?**  
A: Data is in-memory. Snapshots are saved to dataDir.

**Q: How to reset the database?**  
A: Stop the server, delete dataDir, and restart.

