(Due to technical issues, the search service is temporarily unavailable.)

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
- **RESP Protocol Support**: Compatible with standard Redis CLI tools.
- **Rate Limiting**: Protect against abusive clients.
- **Health Monitoring**: Track connections, latency, and memory usage.
- **TLS Support**: Secure client-server communication.

---

## Installation

### Prerequisites
- **Go 1.21+**
- **Redis CLI** (for testing commands)

### Steps
1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/nodensync.git
   cd nodensync
   ```

2. Build the binary:
   ```bash
   go build -o nodensync cmd/server/main.go
   ```

3. Start the server (default: :6379):
   ```bash
   ./nodensync --listenAddr :6379
   ```

## Configuration

| Flag | Description | Default |
|------|-------------|---------|
| `--listenAddr` | Address to listen on | `:6379` |
| `--dataDir` | Directory for snapshots | `./data` |
| `--snapshotInterval` | Auto-snapshot interval | `1m` |
| `--rateLimit` | Max commands/sec per client | `3` |
| `--enableTLS` | Enable TLS encryption | `false` |

Example:
```bash
./nodensync --listenAddr :6380 --dataDir /mnt/data --rateLimit 10 --enableTLS
```

---

## Command Reference

### Core Commands
| Command | Syntax | Description |
|---------|--------|-------------|
| **AUTH** | `AUTH <username> <password>` | Authenticate with role-based access |
| **SET** | `SET <key> <value>` | Store a key-value pair |
| **GET** | `GET <key>` | Retrieve value by key |
| **DEL** | `DEL <key>` | Delete a key |
| **INCR** | `INCR <key>` | Atomically increment integer value |

### Hash Commands
| Command | Syntax | Description |
|---------|--------|-------------|
| **HSET** | `HSET <key> <field> <value>` | Set hash field value |
| **HGET** | `HGET <key> <field>` | Get hash field value |

### Pub/Sub Commands
| Command | Syntax | Description |
|---------|--------|-------------|
| **PUBLISH** | `PUBLISH <channel> <message>` | Broadcast message to channel |
| **SUBSCRIBE** | `SUBSCRIBE <channel>` | Receive messages from channel |
| **UNSUBSCRIBE** | `UNSUBSCRIBE [channel...]` | Stop listening to channels |

### Administration
| Command | Syntax | Description |
|---------|--------|-------------|
| **INFO** | `INFO` | Get server statistics & metrics |
| **ROLE** | `ROLE` | Show node role (leader/follower) |
| **REPLICAOF** | `REPLICAOF <host> <port>` | Configure replication |
| **MONITOR** | `MONITOR` | Stream all executed commands |

### Utility
| Command | Syntax | Description |
|---------|--------|-------------|
| **PING** | `PING` | Check server responsiveness |
| **HELP** | `HELP` | List available commands |

---

## Advanced Features

### Clustering Example
Start 3-node cluster:
```bash
# Node 1 (Leader)
./nodensync --listenAddr :7000

# Node 2
./nodensync --listenAddr :7001

# Node 3
./nodensync --listenAddr :7002
```

Join cluster via Redis CLI:
```redis
REPLICAOF 127.0.0.1 7000
```

### Replication Setup
Configure node as replica:
```redis
127.0.0.1:6380> REPLICAOF 127.0.0.1 6379
```

### Persistence Management
Manual snapshot creation:
```redis
SAVE
```

---

## Monitoring & Metrics

### INFO Output Example
```redis
127.0.0.1:6379> INFO
# Server
version:1.0.0
uptime:2h35m
connected_clients:8
used_memory:256MB

# Replication
role:master
connected_slaves:2
master_repl_offset:89234

# Keyspace
db0:keys=1532
```

---

## Security

### TLS Configuration
1. Generate certificates:
   ```bash
   openssl req -x509 -newkey rsa:4096 -nodes -keyout key.pem -out cert.pem -days 365
   ```

2. Start server with TLS:
   ```bash
   ./nodensync --enableTLS --tlsCert cert.pem --tlsKey key.pem
   ```

3. Connect with Redis CLI:
   ```bash
   redis-cli --tls --cert ./cert.pem --key ./key.pem -p 6379
   ```

---

## Building from Source

1. Install dependencies:
   ```bash
   go get github.com/tidwall/resp
   ```

2. Run tests:
   ```bash
   go test ./...
   ```

3. Build with optimizations:
   ```bash
   go build -ldflags "-s -w" -o nodensync cmd/server/main.go
   ```

---

## Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feat/new-feature`
3. Commit changes: `git commit -am 'Add awesome feature'`
4. Push: `git push origin feat/new-feature`
5. Submit pull request

---

## FAQ

**Q: How is this different from Redis?**  
A: NodeNsync focuses on horizontal scaling and simplicity, while maintaining Redis protocol compatibility.

**Q: Can I use existing Redis clients?**  
A: Yes! Any RESP-compatible client (redis-py, Jedis, etc.) will work.

**Q: Where are snapshots stored?**  
A: In the `dataDir` specified at startup (default: `./data`).

**Q: How to reset the database?**  
A: Stop server and delete all `.json` files in the data directory.
