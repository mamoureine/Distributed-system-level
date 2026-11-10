# Distributed key-value store (JavaScript)

A small, in-memory Node.js simulation of a distributed key-value store. It uses
a SHA-256 consistent-hash ring with virtual nodes, replica copies, and an LRU
cache. The `set()` and `get()` API routes keys and handles replica failover
internally, so callers do not need to know about nodes.

## Run

```powershell
node kv-store.js
```

The demo loads `user:101` through `user:106` with the sample names, exercises
the cache, adds and removes a node (reporting primary-key reassignment), and
reads data while a node is unavailable.

## Test

Requires Node.js 18 or newer; no third-party packages are needed.

```powershell
node --test
```

Writes go to available replicas; reads use available copies and choose the
newest version. If all copies of an existing key are unavailable, the store
raises `NodeUnavailableError`. Membership changes require a healthy cluster,
and node recovery repairs replica copies. This is an educational in-memory
simulation, not a persistent or networked database.
