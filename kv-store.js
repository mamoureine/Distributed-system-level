"use strict";

const { createHash } = require("node:crypto");

class NodeUnavailableError extends Error {
  constructor(message) {
    super(message);
    this.name = "NodeUnavailableError";
  }
}

class ConsistentHashRing {
  constructor(virtualNodes = 100) {
    if (!Number.isInteger(virtualNodes) || virtualNodes < 1) {
      throw new RangeError("virtualNodes must be a positive integer");
    }

    this.virtualNodes = virtualNodes;
    this.nodes = new Set();
    this.points = [];
  }

  static hash(value) {
    return BigInt(`0x${createHash("sha256").update(value).digest("hex")}`);
  }

  addNode(name) {
    if (this.nodes.has(name)) {
      throw new Error(`Node "${name}" already exists`);
    }
    this.nodes.add(name);
    // Virtual points spread each physical node across the hash space.
    for (let index = 0; index < this.virtualNodes; index += 1) {
      const token = `${name}:${index}`;
      this.points.push({ hash: ConsistentHashRing.hash(token), name, token });
    }
    this.sortPoints();
  }

  removeNode(name) {
    if (!this.nodes.has(name)) {
      throw new Error(`Node "${name}" does not exist`);
    }
    this.nodes.delete(name);
    this.points = this.points.filter((point) => point.name !== name);
  }

  owner(key) {
    if (this.points.length === 0) {
      throw new NodeUnavailableError("The cluster has no nodes");
    }
    const hash = ConsistentHashRing.hash(key);
    // Walk clockwise to the first point; wrap around at the end of the ring.
    const index = this.points.findIndex((point) => point.hash >= hash);
    return this.points[index === -1 ? 0 : index].name;
  }

  replicasFor(key, count) {
    if (!Number.isInteger(count) || count < 1) {
      throw new RangeError("replica count must be a positive integer");
    }
    if (this.points.length === 0) return [];

    const hash = ConsistentHashRing.hash(key);
    let index = this.points.findIndex((point) => point.hash >= hash);
    if (index === -1) index = 0;

    const replicas = [];
    const seen = new Set();
    // Continue clockwise, skipping virtual points already represented by a node.
    for (let offset = 0; offset < this.points.length; offset += 1) {
      const name = this.points[(index + offset) % this.points.length].name;
      if (!seen.has(name)) {
        seen.add(name);
        replicas.push(name);
        if (replicas.length === count) break;
      }
    }
    return replicas;
  }

  sortPoints() {
    this.points.sort((left, right) => {
      if (left.hash < right.hash) return -1;
      if (left.hash > right.hash) return 1;
      return left.token.localeCompare(right.token);
    });
  }
}

class LRUCache {
  constructor(capacity = 128) {
    if (!Number.isInteger(capacity) || capacity < 0) {
      throw new RangeError("cache capacity must be a non-negative integer");
    }
    this.capacity = capacity;
    this.items = new Map();
    this.hits = 0;
    this.misses = 0;
  }

  get(key) {
    if (!this.items.has(key)) {
      this.misses += 1;
      return { found: false };
    }
    const value = this.items.get(key);
    // Map iteration order tracks recency: delete and reinsert makes this newest.
    this.items.delete(key);
    this.items.set(key, value);
    this.hits += 1;
    return { found: true, value };
  }

  set(key, value) {
    if (this.capacity === 0) return;
    this.items.delete(key);
    this.items.set(key, value);
    if (this.items.size > this.capacity) {
      // The first Map entry is the least recently used.
      this.items.delete(this.items.keys().next().value);
    }
  }

  clear() {
    this.items.clear();
  }
}

class DistributedKVStore {
  constructor(nodeNames, { replicas = 2, virtualNodes = 100, cacheCapacity = 128 } = {}) {
    if (!Array.isArray(nodeNames) || nodeNames.length === 0) {
      throw new TypeError("Provide at least one node name");
    }
    if (nodeNames.some((name) => typeof name !== "string" || name.length === 0)) {
      throw new TypeError("Node names must be non-empty strings");
    }
    if (new Set(nodeNames).size !== nodeNames.length) {
      throw new Error("Node names must be unique");
    }
    if (!Number.isInteger(replicas) || replicas < 1) {
      throw new RangeError("replicas must be a positive integer");
    }

    this.replicaCount = replicas;
    this.ring = new ConsistentHashRing(virtualNodes);
    this.nodes = new Map();
    this.cache = new LRUCache(cacheCapacity);
    this.version = 0;
    for (const name of nodeNames) this.addInitialNode(name);
  }

  // The public API routes keys internally; callers never select a node.
  set(key, value) {
    this.validateKey(key);
    const placements = this.ring.replicasFor(key, this.replicaCount);
    const available = placements.filter((name) => this.nodes.get(name).available);
    if (available.length === 0) {
      throw new NodeUnavailableError(`No available replica can store key "${key}"`);
    }

    // Versioned records let reads/recovery choose the newest available copy.
    const record = { value, version: ++this.version };
    for (const name of available) this.nodes.get(name).data.set(key, record);
    this.cache.set(key, record);
  }

  get(key) {
    this.validateKey(key);
    const cached = this.cache.get(key);
    if (cached.found) return cached.value.value;

    const placements = this.ring.replicasFor(key, this.replicaCount);
    let latest;
    // A failed primary can be bypassed by reading an available replica.
    for (const name of placements) {
      const node = this.nodes.get(name);
      if (!node.available) continue;
      const record = node.data.get(key);
      if (record && (!latest || record.version > latest.version)) latest = record;
    }

    if (latest) {
      this.cache.set(key, latest);
      return latest.value;
    }
    if ([...this.nodes.values()].some((node) => node.data.has(key))) {
      throw new NodeUnavailableError(`No available replica can serve key "${key}"`);
    }
    return undefined;
  }

  addNode(name) {
    this.ensureHealthyCluster();
    if (this.nodes.has(name)) throw new Error(`Node "${name}" already exists`);
    return this.changeTopology(name, true);
  }

  removeNode(name) {
    this.ensureHealthyCluster();
    if (!this.nodes.has(name)) throw new Error(`Node "${name}" does not exist`);
    if (this.nodes.size === 1) throw new Error("Cannot remove the last node");
    return this.changeTopology(name, false);
  }

  failNode(name) {
    this.getNode(name).available = false;
  }

  recoverNode(name) {
    const recovering = this.getNode(name);
    recovering.available = true;
    this.repairReplicas(name);
    this.cache.clear();
  }

  clearCache() {
    this.cache.clear();
  }

  addInitialNode(name) {
    this.ring.addNode(name);
    this.nodes.set(name, { name, available: true, data: new Map() });
  }

  changeTopology(name, joining) {
    const values = this.collectLatestValues();
    const oldOwners = new Map(
      [...values.keys()].map((key) => [key, this.ring.owner(key)]),
    );

    // Rebuild replica placement for the new ring using the saved latest values.
    if (joining) this.addInitialNode(name);
    else {
      this.ring.removeNode(name);
      this.nodes.delete(name);
    }

    for (const node of this.nodes.values()) node.data.clear();
    for (const [key, record] of values) {
      for (const replica of this.ring.replicasFor(key, this.replicaCount)) {
        this.nodes.get(replica).data.set(key, record);
      }
    }
    this.cache.clear();

    let movedPrimaryKeys = 0;
    // Count primary ownership changes; replica copies are also refreshed above.
    for (const [key, owner] of oldOwners) {
      if (owner !== this.ring.owner(key)) movedPrimaryKeys += 1;
    }
    return { movedPrimaryKeys, totalKeys: values.size };
  }

  collectLatestValues() {
    const values = new Map();
    for (const node of this.nodes.values()) {
      for (const [key, record] of node.data) {
        const current = values.get(key);
        if (!current || record.version > current.version) values.set(key, record);
      }
    }
    return values;
  }

  repairReplicas(recoveringName) {
    const values = this.collectLatestValues();
    for (const [key, record] of values) {
      for (const name of this.ring.replicasFor(key, this.replicaCount)) {
        this.nodes.get(name).data.set(key, record);
      }
    }
    // Recovered nodes receive current copies before they serve traffic again.
    this.nodes.get(recoveringName).available = true;
  }

  ensureHealthyCluster() {
    if ([...this.nodes.values()].some((node) => !node.available)) {
      throw new NodeUnavailableError("Recover all nodes before changing cluster membership");
    }
  }

  getNode(name) {
    const node = this.nodes.get(name);
    if (!node) throw new Error(`Node "${name}" does not exist`);
    return node;
  }

  validateKey(key) {
    if (typeof key !== "string" || key.length === 0) {
      throw new TypeError("Keys must be non-empty strings");
    }
  }
}

function runDemo() {
  const records = [
    ["user:101", { name: "Alice" }],
    ["user:102", { name: "Bob" }],
    ["user:103", { name: "Charlie" }],
    ["user:104", { name: "Diana" }],
    ["user:105", { name: "Eve" }],
    ["user:106", { name: "Frank" }],
  ];
  const store = new DistributedKVStore(["node-1", "node-2", "node-3"], {
    replicas: 2,
    cacheCapacity: 3,
  });

  console.log("Loading sample users:");
  for (const [key, value] of records) {
    store.set(key, value);
    console.log(`  ${key} -> ${JSON.stringify(store.get(key))}`);
  }

  store.clearCache();
  store.get("user:101");
  store.get("user:101");
  console.log(`\nCache: ${store.cache.hits} hits, ${store.cache.misses} misses`);

  const joined = store.addNode("node-4");
  console.log(
    `\nNode joins: ${joined.movedPrimaryKeys} of ${joined.totalKeys} primary keys reassigned`,
  );
  const left = store.removeNode("node-4");
  console.log(
    `Node leaves: ${left.movedPrimaryKeys} of ${left.totalKeys} primary keys reassigned`,
  );

  const failedPrimary = store.ring.owner("user:103");
  store.failNode(failedPrimary);
  store.clearCache();
  console.log(`\n${failedPrimary} fails; user:103 still reads ${JSON.stringify(store.get("user:103"))}`);
  store.recoverNode(failedPrimary);

  console.log("\nUsers through the same transparent get() API:");
  for (const [key] of records) console.log(`  ${key} -> ${JSON.stringify(store.get(key))}`);
}

module.exports = {
  ConsistentHashRing,
  DistributedKVStore,
  LRUCache,
  NodeUnavailableError,
};

if (require.main === module) runDemo();
