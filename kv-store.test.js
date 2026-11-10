"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const { DistributedKVStore, NodeUnavailableError } = require("./kv-store");

function makeStore() {
  const store = new DistributedKVStore(["node-1", "node-2", "node-3"], {
    replicas: 2,
    virtualNodes: 100,
    cacheCapacity: 2,
  });
  for (let index = 0; index < 100; index += 1) {
    store.set(`key-${index}`, `value-${index}`);
  }
  return store;
}

test("node join and leave preserve all values and move only affected primaries", () => {
  const store = makeStore();
  const before = new Map(
    [...store.collectLatestValues().keys()].map((key) => [key, store.ring.owner(key)]),
  );

  const joined = store.addNode("node-4");
  const expectedMoves = [...before].filter(
    ([key, owner]) => owner !== store.ring.owner(key),
  ).length;
  assert.equal(joined.movedPrimaryKeys, expectedMoves);
  assert.ok(joined.movedPrimaryKeys < joined.totalKeys);
  assert.ok(joined.movedPrimaryKeys > 0);

  store.removeNode("node-4");
  for (let index = 0; index < 100; index += 1) {
    assert.equal(store.get(`key-${index}`), `value-${index}`);
  }
});

test("LRU cache evicts the least recently used key", () => {
  const store = new DistributedKVStore(["node-1"], { cacheCapacity: 2 });
  store.set("a", 1);
  store.set("b", 2);
  store.get("a");
  store.set("c", 3);

  store.clearCache();
  store.get("b");
  assert.equal(store.cache.misses, 1);
  store.get("a");
  assert.equal(store.cache.hits, 1);
});

test("replica reads and writes remain available during a node failure", () => {
  const store = new DistributedKVStore(["node-1", "node-2", "node-3"], {
    replicas: 2,
  });
  store.set("user:101", { name: "Alice" });
  const primary = store.ring.owner("user:101");

  store.failNode(primary);
  store.clearCache();
  assert.deepEqual(store.get("user:101"), { name: "Alice" });
  store.set("user:101", { name: "Alicia" });
  store.recoverNode(primary);
  assert.deepEqual(store.get("user:101"), { name: "Alicia" });
});

test("reads report limited availability when every replica is down", () => {
  const store = new DistributedKVStore(["node-1", "node-2"], { replicas: 2 });
  store.set("key", "value");
  for (const name of store.nodes.keys()) store.failNode(name);
  store.clearCache();

  assert.throws(() => store.get("key"), NodeUnavailableError);
});

test("membership changes are blocked while a node is down", () => {
  const store = makeStore();
  store.failNode("node-1");
  assert.throws(() => store.addNode("node-4"), NodeUnavailableError);
});
