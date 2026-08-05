#!/bin/bash
# One-shot cluster bootstrap: initiates the config server + shard replica sets,
# then registers both shards with mongos and shards the logs collection.
# Idempotent by design (checks current state before mutating) so it's safe to
# re-run if the mongo-init container itself gets restarted against a cluster
# that was already initialized (e.g. after `docker compose down` without `-v`).

MONGOSH="mongosh --quiet"

wait_for_mongo() {
  local host="$1"
  echo "[wait] $host"
  until $MONGOSH --host "$host" --eval "db.adminCommand('ping')" >/dev/null 2>&1; do
    sleep 2
  done
  echo "[up] $host"
}

wait_for_primary() {
  local host="$1"
  echo "[wait-primary] $host"
  until $MONGOSH --host "$host" --eval "db.hello().isWritablePrimary" 2>/dev/null | grep -q "true"; do
    sleep 2
  done
  echo "[primary-elected] $host"
}

for host in configsvr1:27019 configsvr2:27019 configsvr3:27019 \
            shard1a:27018 shard1b:27018 shard1c:27018 \
            shard2a:27018 shard2b:27018 shard2c:27018; do
  wait_for_mongo "$host"
done

echo "[init] config server replica set (csrs)"
$MONGOSH --host configsvr1:27019 --eval '
try {
  rs.status();
  print("csrs already initiated");
} catch (e) {
  rs.initiate({
    _id: "csrs",
    configsvr: true,
    members: [
      { _id: 0, host: "configsvr1:27019" },
      { _id: 1, host: "configsvr2:27019" },
      { _id: 2, host: "configsvr3:27019" }
    ]
  });
  print("csrs initiated");
}
'

echo "[init] shard1 replica set (shard1rs)"
$MONGOSH --host shard1a:27018 --eval '
try {
  rs.status();
  print("shard1rs already initiated");
} catch (e) {
  rs.initiate({
    _id: "shard1rs",
    members: [
      { _id: 0, host: "shard1a:27018" },
      { _id: 1, host: "shard1b:27018" },
      { _id: 2, host: "shard1c:27018", arbiterOnly: true }
    ]
  });
  print("shard1rs initiated");
}
'

echo "[init] shard2 replica set (shard2rs)"
$MONGOSH --host shard2a:27018 --eval '
try {
  rs.status();
  print("shard2rs already initiated");
} catch (e) {
  rs.initiate({
    _id: "shard2rs",
    members: [
      { _id: 0, host: "shard2a:27018" },
      { _id: 1, host: "shard2b:27018" },
      { _id: 2, host: "shard2c:27018", arbiterOnly: true }
    ]
  });
  print("shard2rs initiated");
}
'

wait_for_primary configsvr1:27019
wait_for_primary shard1a:27018
wait_for_primary shard2a:27018

wait_for_mongo mongos:27017

echo "[init] registering shards + sharding mini_splunk.logs via mongos"
$MONGOSH --host mongos:27017 --eval '
try { sh.addShard("shard1rs/shard1a:27018,shard1b:27018,shard1c:27018"); }
catch (e) { print("addShard shard1rs: " + e); }

try { sh.addShard("shard2rs/shard2a:27018,shard2b:27018,shard2c:27018"); }
catch (e) { print("addShard shard2rs: " + e); }

try { sh.enableSharding("mini_splunk"); }
catch (e) { print("enableSharding: " + e); }

db.getSiblingDB("mini_splunk").logs.createIndex({ _id: "hashed" });

try { sh.shardCollection("mini_splunk.logs", { _id: "hashed" }); }
catch (e) { print("shardCollection: " + e); }
'

echo "[done] cluster ready"
