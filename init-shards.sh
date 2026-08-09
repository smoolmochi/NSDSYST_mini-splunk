#!/bin/bash
# Stage 2: register both shards with mongos and shard the logs collection.
# Runs AFTER mongos is up and healthy (which is only possible once
# init-replicasets.sh has already given csrs a primary). Idempotent.

MONGOSH="mongosh --quiet"

wait_for_mongo() {
  local host="$1"
  echo "[wait] $host"
  until $MONGOSH --host "$host" --eval "db.adminCommand('ping')" >/dev/null 2>&1; do
    sleep 2
  done
  echo "[up] $host"
}

wait_for_mongo mongos:27017

echo "[init] setting cluster-wide default write concern (required because each shard has an arbiter)"
$MONGOSH --host mongos:27017 --eval '
db.adminCommand({ setDefaultRWConcern: 1, defaultWriteConcern: { w: 1 } });
'

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

echo "[verify] current shard status"
$MONGOSH --host mongos:27017 --eval 'sh.status()'

echo "[done] cluster ready"
