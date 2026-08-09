#!/bin/bash
# Stage 1: initiate the config server + shard replica sets ONLY.
# Must run and complete BEFORE mongos starts, since mongos cannot become
# healthy until csrs has an elected primary. Idempotent (checks rs.status()
# before mutating) so it's safe to re-run.

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

echo "[done] replica sets ready"
