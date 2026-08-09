# Mini-Splunk — Command Cheat Sheet

Run everything from the project folder:
```bash
cd ~/NSDSYST/NSDSYST_mini-splunk
```

---

## 0. Erase everything and start from absolute zero

Only do this when you want a genuinely clean cluster (wipes all Mongo data + RabbitMQ data).

```bash
sudo docker compose down -v
```

---

## 1. Build and bring the whole stack up

```bash
sudo docker compose up -d --build --scale worker=3
```
`--scale worker=N` sets how many worker replicas start — change `3` to whatever count you want.

---

## 2. One-time cluster initialization (only needed after `down -v`)

Watch stage 1 (replica sets) finish first:
```bash
sudo docker compose logs -f mongo-init-rs
```
Wait for `[done] replica sets ready`, then `Ctrl+C`.

Watch stage 2 (shard registration):
```bash
sudo docker compose logs -f mongo-init-shards
```
Wait for `[done] cluster ready`, then `Ctrl+C`.

> If you did **not** run `down -v`, these two containers just re-check state and exit — safe to skip watching them.

---

## 3. Confirm everything is healthy

```bash
sudo docker compose ps
```
Everything should show `Up` (Mongo/RabbitMQ services also show `healthy`).

Check shard status directly if you want to confirm sharding specifically:
```bash
sudo docker compose exec mongos mongosh --quiet --eval "sh.status()"
```

---

## 4. Watching logs

**All workers combined, live:**
```bash
sudo docker compose logs -f worker
```

**One specific worker/gateway, live (get exact names first):**
```bash
sudo docker compose ps worker
sudo docker logs -f nsdsyst_mini-splunk-worker-1
sudo docker logs -f nsdsyst_mini-splunk-worker-2
sudo docker logs -f nsdsyst_mini-splunk-gateway-1
```

**Only new lines from right now (skip old scrollback):**
```bash
sudo docker logs -f --since 0s nsdsyst_mini-splunk-gateway-1
```

**Last 20 lines only, no live stream:**
```bash
sudo docker compose logs --tail 20 worker
```

**A killed/stopped container's log history (works even if it's dead, as long as it hasn't been removed):**
```bash
sudo docker logs nsdsyst_mini-splunk-worker-1
```

---

## 5. Running the client

```bash
sudo docker compose --profile client run --rm client
```

Inside the `client>` prompt:
```
INGEST sample-logs/SVR1_server_auth_syslog.txt gateway:8080
QUERY gateway:8080 SEARCH_HOST SYSSVR1
QUERY gateway:8080 SEARCH_SEVERITY ERROR
QUERY gateway:8080 SEARCH_DAEMON sshd
QUERY gateway:8080 SEARCH_KEYWORD password
QUERY gateway:8080 SEARCH_DATE Feb 7
QUERY gateway:8080 COUNT_KEYWORD Invalid
PURGE gateway:8080
```

---

## 6. Checking MongoDB directly

**Count all stored logs:**
```bash
sudo docker compose exec mongos mongosh --quiet --eval "db.getSiblingDB('mini_splunk').logs.countDocuments()"
```

**Clear all stored logs (without going through PURGE):**
```bash
sudo docker compose exec mongos mongosh --quiet --eval "db.getSiblingDB('mini_splunk').logs.deleteMany({})"
```

---

## 7. Checking RabbitMQ directly

**Queue depth + consumer counts:**
```bash
sudo docker compose exec rabbitmq rabbitmqctl list_queues name messages consumers
```

**Which workers are actually attached right now:**
```bash
sudo docker compose exec rabbitmq rabbitmqctl list_consumers
```

**RabbitMQ dashboard in a browser** (from your own PC, via the Proxmox external port for your VM):
```
http://103.231.240.136:<external port mapped to 15672>
```
Login: `rabbituser` / `rabbit1234`

---

## 8. Scaling workers up/down (live, no rebuild needed)

```bash
sudo docker compose up -d --scale worker=5
sudo docker compose up -d --scale worker=1
```

Clean up any leftover containers that no longer match the compose file (e.g. after restructuring services):
```bash
sudo docker compose up -d --remove-orphans
```

---

## 9. Restarting services

```bash
sudo docker compose restart gateway worker
```

---

## 10. Chaos test — kill a worker mid-ingest

```bash
sudo docker kill nsdsyst_mini-splunk-worker-1
```
Then check recovery:
```bash
sudo docker compose exec rabbitmq rabbitmqctl list_consumers
sudo docker compose exec rabbitmq rabbitmqctl list_queues name messages consumers
```
Restore the worker count:
```bash
sudo docker compose up -d --scale worker=3
```
Confirm final data integrity:
```bash
sudo docker compose exec mongos mongosh --quiet --eval "db.getSiblingDB('mini_splunk').logs.countDocuments()"
```

---

## 11. tmux quick reference (for watching multiple logs at once)

```bash
tmux new -s minisplunk-test        # start a new session
tmux attach -t minisplunk-test     # reattach to an existing one
tmux ls                            # list sessions
tmux kill-session -t minisplunk-test
```

Inside tmux:
- `Ctrl+B` then `"` — split pane horizontally
- `Ctrl+B` then `%` — split pane vertically
- `Ctrl+B` then arrow key — move between panes
- `Ctrl+B` then `D` — detach (leaves it running in background)

---

## 12. Full shutdown (keep data for next time)

```bash
sudo docker compose down
```
(no `-v` — this keeps your Mongo/RabbitMQ data intact for next session)
