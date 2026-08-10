import pika
import json
import re
import pymongo
from pymongo.errors import DuplicateKeyError
import hashlib
import os
import socket

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "127.0.0.1")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")

mongo_client = pymongo.MongoClient(MONGO_URI)
db = mongo_client["mini_splunk"]
logs_collection = db["logs"]

SYSLOG_PATTERN = re.compile(
    r"^(?P<timestamp>[A-Za-z]{3}\s+\d+\s+\d{2}:\d{2}:\d{2})\s+" # Feb 22 00:05:38
    r"(?P<host>\S+)\s+"                                        # SYSSVR1
    r"(?P<daemon>[^:]+):\s+"                                   # systemd[1]
    r"(?P<message>.*)$"                                        # rest of msg
)

PURGE_EXCHANGE = "purge_control"  #change to whatever
PURGE_ACK_QUEUE = "purge_ack_queue"

WORKER_ID = os.getenv("WORKER_ID", f"worker-{socket.gethostname()}")

INGEST_CONSUMER_TAG = "ingest-consumer"

paused = False
processed_count = 0

def send_control_ack(channel, status):
    response = {
        "worker_id": WORKER_ID,
        "status": status
    }

    channel.basic_publish(
        exchange="",
        routing_key=PURGE_ACK_QUEUE,
        body=json.dumps(response)
    )

    print(f" [*] Sent {status} confirmation as {WORKER_ID}")

def parse_syslog_line(line):
    is_match = SYSLOG_PATTERN.match(line)
    if not is_match:
        return None

    data = is_match.groupdict()

    msg_upper = data['message'].upper()
    if "ERROR" in msg_upper or "FAIL" in msg_upper or "FATAL" in msg_upper:
        severity = "ERROR"
    elif "WARN" in msg_upper:
        severity = "WARNING"
    else:
        severity = "INFO"

    return {
        "timestamp": data['timestamp'],
        "host": data['host'],
        "daemon": data['daemon'],
        "severity": severity,
        "message": data['message']
    }

def save_to_mongo(parsed_log, ingestion_id, line_number, raw_line):
    unique_value = (f"{ingestion_id}:{line_number}:{raw_line}")
    parsed_log["_id"] = hashlib.sha256(unique_value.encode()).hexdigest()

    parsed_log["ingestion_id"] = ingestion_id
    parsed_log["line_number"] = line_number
    parsed_log["raw_line"] = raw_line

    try:
        logs_collection.insert_one(parsed_log)
        return True
    except DuplicateKeyError:
        return True
    except Exception as e:
        print(f"[MONGO ERROR] {e}")
        return False

def callback(ch, method, properties, body):
    global paused, processed_count
    if paused:
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    payload = json.loads(body.decode("utf-8"))

    ingestion_id = payload["ingestion_id"]
    line_number = payload["line_number"]
    raw_line = payload["raw_line"]

    parsed = parse_syslog_line(raw_line)

    if parsed:
        saved = save_to_mongo(parsed, ingestion_id, line_number, raw_line)
        if not saved:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return
        processed_count += 1
        if processed_count <= 50:
             print(f"[{WORKER_ID}] processing: {parsed['host']} - {parsed['message'][:50]}")
        elif processed_count % 1000 == 0:
            print(f"[{WORKER_ID}] ... {processed_count} lines processed so far ...")
    # only ack AFTER the save — this is what makes it survive a crash mid-job
    ch.basic_ack(delivery_tag=method.delivery_tag)

def control_callback(ch, method, properties, body):
    global paused

    command = body.decode("utf-8").strip().upper()

    if command == "LOCK" and not paused:
        paused = True
        ch.basic_cancel(consumer_tag=INGEST_CONSUMER_TAG)
        print(" [!] PURGE in progress — ingest consumer stopped")
        send_control_ack(ch, "LOCKED")

    elif command == "UNLOCK" and paused:
        paused = False
        ch.basic_consume(queue="ingest_queue", on_message_callback=callback, auto_ack=False, consumer_tag=INGEST_CONSUMER_TAG)
        print(" [*] PURGE finished — ingest consumer resumed")
        send_control_ack(ch, "UNLOCKED")

    else:
        print(f" [?] Unrecognized control message: {command}")

def main():
    connection = None

    try:
        credentials = pika.PlainCredentials('rabbituser', 'rabbit1234')
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(RABBITMQ_HOST, RABBITMQ_PORT, '/', credentials))
        channel = connection.channel()
        channel.queue_declare(queue='ingest_queue', durable=True)
        channel.queue_declare(queue=PURGE_ACK_QUEUE)

        # process one message at a time per worker, so work spreads across workers
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue='ingest_queue', on_message_callback=callback, auto_ack=False, consumer_tag=INGEST_CONSUMER_TAG)
        channel.exchange_declare(exchange=PURGE_EXCHANGE, exchange_type='fanout')
        # Exclusive, auto-named queue: each worker gets its own copy of every broadcast.
        result = channel.queue_declare(queue='', exclusive=True)
        control_queue_name = result.method.queue
        channel.queue_bind(exchange=PURGE_EXCHANGE, queue=control_queue_name)
        channel.basic_consume(queue=control_queue_name, on_message_callback=control_callback, auto_ack=True)

        print(f" [*] Worker ready as {WORKER_ID}, waiting for log lines...")
        channel.start_consuming()

    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Worker interrupted by user.")

    except pika.exceptions.AMQPError as error:
        print(f"\n[RABBITMQ ERROR] {error}")

    finally:
        if connection and connection.is_open:
            connection.close()

        mongo_client.close()
        print("[SHUTDOWN] Worker stopped cleanly.")

if __name__ == '__main__':
    main()
