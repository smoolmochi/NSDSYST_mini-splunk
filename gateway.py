import socket
import threading
import pika
import pymongo
import re
import shlex
import time
import json

HOST = "0.0.0.0"
PORT = 8080
SIZE = 1024
FORMAT = "utf-8"

RABBITMQ_HOST = "127.0.0.1"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "rabbituser"
RABBITMQ_PASSWORD = "rabbit1234"

INGEST_QUEUE = "ingest_queue"
PURGE_EXCHANGE = "purge_control"
PURGE_ACK_QUEUE = "purge_ack_queue"
EXPECTED_WORKERS = 1
PURGE_TIMEOUT_SECONDS = 10

MONGO_URI = "mongodb://127.0.0.1:27017"
MONGO_DATABASE = "mini_splunk"
MONGO_COLLECTION = "logs"

mongo_client = pymongo.MongoClient(MONGO_URI)
logs_collection = mongo_client[MONGO_DATABASE][MONGO_COLLECTION]

def send_with_length(conn, message):
    encoded_message = message.encode(FORMAT)
    header = f"{len(encoded_message):<16}".encode(FORMAT)
    conn.sendall(header + encoded_message)

def connect_to_rabbitmq():
    credentials = pika.PlainCredentials(
        RABBITMQ_USER,
        RABBITMQ_PASSWORD
    )

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            virtual_host="/",
            credentials=credentials
        )
    )

    channel = connection.channel()

    # temp
    channel.queue_declare(queue=INGEST_QUEUE)

    return connection, channel

def perform_purge():
    rabbit_connection = None
    channel = None
    lock_sent = False
    unlock_sent = False

    try:
        rabbit_connection, channel = connect_to_rabbitmq()

        channel.exchange_declare(exchange=PURGE_EXCHANGE, exchange_type="fanout")
        channel.queue_declare(queue=PURGE_ACK_QUEUE)
        channel.queue_purge(queue=PURGE_ACK_QUEUE) # remove acks from previous
        channel.basic_publish(exchange=PURGE_EXCHANGE, routing_key="", body="LOCK")
        lock_sent = True

        print("[PURGE] Lock sent. Waiting for workers...")

        locked_workers = set()
        deadline = time.time() + PURGE_TIMEOUT_SECONDS

        while (len(locked_workers) < EXPECTED_WORKERS and time.time() < deadline):
            method, properties, body = channel.basic_get(queue=PURGE_ACK_QUEUE, auto_ack=False)

            if method is None:
                time.sleep(0.1)
                continue

            acknowledgement = json.loads(body.decode(FORMAT))

            if acknowledgement.get("status") == "LOCKED":
                worker_id = acknowledgement.get("worker_id")

                if worker_id:
                    locked_workers.add(worker_id)
                    print(f"[PURGE] {worker_id} is locked.")

            channel.basic_ack(delivery_tag=method.delivery_tag)

        if len(locked_workers) < EXPECTED_WORKERS:
            raise TimeoutError("Not all workers confirmed LOCK.")

        # delete messages that have not reached a worker
        queue_result = channel.queue_purge(queue=INGEST_QUEUE)
        queued_deleted = queue_result.method.message_count

        # delete parse logs from MongoDB
        database_result = logs_collection.delete_many({})
        stored_deleted = database_result.deleted_count

        # allow workers to consume again
        channel.basic_publish(exchange=PURGE_EXCHANGE, routing_key="", body="UNLOCK")
        unlock_sent = True

        return queued_deleted, stored_deleted

    finally:
        if (channel is not None and rabbit_connection and rabbit_connection.is_open and lock_sent and not unlock_sent):
            try:
                channel.basic_publish(exchange=PURGE_EXCHANGE, routing_key="", body="UNLOCK")
            except pika.exceptions.AMQPError as error:
                print(f"[PURGE ERROR] Could not send emergency UNLOCK: {error}")

        if rabbit_connection and rabbit_connection.is_open:
            rabbit_connection.close()

def search_logs(field, search_term, exact_match=True):
    escaped_term = re.escape(search_term)

    if exact_match:
        pattern = f"^{escaped_term}$"
    else:
        pattern = f"^{escaped_term}"

    results = logs_collection.find({
        field: {
            "$regex": pattern,
            "$options": "i"
        }
    })

    matches = []

    for index, log in enumerate(results, start=1):
        line = (
            f"{index}. {log['timestamp']} "
            f"{log['host']} "
            f"{log['daemon']}: "
            f"{log['message']}"
        )
        matches.append(line)

    return matches

def search_logs_by_host(hostname):
    return search_logs(
        field="host",
        search_term=hostname,
        exact_match=True
    )

def search_logs_by_daemon(daemon_name):
    return search_logs(
        field="daemon",
        search_term=daemon_name,
        exact_match=False
    )

def search_logs_by_severity(severity):
    return search_logs(
        field="severity",
        search_term=severity,
        exact_match=True
    )

def search_logs_by_date(date_term):
    date_parts = date_term.split()
    pattern = "^" + r"\s+".join(re.escape(part) for part in date_parts)

    results = logs_collection.find({
        "timestamp": {
            "$regex": pattern,
            "$options": "i"
        }
    })

    matches = []

    for index, log in enumerate(results, start=1):
        line = (
            f"{index}. {log['timestamp']} "
            f"{log['host']} "
            f"{log['daemon']} "
            f"{log['message']}"
        )
        matches.append(line)

    return matches

def search_logs_by_keyword(keyword):
    pattern = rf"\b{re.escape(keyword)}\b"

    results = logs_collection.find({
        "message": {
            "$regex": pattern,
            "$options": "i"
        }
    })

    matches =[]

    for index, log in enumerate(results, start=1):
        line = (
            f"{index}. {log['timestamp']} "
            f"{log['host']} "
            f"{log['daemon']}: "
            f"{log['message']}"
        )
        matches.append(line)

    return matches

def count_keyword(keyword):
    pattern = rf"\b{re.escape(keyword)}\b"

    pipeline = [
        {
            "$project": {
                "occurrences": {
                    "$size": {
                        "$regexFindAll": {
                            "input": "$message",
                            "regex": pattern,
                            "options": "i"
                        }
                    }
                }
            }
        },
        {
            "$group": {
                "_id": None,
                "total": {"$sum": "$occurrences"}
            }
        }
    ]

    result = list(logs_collection.aggregate(pipeline))

    if not result:
        return 0

    return result[0]["total"]

QUERY_HANDLERS = {
    "SEARCH_DATE": search_logs_by_date,
    "SEARCH_HOST": search_logs_by_host,
    "SEARCH_DAEMON": search_logs_by_daemon,
    "SEARCH_SEVERITY": search_logs_by_severity,
    "SEARCH_KEYWORD": search_logs_by_keyword
}

def handle_client(conn, addr):
    rabbit_connection = None

    try:
        command = conn.recv(SIZE).decode(FORMAT).strip()
        print(f"[RECEIVED] {addr}: {command}")

        parts = shlex.split(command)

        if not parts:
            send_with_length(conn, "[ERROR] Empty command.")
            return

        action = parts[0].upper()

        if action == "QUERY":
            if len(parts) < 4:
                send_with_length(conn, "[ERROR] Invalid QUERY command.")
                return

            search_type = parts[2].upper()
            search_term = " ".join(parts[3:])

            if search_type == "COUNT_KEYWORD":
                total = count_keyword(search_term)

                response = (
                    f"[SUCCESS] The keyword '{search_term}' "
                    f"appears {total} time(s)."
                )

                send_with_length(conn, response)
                return

            search_handler = QUERY_HANDLERS.get(search_type)

            if search_handler is None:
                send_with_length(
                    conn,
                    f"[ERROR] Query type '{search_type}' is not available yet."
                )
                return

            matches = search_handler(search_term)

            if matches:
                response = (
                    f"[SUCCESS] Found {len(matches)} matching entries "
                    f"for {search_type} '{search_term}':\n"
                    + "\n".join(matches)
                )
            else:
                response = (
                    f"[SUCCESS] Found 0 matching entries "
                    f"for {search_type} '{search_term}'."
                )

            send_with_length(conn, response)
            return

        if action == "PURGE":
            if len(parts) != 2:
                send_with_length(conn, "[ERROR] Invalid PURGE command.")
                return

            try:
                queued_deleted, stored_deleted = perform_purge()

                response = (
                    f"[SUCCESS] Purge completed. "
                    f"{queued_deleted} queued messages and "
                    f"{stored_deleted} stored logs were deleted."
                )

            except TimeoutError as error:
                response = f"[ERROR] Purge failed: {error}"

            except Exception as error:
                print(f"[PURGE ERROR] {error}")
                response = "[ERROR] Purge failed because of an internal error."

            send_with_length(conn, response)
            return
        
        if action != "INGEST":
            send_with_length(conn, f"[ERROR] Unknown command '{action}'.")
            return

        rabbit_connection, channel = connect_to_rabbitmq()
        channel.confirm_delivery()

        conn.sendall("READY_FOR_FILE".encode(FORMAT))

        buffer = ""
        queued_count = 0

        while True:
            chunk = conn.recv(SIZE).decode(FORMAT)

            if not chunk: break

            buffer += chunk

            if "<EOF>" in buffer:
                buffer = buffer.replace("<EOF>", "")
                upload_finished = True
            else:
                upload_finished = False

            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                line = line.strip()

                if line:
                    channel.basic_publish(
                        exchange="",
                        routing_key=INGEST_QUEUE,
                        body=line
                    )
                    queued_count += 1

            if upload_finished: break

        if buffer.strip():
            channel.basic_publish(
                exchange="",
                routing_key=INGEST_QUEUE,
                body=buffer.strip()
            )
            queued_count += 1

        response = (
            f"[SUCCESS] File received. "
            f"{queued_count:,} log lines sent to RabbitMQ."
        )

        send_with_length(conn, response)
        print(response)

    except pika.exceptions.AMQPError as error:
        print(f"[RABBITMQ ERROR] {error}")

        send_with_length(
            conn,
            "[ERROR] Could not send the logs to RabbitMQ"
        )
    except Exception as error:
        print(f"[ERROR] {addr}: {error}")
    finally:
        if rabbit_connection and rabbit_connection.is_open:
            rabbit_connection.close()
        conn.close()

def start_gateway():
    gateway = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    gateway.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    gateway.bind((HOST, PORT))
    gateway.listen()

    print(f"[STARTING] Gateway running on port {PORT}")

    try:
        while True:
            conn, addr = gateway.accept()

            thread = threading.Thread(
                target=handle_client,
                args=(conn, addr)
            )
            thread.start()

    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Gateway stopped.")
    finally:
        gateway.close()

if __name__ == "__main__":
    start_gateway()