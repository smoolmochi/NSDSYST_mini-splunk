import pika
import json
import re

SYSLOG_PATTERN = re.compile(
    r"^(?P<timestamp>[A-Za-z]{3}\s+\d+\s+\d{2}:\d{2}:\d{2})\s+" # Feb 22 00:05:38
    r"(?P<host>\S+)\s+"                                        # SYSSVR1
    r"(?P<daemon>[^:]+):\s+"                                   # systemd[1]
    r"(?P<message>.*)$"                                        # rest of msg
)

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

def save_to_mongo(parsed_log):
    # TODO: replace with real MongoDB insert once the cluster exists
    print(f"[WOULD SAVE] {parsed_log}")

def callback(ch, method, properties, body):
    line = body.decode("utf-8")
    parsed = parse_syslog_line(line)

    if parsed:
        save_to_mongo(parsed)

    # only ack AFTER the save — this is what makes it survive a crash mid-job
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    credentials = pika.PlainCredentials('rabbituser', 'rabbit1234')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('<gateway_or_rabbitmq_ip>', 5672, '/', credentials))
    channel = connection.channel()
    channel.queue_declare(queue='ingest_queue')

    # process one message at a time per worker, so work spreads across workers
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='ingest_queue', on_message_callback=callback, auto_ack=False)

    print(" [*] Worker ready, waiting for log lines...")
    channel.start_consuming()

if __name__ == '__main__':
    main()