import pika

credentials = pika.PlainCredentials('rabbituser', 'rabbit1234')
connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost', 5672, '/', credentials))
channel = connection.channel()
channel.queue_declare(queue='ingest_queue')
channel.confirm_delivery()

sent = 0
failed = 0
with open('SVR1_server_auth_syslog.txt') as f:
    for i, line in enumerate(f):
        if i >= 25000:
            break
        try:
            channel.basic_publish(exchange='', routing_key='ingest_queue', body=line.strip())
            sent += 1
        except pika.exceptions.UnroutableError:
            failed += 1

print(f">> Confirmed sent: {sent}, failed: {failed}")
connection.close()

"""
count = 0
with open('SVR1_server_auth_syslog.txt') as f:
    for line in f:
        line = line.strip()
        if line:
            try:
            channel.basic_publish(exchange='', routing_key='ingest_queue', body=line.strip())
            sent += 1
        except pika.exceptions.UnroutableError:
            failed += 1

print(f">> Confirmed sent: {sent}, failed: {failed}")
connection.close()

"""