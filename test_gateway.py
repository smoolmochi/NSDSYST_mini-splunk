import pika

credentials = pika.PlainCredentials('rabbituser', 'rabbit1234')
connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost', 5672, '/', credentials))
channel = connection.channel()
channel.queue_declare(queue='ingest_queue')

with open('SVR1_server_auth_syslog.txt') as f:
    for i, line in enumerate(f):
        if i >= 10:   # just the first 10 lines for now
            break
        channel.basic_publish(exchange='', routing_key='ingest_queue', body=line.strip())

print(">> Sent 10 real log lines")
connection.close()