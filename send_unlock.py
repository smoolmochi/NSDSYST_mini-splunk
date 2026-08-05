import pika

credentials = pika.PlainCredentials('rabbituser', 'rabbit1234')
connection = pika.BlockingConnection(
    pika.ConnectionParameters('127.0.0.1', 5672, '/', credentials))
    # pika.ConnectionParameters("RABBITMQ_VM_IP", 5672, "/", credentials)
channel = connection.channel()
channel.exchange_declare(exchange='purge_control', exchange_type='fanout')

channel.basic_publish(exchange='purge_control', routing_key='', body='UNLOCK')
print(">> Sent UNLOCK")

connection.close()
