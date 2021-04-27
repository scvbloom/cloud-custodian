#!/usr/bin/env python3
import pika, sys, os, json
from tools.c7n_org.c7n_org.override import overrun
# from c7n_org.override import overrun
# from async_publisher import CustodianPublisher
import time
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.getenv("ENV_PATH"))

exchange = os.getenv("RABBITMQ_EXCHANGE")
routing_key = os.getenv("RABBITMQ_ROUTING_KEY")
virtual_host = os.getenv("RABBITMQ_VHOST")
host = os.getenv("RABBITMQ_HOST")

creds = pika.ConnectionParameters(
    heartbeat=1800,
    blocked_connection_timeout=300,
    credentials=pika.PlainCredentials(
        os.getenv("RABBITMQ_USERNAME"),
        os.getenv("RABBITMQ_PASSWORD")),
    host=host, virtual_host=virtual_host)


def aws_callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    print(" [x] Properties %r" % properties)
    print(" [x] Channel %r" % ch)
    print(" [x] Methods %r" % method)
    (_, counts), config = overrun(body, "", cloud="aws")
    connection = pika.BlockingConnection(creds)
    channel = connection.channel()
    for p in counts:
        msg = {
            "violationCount": counts[p],
            "cloudProvider": "aws",
            "policyName": p,
            "scanTime": time.time(),
            "eventId": config["event_id"],
            "resource": config["source"],
            "accountId": config["account_id"],
        }
        channel.basic_publish(
            body=json.dumps(msg, ensure_ascii=False),
            exchange=exchange, routing_key=routing_key)


def gcp_callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    print(" [x] Properties %r" % properties)
    print(" [x] Channel %r" % ch)
    print(" [x] Methods %r" % method)
    (_, counts), config = overrun(body, "", cloud="gcp")
    connection = pika.BlockingConnection(creds)
    channel = connection.channel()
    for p in counts:
        msg = {
            "violationCount": counts[p],
            "cloudProvider": "gcp",
            "policyName": p,
            "scanTime": time.time(),
            "eventId": config["event_id"],
            "resource": config["source"],
            "accountId": config["account_id"],
        }
        channel.basic_publish(
            body=json.dumps(msg, ensure_ascii=False),
            exchange=exchange, routing_key=routing_key)


def azure_callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    print(" [x] Properties %r" % properties)
    print(" [x] Channel %r" % ch)
    print(" [x] Methods %r" % method)
    (_, counts), config = overrun(body, "", cloud="azure")
    connection = pika.BlockingConnection(creds)
    channel = connection.channel()
    for p in counts:
        msg = {
            "violationCount": counts[p],
            "cloudProvider": "azure",
            "policyName": p,
            "scanTime": time.time(),
            "eventId": config["event_id"],
            "resource": config["source"],
            "accountId": config["account_id"],
        }
        channel.basic_publish(
            body=json.dumps(msg, ensure_ascii=False),
            exchange=exchange, routing_key=routing_key)


def main():
    connection = pika.BlockingConnection(creds)
    channel = connection.channel()

    channel.basic_consume(queue=os.getenv("RABBITMQ_AWS_QUEUE"), on_message_callback=aws_callback,
                          auto_ack=True)
    channel.basic_consume(queue=os.getenv("RABBITMQ_GCP_QUEUE"), on_message_callback=gcp_callback,
                          auto_ack=True)
    channel.basic_consume(queue=os.getenv("RABBITMQ_AZURE_QUEUE"), on_message_callback=azure_callback,
                          auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
