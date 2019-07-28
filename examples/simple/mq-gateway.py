#!/usr/bin/python

import pika
from flask import Flask, request
import _thread
import queue
import uuid

app = Flask(__name__)

class Client:

    def __init__(self):
        credentials = pika.PlainCredentials('xxx', 'xxx')
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='xxx', credentials=credentials))

        self.channel = self.connection.channel()
        self.response = None

        result = self.channel.queue_declare(queue="POST_/v1/echo_out", exclusive=False,  durable=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        self.response = body

    def call(self, body):
        self.response = None
        self.channel.basic_publish(
            exchange='',
            routing_key='POST_/v1/echo',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
            ),
            body=body)
        while self.response is None:
            self.connection.process_data_events()
        self.connection.close()
        return self.response


@app.route('/v1/echo', methods=['POST'])
def echo():
    c = Client()
    return c.call(request.data)


if __name__ == '__main__':
    credentials = pika.PlainCredentials('xxx', 'xxx')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='xxx', credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue='POST_/v1/echo', durable=True)

    print("start app")
    app.run(debug=True)




