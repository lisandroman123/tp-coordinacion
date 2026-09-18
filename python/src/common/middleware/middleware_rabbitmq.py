import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareMessageError, MessageMiddlewareDisconnectedError, MessageMiddlewareCloseError, MessageMiddlewareDeleteError


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        self.queue = queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()                        
        self.channel.queue_declare(queue=queue_name)

    def send(self, message):
        try:
            self.channel.basic_publish(exchange='',
                                  routing_key=self.queue,
                                  body=message)        
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError() from e
        
    def start_consuming(self, on_message_callback):
        def callback(channel, method, properties, body):        
            def ack():
                channel.basic_ack(delivery_tag=method.delivery_tag)
        
            def nack():
                channel.basic_nack(delivery_tag=method.delivery_tag)
        
            on_message_callback(body, ack, nack)
                    
        try:
            self.channel.basic_consume(queue=self.queue, on_message_callback=callback)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e        

    def close(self):
        try:
            self.connection.close()
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareCloseError() from e

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        
class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.exchange=exchange_name
        self.binding_keys=routing_keys
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='topic')
        self.result = self.channel.queue_declare(queue='', exclusive=True)
        for binding_key in routing_keys:                        
            self.channel.queue_bind(exchange=exchange_name, queue=self.result.method.queue, routing_key=binding_key)             

    def send(self, message):
        try:
            for binding_key in self.binding_keys:
                self.channel.basic_publish(exchange=self.exchange, routing_key=binding_key, body=message)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError() from e

    def start_consuming(self, on_message_callback):
        def callback(channel, method, properties, body):

            def ack():
                channel.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                channel.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(body,ack,nack)                    
        try:
            self.channel.basic_consume(queue=self.result.method.queue, on_message_callback=callback)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except Exception as e:
            raise MessageMiddlewareMessageError() from e
        
    def close(self):
        try:
            self.connection.close()
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareCloseError() from e

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e


