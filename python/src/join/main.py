import os
import logging
import signal
from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )        
        self.aggregator_acked = {}

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        fruit_top, client_id = message_protocol.internal.deserialize(message)
        if client_id not in self.aggregator_acked:
            self.aggregator_acked[client_id] = 1
            self.output_queue.send(
                        message_protocol.internal.serialize([
                            fruit_top,
                            client_id
                        ])
                    )                
        ack()

    def handle_sigterm(self):
        self.input_queue.stop_consuming()
        self.input_queue.close()

    def start(self):
        signal.signal(
                    signal.SIGTERM,
                    lambda signum, frame: self.handle_sigterm(),
                )
        self.input_queue.start_consuming(self.process_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
