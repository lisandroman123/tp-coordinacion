import os
import logging
import bisect
import signal 

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])
AMOUNT_OF_FIELDS = 3

class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top = {}    
        self.sigterm_received = False    
        

    def _process_data(self, fruit, amount, client_id):
        logging.info("Processing data message")        

        if client_id not in self.fruit_top:
            self.fruit_top[client_id] = []

        client_top = self.fruit_top[client_id]

        bisect.insort(client_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self,client_id):
        logging.info(f"Process eof aggregation client_id: {client_id}")                        

        fruit_chunk = list(self.fruit_top[client_id][-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        self.output_queue.send(message_protocol.internal.serialize([fruit_top,client_id]))
        print(f"enviado el fruit top {fruit_top} con el client id {client_id}")
        del self.fruit_top[client_id] 
    
    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == AMOUNT_OF_FIELDS:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def handle_sigterm(self):
        self.sigterm_received = True
        self.input_exchange.stop_consuming()        

    def start(self):
        signal.signal(
            signal.SIGTERM,
            lambda signum, frame: self.handle_sigterm(),
        )
        try:
            self.input_exchange.start_consuming(self.process_messsage)
        except Exception:
            if self.sigterm_received:
                return
            raise


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
