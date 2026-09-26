import os
import logging
import threading
import signal 
from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
AMOUNT_OF_FIELDS = 3

class SumsAckedMonitor:
    def __init__(self):
        self.sums_acked = {}
        self.lock = threading.Lock()

    def start_client(self, client_id):
        with self.lock:
            self.sums_acked[client_id] = 1

    def sum_client(self, client_id):
        with self.lock:
            self.sums_acked[client_id] += 1

    def get_sum(self, client_id):
        return self.sums_acked[client_id]

class MonitorFruitAmounts:
    def __init__(self):
        self.amount_by_fruit = {}
        self.client_id_processing = -1
        self.condition = threading.Condition()

    def add_fruit_amount_for_client(self,fruit,amount,client_id):        
        with self.condition:
            self.client_id_processing = client_id            
            if client_id not in self.amount_by_fruit:
                self.amount_by_fruit.setdefault(client_id, {})

            self.amount_by_fruit[client_id][fruit] = self.amount_by_fruit[client_id].get(
                fruit,            
                fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))
            self.client_id_processing = -1
            self.condition.notify_all()

    def get_fruits_amount_for_client(self,client_id):
        with self.condition:
            while self.client_id_processing == client_id:
                self.condition.wait()        
            return list(self.amount_by_fruit[client_id].values())

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)                
        self.monitor_sums = SumsAckedMonitor()
        self.monitor_fruit_amounts = MonitorFruitAmounts() 

    def _process_data(self, fruit, amount, client_id):
        self.monitor_fruit_amounts.add_fruit_amount_for_client(fruit,amount,client_id)
    
    def _process_eof(self,client_id):                
        if SUM_AMOUNT > 1:            
            logging.info(f"EOF OF {client_id}")
            for i in range (0,SUM_AMOUNT):
                if i != ID:                            
                    self.output_exchange.send_to(message_protocol.internal.serialize([client_id,ID]),f"{SUM_PREFIX}_{i}")                        
                    self.monitor_sums.start_client(client_id)                 
        else:
            self._broadcast_aggregator_eof(client_id)
    
    def _process_sums_eof(self,client_id,sum_id):
        if sum_id != ID:
            fruits = self.monitor_fruit_amounts.get_fruits_amount_for_client(client_id)            
            for final_fruit_item in fruits:                
                self.data_eof_exchange.send_to(message_protocol.internal.serialize([final_fruit_item.fruit, final_fruit_item.amount, client_id]),f"{SUM_PREFIX}_{sum_id}")
            self.data_eof_exchange.send_to(message_protocol.internal.serialize([client_id,sum_id]),f"{SUM_PREFIX}_{sum_id}") 
            logging.info(f"EOF OF {client_id} FROM {ID} TO {sum_id}")        
        else: 
            self.monitor_sums.sum_client(client_id)
            sums = self.monitor_sums.get_sum(client_id)
            logging.info(f"EOF OF A SUM +1 TOTAL SUM: {sums}")                                           
            if sums == SUM_AMOUNT:
                logging.info("ALL END OF FILES OF OTHER SUMS REACHED")
                self._broadcast_aggregator_eof(client_id)

    def get_aggregator_id(self,client_id):
        return (client_id % AGGREGATION_AMOUNT)

    def _broadcast_aggregator_eof(self,client_id):                
        fruits = self.monitor_fruit_amounts.get_fruits_amount_for_client(client_id)
        aggregator_id = self.get_aggregator_id(client_id)
        for i in range(0,AGGREGATION_AMOUNT):            
            if i == aggregator_id:
                for final_fruit_item in fruits:                        
                    self.data_output_exchanges[i].send_to(message_protocol.internal.serialize([final_fruit_item.fruit, final_fruit_item.amount,client_id]), f"{AGGREGATION_PREFIX}_{aggregator_id}")                    
                logging.info(f"Broadcasting EOF message: {client_id, ID}")            
                self.data_output_exchanges[i].send_to(message_protocol.internal.serialize([client_id]), f"{AGGREGATION_PREFIX}_{aggregator_id}")    

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == AMOUNT_OF_FIELDS:
            self._process_data(*fields)
        elif len(fields) == 2:            
            self._process_sums_eof(*fields)
        elif len(fields) == 1:
            self._process_eof(*fields)            
        ack()

    def _start_handler_consuming(self):
        self.output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST,SUM_PREFIX,[f"{SUM_PREFIX}_{ID}"])
        self.input_queue.start_consuming(self.process_data_messsage)

    def _run(self):
        if SUM_AMOUNT>1:
            self.data_eof_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, SUM_PREFIX, [f"{SUM_PREFIX}_{ID}"]
            )            
            logging.info(f"EXCHANGE CREATED: binding_key {SUM_PREFIX}_{ID}")

            self.data_eof_exchange.start_consuming(self.process_data_messsage)

    def start(self):
        input_thread = threading.Thread(
            target=self._start_handler_consuming,            
        )        
        input_thread.start()
        signal.signal(
            signal.SIGTERM,
            lambda signum, frame: self.handle_sigterm(),
        )
        self._run()
        input_thread.join()

    def handle_sigterm(self):        
        self.input_queue.stop_consuming()
        self.input_queue.close()  
        self.data_eof_exchange.stop_consuming()
        self.data_eof_exchange.close()                         

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()    
    sum_filter.start()
    return 0

if __name__ == "__main__":
    main()
