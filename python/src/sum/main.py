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
FRUIT_AMOUNT_TYPE = 3
EOF_SUM_TYPE = 2 
EOF_HANDLER_TYPE = 1


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
        self.gateway_queue = middleware.MessageMiddlewareQueueRabbitMQ(
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
        self.sigterm_received = False        

    def _process_data(self, fruit, amount, client_id):
        self.monitor_fruit_amounts.add_fruit_amount_for_client(fruit,amount,client_id)
    
    def _process_eof(self,client_id, send_exchange=None):                
        if SUM_AMOUNT > 1:            
            logging.info(f"EOF OF {client_id}")
            for i in range (0,SUM_AMOUNT):
                if i != ID:                            
                    send_exchange.send_to(message_protocol.internal.serialize([client_id,ID]),f"{SUM_PREFIX}_{i}")                        
                    self.monitor_sums.start_client(client_id)                 
        else:
            self._send_aggregator_eof(client_id)
    
    def _process_sums_eof(self,client_id,sum_id):
        if sum_id != ID:
            fruits = self.monitor_fruit_amounts.get_fruits_amount_for_client(client_id)            
            for final_fruit_item in fruits:                
                self.receive_exchange.send_to(message_protocol.internal.serialize([final_fruit_item.fruit, final_fruit_item.amount, client_id]),f"{SUM_PREFIX}_{sum_id}")
            self.receive_exchange.send_to(message_protocol.internal.serialize([client_id,sum_id]),f"{SUM_PREFIX}_{sum_id}")             
        else: 
            self.monitor_sums.sum_client(client_id)
            sums = self.monitor_sums.get_sum(client_id)                                                     
            if sums == SUM_AMOUNT:                
                self._send_aggregator_eof(client_id)

    def get_aggregator_id(self,client_id):
        return (client_id % AGGREGATION_AMOUNT)

    def _send_aggregator_eof(self,client_id):                
        fruits = self.monitor_fruit_amounts.get_fruits_amount_for_client(client_id)
        aggregator_id = self.get_aggregator_id(client_id)
        for i in range(0,AGGREGATION_AMOUNT):            
            if i == aggregator_id:
                for final_fruit_item in fruits:                        
                    self.data_output_exchanges[i].send_to(message_protocol.internal.serialize([final_fruit_item.fruit, final_fruit_item.amount,client_id]), f"{AGGREGATION_PREFIX}_{aggregator_id}")                    
                logging.info(f"Sending EOF message: {client_id, ID}")            
                self.data_output_exchanges[i].send_to(message_protocol.internal.serialize([client_id]), f"{AGGREGATION_PREFIX}_{aggregator_id}")    

    def _start_handler_consuming(self): 
        logging.info("MAIN: starting handler consume")
        send_exchange = None
        if SUM_AMOUNT>1:   
            send_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST,SUM_PREFIX,[f"{SUM_PREFIX}_{ID}"])
        def _process_data_messsage(message, ack, nack):
            try:
                fields = message_protocol.internal.deserialize(message)
                if len(fields) == FRUIT_AMOUNT_TYPE:
                    self._process_data(*fields)
                elif len(fields) == EOF_HANDLER_TYPE:            
                    self._process_eof(*fields, send_exchange)                           
                ack()
            except Exception as e:
                logging.error(e)
                nack()
                send_exchange.stop_consuming()
        self.gateway_queue.start_consuming(_process_data_messsage)
        if send_exchange != None:
            send_exchange.close()

        logging.info("MAIN: handler consume finished")
        
    def _run(self):
        logging.info("INPUT THREAD: started")        
        self.receive_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_PREFIX, [f"{SUM_PREFIX}_{ID}"]
        ) 
        def process_data_messsage(message, ack, nack):
            try:
                fields = message_protocol.internal.deserialize(message)
                if len(fields) == FRUIT_AMOUNT_TYPE:
                    self._process_data(*fields)        
                elif len(fields) == EOF_SUM_TYPE:
                    self._process_sums_eof(*fields)            
                ack()
            except Exception as e:
                logging.error(e)
                nack()  
                self.receive_exchange.stop_consuming()                                           
        self.receive_exchange.start_consuming(process_data_messsage)             
        self.receive_exchange.close()        

    def start(self):    
        signal.signal(
                    signal.SIGTERM,
                    lambda signum, frame: self.handle_sigterm(),
                )                
        try:     
            if SUM_AMOUNT > 1:
                input_thread = threading.Thread(
                            target=self._run,            
                        )   
                input_thread.start()        
                self._start_handler_consuming()             
            else:
                self._start_handler_consuming()                        
        except Exception:
            if self.sigterm_received:
                return 
            raise 
        finally:
            input_thread.join()
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.close()

    def handle_sigterm(self):        
        if not self.sigterm_received:
            self.sigterm_received = True
            self.gateway_queue.stop_consuming()
            self.gateway_queue.close() 
            self.receive_exchange.add_callback_threadsafe(self.receive_exchange.stop_consuming)            
                                             
def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()    
    sum_filter.start()
    return 0

if __name__ == "__main__":
    main()
