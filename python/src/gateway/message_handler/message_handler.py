from common import message_protocol
import threading 

class MessageHandler:

    _instace_count = 0
    _lock = threading.Lock()

    def __init__(self):
        with self._lock:
            self.instance_id = MessageHandler._instace_count
            MessageHandler._instace_count+=1

    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([fruit, amount, self.instance_id])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.instance_id])

    def deserialize_result_message(self, message):
        fruit_top, client_id = message_protocol.internal.deserialize(message)        
        if client_id == self.instance_id:
            return fruit_top
