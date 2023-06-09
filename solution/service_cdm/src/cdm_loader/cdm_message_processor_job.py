import time, json
from datetime import datetime
import uuid

def get_uuid(v):
    uuid_ = uuid.uuid5(name=str(v), namespace=uuid.NAMESPACE_OID)
    return uuid_

class CdmMessageProcessor:
    def __init__(self,KafkaConsumer,KafkaProducer,CdmRepository,_batch_size,logger) -> None:
        self._consumer = KafkaConsumer
        self._producer = KafkaProducer
        self._cdm_repository = CdmRepository
        if _batch_size:
            self._batch_size = _batch_size
        else:
            self._batch_size = 100
        self._logger = logger

    def update_cdm(self,msg):
        mart_name = msg['mart']
        msg.pop('mart')
        columns = list(msg.keys())
        values = list(msg.values())
        print(columns)
        print(values)
        self._cdm_repository.update_mart(mart_name,columns,values)

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")
        i = 0
        while i < self._batch_size:
            message = self._consumer.consume()
            self._logger.info(i)
            i = i+1
            if message is None:
                break
            if message.get('mart'):
               self.update_cdm(message)
        
        self._logger.info(f"{datetime.utcnow()}: FINISH")
"""цикл while обрабатывает сообщения из очереди до тех пор, 
пока не будет обработано заданное число сообщений (batch_size). 
Если в очереди больше нет сообщений, то цикл прерывается.
 Если полученное сообщение содержит ключевое слово "mart", 
 то вызывается метод update_cdm, который обновляет хранилище данных CDM."""