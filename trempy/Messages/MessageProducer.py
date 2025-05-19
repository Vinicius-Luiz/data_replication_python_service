from trempy.Loggings.Logging import ReplicationLogger
from trempy.Messages.Exceptions.Exception import *
from trempy.Messages.Message import Message
from trempy.Shared.Utils import Utils
from typing import Dict, List
import time
import json
import pika

logger = ReplicationLogger()

class MessageProducer(Message):
    def __init__(self, task_name: str):
        super().__init__(task_name=task_name)

    def publish_message(self, messages: List[Dict]) -> None:
        try:
            for message in messages:
                message_id = Utils.hash_6_chars()
                message_dumps = json.dumps(message)

                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=self.routing_key,
                    body=message_dumps,
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # Persistente (sobrevive a reinicializações)
                        content_type="application/json",
                        headers={
                            "version": "1.1.0",
                            "transaction_id": message.get("id"),
                            "timestamp": int(time.time()),
                        },
                        message_id=message_id,
                    ),
                )
                logger.info(
                    f"MESSAGE - Publicado '{self.routing_key}': {message.get('id')}/{message_id}"
                )

        except Exception as e:
            e = MessageProducerException(f"Erro ao publicar mensagem: {str(e)}")
            logger.critical(e)