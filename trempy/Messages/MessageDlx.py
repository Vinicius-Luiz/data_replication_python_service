from trempy.Metadata.MetadataConnectionManager import MetadataConnectionManager
from pika.adapters.blocking_connection import BlockingChannel
from trempy.Loggings.Logging import ReplicationLogger
from trempy.Messages.Exceptions.Exception import *
from pika.spec import Basic, BasicProperties
from trempy.Messages.Message import Message
import time
import json
import os

logger = ReplicationLogger()


class MessageDlx(Message):
    DLX_QUEUE_NAME_PATTERN = "dlx_queue_{task_name}"

    def __init__(
        self,
        task_name: str,
        prefetch_count: int = 1,
    ):
        super().__init__(task_name=task_name)
        self.prefetch_count = prefetch_count
        self.dlx_queue_name = self.DLX_QUEUE_NAME_PATTERN.format(task_name=task_name)
        self.__setup()

    def __setup(self):
        try:
            self.channel.queue_declare(
                queue=self.dlx_queue_name,
                durable=self.durable,
            )
            self.channel.queue_bind(
                exchange=self.dlx_exchange_name,
                queue=self.dlx_queue_name,
                routing_key=self.dlx_routing_key,
            )
            self.channel.basic_qos(prefetch_count=self.prefetch_count)

            self.channel.basic_consume(
                queue=self.dlx_queue_name,
                auto_ack=True,
                on_message_callback=self.__callback,
            )

        except Exception as e:
            e = MessageDlxException(
                f"Erro ao configurar o consumidor Dlx de mensagens: {str(e)}"
            )
            logger.critical(e)

    def delete_queue(self):
        self.channel.queue_delete(
            queue=self.dlx_queue_name, if_unused=False, if_empty=False
        )

    def __callback(
        self,
        ch: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ) -> None:

        logger.info(f"MESSAGE DLX - Processando mensagem falha ({method.delivery_tag})")
        message = body.decode()

        try:
            with MetadataConnectionManager() as metadata_manager:
                metadata_manager.insert_dlx_message(
                    data={
                        "task_name": self.task_name,
                        "transaction_id": properties.headers.get("transaction_id"),
                        "message_id": properties.message_id,
                        "delivery_tag": method.delivery_tag,
                        "routing_key": method.routing_key,
                        "body": message,
                    }
                )

        except Exception as e:
            e = MessageDlxException(f"Erro ao salvar a mensagem: {str(e)}")
            logger.error(e)

    def start_consuming(self):
        logger.info(f"MESSAGE DLX - Consumer Esperando por mensagens")
        self.channel.start_consuming()
