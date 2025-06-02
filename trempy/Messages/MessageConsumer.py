from trempy.Metadata.MetadataConnectionManager import MetadataConnectionManager
from pika.adapters.blocking_connection import BlockingChannel
from trempy.Loggings.Logging import ReplicationLogger
from trempy.Messages.Exceptions.Exception import *
from pika.spec import Basic, BasicProperties
from trempy.Messages.Message import Message
from typing import Callable
import json

logger = ReplicationLogger()


class MessageConsumer(Message):
    QUEUE_NAME_PATTERN = "trempy_queue_{task_name}"

    def __init__(
        self,
        task_name: str,
        external_callback: Callable,
        prefetch_count: int = 1,
        auto_ack: bool = False,
    ):
        super().__init__(task_name=task_name)

        self.queue_name = self.QUEUE_NAME_PATTERN.format(task_name=task_name)

        self.prefetch_count = prefetch_count
        self.auto_ack = auto_ack
        self.external_callback = external_callback

        self.__setup()

    def __setup(self):

        try:
            # Configuração da Dead Letter Exchange (DLX)
            args = {
                "x-dead-letter-exchange": self.dlx_exchange_name,
                "x-dead-letter-routing-key": self.dlx_routing_key,
            }

            # Declara a fila principal com:
            # - arguments=args: Configurações especiais (DLX)
            self.channel.queue_declare(
                queue=self.queue_name, durable=self.durable, arguments=args
            )

            # Vincula a fila à exchange com:
            self.channel.queue_bind(
                exchange=self.exchange_name,
                queue=self.queue_name,
                routing_key=self.routing_key,
            )

            # Configura qualidade de serviço (QoS):
            # prefetch_count=1 -> Processa 1 mensagem por vez
            self.channel.basic_qos(prefetch_count=self.prefetch_count)

            # Inicia o consumo da fila
            self.channel.basic_consume(
                queue=self.queue_name,
                auto_ack=self.auto_ack,
                on_message_callback=self.__callback,
            )

        except Exception as e:
            e = MessageConsumerException(
                f"Erro ao configurar o consumidor de mensagens: {str(e)}"
            )
            logger.critical(e)

    def delete_queue(self):
        self.channel.queue_delete(
            queue=self.queue_name, if_unused=False, if_empty=False
        )

    def __callback(
        self,
        ch: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ) -> None:

        try:
            with MetadataConnectionManager() as metadata_manager:
                message: dict = json.loads(body.decode())
                metadata_manager.update_stats_message(
                    {
                        "transaction_id": properties.headers.get("transaction_id"),
                        "column": "received",
                        "value": message.get("batch_size"),
                    }
                )
                # logger.info(
                #     f"MESSAGE - Recebido: ({method.delivery_tag}): {properties.headers.get('transaction_id')}/{properties.message_id}"
                # )

                message["delivery_tag"] = method.delivery_tag
                message["transaction_id"] = properties.headers.get("transaction_id")

                if self.external_callback:
                    self.external_callback(message, ch)

        except Exception as e:
            e = MessageConsumerException(f"Erro ao processar mensagem: {str(e)}")
            logger.error(e)

    def start_consuming(self):
        logger.info(f"MESSAGE - Consumer Esperando por mensagens")
        self.channel.start_consuming()
