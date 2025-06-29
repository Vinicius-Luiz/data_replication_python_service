from pika.adapters.blocking_connection import BlockingChannel
import pika
import os
from typing import Optional


class Message:
    DLX_EXCHANGE_NAME_PATTERN = "trempy_dlx_exchange_{task_name}"
    EXCHANGE_NAME_PATTERN = "trempy_exchange_{task_name}"

    DLX_ROUTING_KEY_PATTERN = "dlx.{task_name}"
    ROUNTING_KEY_PATTERN = "trempy_routing_key_{task_name}"

    def __init__(
        self,
        task_name: str,
        host: Optional[str] = None,
        exchange_type: str = "direct",
        durable: bool = True,
    ):
        self.task_name = task_name

        self.dlx_exchange_name = self.DLX_EXCHANGE_NAME_PATTERN.format(
            task_name=task_name
        )
        self.exchange_name = self.EXCHANGE_NAME_PATTERN.format(task_name=task_name)

        self.dlx_routing_key = self.DLX_ROUTING_KEY_PATTERN.format(task_name=task_name)
        self.routing_key = self.ROUNTING_KEY_PATTERN.format(task_name=task_name)

        self.host = host if host is not None else os.getenv('RABBITMQ_HOST', 'localhost')
        self.exchange_type = exchange_type
        self.durable = durable

        self.channel = self.__create_connection()

        self.__declare_dlx_exchange()

        self.__declare_exchange()

    def __create_connection(self) -> BlockingChannel:
        connection_parameters = pika.ConnectionParameters(
            host=self.host, connection_attempts=5, retry_delay=3
        )
        connection = pika.BlockingConnection(connection_parameters)

        return connection.channel()

    def __declare_exchange(self) -> None:
        self.channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type=self.exchange_type,
            durable=self.durable,
        )

    def __declare_dlx_exchange(self) -> None:
        self.channel.exchange_declare(
            exchange=self.dlx_exchange_name,
            exchange_type=self.exchange_type,
            durable=self.durable,
        )

    def delete_exchange(self) -> None:
        self.channel.exchange_delete(self.exchange_name, if_unused=False)

    def delete_dlx_exchange(self) -> None:
        self.channel.exchange_delete(self.dlx_exchange_name, if_unused=False)
