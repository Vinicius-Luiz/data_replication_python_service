from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties
from trempy.Shared.Utils import Utils
from typing import Dict, Callable
import time
import json
import pika


class Message:
    DLX_EXCHANGE_NAME_PATTERN = "trempy_dlx_exchange_{task_name}"
    EXCHANGE_NAME_PATTERN = "trempy_exchange_{task_name}"

    DLX_ROUTING_KEY_PATTERN = "dlx.{task_name}"
    ROUNTING_KEY_PATTERN = "trempy_routing_key_{task_name}"

    def __init__(
        self,
        task_name: str,
        host: str = "localhost",
        exchange_type: str = "direct",
        durable: bool = True,
    ):
        self.dlx_exchange_name = self.DLX_EXCHANGE_NAME_PATTERN.format(
            task_name=task_name
        )
        self.exchange_name = self.EXCHANGE_NAME_PATTERN.format(task_name=task_name)

        self.dlx_routing_key = self.DLX_ROUTING_KEY_PATTERN.format(task_name=task_name)
        self.routing_key = self.ROUNTING_KEY_PATTERN.format(task_name=task_name)

        self.host = host
        self.exchange_type = exchange_type
        self.durable = durable

        self.channel = self.__create_connection()

        self.declare_dlx_exchange()

        self.declare_exchange()

    def __create_connection(self) -> BlockingChannel:
        connection_parameters = pika.ConnectionParameters(
            host=self.host, connection_attempts=5, retry_delay=3
        )
        connection = pika.BlockingConnection(connection_parameters)

        return connection.channel()

    def declare_exchange(self) -> None:
        self.channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type=self.exchange_type,
            durable=self.durable,
        )

    def declare_dlx_exchange(self) -> None:
        self.channel.exchange_declare(
            exchange=self.dlx_exchange_name,
            exchange_type=self.exchange_type,
            durable=self.durable,
        )


class MessageProducer(Message):
    def __init__(self, task_name: str):
        super().__init__(task_name=task_name)

    def publish_message(self, message: Dict) -> None:
        message_dumps = json.dumps(message)

        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=self.routing_key,  # Roteamento para filas com esta binding key
            body=message_dumps,
            properties=pika.BasicProperties(
                delivery_mode=2,  # Persistente (sobrevive a reinicializações)
                content_type="application/json",
                headers={"version": "1.0.0"},
                message_id=message.get("id"),
            ),
        )
        Utils.log_debug(
            f" [x] Sent '{self.routing_key}':{message['id']}"
        )  # Log truncado


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

        self.setup()

    def setup(self):

        # Configuração da Dead Letter Exchange (DLX)
        args = {
            "x-dead-letter-exchange": self.dlx_exchange_name,  # Exchange para mensagens falhas
            "x-dead-letter-routing-key": self.dlx_routing_key,  # Routing key padrão para DLX
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

    def __callback(
        self,
        ch: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ) -> None:

        message: dict = json.loads(body.decode())
        Utils.log_debug(f" [x] Received '{self.routing_key}':{message['id']}...")

        message["delivery_tag"] = method.delivery_tag

        if self.external_callback:
            self.external_callback(message, ch)

    def start_consuming(self):
        Utils.log_debug(" [*] Waiting for messages. To exit press CTRL+C")
        self.channel.start_consuming()


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
        self.setup()

    def setup(self):
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

    def __callback(
        self,
        ch: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ) -> None:
        message = body.decode()
        Utils.log_debug(
            f" [DLX] Processing failed message (delivery_tag={method.delivery_tag})"
        )

        try:
            error_dir = "trempy\\Messages\\log\\dlx\\"
            import os

            os.makedirs(error_dir, exist_ok=True)

            # Nome do arquivo inclui delivery_tag para referência
            filename = f"failed_{method.delivery_tag}_{int(time.time())}.json"
            filepath = os.path.join(error_dir, filename)

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "delivery_tag": method.delivery_tag,  # Adicionado para rastreamento
                        "message": json.loads(message),
                        "routing_key": method.routing_key,
                    },
                    f,
                    indent=4,
                )

            Utils.log_debug(
                f" [DLX] Saved message (tag={method.delivery_tag}) to {filepath}"
            )

        except Exception as e:
            Utils.log_debug(
                f" [DLX!!] Error saving message (tag={method.delivery_tag}): {str(e)}"
            )
            # Não faz NACK (a mensagem já foi auto-ack'ed)

    def start_consuming(self):
        self.channel.start_consuming()
