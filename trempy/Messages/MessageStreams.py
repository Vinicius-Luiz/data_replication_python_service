import json
import logging
import time
from typing import Dict, Callable
from rstream import Producer, Consumer, AMQPMessage, OffsetType


class MessageStream:
    """
    Classe base para operações com RabbitMQ Streams
    """

    STREAM_NAME_PATTERN = "trempy_stream_{task_name}"

    def __init__(
        self,
        task_name: str,
        host: str = "localhost",
        port: int = 5552,
        username: str = "guest",
        password: str = "guest",
        stream_max_length_bytes: int = 1000000000,  # 1GB
        stream_max_age: str = "1h",  # Rotação horária
    ):
        """
        Inicializa a conexão com o stream

        Args:
            task_name: Nome da tarefa/stream
            host: Endereço do servidor RabbitMQ
            port: Porta do plugin de streams (padrão 5552)
            username: Usuário para autenticação
            password: Senha para autenticação
            stream_max_length_bytes: Tamanho máximo do stream em bytes
            stream_max_age: Idade máxima do stream (ex: '1h', '7d')
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.stream_name = self.STREAM_NAME_PATTERN.format(task_name=task_name)
        self.stream_args = {
            "max-length-bytes": stream_max_length_bytes,
            "max-age": stream_max_age,
        }

        # Configuração básica de logging
        logging.basicConfig(
            format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO
        )


class MessageStreamProducer(MessageStream):
    """
    Classe para produção de mensagens em streams
    """

    def __init__(self, task_name: str, **kwargs):
        """
        Inicializa o producer e declara o stream

        Args:
            task_name: Nome da tarefa/stream
            **kwargs: Argumentos adicionais para MessageStream
        """
        super().__init__(task_name=task_name, **kwargs)
        self._initialize_producer()

    def _initialize_producer(self):
        """Configura a conexão e declara o stream"""
        try:
            self.producer = Producer(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
            )

            self.producer.declare_stream(
                stream=self.stream_name, exists_ok=True, arguments=self.stream_args
            )

            logging.info(f"Producer initialized for stream '{self.stream_name}'")

        except Exception as e:
            logging.error(f"Failed to initialize producer: {str(e)}")
            raise

    def publish_message(self, message: Dict) -> None:
        """
        Publica uma mensagem no stream

        Args:
            message: Dicionário com os dados da mensagem
        """
        try:
            msg = AMQPMessage(
                body=json.dumps(message).encode(),
                properties={
                    "message_id": message.get("id"),
                    "content_type": "application/json",
                    "timestamp": int(time.time() * 1000),  # Unix timestamp em ms
                    "headers": {"version": "1.0.0"},
                },
            )

            self.producer.send(stream=self.stream_name, message=msg)

            logging.debug(f"Sent to stream '{self.stream_name}': {message.get('id')}")

        except Exception as e:
            logging.error(f"Failed to publish message: {str(e)}")
            raise

    def close(self):
        """Fecha a conexão do producer"""
        if hasattr(self, "producer"):
            self.producer.close()
            logging.info("Producer connection closed")


class MessageStreamConsumer(MessageStream):
    """
    Classe para consumo de mensagens de streams com tracking de offset
    """

    def __init__(
        self,
        task_name: str,
        external_callback: Callable,
        consumer_name: str = "default_consumer",
        credit: int = 1000,
        **kwargs,
    ):
        """
        Inicializa o consumer

        Args:
            task_name: Nome da tarefa/stream
            external_callback: Função para processar mensagens (recebe message, context)
            consumer_name: Identificador único para tracking de offset
            credit: Número de mensagens para pré-buscar
            **kwargs: Argumentos adicionais para MessageStream
        """
        super().__init__(task_name=task_name, **kwargs)
        self.external_callback = external_callback
        self.consumer_name = consumer_name
        self.credit = credit
        self._initialize_consumer()

    def _initialize_consumer(self):
        """Configura a conexão e inscrição no stream"""
        try:
            self.consumer = Consumer(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                client_properties={"consumer_name": self.consumer_name},
                callback=self._internal_callback,
            )

            # Começa da última mensagem não confirmada
            self.consumer.subscribe(
                stream=self.stream_name,
                offset_specification=OffsetType.NEXT,
                credit=self.credit,
            )

            logging.info(
                f"Consumer '{self.consumer_name}' initialized for stream '{self.stream_name}'"
            )

        except Exception as e:
            logging.error(f"Failed to initialize consumer: {str(e)}")
            raise

    def _internal_callback(self, message, context):
        """
        Callback interno que gerencia o ciclo de vida da mensagem

        Args:
            message: Mensagem recebida
            context: Contexto com métodos de controle (ack, etc.)
        """
        try:
            msg_data = json.loads(message.body.decode())
            logging.debug(f"Processing message {message.properties.get('message_id')}")

            # Adiciona metadados ao contexto
            processing_context = {
                "offset": context.offset,
                "timestamp": message.properties.get("timestamp"),
                "message_id": message.properties.get("message_id"),
                "stream": self.stream_name,
                "consumer": self.consumer_name,
                "ack": context.ack,
                "properties": message.properties,
            }

            # Chama o callback externo
            self.external_callback(msg_data, processing_context)

            # Confirma o offset se o callback não levantou exceção
            context.ack()

        except json.JSONDecodeError:
            logging.error("Failed to decode message body")
            # Não faz ack - mensagem será reprocessada
        except Exception as e:
            logging.error(f"Error processing message: {str(e)}")
            # Não faz ack - mensagem será reprocessada

    def start_consuming(self, auto_reconnect: bool = True):
        """
        Inicia o consumo do stream

        Args:
            auto_reconnect: Se True, tenta reconectar automaticamente em caso de falha
        """
        logging.info(
            f"Starting consumer '{self.consumer_name}' on stream '{self.stream_name}'"
        )

        while True:
            try:
                self.consumer.run()  # Loop de consumo bloqueante
            except KeyboardInterrupt:
                logging.info("Consumer stopped by user")
                break
            except Exception as e:
                logging.error(f"Consumer error: {str(e)}")
                if not auto_reconnect:
                    raise

                logging.info("Attempting to reconnect in 5 seconds...")
                time.sleep(5)
                self._initialize_consumer()  # Reconfigura o consumer

    def close(self):
        """Fecha a conexão do consumer"""
        if hasattr(self, "consumer"):
            self.consumer.close()
            logging.info("Consumer connection closed")


# Exemplo de uso
if __name__ == "__main__":
    # Exemplo de producer
    def run_producer_example():
        producer = MessageStreamProducer("data_replication")
        try:
            for i in range(5):
                message = {
                    "id": f"msg_{i}",
                    "data": {"value": i, "timestamp": time.time()},
                }
                producer.publish_message(message)
                time.sleep(0.5)
        finally:
            producer.close()

    # Exemplo de consumer
    def run_consumer_example():
        def process_message(message, context):
            print(f"\nProcessing message {context['message_id']}")
            print(f"Content: {message}")
            print(f"Offset: {context['offset']}")

            # Simula processamento
            time.sleep(1)

            # Confirmação é feita automaticamente pelo _internal_callback

        consumer = MessageStreamConsumer(
            task_name="data_replication",
            external_callback=process_message,
            consumer_name="example_consumer",
        )

        try:
            consumer.start_consuming()
        except KeyboardInterrupt:
            consumer.close()

    # Descomente para testar
    # run_producer_example()
    # run_consumer_example()
