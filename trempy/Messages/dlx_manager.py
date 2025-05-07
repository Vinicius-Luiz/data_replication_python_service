import pika
from typing import Optional, Callable
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties


class DLXManager:
    """
    Gerencia a Dead Letter Exchange (DLX) para tratamento de mensagens falhas no RabbitMQ.

    Responsabilidades:
    1. Configura a infraestrutura DLX (exchange + fila)
    2. Processa mensagens falhas
    3. Fornece mecanismo para callback customizado

    Padrões implementados:
    - Exchange tipo 'topic' para roteamento flexível
    - Filas duráveis para persistência
    - Confirmação manual (manual ack)
    """

    def __init__(self, host: str = "localhost") -> None:
        """
        Inicializa o DLXManager estabelecendo conexão com o RabbitMQ.

        Parâmetros:
            host (str): Endereço do servidor RabbitMQ. Default: 'localhost'

        Raises:
            pika.exceptions.AMQPConnectionError: Se a conexão falhar
        """
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self._setup_infrastructure()

    def _setup_infrastructure(self) -> None:
        """
        Configura a infraestrutura básica da DLX:
        - Exchange 'dlx_replication' (tópico durável)
        - Fila 'failed_replication_tasks' (durável)
        - Binding com routing key 'dlx.*'

        Método chamado automaticamente no __init__
        """
        # Exchange para mensagens falhas (tipo topic permite roteamento complexo)
        self.channel.exchange_declare(
            exchange="dlx_replication",
            exchange_type="topic",  # Permite padrões de routing key
            durable=True,  # Sobrevive a reinicializações do broker
        )

        # Fila principal para mensagens falhas
        self.channel.queue_declare(
            queue="failed_replication_tasks",
            durable=True,  # Persistente
            arguments={"x-queue-mode": "lazy"},  # Mensagens em disco imediatamente
        )

        # Vincula a fila à exchange com padrão de routing key
        self.channel.queue_bind(
            exchange="dlx_replication",
            queue="failed_replication_tasks",
            routing_key="dlx.*",  # Aceita qualquer routing key começando com 'dlx.'
        )

    def start_consuming(
        self,
        callback: Optional[
            Callable[[BlockingChannel, Basic.Deliver, BasicProperties, bytes], None]
        ] = None,
    ) -> None:
        """
        Inicia o consumo da fila de mensagens falhas.

        Parâmetros:
            callback (Callable|None): Função customizada para processamento.
                Se None, usa o callback padrão (_default_callback).

                Assinatura do callback:
                def callback(channel, method, properties, body) -> None

        Comportamento:
            - Bloqueante (runs forever)
            - Manual ack requerido
            - Prefetch_count=1 implícito
        """
        self.channel.basic_qos(prefetch_count=1)  # Processa 1 mensagem por vez

        self.channel.basic_consume(
            queue="failed_replication_tasks",
            on_message_callback=callback or self._default_callback,
            auto_ack=False,  # Confirmação manual obrigatória
        )

        print(" [*] DLXManager aguardando mensagens falhas. CTRL+C para sair")
        self.channel.start_consuming()

    def _default_callback(
        self,
        ch: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ) -> None:
        """
        Callback padrão para mensagens falhas (implementação básica).

        Parâmetros:
            ch (BlockingChannel): Canal RabbitMQ
            method (Basic.Deliver): Metadados de entrega
            properties (BasicProperties): Propriedades da mensagem
            body (bytes): Corpo da mensagem (payload)

        Comportamento:
            - Loga o conteúdo da mensagem
            - Confirma o processamento (ACK)
            - Em produção, deveria integrar com sistema de alertas/logs
        """
        try:
            message = body.decode()
            print(f" [DLX] Mensagem falha recebida - Routing Key: {method.routing_key}")
            print(f" [DLX] Conteúdo: {message[:200]}...")  # Log parcial
            print(f" [DLX] Headers: {properties.headers or 'Nenhum'}")

            # TODO: Em produção, implementar:
            # 1. Log estruturado (ex: JSON)
            # 2. Alertas (Email, Slack, etc.)
            # 3. Métricas (Prometheus, Datadog)

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            print(f" [DLX-ERRO] Falha ao processar callback: {str(e)}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


# Exemplo de uso
if __name__ == "__main__":

    def custom_callback(
        ch: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ):
        _ = properties # Explicitamente marcado como não usado (evita warnings em linters)
        """Exemplo de callback customizado"""
        print(f" [CUSTOM] Processando mensagem falha: {method.routing_key}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    dlx_manager = DLXManager()

    # Opção 1: Usa callback padrão (apenas logging)
    # dlx_manager.start_consuming()

    # Opção 2: Usa callback customizado
    dlx_manager.start_consuming(callback=custom_callback)
