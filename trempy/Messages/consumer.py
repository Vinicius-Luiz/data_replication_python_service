# from dlx_manager import DLXManager
import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties
import json

# No setup do consumer:
# dlx_manager = DLXManager()


def structure_capture_changes_to_dataframe(message, error=False):
    # Lógica de transformação dos dados
    try:
        data = json.loads(message)
        with open("trempy\Messages\log\consumer_test.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        # Processamento simulado
        if error:
            raise ValueError("Erro no processamento")
        return True
    except Exception as e:
        print(f"Erro no processamento: {str(e)}")
        return False


def setup_consumer(task_name: str) -> BlockingChannel:
    """
    Configura um consumidor RabbitMQ para processar mensagens de uma fila específica.

    Parâmetros:
        task_name (str): Nome da fila/task que será consumida. Também usado como routing key.

    Retorna:
        BlockingChannel: Canal RabbitMQ configurado e pronto para iniciar consumo.

    Comportamento:
        - Cria uma fila durável com DLX configurada
        - Define prefetch=1 para processamento serializado
        - Só confirma mensagens (ACK) após processamento bem-sucedido
        - Rejeita mensagens falhas (NACK) enviando-as para a Dead Letter Exchange
    """

    # Estabelece conexão com o servidor RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
    channel = connection.channel()

    # Configuração da Dead Letter Exchange (DLX)
    args = {
        "x-dead-letter-exchange": "dlx_replication",  # Exchange para mensagens falhas
        "x-dead-letter-routing-key": f"dlx.{task_name}",  # Routing key padrão para DLX
    }

    # Declara a fila principal com:
    # - durable=True: Sobrevive a reinicializações do broker
    # - arguments=args: Configurações especiais (DLX)
    channel.queue_declare(queue=task_name, durable=True, arguments=args)

    # Declara a exchange SE não existir
    channel.exchange_declare(
        exchange='data_replication',
        exchange_type='direct',
        durable=True
    )

    # Vincula a fila à exchange com:
    # - exchange="data_replication": Nome da exchange direct
    # - routing_key=task_name: Filtro para mensagens
    channel.queue_bind(
        exchange="data_replication", queue=task_name, routing_key=task_name
    )

    # Configura qualidade de serviço (QoS):
    # prefetch_count=1 -> Processa 1 mensagem por vez
    channel.basic_qos(prefetch_count=1)

    def callback(
        ch: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes,
    ) -> None:
        """
        Callback chamado quando uma mensagem é recebida.

        Parâmetros:
            ch (BlockingChannel): Canal RabbitMQ
            method (Basic.Deliver): Metadados de entrega (contém delivery_tag, etc.)
            properties (BasicProperties): Propriedades da mensagem
            body (bytes): Corpo da mensagem (payload)
        """
        _ = properties  # Explicitamente marcado como não usado (evita warnings em linters)

        message = body.decode()
        print(f" [x] Received {message}")

        try:
            # Processa a mensagem e só confirma se bem-sucedido
            if structure_capture_changes_to_dataframe(message, False):
                ch.basic_ack(delivery_tag=method.delivery_tag)
                print(" [x] Processamento concluído com sucesso")
            else:
                # Rejeita mensagem (sem reenfileirar) -> vai para DLX
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                print(" [!] Mensagem rejeitada (enviada para DLX)")
        except Exception as e:
            # Garante que exceções não quebrem o consumidor
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            print(f" [!!] Erro crítico: {str(e)}. Mensagem enviada para DLX")

    # Inicia o consumo com:
    # - auto_ack=False: Confirmação manual necessária
    channel.basic_consume(queue=task_name, on_message_callback=callback, auto_ack=False)

    return channel


# Exemplo de uso
consumer = setup_consumer("user_data_sync")
print(" [*] Waiting for messages. To exit press CTRL+C")
consumer.start_consuming()
