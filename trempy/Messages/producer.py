import pika
import json
from time import time
from typing import Any, Dict
from pika.adapters.blocking_connection import BlockingChannel

def setup_producer() -> BlockingChannel:
    """
    Configura e retorna um canal RabbitMQ para publicação de mensagens.
    
    Retorna:
        BlockingChannel: Canal configurado pronto para publicar mensagens
        
    Comportamento:
        1. Estabelece conexão com RabbitMQ local
        2. Declara uma exchange durável do tipo 'direct'
        3. Retorna o canal configurado
        
    Exceções:
        pika.exceptions.AMQPConnectionError: Se falhar ao conectar no RabbitMQ
    """
    # Cria conexão bloqueante (simples para exemplos)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost")
    )
    
    # Obtém um canal da conexão
    channel = connection.channel()

    # Declara exchange com:
    # - Nome: data_replication
    # - Tipo: direct (roteamento por routing key)
    # - Durável: Sobrevive a reinicializações do broker
    channel.exchange_declare(
        exchange="data_replication",
        exchange_type="direct",
        durable=True  # Persistente
    )

    return channel


def publish_task(
    channel: BlockingChannel,
    task_name: str,
    data: Dict[str, Any]
) -> None:
    """
    Publica uma mensagem na exchange configurada.
    
    Parâmetros:
        channel (BlockingChannel): Canal RabbitMQ válido
        task_name (str): Nome da task/routing key para roteamento
        data (Dict[str, Any]): Dados a serem serializados como JSON
        
    Comportamento:
        1. Serializa os dados para JSON
        2. Publica com:
           - Entrega persistente (delivery_mode=2)
           - Content-Type como application/json
        3. Loga a mensagem enviada
        
    Exceções:
        pika.exceptions.ChannelClosed: Se o canal estiver inválido
        json.JSONEncodeError: Se os dados não forem serializáveis
    """
    message = json.dumps(data)  # Serializa para JSON

    channel.basic_publish(
        exchange="data_replication",
        routing_key=task_name,  # Roteamento para filas com esta binding key
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  # Persistente (sobrevive a reinicializações)
            content_type="application/json",  # Tipo do conteúdo
            headers={"version": "1.0.0"},  # Metadados customizados
            message_id="123e4567-e89b-12d3",  # ID único
            timestamp=int(time())  # Quando foi enviada
        ),
    )
    print(f" [x] Sent '{task_name}':{message[:100]}...")  # Log truncado


# Exemplo de uso seguro
if __name__ == "__main__":
    try:
        # Configura
        channel = setup_producer()
        
        # Publica
        publish_task(
            channel=channel,
            task_name="user_data_sync",
            data={"action": "update", "id": 42}
        )
    except Exception as e:
        print(f"Erro: {str(e)}")
        # Em produção, adicionar retry lógico aqui