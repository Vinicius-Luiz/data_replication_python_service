from trempy.Endpoints.Factory.EndpointFactory import EndpointFactory
from pika.adapters.blocking_connection import BlockingChannel
from trempy.Messages.MessageConsumer import MessageConsumer
from trempy.Loggings.Logging import ReplicationLogger
from task.credentials import credentials
from trempy.Shared.Utils import Utils
from trempy.Tasks.Task import Task

ReplicationLogger.configure_logging()


def external_callback(changes_structured: dict, channel: BlockingChannel):
    task.execute_target(changes_structured=changes_structured, channel=channel)


task: Task = Utils.read_task_pickle()

target_endpoint = EndpointFactory.create_endpoint(**credentials.get("target_endpoint"))
task.add_endpoint(target_endpoint)

if task.replication_type.value == "cdc":

    consumer = MessageConsumer(
        task_name=task.task_name, external_callback=external_callback
    )

    consumer.start_consuming()

elif task.replication_type.value == "full_load":
    task.execute_target()

task.clean_endpoints()  # TODO ver se vale a pena deixar sempre ligado
