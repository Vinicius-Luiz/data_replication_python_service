from trempy.Endpoints.Factory.EndpointFactory import EndpointFactory
from trempy.Messages.MessageProducer import MessageProducer
from trempy.Loggings.Logging import ReplicationLogger
from task.credentials import credentials
from trempy.Shared.Utils import Utils
from trempy.Tasks.Task import Task



ReplicationLogger.configure_logging()

task: Task = Utils.read_task_pickle()

source_endpoint = EndpointFactory.create_endpoint(**credentials.get("source_endpoint"))

task.add_endpoint(source_endpoint)

changes_structured: list = task.execute_source()

task.clean_endpoints()

if changes_structured and task.replication_type.value == 'cdc':

    producer = MessageProducer(task_name=task.task_name)
    producer.publish_message(messages=changes_structured)


Utils.write_task_pickle(task)