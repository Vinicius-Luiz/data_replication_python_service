from trempy.Endpoints.Factory.EndpointFactory import EndpointFactory
from trempy.Messages.Message import MessageProducer
from task.credentials import credentials
from trempy.Shared.Utils import Utils
from trempy.Tasks.Task import Task
from datetime import datetime
import json



Utils.configure_logging()

task: Task = Utils.read_task_pickle()

source_endpoint = EndpointFactory.create_endpoint(**credentials.get("source_endpoint"))

task.add_endpoint(source_endpoint)

changes_structured: dict = task.execute_source()

task.clean_endpoints()

if changes_structured and task.replication_type.value == 'cdc':

    producer = MessageProducer(task_name=task.task_name)
    producer.publish_message(message=changes_structured)

    # TODO temporário
    with open(f'data\cdc_data\{int(datetime.now().timestamp())}_{changes_structured["id"]}.json', 'w') as f:
        json.dump(changes_structured, f)



Utils.write_task_pickle(task)