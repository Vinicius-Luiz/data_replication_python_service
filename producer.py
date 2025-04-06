from Entities.Endpoints.Factory.EndpointFactory import EndpointFactory
from Entities.Shared.Utils import Utils
from Entities.Tasks.Task import Task
from task.credentials import credentials

Utils.configure_logging()

task: Task = Utils.read_task_pickle()

source_endpoint = EndpointFactory.create_endpoint(**credentials.get("source_endpoint"))

task.add_endpoint(source_endpoint)

task.execute_source()

task.clean_endpoints()

Utils.write_task_pickle(task)
