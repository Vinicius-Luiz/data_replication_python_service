from Entities.Endpoints.Factory.EndpointFactory import EndpointFactory
from Entities.Shared.Utils import Utils
from Entities.Tasks.Task import Task
from task.credentials import credentials

Utils.configure_logging()

task: Task = Utils.read_task_pickle()

target_endpoint = EndpointFactory.create_endpoint(**credentials.get("target_endpoint"))

task.add_endpoint(target_endpoint)

task.execute_target()

task.clean_endpoints()
