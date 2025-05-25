from trempy.Endpoints.Factory.EndpointFactory import EndpointFactory
from trempy.Loggings.Logging import ReplicationLogger
from task.credentials import credentials
from trempy.Shared.Utils import Utils
from trempy.Tasks.Task import Task


ReplicationLogger.configure_logging()

task: Task = Utils.read_task_pickle()

source_endpoint = EndpointFactory.create_endpoint(**credentials.get("source_endpoint"))
task.add_endpoint(source_endpoint)

task.execute_source_full_load()

task.execute_source_cdc()

task.clean_endpoints()

Utils.write_task_pickle(task)
