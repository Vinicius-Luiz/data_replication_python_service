from trempy.Endpoints.Factory.EndpointFactory import EndpointFactory
from trempy.Loggings.Logging import ReplicationLogger
from task.credentials import credentials
from trempy.Shared.Utils import Utils
from trempy.Tasks.Task import Task


ReplicationLogger.configure_logging()

task: Task = Utils.read_task_pickle()

target_endpoint = EndpointFactory.create_endpoint(**credentials.get("target_endpoint"))
task.add_endpoint(target_endpoint)

not task.execute_target_full_load() and task.execute_target_cdc()

task.clean_endpoints()  # TODO ver se vale a pena deixar sempre ligado

Utils.write_task_pickle(task)
