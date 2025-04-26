from trempy.Endpoints.Factory.EndpointFactory import EndpointFactory
from trempy.Shared.Utils import Utils
from trempy.Tasks.Task import Task
from task.credentials import credentials
from time import sleep

Utils.configure_logging()

task: Task = Utils.read_task_pickle()

target_endpoint = EndpointFactory.create_endpoint(**credentials.get("target_endpoint"))

task.add_endpoint(target_endpoint)

while True: # TODO while em caráter temporário
    task.execute_target()
    sleep(task.interval_seconds)

task.clean_endpoints()
