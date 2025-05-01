from trempy.Endpoints.Factory.EndpointFactory import EndpointFactory
from trempy.Shared.Utils import Utils
from trempy.Tasks.Task import Task
from task.credentials import credentials
from time import sleep

Utils.configure_logging()

task: Task = Utils.read_task_pickle()

target_endpoint = EndpointFactory.create_endpoint(**credentials.get("target_endpoint")) # TODO criar endpoint somente quando vinher uma mensagem

task.add_endpoint(target_endpoint)

while True: # TODO while em caráter temporário
    task.execute_target()

    if task.replication_type.value == 'cdc':
        sleep(task.interval_seconds)
        
    if task.replication_type.value == 'full_load':
        break

task.clean_endpoints()
