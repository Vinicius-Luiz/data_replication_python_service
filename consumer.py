from trempy.Metadata.MetadataConnectionManager import MetadataConnectionManager
from trempy.Endpoints.Factory.EndpointFactory import EndpointFactory
from trempy.Loggings.Logging import ReplicationLogger
from trempy.Shared.Utils import Utils
from trempy.Tasks.Task import Task


ReplicationLogger.configure_logging()

with MetadataConnectionManager() as metadata_manager:
    current_replication_type = metadata_manager.get_metadata_config(
        "CURRENT_REPLICATION_TYPE"
    )
    full_load_finished = metadata_manager.get_metadata_config("FULL_LOAD_FINISHED")


task: Task = Utils.read_task_pickle()
credentials = Utils.read_credentials()

target_endpoint = EndpointFactory.create_endpoint(**credentials.get("target_endpoint"))
task.add_endpoint(target_endpoint)

if current_replication_type == "full_load" and not full_load_finished:
    task.execute_target_full_load()
if current_replication_type == "cdc":
    task.execute_target_cdc()

task.clean_endpoints()  # TODO ver se vale a pena deixar sempre ligado

Utils.write_task_pickle(task)
