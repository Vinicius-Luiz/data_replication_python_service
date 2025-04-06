from Entities.Endpoints.Factory.EndpointFactory import EndpointFactory
from Entities.Transformations.Transformation import Transformation
from Entities.Filters.Filter import Filter
from Entities.Tasks.Task import Task
from tasks.fl_employees.Credentials import credentials
from dotenv import load_dotenv
import logging
import json

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    filename="app.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

load_dotenv()

if __name__ == "__main__":
    with open(r"tasks\fl_employees\TaskSettings.json", "r", encoding="utf-8") as f:
        task_settings = json.load(f)

    endpoint_source = EndpointFactory.create_endpoint(
        **credentials.get("source_endpoint")
    )

    endpoint_target = EndpointFactory.create_endpoint(
        **credentials.get("target_endpoint")
    )

    task = Task(
        source_endpoint=endpoint_source,
        target_endpoint=endpoint_target,
        **task_settings.get("task")
    )

    task.add_tables(task_settings.get("tables"))

    for filter in task_settings.get("filters"):
        filter_config = Filter(**filter.get("settings"))

        schema_name = filter.get("table_info").get("schema_name")
        table_name = filter.get("table_info").get("table_name")

        task.add_filter(
            schema_name=schema_name, table_name=table_name, filter=filter_config
        )

    for transformation in task_settings.get("transformations"):
        transformation_config = Transformation(**transformation.get("settings"))

        schema_name = transformation.get("table_info").get("schema_name")
        table_name = transformation.get("table_info").get("table_name")

        task.add_transformation(
            schema_name=schema_name,
            table_name=table_name,
            transformation=transformation_config,
        )

    task.run()
