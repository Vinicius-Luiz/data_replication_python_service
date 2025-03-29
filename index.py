from Entities.Endpoints.Factory.EndpointFactory import EndpointFactory
from Entities.Transformations.Transformation import Transformation
from Entities.Tasks.Task import Task
from tasks.fl_employees import TaskSettings
from dotenv import load_dotenv
import logging

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(filename='app.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

if __name__ == "__main__":
    endpoint_source = EndpointFactory.create_endpoint(
        **TaskSettings.TASK.get('source_endpoint')
    )

    endpoint_target = EndpointFactory.create_endpoint(
        **TaskSettings.TASK.get('target_endpoint')
    )
  
    task = Task(source_endpoint = endpoint_source,
                target_endpoint = endpoint_target,
                **TaskSettings.TASK.get('task'))
    
    task.add_tables(TaskSettings.TASK.get('tables'))

    for transformation in TaskSettings.TASK.get('transformations'):
        transformation_config = Transformation(**transformation.get('settings'))
    
        schema_name = transformation.get('table_info').get('schema_name')
        table_name  = transformation.get('table_info').get('table_name')
        
        task.add_transformation(schema_name = schema_name,
                                table_name = table_name,
                                transformation = transformation_config)

    task.run()