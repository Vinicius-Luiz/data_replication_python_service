from Entities.Endpoints.Factory.EndpointFactory import EndpointFactory
from Entities.Transformations.Transformation import Transformation
from Entities.Shared.Types import *
from Entities.Tasks.Task import Task
from dotenv import load_dotenv
import logging
import os

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(filename='app.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

if __name__ == "__main__":
    logging.info("Obtendo credenciais")
    dbname_source = os.getenv('DB_NAME_POSTGRESQL_SOURCE')
    dbname_target = os.getenv('DB_NAME_POSTGRESQL_TARGET')
    user = os.getenv('DB_USER_POSTGRESQL')
    password = os.getenv('DB_PASSWORD_POSTGRESQL')
    host = os.getenv('DB_HOST_POSTGRESQL')
    port = os.getenv('DB_PORT_POSTGRESQL')

    credentials_source = {
        'dbname': dbname_source,
        'user': user,
        'password': password,
        'host': host,
        'port': port
    }
    credentials_target = {
        'dbname': dbname_target,
        'user': user,
        'password': password,
        'host': host,
        'port': port
    }


    endpoint_source = EndpointFactory.create_endpoint(
        database_type=DatabaseType.POSTGRESQL,
        endpoint_type=EndpointType.SOURCE,
        endpoint_name='Source_PostgreSQL',
        credentials=credentials_source
    )

    endpoint_target = EndpointFactory.create_endpoint(
        database_type=DatabaseType.POSTGRESQL,
        endpoint_type=EndpointType.TARGET,
        endpoint_name='Target_PostgreSQL',
        credentials=credentials_target
    )
  
    task = Task(task_name = 'fl-employees',
                source_endpoint = endpoint_source,
                target_endpoint = endpoint_target,
                replication_type = TaskType.FULL_LOAD)
    
    table_names = [{'schema_name': 'employees', 'table_name': 'salary', 'priority': 1},
                   {'schema_name': 'employees', 'table_name': 'employee', 'priority': 0}]
    task.add_tables(table_names)

    modify_table_name = Transformation(
        transformation_type=TransformationType.MODIFY_TABLE_NAME,
        description='Modificar nome da tabela adicionando o sufixo "_target"',
        contract={'target_table_name': 'salary_target'})
    
    task.add_transformation(schema_name = 'employees', table_name = 'salary', transformation = modify_table_name)

    task.run()