from Entities.Shared.Types import *
from dotenv import load_dotenv
import os

load_dotenv()

TASK = {
    "task": {
        "task_name": "fl-employees",
        "replication_type": TaskType.FULL_LOAD,
        "create_table_if_not_exists": True,
        "truncate_before_insert": True,
        "recreate_table_if_exists": True
    },
    "tables": [
        {"schema_name": "employees", "table_name": "salary", "priority": 1},
        {"schema_name": "employees", "table_name": "employee", "priority": 0}
    ],
    "transformations": [
        {
            "table_info": {
                "schema_name": "employees",
                "table_name": "salary"
            },
            "settings": {
                "transformation_type": TransformationType.MODIFY_TABLE_NAME,
                "description": "Modificar nome da tabela adicionando o sufixo '_target'",
                "contract": {"target_table_name": "salary_target"}
            }
        }
    ],
    "source_endpoint": {
        "database_type": DatabaseType.POSTGRESQL,
        "endpoint_type": EndpointType.SOURCE,
        "endpoint_name": "Source_PostgreSQL",
        "credentials": {
            "dbname": os.getenv("DB_NAME_POSTGRESQL_SOURCE"),
            "user": os.getenv("DB_USER_POSTGRESQL"),
            "password": os.getenv("DB_PASSWORD_POSTGRESQL"),
            "host": os.getenv("DB_HOST_POSTGRESQL"),
            "port": os.getenv("DB_PORT_POSTGRESQL")
        }
    },
    "target_endpoint": {
        "database_type": DatabaseType.POSTGRESQL,
        "endpoint_type": EndpointType.TARGET,
        "endpoint_name": "Target_PostgreSQL",
        "credentials": {
            "dbname": os.getenv("DB_NAME_POSTGRESQL_TARGET"),
            "user": os.getenv("DB_USER_POSTGRESQL"),
            "password": os.getenv("DB_PASSWORD_POSTGRESQL"),
            "host": os.getenv("DB_HOST_POSTGRESQL"),
            "port": os.getenv("DB_PORT_POSTGRESQL")
        }
    }
}