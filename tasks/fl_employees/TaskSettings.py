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
        "recreate_table_if_exists": True,
    },
    "tables": [
        {"schema_name": "employees", "table_name": "salary", "priority": 1},
        {"schema_name": "employees", "table_name": "employee", "priority": 0},
    ],
    "transformations": [
        {
            "table_info": {"schema_name": "employees", "table_name": "salary"},
            "settings": {
                "transformation_type": TransformationType.MODIFY_TABLE_NAME,
                "description": "Alterando nome da tabela para português",
                "contract": {"target_table_name": "salario"},
                "priority": 0,
            },
        },
        {
            "table_info": {"schema_name": "employees", "table_name": "salary"},
            "settings": {
                "transformation_type": TransformationType.MODIFY_SCHEMA_NAME,
                "description": "Alterando nome do schema para português",
                "contract": {"target_schema_name": "funcionarios"},
                "priority": 1,
            },
        },
        {
            "table_info": {"schema_name": "employees", "table_name": "salary"},
            "settings": {
                "transformation_type": TransformationType.MODIFY_COLUMN_NAME,
                "description": "Modificando 'employee_id' para 'funcionario_id'",
                "contract": {
                    "column_name": "employee_id",
                    "target_column_name": "funcionario_id",
                },
                "priority": 2,
            },
        },
        {
            "table_info": {"schema_name": "employees", "table_name": "salary"},
            "settings": {
                "transformation_type": TransformationType.MODIFY_COLUMN_NAME,
                "description": "Modificando 'amount' para 'quantia'",
                "contract": {"column_name": "amount", "target_column_name": "quantia"},
                "priority": 3,
            },
        },
        {
            "table_info": {"schema_name": "employees", "table_name": "salary"},
            "settings": {
                "transformation_type": TransformationType.MODIFY_COLUMN_NAME,
                "description": "Modificando 'from_date' para 'data_inicio'",
                "contract": {
                    "column_name": "from_date",
                    "target_column_name": "data_inicio",
                },
                "priority": 4,
            },
        },
        {
            "table_info": {"schema_name": "employees", "table_name": "salary"},
            "settings": {
                "transformation_type": TransformationType.MODIFY_COLUMN_NAME,
                "description": "Modificando 'to_date' para 'data_fim'",
                "contract": {
                    "column_name": "to_date",
                    "target_column_name": "data_fim",
                },
                "priority": 5,
            },
        },
        {
            "table_info": {"schema_name": "employees", "table_name": "employee"},
            "settings": {
                "transformation_type": TransformationType.MODIFY_COLUMN_VALUE,
                "description": "Formata coluna 'first_name' com tudo em maiúsculo",
                "contract": {
                    "column_name": "first_name",
                    "operation": "uppercase"
                },
                "priority": 6,
            },
        },
        {
            "table_info": {"schema_name": "employees", "table_name": "employee"},
            "settings": {
                "transformation_type": TransformationType.MODIFY_COLUMN_VALUE,
                "description": "Formata coluna 'last_name' com tudo em maiúsculo",
                "contract": {
                    "column_name": "last_name",
                    "operation": "uppercase"
                },
                "priority": 7,
            },
        },
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
            "port": os.getenv("DB_PORT_POSTGRESQL"),
        },
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
            "port": os.getenv("DB_PORT_POSTGRESQL"),
        },
    },
}
