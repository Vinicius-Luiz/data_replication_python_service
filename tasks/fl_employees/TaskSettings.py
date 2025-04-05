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
        {
            "schema_name": "employees",
            "table_name": "salary",
            "priority": PriorityType.HIGH,
        },
        {
            "schema_name": "employees",
            "table_name": "employee",
            "priority": PriorityType.NORMAL,
        },
        {
            "schema_name": "employees",
            "table_name": "department_manager",
            "priority": PriorityType.VERY_HIGH,
        },
    ],
    "filters": [
        {
            "table_info": {
                "schema_name": "employees",
                "table_name": "department_manager",
            },
            "settings": {
                "filter_type": FilterType.DATE_EQUALS,
                "description": "Filtrando data de inicio igual a 9999-01-01",
                "column_name": "to_date",
                "value": "9999-01-01",
            },
        },
        {
            "table_info": {
                "schema_name": "employees",
                "table_name": "employee",
            },
            "settings": {
                "filter_type": FilterType.EQUALS,
                "description": "Filtrando gênero feminino",
                "column_name": "gender",
                "value": "F",
            },
        },
    ],
    "transformations": [
        {
            "table_info": {"schema_name": "employees", "table_name": "employee"},
            "settings": {
                "transformation_type": TransformationType.MODIFY_TABLE_NAME,
                "description": "Alterando nome da tabela para considerar somente funcionários do gênero feminino",
                "contract": {"target_table_name": "employee_female"},
                "priority": PriorityType.VERY_HIGH,
            },
        },
        {
            "table_info": {"schema_name": "employees", "table_name": "salary"},
            "settings": {
                "transformation_type": TransformationType.MODIFY_TABLE_NAME,
                "description": "Alterando nome da tabela para português",
                "contract": {"target_table_name": "salario"},
                "priority": PriorityType.VERY_HIGH,
            },
        },
        {
            "table_info": {"schema_name": "employees", "table_name": "salary"},
            "settings": {
                "transformation_type": TransformationType.MODIFY_SCHEMA_NAME,
                "description": "Alterando nome do schema para português",
                "contract": {"target_schema_name": "funcionarios"},
                "priority": PriorityType.VERY_HIGH,
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
                "priority": PriorityType.NORMAL,
            },
        },
        {
            "table_info": {"schema_name": "employees", "table_name": "salary"},
            "settings": {
                "transformation_type": TransformationType.MODIFY_COLUMN_NAME,
                "description": "Modificando 'amount' para 'quantia'",
                "contract": {"column_name": "amount", "target_column_name": "quantia"},
                "priority": PriorityType.HIGH,
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
                "priority": PriorityType.HIGH,
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
                "priority": PriorityType.HIGH,
            },
        },
        {
            "table_info": {"schema_name": "employees", "table_name": "salary"},
            "settings": {
                "transformation_type": TransformationType.CREATE_COLUMN,
                "description": "Obter diferença de anos entre o data_inicio e data_fim do salario",
                "contract": {
                    "operation": TransformationOperationType.DATE_DIFF_YEARS,
                    "new_column_name": "periodo_anos",
                    "depends_on": ["from_date", "to_date"],
                    "round_result": True,
                },
                "priority": PriorityType.NORMAL,
            },
        },
        {
            "table_info": {"schema_name": "employees", "table_name": "salary"},
            "settings": {
                "transformation_type": TransformationType.MODIFY_COLUMN_VALUE,
                "description": "Obter salário mensal",
                "contract": {
                    "operation": TransformationOperationType.MATH_EXPRESSION,
                    "column_name": "amount",
                    "expression": "value / 12",
                },
                "priority": PriorityType.NORMAL,
            },
        },
        {
            "table_info": {"schema_name": "employees", "table_name": "employee"},
            "settings": {
                "transformation_type": TransformationType.MODIFY_COLUMN_VALUE,
                "description": "Formata coluna 'first_name' com tudo em maiúsculo",
                "contract": {
                    "operation": TransformationOperationType.UPPERCASE,
                    "column_name": "first_name",
                },
                "priority": PriorityType.NORMAL,
            },
        },
        {
            "table_info": {"schema_name": "employees", "table_name": "employee"},
            "settings": {
                "transformation_type": TransformationType.MODIFY_COLUMN_VALUE,
                "description": "Formata coluna 'last_name' com tudo em maiúsculo",
                "contract": {
                    "operation": TransformationOperationType.UPPERCASE,
                    "column_name": "last_name",
                },
                "priority": PriorityType.NORMAL,
            },
        },
        {
            "table_info": {"schema_name": "employees", "table_name": "employee"},
            "settings": {
                "transformation_type": TransformationType.CREATE_COLUMN,
                "description": "Cria coluna 'full_name'",
                "contract": {
                    "operation": TransformationOperationType.CONCAT,
                    "new_column_name": "full_name",
                    "depends_on": ["first_name", "last_name"],
                    "separator": " ",
                },
                "priority": PriorityType.NORMAL,
            },
        },
        {
            "table_info": {"schema_name": "employees", "table_name": "employee"},
            "settings": {
                "transformation_type": TransformationType.CREATE_COLUMN,
                "description": "Carimbar como python quem realizou a replicação dos dados",
                "contract": {
                    "operation": TransformationOperationType.LITERAL,
                    "new_column_name": "updated_by",
                    "value": "PYTHON",
                },
                "priority": PriorityType.NORMAL,
            },
        },
        {
            "table_info": {"schema_name": "employees", "table_name": "employee"},
            "settings": {
                "transformation_type": TransformationType.CREATE_COLUMN,
                "description": "Carimbar data de sincronização",
                "contract": {
                    "operation": TransformationOperationType.DATE_NOW,
                    "new_column_name": "sync_date",
                },
                "priority": PriorityType.NORMAL,
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
