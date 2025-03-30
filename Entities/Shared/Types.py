from enum import Enum

class DatabaseType(Enum):
    POSTGRESQL = 'PostgreSQL'

class EndpointType(Enum):
    SOURCE = 'Source'
    TARGET = 'Target'

class TaskType(Enum):
    FULL_LOAD = 'Full Load'
    CDC = 'CDC'
    FULL_LOAD_CDC = 'Full Load e CDC'

class TransformationType(Enum):
    CREATE_COLUMN = 'Criar coluna'
    MODIFY_SCHEMA_NAME = 'Modificar nome do schema da tabela'
    MODIFY_TABLE_NAME = 'Modificar nome da tabela'
    MODIFY_COLUMN_NAME = 'Modificar nome de coluna'
    MODIFY_COLUMN_VALUE = 'Modificar valores da coluna'