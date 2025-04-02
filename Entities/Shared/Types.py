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

class OperationType(Enum):
    FORMAT_DATE = 'format_date'
    UPPERCASE = 'uppercase'
    LOWERCASE = 'lowercase'
    TRIM      = 'trim'
    EXTRACT_YEAR = 'extract_year'
    EXTRACT_MONTH = 'extract_month'
    EXTRACT_DAY = 'extract_day'
    MATH_EXPRESSION = 'math_expression'
    LITERAL = 'literal'
    DATE_NOW = 'date_now'
    CONCAT = 'concat'
    DATE_DIFF_YEARS = 'date_diff_years'

class PriorityType(Enum):
    VERY_HIGH = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    VERY_LOW = 4