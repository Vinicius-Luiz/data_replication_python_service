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

class TransformationOperationType(Enum):
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

class FilterType(Enum):
    EQUALS = 'equals'
    NOT_EQUALS = 'not_equals'
    GREATER_THAN = 'greater_than'
    GREATER_THAN_OR_EQUAL = 'greater_than_or_equal'
    LESS_THAN = 'less_than'
    LESS_THAN_OR_EQUAL = 'less_than_or_equal'
    IN = 'in'
    NOT_IN = 'not_in'
    IS_NULL = 'is_null'
    IS_NOT_NULL = 'is_not_null'
    STARTS_WITH = 'starts_with'
    ENDS_WITH = 'ends_with'
    CONTAINS = 'contains'
    NOT_CONTAINS = 'not_contains'
    BETWEEN = 'between'
    NOT_BETWEEN = 'not_between'
    DATE_EQUALS = 'date_equals'
    DATE_NOT_EQUALS = 'date_not_equals'
    DATE_GREATER_THAN = 'date_greater_than'
    DATE_GREATER_THAN_OR_EQUAL = 'date_greater_than_or_equal'
    DATE_LESS_THAN = 'date_less_than'
    DATE_LESS_THAN_OR_EQUAL = 'date_less_than_or_equal'
    DATE_BETWEEN = 'date_between'
    DATE_NOT_BETWEEN = 'date_not_between'

class PriorityType(Enum):
    VERY_HIGH = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    VERY_LOW = 4