from enum import Enum

class DatabaseType(Enum):
    POSTGRESQL = 'postgresql'

class EndpointType(Enum):
    SOURCE = 'source'
    TARGET = 'target'

class TaskType(Enum):
    FULL_LOAD = 'full_load'
    CDC = 'cdc'

class CdcModeType(Enum):
    DEFAULT = 'default'
    UPSERT = 'upsert'
    SCD2 = 'scd2'

class TransformationType(Enum):
    CREATE_COLUMN = 'create_column'
    MODIFY_SCHEMA_NAME = 'modify_schema_name'
    MODIFY_TABLE_NAME = 'modify_table_name'
    MODIFY_COLUMN_NAME = 'modify_column_name'
    MODIFY_COLUMN_VALUE = 'modify_column_value'
    ADD_PRIMARY_KEY = 'add_primary_key'
    REMOVE_PRIMARY_KEY = 'remove_primary_key'

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