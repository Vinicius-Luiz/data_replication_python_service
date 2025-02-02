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