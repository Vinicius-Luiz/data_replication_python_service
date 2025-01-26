from enum import Enum

class DatabaseType(Enum):
    POSTGRESQL = 'PostgreSQL'

class EndpointType(Enum):
    SOURCE = 'Source'
    TARGET = 'Target'