from enum import Enum
class DatabaseType(Enum):
    POSTGRESQL = 'PostgreSQL'

class EndpointType(Enum):
    SOURCE = 'Source'
    TARGET = 'Target'

class Endpoint:
    def __init__(self, database_type: DatabaseType, endpoint_type: EndpointType, endpoint_name: str, credentials: dict):
        self.database_type = database_type
        self.endpoint_type = endpoint_type
        self.endpoint_name = endpoint_name
        self.credentials = credentials

        self.validate()

        self.id = f'{self.database_type.name}_{self.endpoint_type.name}_{self.endpoint_name}'

    def validate(self) -> None:
        if self.database_type not in DatabaseType:
            raise ValueError('Invalid database type.')
        if self.endpoint_type not in EndpointType:
            raise ValueError('Invalid endpoint type.')
