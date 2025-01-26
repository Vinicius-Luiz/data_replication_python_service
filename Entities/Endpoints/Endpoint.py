from abc import ABC, abstractmethod
from Entities.Endpoints.Decorators.EndpointDecorators import *
from Entities.Shared.Types import DatabaseType, EndpointType

class Endpoint(ABC):
    PATH_FULL_LOAD_STAGING_AREA = 'data/full_load_data/'
    PATH_CDC_STAGING_AREA       = 'data/cdc_data/'
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
    
    @abstractmethod
    @source_method
    def get_schemas(self) -> list:
        pass

    @abstractmethod
    @source_method
    def get_tables(self, schema: str) -> list:
        pass

    @abstractmethod
    @source_method
    def get_table_details(self, schema: str, table: str) -> list:
        pass

    @abstractmethod
    @source_method
    def get_table_primary_key(self, schema: str, table: str) -> list:
        pass

    @abstractmethod
    @source_method
    def get_table_columns(self, schema: str, table: str) -> list:
        pass

    @abstractmethod
    @source_method
    def get_full_load_from_table(self, schema: str, table: str) -> dict:
        pass