from Entities.Endpoints.Endpoint import DatabaseType, EndpointType
from Entities.Endpoints.EndpointPostgreSQL import EndpointPostgreSQL


class EndpointFactory:
    @staticmethod
    def create_endpoint(database_type: DatabaseType, endpoint_type: EndpointType, endpoint_name: str, credentials: dict):
        if database_type == DatabaseType.POSTGRESQL:
            return EndpointPostgreSQL(endpoint_type, endpoint_name, credentials)
        else:
            raise ValueError(f"Unsupported database type: {database_type}")
