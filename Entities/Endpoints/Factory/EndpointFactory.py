from Entities.Endpoints.Endpoint import DatabaseType, EndpointType
from Entities.Endpoints.EndpointPostgreSQL import EndpointPostgreSQL
import logging


class EndpointFactory:
    @staticmethod
    def create_endpoint(database_type: DatabaseType, endpoint_type: EndpointType, endpoint_name: str, credentials: dict):
        """
        Cria um endpoint de acordo com o tipo de banco de dados e credenciais fornecidos.

        Args:
            database_type (DatabaseType): Tipo do banco de dados.
            endpoint_type (EndpointType): Tipo do endpoint (fonte ou destino).
            endpoint_name (str): Nome do endpoint.
            credentials (dict): Credenciais do banco de dados.

        Returns:
            Endpoint: Inst ncia do endpoint criado.

        Raises:
            ValueError: Se o tipo do banco de dados for inválido.
        """

        logging.info(f"ENDPOINT FACTORY - Conectando ao banco de dados {endpoint_name} como {endpoint_type.name}")
        if database_type == DatabaseType.POSTGRESQL:
            return EndpointPostgreSQL(endpoint_type, endpoint_name, credentials)
        else:
            raise ValueError(f"Database type {database_type} inválido")
