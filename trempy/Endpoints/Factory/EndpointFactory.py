from trempy.Endpoints.Databases.PostgreSQL.Endpoint import EndpointPostgreSQL
from trempy.Endpoints.Endpoint import Endpoint
from trempy.Endpoints.Endpoint import DatabaseType, EndpointType
from trempy.Loggings.Logging import ReplicationLogger
from trempy.Endpoints.Exceptions.Exception import *

logger = ReplicationLogger()


class EndpointFactory:
    @staticmethod
    def create_endpoint(
        database_type: str,
        endpoint_type: str,
        endpoint_name: str,
        credentials: dict,
        batch_cdc_size: int = 1000,
    ) -> Endpoint:
        """
        Cria um endpoint de acordo com o tipo de banco de dados e credenciais fornecidos.

        Args:
            database_type (str): Tipo do banco de dados.
            endpoint_type (str): Tipo do endpoint (fonte ou destino).
            endpoint_name (str): Nome do endpoint.
            credentials (dict): Credenciais do banco de dados.
            batch_cdc_size (int): Tamanho do lote para CDC.

        Returns:
            Endpoint: Inst ncia do endpoint criado.

        Raises:
            ValueError: Se o tipo do banco de dados for inválido.
        """

        database_type = DatabaseType(database_type)
        endpoint_type = EndpointType(endpoint_type)

        logger.info(
            f"ENDPOINT FACTORY - Conectando ao banco de dados {endpoint_name} como {endpoint_type.name}", required_types="full_load"
        )
        if database_type == DatabaseType.POSTGRESQL:
            return EndpointPostgreSQL(
                endpoint_type, endpoint_name, credentials, batch_cdc_size
            )
        else:
            e = InvalidDatabaseTypeError(
                "Tipo de banco de dados inválido", database_type
            )
            logger.critical(e)
