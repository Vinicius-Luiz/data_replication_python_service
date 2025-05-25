from trempy.Loggings.Logging import ReplicationLogger
from trempy.Endpoints.Exceptions.Exception import *
import psycopg2

logger = ReplicationLogger()


class ConnectionManager:
    """Responsabilidade: Gerenciar a conexão com o banco de dados PostgreSQL."""

    def __init__(self, credentials: dict):
        try:
            temp_credentials = credentials.copy()
            self.connection = self.__connect(temp_credentials)
        finally:
            del temp_credentials

        logger.debug(self.connection.get_dsn_parameters(), required_types=["full_load"])

    def __connect(self, credentials: dict) -> psycopg2.extensions.connection:
        """
        Estabelece uma conexão com o banco de dados.

        Args:
            credentials (dict): Credenciais do banco de dados.

        Returns:
            psycopg2.extensions.connection: Conexão estabelecida com o banco de dados.

        Raises:
            EndpointError: Se houver um erro ao conectar ao banco de dados.
        """

        try:
            return psycopg2.connect(**credentials)
        except Exception as e:
            e = EndpointError(f"Erro ao conectar ao banco de dados: {e}")
            logger.critical(e)

    def cursor(self) -> psycopg2.extensions.cursor:
        """
        Retorna um cursor para a conexão atual do banco de dados.

        Returns:
            psycopg2.extensions.cursor: Cursor para a conexão atual do banco de dados.
        """

        return self.connection.cursor()

    def close(self) -> None:
        """Fecha a conexão com o banco de dados."""

        self.connection.close()

    def commit(self) -> None:
        """Faz commit na transa o atual."""

        self.connection.commit()

    def rollback(self) -> None:
        """Desfaz as altera es realizadas na transa o atual."""

        self.connection.rollback()
