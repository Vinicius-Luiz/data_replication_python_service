from Entities.Endpoints.Endpoint import Endpoint, EndpointType, DatabaseType
from Entities.Shared.Queries import PostgreSQLQueries
import psycopg2

class EndpointPostgreSQL(Endpoint):

    def __init__(self, endpoint_type: EndpointType, endpoint_name: str, credentials: dict):
        super().__init__(DatabaseType.POSTGRESQL, endpoint_type, endpoint_name, credentials)

        self.connection = self.connect()
        del self.credentials  # Remove credenciais após a conexão por segurança

    def connect(self) -> psycopg2.extensions.connection:
        """Realiza a conexão com o banco PostgreSQL."""
        connection = psycopg2.connect(**self.credentials)
        return connection

    def cursor(self) -> psycopg2.extensions.cursor:
        """Obtém um cursor da conexão."""
        return self.connection.cursor()

    def close(self) -> None:
        """Fecha a conexão com o banco."""
        self.connection.close()

    def commit(self) -> None:
        """Confirma as alterações no banco."""
        self.connection.commit()

    def rollback(self) -> None:
        """Desfaz as alterações no banco."""
        self.connection.rollback()

    def get_schemas(self) -> list:
        """Obtém os schemas do banco de dados."""
        cursor = self.cursor()
        cursor.execute(PostgreSQLQueries.GET_SCHEMAS)
        data = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return data
    
    def get_tables(self, schema) -> list:
        """Obtém as tabelas de um schema do banco de dados."""
        cursor = self.cursor()
        cursor.execute(PostgreSQLQueries.GET_TABLES, (schema,))
        data = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return data