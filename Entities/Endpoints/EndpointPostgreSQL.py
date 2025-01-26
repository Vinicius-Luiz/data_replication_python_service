from Entities.Endpoints.Endpoint import Endpoint, EndpointType, DatabaseType
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

    def cursor(self):
        """Obtém um cursor da conexão."""
        return self.connection.cursor()

    def close(self):
        """Fecha a conexão com o banco."""
        self.connection.close()

    def commit(self):
        """Confirma as alterações no banco."""
        self.connection.commit()

    def rollback(self):
        """Desfaz as alterações no banco."""
        self.connection.rollback()
