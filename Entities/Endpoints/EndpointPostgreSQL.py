from Entities.Endpoints.Endpoint import Endpoint, EndpointType, DatabaseType
from Entities.Shared.Queries import PostgreSQLQueries
from Entities.Tables.Table import Table
from Entities.Columns.Column import Column
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
    
    def get_table_details(self, schema: str, table: str) -> Table:
        """Obtém os detalhes de uma tabela do banco de dados."""
        cursor = self.cursor()

        cursor.execute(PostgreSQLQueries.GET_TABLE_DETAILS, (schema, table))
        metadata_table = cursor.fetchone()
        table = Table(schema_name=metadata_table[0],
                      table_name=metadata_table[1],
                      estimated_row_count=metadata_table[2],
                      table_size=metadata_table[3])
        
        
        cursor.execute(PostgreSQLQueries.GET_TABLE_PRIMARY_KEY, (table.schema_name, table.table_name))
        primary_keys = [row[0] for row in cursor.fetchall()]
        
        cursor.execute(PostgreSQLQueries.GET_TABLE_COLUMNS, (table.schema_name, table.table_name))
        for row in cursor.fetchall():
            is_primary_key = True if row[2] in primary_keys else False
            table.columns.append(Column(name=row[2],
                                       data_type=row[3],
                                       udt_name=row[4],
                                       character_maximum_length=row[5],
                                       ordinal_position=row[6],
                                       is_primary_key=is_primary_key))
        
        cursor.close()
        return table
        
