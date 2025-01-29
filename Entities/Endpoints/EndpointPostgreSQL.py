from Entities.Endpoints.Endpoint import Endpoint, EndpointType, DatabaseType
from Entities.Shared.Queries import PostgreSQLQueries
from Entities.Tables.Table import Table
from Entities.Columns.Column import Column
from psycopg2 import sql
from psycopg2.extras import execute_values
from time import time
import psycopg2
import pandas as pd

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
        
        cursor.close()

        primary_keys = self.get_table_primary_key(table)
        
        table = self.get_table_columns(table, primary_keys)
        
        return table
    
    def get_table_primary_key(self, table: Table) -> list:
        cursor = self.cursor()

        cursor.execute(PostgreSQLQueries.GET_TABLE_PRIMARY_KEY, (table.schema_name, table.table_name))
        primary_keys = [row[0] for row in cursor.fetchall()]

        cursor.close()
        return primary_keys
    
    def get_table_columns(self, table: Table, primary_keys: list) -> Table:
        cursor = self.cursor()

        cursor.execute(PostgreSQLQueries.GET_TABLE_COLUMNS, (table.schema_name, table.table_name))
        for row in cursor.fetchall():
            is_primary_key = True if row[2] in primary_keys else False
            nullable = True if row[6] == 'YES' else False
            table.columns.append(Column(name=row[2],
                                       data_type=row[3],
                                       udt_name=row[4],
                                       character_maximum_length=row[5],
                                       nullable=nullable,
                                       ordinal_position=row[7],
                                       is_primary_key=is_primary_key))

        cursor.close()

        return table
    
    def get_full_load_from_table(self, schema: str, table: str) -> dict:
        initial_time = time()
        cursor = self.cursor()

        cursor.execute(PostgreSQLQueries.GET_FULL_LOAD_FROM_TABLE.format(schema = schema, table = table))
        data = cursor.fetchall()
        cursor.close()

        df = pd.DataFrame(data, columns=[desc[0] for desc in cursor.description])
        df.to_csv(f'{self.PATH_FULL_LOAD_STAGING_AREA}{schema}_{table}.csv', index=False)

        end_time = time()
        return {
            'rowcount': cursor.rowcount,
            'rownumber': cursor.rownumber,
            'statusmessage': cursor.statusmessage,
            'file_path': f'{self.PATH_FULL_LOAD_STAGING_AREA}{schema}_{table}.csv',
            'time_elapsed': '{:.2f}'.format(end_time - initial_time)
        }
        
    def insert_full_load_into_table(self, target_schema_name: str, target_table_name: str,
                            path_data: str, source_table: Table,
                            create_table_if_not_exists: bool, recreate_table_if_exists: bool, truncate_before_insert: bool) -> dict:
        """
        Realiza uma carga completa de dados (full load) em uma tabela de destino.
        """
        initial_time = time()
        cursor = self.cursor()
        try:
            df = pd.read_csv(path_data)

            self._manage_table(
                cursor=cursor,
                schema_name=target_schema_name,
                table_name=target_table_name,
                source_table=source_table,
                create_if_not_exists=create_table_if_not_exists,
                recreate_if_exists=recreate_table_if_exists,
                truncate_before_insert=truncate_before_insert
            )

            self._insert_data(
                cursor=cursor,
                schema_name=target_schema_name,
                table_name=target_table_name,
                data=df
            )

            self.commit()
            end_time = time()
            return {'message': 'Full load data inserted successfully', 'success': True, 'time_elapsed': '{:.2f}'.format(end_time - initial_time)}
        except Exception as e:
            self.rollback()
            return {'message': str(e), 'success': False}
        finally:
            cursor.close()

    def _manage_table(self, cursor: psycopg2.extensions.cursor, schema_name: str, table_name: str,
                    source_table: Table, create_if_not_exists: bool,
                    recreate_if_exists: bool, truncate_before_insert: bool):
        """
        Gerencia a tabela de destino: drop, create e truncate, conforme necessário.
        """
        if recreate_if_exists:
            create_if_not_exists = True
            cursor.execute(PostgreSQLQueries.DROP_TABLE.format(schema=schema_name, table=table_name))
        
        if create_if_not_exists:
            cursor.execute(PostgreSQLQueries.CHECK_TABLE_EXISTS, (schema_name, table_name))
            if cursor.fetchone()[0] == 0:
                target_table = source_table.copy()
                target_table.schema_name = schema_name
                target_table.table_name = table_name
                create_table_sql = target_table.mount_create_table()
                cursor.execute(create_table_sql)

        if truncate_before_insert:
            cursor.execute(PostgreSQLQueries.TRUNCATE_TABLE.format(schema=schema_name, table=table_name))

    def _insert_data(self, cursor, schema_name: str, table_name: str, data: pd.DataFrame):
        """
        Insere os dados no banco de forma eficiente usando execute_values.
        """
        query = sql.SQL(PostgreSQLQueries.INSERT_FULL_LOAD_DATA.format(
            schema=schema_name, table=table_name, columns=', '.join(data.columns)
        ))

        # Converter DataFrame para lista de tuplas
        records = [tuple(row) for row in data.itertuples(index=False, name=None)]
        execute_values(cursor, query, records)