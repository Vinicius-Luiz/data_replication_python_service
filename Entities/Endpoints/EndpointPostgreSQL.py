from Entities.Endpoints.Endpoint import Endpoint, EndpointType, DatabaseType
from Entities.Shared.Queries import PostgreSQLQueries
from Entities.Tables.Table import Table
from Entities.Columns.Column import Column
from psycopg2 import sql
from psycopg2.extras import execute_values
from time import time
import pandas as pd
import psycopg2
import logging
import os

class EndpointPostgreSQL(Endpoint):

    def __init__(self, endpoint_type: EndpointType, endpoint_name: str, credentials: dict, periodicity_in_seconds_of_reading_from_source: int = 10):
        """
        Inicializa um endpoint para PostgreSQL, gerenciando a conexão.

        Args:
            endpoint_type (EndpointType): Tipo do endpoint (fonte ou destino).
            endpoint_name (str): Nome do endpoint.
            credentials (dict): Credenciais do banco de dados.
        """
        # Inicializa o Endpoint
        super().__init__(DatabaseType.POSTGRESQL, endpoint_type, endpoint_name, periodicity_in_seconds_of_reading_from_source)

        try:
            # Salva temporariamente as credenciais para tentar conectar
            temp_credentials = credentials.copy()
            self.connection = self.connect(temp_credentials)
        finally:
            del temp_credentials  # Remove as credenciais para evitar exposição

    def connect(self, credentials: dict) -> psycopg2.extensions.connection:
        """Estabelece a conexão com o PostgreSQL."""
        try:
            return psycopg2.connect(**credentials)
        except Exception as e:
            logging.critical(f"Erro ao conectar ao banco de dados: {e}")
            raise ValueError(f"Erro ao conectar ao banco de dados: {e}")

    def cursor(self) -> psycopg2.extensions.cursor:
        """Retorna um cursor para a conexão atual."""
        return self.connection.cursor()

    def close(self) -> None:
        """Fecha a conexão com o banco de dados."""
        self.connection.close()

    def commit(self) -> None:
        """Realiza um commit na transação atual."""
        self.connection.commit()

    def rollback(self) -> None:
        """Faz rollback na transação atual."""
        self.connection.rollback()
    
    def get_schemas(self) -> list:
        """Obtém os esquemas disponíveis no banco de dados."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(PostgreSQLQueries.GET_SCHEMAS)
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logging.critical(f"Erro ao obter os esquemas: {e}")
            return []

    def get_tables(self, schema: str) -> list:
        """Obtém as tabelas de um esquema específico."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(PostgreSQLQueries.GET_TABLES, (schema,))
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logging.critical(f"Erro ao obter as tabelas: {e}")
            return []

    def get_table_details(self, schema: str, table: str) -> Table:
        """Obtém os detalhes de uma tabela específica."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(PostgreSQLQueries.GET_TABLE_DETAILS, (schema, table))
                metadata_table = cursor.fetchone()
                if not metadata_table:
                    raise ValueError(f"Tabela {schema}.{table} não encontrada.")

                table_obj = Table(
                    schema_name=metadata_table[0],
                    table_name=metadata_table[1],
                    estimated_row_count=metadata_table[2],
                    table_size=metadata_table[3]
                )

                primary_keys = self.get_table_primary_key(table_obj)
                return self.get_table_columns(table_obj, primary_keys)
        except Exception as e:
            logging.critical(f"Erro ao obter os detalhes da tabela: {e}")
            raise ValueError(f"Erro ao obter os detalhes da tabela: {e}")

    def get_table_primary_key(self, table: Table) -> list:
        """Obtém as chaves primárias de uma tabela."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(PostgreSQLQueries.GET_TABLE_PRIMARY_KEY, (table.schema_name, table.table_name))
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logging.critical(f"Erro ao obter as chaves primárias da tabela: {e}")
            raise ValueError(f"Erro ao obter as chaves primárias da tabela: {e}")

    def get_table_columns(self, table: Table, primary_keys: list) -> Table:
        """Obtém as colunas de uma tabela e adiciona ao objeto Table."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(PostgreSQLQueries.GET_TABLE_COLUMNS, (table.schema_name, table.table_name))
                for row in cursor.fetchall():
                    is_primary_key = row[2] in primary_keys
                    nullable = row[6] == 'YES'
                    table.columns.append(Column(
                        name=row[2],
                        data_type=row[3],
                        udt_name=row[4],
                        character_maximum_length=row[5],
                        nullable=nullable,
                        ordinal_position=row[7],
                        is_primary_key=is_primary_key
                    ))
            return table
        except Exception as e:
            logging.critical(f"Erro ao obter as colunas da tabela: {e}")
            raise ValueError(f"Erro ao obter as colunas da tabela: {e}")

    def get_full_load_from_table(self, schema: str, table: str) -> dict:
        """Realiza um full load de uma tabela e salva como CSV."""
        try:
            initial_time = time()
            file_path = f'{self.PATH_FULL_LOAD_STAGING_AREA}{schema}_{table}.csv'

            with self.connection.cursor() as cursor:
                cursor.execute(PostgreSQLQueries.GET_FULL_LOAD_FROM_TABLE.format(schema=schema, table=table))
                data = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

                df = pd.DataFrame(data, columns=columns)
                df.to_csv(file_path, index=False)

                return {
                    'success': True,
                    'rowcount': cursor.rowcount,
                    'statusmessage': cursor.statusmessage,
                    'file_path': file_path,
                    'time_elapsed': f"{time() - initial_time:.2f}s"
                }
        except Exception as e:
            logging.critical(f"Erro ao obter a carga completa da tabela: {e}")
            raise ValueError(f"Erro ao obter a carga completa da tabela: {e}")

    def insert_full_load_into_table(self, target_schema: str, target_table: str, source_table: Table,
                                    create_table_if_not_exists: bool, recreate_table_if_exists: bool, truncate_before_insert: bool) -> dict:
        """Insere dados completos em uma tabela de destino."""
        try:
            initial_time = time()
            path_data = f'{self.PATH_FULL_LOAD_STAGING_AREA}{source_table.schema_name}_{source_table.table_name}.csv'
            df = pd.read_csv(path_data)

            with self.connection.cursor() as cursor:
                self._manage_table(cursor, target_schema, target_table, source_table,
                                   create_table_if_not_exists, recreate_table_if_exists, truncate_before_insert)

                self._insert_data(cursor, target_schema, target_table, df)

                self.commit()
                os.remove(path_data)

                return {'message': 'Full load data inserted successfully', 'success': True, 'time_elapsed': f"{time() - initial_time:.2f}s"}
        except Exception as e:
            self.rollback()
            logging.critical(f"Erro ao inserir dados na tabela: {e}")
            raise ValueError(f"Erro ao inserir dados na tabela: {e}")
        
    def _manage_table(
        self,
        cursor: psycopg2.extensions.cursor,
        schema_name: str,
        table_name: str,
        source_table: Table,
        create_if_not_exists: bool,
        recreate_if_exists: bool,
        truncate_before_insert: bool
    ) -> None:
        """
        Gerencia a estrutura da tabela no banco de dados, incluindo criação, recriação e truncamento.
        
        Args:
            cursor (psycopg2.extensions.cursor): Cursor do banco de dados para execução de comandos SQL.
            schema_name (str): Nome do schema onde a tabela está localizada.
            table_name (str): Nome da tabela a ser gerenciada.
            source_table (Table): Objeto representando a estrutura da tabela de origem.
            create_if_not_exists (bool): Se True, cria a tabela caso ela não exista.
            recreate_if_exists (bool): Se True, recria a tabela caso ela já exista.
            truncate_before_insert (bool): Se True, trunca a tabela antes da inserção dos dados.
        """
        try:
            if recreate_if_exists:
                create_if_not_exists = True
                logging.info(f"Removendo a tabela {schema_name}.{table_name}")
                cursor.execute(PostgreSQLQueries.DROP_TABLE.format(schema=schema_name, table=table_name))
            
            if create_if_not_exists:
                cursor.execute(PostgreSQLQueries.CHECK_TABLE_EXISTS, (schema_name, table_name))
                if cursor.fetchone()[0] == 0:
                    target_table = source_table.copy()
                    target_table.schema_name = schema_name
                    target_table.table_name = table_name
                    create_table_sql = target_table.mount_create_table()
                    logging.info(f"Criando a tabela {schema_name}.{table_name}")
                    cursor.execute(create_table_sql)
            
            if truncate_before_insert:
                logging.info(f"Truncando a tabela {schema_name}.{table_name}")
                cursor.execute(PostgreSQLQueries.TRUNCATE_TABLE.format(schema=schema_name, table=table_name))
        except Exception as e:
            logging.error(f"Erro ao gerenciar a tabela {schema_name}.{table_name}: {e}")
            raise

    def _insert_data(
        self,
        cursor: psycopg2.extensions.cursor,
        schema_name: str,
        table_name: str,
        data: pd.DataFrame
    ) -> None:
        """
        Insere dados de um DataFrame em uma tabela do banco de dados usando o comando COPY.
        
        Args:
            cursor (psycopg2.extensions.cursor): Cursor do banco de dados para execução de comandos SQL.
            schema_name (str): Nome do schema onde a tabela está localizada.
            table_name (str): Nome da tabela onde os dados serão inseridos.
            data (pd.DataFrame): DataFrame contendo os dados a serem inseridos.
        """
        try:
            query = sql.SQL(PostgreSQLQueries.INSERT_FULL_LOAD_DATA.format(
                schema=schema_name, table=table_name, columns=', '.join(data.columns)
            ))
            
            # Converter DataFrame para lista de tuplas
            records = [tuple(row) for row in data.itertuples(index=False, name=None)]
            
            logging.info(f"Inserindo {len(records)} registros na tabela {schema_name}.{table_name}")
            execute_values(cursor, query, records)
        except Exception as e:
            logging.error(f"Erro ao inserir dados na tabela {schema_name}.{table_name}: {e}")
            raise
