from Entities.Endpoints.Endpoint import Endpoint, EndpointType, DatabaseType
from Entities.Shared.Queries import PostgreSQLQueries
from Entities.Tables.Table import Table
from Entities.Columns.Column import Column
from psycopg2 import sql
from psycopg2.extras import execute_values
from time import time
import polars as pl
import psycopg2
import logging
import os


class EndpointPostgreSQL(Endpoint):

    def __init__(
        self,
        endpoint_type: EndpointType,
        endpoint_name: str,
        credentials: dict,
        periodicity_in_seconds_of_reading_from_source: int = 10,
    ):
        """
        Inicializa um endpoint para PostgreSQL, gerenciando a conexão.

        Args:
            endpoint_type (EndpointType): Tipo do endpoint (fonte ou destino).
            endpoint_name (str): Nome do endpoint.
            credentials (dict): Credenciais do banco de dados.
        """
        super().__init__(
            DatabaseType.POSTGRESQL,
            endpoint_type,
            endpoint_name,
            periodicity_in_seconds_of_reading_from_source,
        )

        try:
            temp_credentials = credentials.copy()
            self.connection = self.connect(temp_credentials)
        finally:
            del temp_credentials

        logging.info(f"ENDPOINT - {endpoint_name} conectado")
        logging.debug(self.connection.get_dsn_parameters())

    def connect(self, credentials: dict) -> psycopg2.extensions.connection:
        """
        Estabelece uma conexão com o banco de dados.

        Args:
            credentials (dict): Credenciais do banco de dados.

        Returns:
            psycopg2.extensions.connection: Conexão estabelecida com o banco de dados.

        Raises:
            ValueError: Se houver um erro ao conectar ao banco de dados.
        """

        try:
            return psycopg2.connect(**credentials)
        except Exception as e:
            logging.critical(f"ENDPOINT - Erro ao conectar ao banco de dados: {e}")
            raise ValueError(f"ENDPOINT - Erro ao conectar ao banco de dados: {e}")

    def cursor(self) -> psycopg2.extensions.cursor:
        """
        Retorna um cursor para a conexão atual do banco de dados.

        Returns:
            psycopg2.extensions.cursor: Cursor para a conexão atual do banco de dados.
        """

        return self.connection.cursor()

    def close(self) -> None:
        """
        Fecha a conexão com o banco de dados.

        Returns:
            None
        """

        self.connection.close()

    def commit(self) -> None:
        """Faz commit na transa o atual."""

        self.connection.commit()

    def rollback(self) -> None:
        """Desfaz as altera es realizadas na transa o atual."""

        self.connection.rollback()

    def get_schemas(self) -> list:
        """Obtém a lista de esquemas do banco de dados.

        Returns:
            list: Lista de esquemas disponíveis no banco de dados.
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(PostgreSQLQueries.GET_SCHEMAS)
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logging.critical(f"ENDPOINT - Erro ao obter os esquemas: {e}")
            return []

    def get_tables(self, schema: str) -> list:
        """Obtém a lista de tabelas de um esquema específico.

        Args:
            schema (str): Nome do esquema.

        Returns:
            list: Lista de tabelas do esquema.
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(PostgreSQLQueries.GET_TABLES, (schema,))
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logging.critical(f"ENDPOINT - Erro ao obter as tabelas: {e}")
            return []

    def get_table_details(self, schema: str, table: str) -> Table:
        """
        Obtém os detalhes de uma tabela específica.

        Args:
            schema (str): Nome do esquema da tabela.
            table (str): Nome da tabela.

        Returns:
            Table: Objeto representando a estrutura da tabela.
        """

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
                    table_size=metadata_table[3],
                )

                primary_keys = self.get_table_primary_key(table_obj)
                return self.get_table_columns(table_obj, primary_keys)
        except Exception as e:
            logging.critical(f"ENDPOINT - Erro ao obter os detalhes da tabela: {e}")
            raise ValueError(f"ENDPOINT - Erro ao obter os detalhes da tabela: {e}")

    def get_table_primary_key(self, table: Table) -> list:
        """
        Obtém a lista de colunas que compõem a chave primária de uma tabela específica.

        Args:
            table (Table): Objeto representando a estrutura da tabela.

        Returns:
            list: Lista de colunas que compõem a chave primária da tabela.
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    PostgreSQLQueries.GET_TABLE_PRIMARY_KEY,
                    (table.schema_name, table.table_name),
                )
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logging.critical(
                f"ENDPOINT - Erro ao obter as chaves primárias da tabela: {e}"
            )
            raise ValueError(
                f"ENDPOINT - Erro ao obter as chaves primárias da tabela: {e}"
            )

    def get_table_columns(self, table: Table, primary_keys: list) -> Table:
        """
        Obtém as colunas de uma tabela específica e atualiza o objeto Table com
        os detalhes das colunas.

        Args:
            table (Table): Objeto representando a estrutura da tabela.
            primary_keys (list): Lista de colunas que compõem a chave primária da tabela.

        Returns:
            Table: Objeto Table atualizado com a estrutura das colunas da tabela.
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    PostgreSQLQueries.GET_TABLE_COLUMNS,
                    (table.schema_name, table.table_name),
                )
                for row in cursor.fetchall():
                    is_primary_key = row[2] in primary_keys
                    nullable = row[6] == "YES"
                    table.columns[row[2]] = Column(
                        name=row[2],
                        data_type=row[3],
                        udt_name=row[4],
                        character_maximum_length=row[5],
                        nullable=nullable,
                        ordinal_position=row[7],
                        is_primary_key=is_primary_key,
                    )
            return table
        except Exception as e:
            logging.critical(f"ENDPOINT - Erro ao obter as colunas da tabela: {e}")
            raise ValueError(f"ENDPOINT - Erro ao obter as colunas da tabela: {e}")

    def get_full_load_from_table(self, table: Table) -> dict:
        """
        Realiza a extra o completa dos dados de uma tabela.

        Args:
            table (Table): Objeto representando a estrutura da tabela.

        Returns:
            dict: Dicionário contendo o log de execução do método
        """

        try:
            initial_time = time()

            with self.connection.cursor() as cursor:
                cursor.execute(
                    PostgreSQLQueries.GET_FULL_LOAD_FROM_TABLE.format(
                        schema=table.schema_name, table=table.table_name
                    )
                )
                data = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

                df = pl.DataFrame(data, schema=columns, orient="row")
                df.write_parquet(table.path_data)

                return {
                    "success": True,
                    "rowcount": cursor.rowcount,
                    "statusmessage": cursor.statusmessage,
                    "path_data": table.path_data,
                    "time_elapsed": f"{time() - initial_time:.2f}s",
                }
        except Exception as e:
            logging.critical(f"ENDPOINT - Erro ao obter a carga completa: {e}")
            raise ValueError(f"ENDPOINT - Erro ao obter a carga completa: {e}")

    def insert_full_load_into_table(
        self,
        table: Table,
        create_table_if_not_exists: bool,
        recreate_table_if_exists: bool,
        truncate_before_insert: bool,
    ) -> dict:
        """
        Insere dados completos em uma tabela de destino.

        Args:
            table (Table): Objeto representando a estrutura da tabela.
            create_table_if_not_exists (bool): Se True, cria a tabela caso ela não exista.
            recreate_table_if_exists (bool): Se True, recria a tabela caso ela já exista.
            truncate_before_insert (bool): Se True, trunca a tabela antes da inserção dos dados.

        Returns:
            dict: Dicionário contendo o log de execução do método.
        """

        try:
            initial_time = time()

            with self.connection.cursor() as cursor:
                self._manage_table(
                    cursor,
                    table,
                    create_table_if_not_exists,
                    recreate_table_if_exists,
                    truncate_before_insert,
                )

                self._insert_data(cursor, table)

                self.commit()
                os.remove(table.path_data)

                return {
                    "message": "Full load data inserted successfully",
                    "success": True,
                    "time_elapsed": f"{time() - initial_time:.2f}s",
                }
        except Exception as e:
            self.rollback()
            logging.critical(f"ENDPOINT - Erro ao inserir dados: {e}")
            raise ValueError(f"ENDPOINT - Erro ao inserir dados: {e}")

    def _manage_table(
        self,
        cursor: psycopg2.extensions.cursor,
        table: Table,
        create_if_not_exists: bool,
        recreate_if_exists: bool,
        truncate_before_insert: bool,
    ) -> None:
        """
        Gerencia a tabela de destino antes de inserir os dados.

        Se for informado que a tabela deve ser recriada, remove a tabela se ela existir.
        Verifica se a tabela existe e a cria caso não exista.
        Trunca a tabela antes de inserir dados se for informado.

        Args:
            cursor (psycopg2.extensions.cursor): Cursor do banco de dados para execução de comandos SQL.
            table (Table): Objeto representando a estrutura da tabela de origem.
            create_if_not_exists (bool): Indica se a tabela deve ser criada caso não exista.
            recreate_if_exists (bool): Indica se a tabela deve ser recriada caso já exista.
            truncate_before_insert (bool): Indica se a tabela deve ser truncada antes da inserção dos dados.

        Returns:
            None: Este método não retorna valores.

        Raises:
            psycopg2.Error: Se ocorrer um erro durante a execução dos comandos SQL.
        """

        try:
            if recreate_if_exists:
                create_if_not_exists = True
                logging.info(
                    f"ENDPOINT - Removendo tabela {table.target_schema_name}.{table.target_table_name}"
                )
                cursor.execute(
                    PostgreSQLQueries.DROP_TABLE.format(
                        schema=table.target_schema_name, table=table.target_table_name
                    )
                )

            if create_if_not_exists:
                cursor.execute(
                    PostgreSQLQueries.CHECK_TABLE_EXISTS,
                    (table.target_schema_name, table.target_table_name),
                )
                if cursor.fetchone()[0] == 0:
                    create_table_sql = table.mount_create_table()
                    logging.info(
                        f"ENDPOINT - Criando tabela {table.target_schema_name}.{table.target_table_name}"
                    )
                    cursor.execute(create_table_sql)

            if truncate_before_insert:
                logging.info(
                    f"ENDPOINT - Truncando tabela {table.target_schema_name}.{table.target_table_name}"
                )
                cursor.execute(
                    PostgreSQLQueries.TRUNCATE_TABLE.format(
                        schema=table.target_schema_name, table=table.target_table_name
                    )
                )
        except Exception as e:
            logging.error(
                f"ENDPOINT - Erro ao gerenciar tabela {table.target_schema_name}.{table.target_table_name}: {e}"
            )
            raise ValueError(
                f"ENDPOINT - Erro ao gerenciar tabela {table.target_schema_name}.{table.target_table_name}: {e}"
            )

    def _insert_data(self, cursor: psycopg2.extensions.cursor, table: Table) -> None:
        """
        Insere dados completos na tabela de destino.

        Args:
            cursor (psycopg2.extensions.cursor): Cursor do banco de dados para execução de comandos SQL.
            table (Table): Objeto representando a estrutura da tabela de origem e os dados a serem inseridos.

        Returns:
            None
        """

        try:
            table_column_names = [
                col.name
                for col in sorted(
                    table.columns.values(), key=lambda col: col.ordinal_position
                )
            ]
            query = sql.SQL(
                PostgreSQLQueries.INSERT_FULL_LOAD_DATA.format(
                    schema=table.target_schema_name,
                    table=table.target_table_name,
                    columns=", ".join(table_column_names),
                )
            )

            records = [tuple(row) for row in table.data.iter_rows()]

            logging.info(
                f"ENDPOINT - Inserindo {len(records)} registros na tabela {table.target_schema_name}.{table.target_table_name}"
            )
            execute_values(cursor, query, records, page_size=10000)
        except Exception as e:
            logging.error(
                f"ENDPOINT - Erro ao inserir dados na tabela {table.target_schema_name}.{table.target_table_name}: {e}"
            )
            raise
