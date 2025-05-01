from trempy.Endpoints.Exceptions.Exception import *
from trempy.Endpoints.Databases.PostgreSQL.Subclasses.ConnectionManager import (
    ConnectionManager,
)
from trempy.Endpoints.Databases.PostgreSQL.Queries.Query import Query
from trempy.Columns.Column import Column
from trempy.Shared.Utils import Utils
from trempy.Tables.Table import Table
import logging


class MetadataReader:
    """Responsabilidade: Ler metadados do banco de dados (schemas, tabelas, colunas)."""

    def __init__(self, connection_manager: ConnectionManager):
        self.connection_manager = connection_manager

    def get_schemas(self) -> list:
        """Obtém a lista de esquemas do banco de dados.

        Returns:
            list: Lista de esquemas disponíveis no banco de dados.
        """

        try:
            with self.connection_manager.cursor() as cursor:
                cursor.execute(Query.GET_SCHEMAS)
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            e = GetSchemaError(f"Erro ao obter os esquemas: {e}")
            Utils.log_exception_and_exit(e)

    def get_tables(self, schema_name: str) -> list:
        """Obtém a lista de tabelas de um esquema específico.

        Args:
            schema_name (str): Nome do esquema.

        Returns:
            list: Lista de tabelas do esquema.
        """

        try:
            with self.connection_manager.cursor() as cursor:
                cursor.execute(Query.GET_TABLES, (schema_name,))
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            e = GetTablesError(f"Erro ao obter as tabelas: {e}")
            Utils.log_exception_and_exit(e)

    def get_table_details(self, table: dict) -> Table:
        """
        Obtém os detalhes de uma tabela específica.

        Args:
            table (dict): Informações da tabela.

        Returns:
            Table: Objeto representando a estrutura da tabela.

        Raises:
            TableNotFoundError: Se a tabela nao for encontrada.
            EndpointError: Se houver um erro ao obter os detalhes da tabela.
        """

        try:
            schema_name = table.get("schema_name")
            table_name = table.get("table_name")
            priority = table.get("priority")

            with self.connection_manager.cursor() as cursor:
                cursor.execute(
                    Query.GET_TABLE_DETAILS, (schema_name, table_name)
                )
                metadata_table = cursor.fetchone()
                if not metadata_table:
                    e = TableNotFoundError(
                        f"Tabela não encontrada.", f"{schema_name}.{table_name}"
                    )
                    Utils.log_exception_and_exit(e)

                table_obj = Table(
                    schema_name=metadata_table[0],
                    table_name=metadata_table[1],
                    priority=priority,
                    estimated_row_count=metadata_table[2],
                    table_size=metadata_table[3],
                )

                primary_keys = self.get_table_primary_key(table_obj)
                return self.get_table_columns(table_obj, primary_keys)
        except Exception as e:
            e = EndpointError(f"Erro ao obter os detalhes da tabela: {e}")
            Utils.log_exception_and_exit(e)

    def get_table_primary_key(self, table: Table) -> list:
        """
        Obtém a lista de colunas que compõem a chave primária de uma tabela específica.

        Args:
            table (Table): Objeto representando a estrutura da tabela.

        Returns:
            list: Lista de colunas que compõem a chave primária da tabela.

        Raises:
            EndpointError: Se houver um erro ao obter as chaves primarias da tabela.
        """

        try:
            with self.connection_manager.cursor() as cursor:
                cursor.execute(
                    Query.GET_TABLE_PRIMARY_KEY,
                    (table.schema_name, table.table_name),
                )
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            e = EndpointError(f"Erro ao obter as chaves primárias da tabela: {e}")
            Utils.log_exception_and_exit(e)

    def get_table_columns(self, table: Table, primary_keys: list) -> Table:
        """
        Obtém as colunas de uma tabela específica e atualiza o objeto Table com
        os detalhes das colunas.

        Args:
            table (Table): Objeto representando a estrutura da tabela.
            primary_keys (list): Lista de colunas que compõem a chave primária da tabela.

        Returns:
            Table: Objeto Table atualizado com a estrutura das colunas da tabela.

        Raises:
            EndpointError: Se houver um erro ao obter as colunas da tabela.
        """

        try:
            with self.connection_manager.cursor() as cursor:
                cursor.execute(
                    Query.GET_TABLE_COLUMNS,
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
            e = EndpointError(f"Erro ao obter as colunas da tabela: {e}")
            Utils.log_exception_and_exit(e)
