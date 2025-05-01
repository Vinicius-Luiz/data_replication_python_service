from trempy.Endpoints.Databases.PostgreSQL.Subclasses.ConnectionManager import (
    ConnectionManager,
)
from trempy.Endpoints.Databases.PostgreSQL.Subclasses.TableManager import (
    TableManager,
)
from trempy.Shared.Utils import Utils
from trempy.Shared.Queries import PostgreSQLQueries
from trempy.Endpoints.Exceptions.Exception import *
from trempy.Tables.Table import Table
from psycopg2.extras import execute_values
from psycopg2 import sql

"""
    insert_cdc_into_table
_insert_cdc_data
_insert_cdc_data_default
"""


class CDCOperationsHandler:
    """Responsabilidade: Lidar com operações CDC (INSERT, UPDATE, DELETE)."""

    def __init__(
        self,
        connection_manager: ConnectionManager,
        table_manager: TableManager,
    ):
        self.connection_manager = connection_manager
        self.table_manager = table_manager

    def insert_cdc_into_table(
        self, mode: str, table: Table, create_table_if_not_exists: bool = False
    ):
        """
        Insere dados de alterações em uma tabela de destino.

        Args:
            table (Table): Objeto representando a estrutura da tabela.
            create_table_if_not_exists (bool): Se True, cria a tabela caso ela não exista.

        Returns:
            dict: Dicionário contendo o log de execução do método.

        Raises:
            EndpointError: Se houver um erro ao inserir os dados.
        """

        try:
            self.table_manager._manage_table(table, create_table_if_not_exists)

            self._insert_cdc_data(table, mode)

            self.connection_manager.commit()

            return {"message": "CDC data inserted successfully", "success": True}

        except Exception as e:
            self.connection_manager.rollback()
            e = EndpointError(f"Erro ao inserir dados no modo CDC ({mode}): {e}")
            Utils.log_exception_and_exit(e)

    def _insert_cdc_data(self, table: Table, mode: str) -> None:
        """
        Insere dados de altera es em uma tabela de destino.

        Insere dados de altera es em uma tabela de destino, considerando o modo
        de inser o especificado. Atualmente, o nico modo implementado  o
        "default", que simplesmente insere todos os dados da tabela.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem e os dados a serem inseridos.
            mode (str): Modo de inser o. Atualmente, o nico modo implementado  o "default".

        Raises:
            InsertCDCError: Se ocorrer um erro ao inserir os dados.
        """

        try:
            match mode:
                case "default":
                    self._insert_cdc_data_default(table)
                case "upsert":
                    raise NotImplementedError  # TODO
                case "scd2":
                    raise NotImplementedError  # TODO
        except Exception as e:
            e = InsertCDCError(f"Erro ao inserir dados no modo CDC ({mode}): {e}", f"{table.target_schema_name}.{table.target_table_name}")
            Utils.log_exception_and_exit(e)

    def _insert_cdc_data_default(self, table: Table) -> None:
        """
        Insere dados de altera es em uma tabela de destino, considerando o modo
        de inser o "default".

        O modo "default" insere todos os dados da tabela, sem considerar o tipo
        de opera o (INSERT, UPDATE, DELETE).

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem e os dados a serem inseridos.

        Raises:
            InsertCDCError: Se ocorrer um erro ao inserir os dados.
        """

        schema = table.target_schema_name
        table_name = table.target_table_name

        # Processar cada operação separadamente para agrupar por tipo
        operations = {"INSERT": [], "UPDATE": [], "DELETE": []}

        for row in table.data.iter_rows(named=True):
            operation = row["$TREM_OPERATION"]
            operations[operation].append(row)

        # Processar INSERTs
        if operations["INSERT"]:
            # Filtrar colunas que não são metadados do CDC
            data_columns = [
                col
                for col in operations["INSERT"][0].keys()
                if not col.startswith("$TREM_")
            ]

            # Preparar registros para insert
            insert_records = [
                [row[col] for col in data_columns] for row in operations["INSERT"]
            ]

            query = sql.SQL(PostgreSQLQueries.CDC_INSERT_DATA).format(
                schema=sql.Identifier(schema),
                table=sql.Identifier(table_name),
                columns=sql.SQL(", ").join(map(sql.Identifier, data_columns)),
            )

            with self.connection_manager.cursor() as cursor:
                execute_values(cursor, query, insert_records, page_size=10000)

        # Processar UPDATEs
        if operations["UPDATE"]:
            # Identificar PKs para cláusula WHERE
            pk_columns = [
                col.name for col in table.columns.values() if col.is_primary_key
            ]
            if not pk_columns:
                e = InsertCDCError(
                    f"Nenhuma PK encontrada para tabela", f"{schema}.{table_name}"
                )
                Utils.log_exception_and_exit(e)

            # Filtrar colunas que não são metadados do CDC
            data_columns = [
                col
                for col in operations["UPDATE"][0].keys()
                if not col.startswith("$TREM_")
            ]

            for row in operations["UPDATE"]:
                # Criar parte SET do UPDATE
                set_parts = []
                where_parts = []
                set_values = []
                where_values = []

                for col in data_columns:
                    if col in pk_columns:
                        where_parts.append(
                            sql.SQL("{col} = %s").format(col=sql.Identifier(col))
                        )
                        where_values.append(row[col])
                    else:
                        set_parts.append(
                            sql.SQL("{col} = %s").format(col=sql.Identifier(col))
                        )
                        set_values.append(row[col])

                query = sql.SQL(PostgreSQLQueries.CDC_UPDATE_DATA).format(
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(table_name),
                    set_clause=sql.SQL(", ").join(set_parts),
                    where_clause=sql.SQL(" AND ").join(where_parts),
                )
            
            with self.connection_manager.cursor() as cursor:
                cursor.execute(query, set_values + where_values)

        # Processar DELETEs
        if operations["DELETE"]:
            # Identificar PKs para cláusula WHERE
            pk_columns = [
                col.name for col in table.columns.values() if col.is_primary_key
            ]
            if not pk_columns:
                e = InsertCDCError(
                    f"Nenhuma PK encontrada para tabela", f"{schema}.{table_name}"
                )
                Utils.log_exception_and_exit(e)

            # Para DELETE, podemos agrupar operações com os mesmos critérios
            delete_records = []
            for row in operations["DELETE"]:
                delete_records.append([row[col] for col in pk_columns])

            query = sql.SQL(PostgreSQLQueries.CDC_DELETE_DATA).format(
                schema=sql.Identifier(schema),
                table=sql.Identifier(table_name),
                pk_columns=sql.SQL(", ").join(map(sql.Identifier, pk_columns)),
            )

            with self.connection_manager.cursor() as cursor:
                execute_values(cursor, query, delete_records, page_size=10000)
