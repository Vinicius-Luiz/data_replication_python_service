from trempy.Endpoints.Databases.PostgreSQL.Subclasses.ConnectionManager import (
    ConnectionManager,
)
from trempy.Endpoints.Databases.PostgreSQL.Subclasses.TableManager import (
    TableManager,
)
from trempy.Endpoints.Databases.PostgreSQL.Queries.Query import Query
from trempy.Endpoints.Exceptions.Exception import *
from trempy.Shared.Utils import Utils
from trempy.Tables.Table import Table
from psycopg2 import sql


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
            e = EndpointError(f"Erro ao inserir dados ({mode}): {e}")
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
                    self._insert_cdc_data_upsert(table)
                case "scd2":
                    raise NotImplementedError  # TODO
        except Exception as e:
            e = CDCDataError(
                f"Erro ao inserir dados no modo CDC ({mode}): {e}",
                f"{table.target_schema_name}.{table.target_table_name}",
            )
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
            UpdateCDCError: Se ocorrer um erro ao atualizar os dados.
            DeleteCDCError: Se ocorrer um erro ao remover os dados.
        """

        for row in table.data.iter_rows(named=True):
            operation = row["$TREM_OPERATION"]
            match operation:
                case "INSERT":
                    self._operation_insert(table, row)
                case "UPDATE":
                    self._operation_update(table, row)
                case "DELETE":
                    self._operation_delete(table, row)

    def _insert_cdc_data_upsert(self, table: Table) -> None:
        """
        Insere dados de altera es em uma tabela de destino, considerando o modo
        de inser o "upsert".

        O modo "upsert" insere todos os dados da tabela, considerando o tipo
        de opera o (INSERT, UPDATE, DELETE). Quando uma opera o de UPDATE
        ocorre, o sistema verifica se a linha j  existe na tabela de destino.
        Se sim, a linha  atualizada. Caso contr rio, a linha  inserida.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem e os dados a serem inseridos.

        Raises:
            InsertCDCError: Se ocorrer um erro ao inserir os dados.
            UpdateCDCError: Se ocorrer um erro ao atualizar os dados.
            DeleteCDCError: Se ocorrer um erro ao remover os dados.
        """

        for row in table.data.iter_rows(named=True):
            operation = row["$TREM_OPERATION"]
            match operation:
                case "INSERT":
                    self._operation_upsert(table, row)
                case "UPDATE":
                    self._operation_upsert(table, row)
                case "DELETE":
                    self._operation_delete(table, row)

    def _operation_insert(self, table: Table, row: dict) -> None:
        """
        Insere um registro em uma tabela de destino, considerando o modo de opera o "INSERT".

        Recebe um registro em formato de dicionário, onde as chaves são os nomes das colunas
        e os valores são os valores a serem inseridos nas respectivas colunas.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem e os dados a serem inseridos.
            row (dict): Dicionário contendo o registro a ser inserido.
        """

        try:
            data_columns = [col for col in row.keys() if not col.startswith("$TREM_")]
            insert_values = [row[col] for col in data_columns]

            value_placeholders = sql.SQL(", ").join(
                [sql.Placeholder()] * len(data_columns)
            )

            query = sql.SQL(Query.CDC_INSERT_DATA).format(
                schema=sql.Identifier(table.target_schema_name),
                table=sql.Identifier(table.target_table_name),
                columns=sql.SQL(", ").join(map(sql.Identifier, data_columns)),
                values=value_placeholders,
            )

            with self.connection_manager.cursor() as cursor:
                cursor.execute(query, insert_values)

        except Exception as e:
            error = InsertCDCError(
                f"Erro ao inserir dados: {e}",
                f"{table.target_schema_name}.{table.target_table_name}",
                cursor.query.decode("utf-8") if cursor.query else "",
            )
            Utils.log_exception(error)

    def _operation_update(self, table: Table, row: dict) -> None:
        """
        Atualiza um registro em uma tabela de destino, considerando o modo de opera o "UPDATE".

        Recebe um registro em formato de dicionário, onde as chaves são os nomes das colunas
        e os valores são os valores a serem atualizados nas respectivas colunas.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem e os dados a serem atualizados.
            row (dict): Dicion rio contendo o registro a ser atualizado.
        """
        try:
            pk_columns = table.get_pk_columns()

            data_columns = [col for col in row.keys() if not col.startswith("$TREM_")]

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

            query = sql.SQL(Query.CDC_UPDATE_DATA).format(
                schema=sql.Identifier(table.target_schema_name),
                table=sql.Identifier(table.target_table_name),
                set_clause=sql.SQL(", ").join(set_parts),
                where_clause=sql.SQL(" AND ").join(where_parts),
            )

            with self.connection_manager.cursor() as cursor:
                cursor.execute(query, set_values + where_values)

        except Exception as e:
            e = UpdateCDCError(
                f"Erro ao atualizar dados: {e}",
                f"{table.target_schema_name}.{table.target_table_name}",
                cursor.query.decode("utf-8") if cursor.query else "",
            )
            Utils.log_exception(e)

    def _operation_delete(self, table: Table, row: dict) -> None:
        """
        Remove um registro de uma tabela de destino com base na chave primária.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.
            row (dict): Dicionário contendo o registro a ser removido.
        """

        try:
            pk_columns = table.get_pk_columns()

            pk_values = [row[col] for col in pk_columns]

            where_parts = [
                sql.SQL("{col} = %s").format(col=sql.Identifier(col))
                for col in pk_columns
            ]

            query = sql.SQL(Query.CDC_DELETE_DATA).format(
                schema=sql.Identifier(table.target_schema_name),
                table=sql.Identifier(table.target_table_name),
                where_clause=sql.SQL(" AND ").join(where_parts),
            )

            with self.connection_manager.cursor() as cursor:
                cursor.execute(query, pk_values)

        except Exception as e:
            e = DeleteCDCError(
                f"Erro ao remover dados: {e}",
                f"{table.target_schema_name}.{table.target_table_name}",
                cursor.query.decode("utf-8") if cursor.query else "",
            )
            Utils.log_exception(e)

    def _operation_upsert(self, table: Table, row: dict) -> None:
        """
        Realiza um UPSERT (INSERT ou UPDATE condicional) em uma tabela de destino usando ON CONFLICT.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem e os dados.
            row (dict): Dicionário contendo o registro a ser upserted.
        """

        try:
            pk_columns = table.get_pk_columns()

            if len(pk_columns) < len(table.columns):
                data_columns = [
                    col for col in row.keys() if not col.startswith("$TREM_")
                ]

                values = [row[col] for col in data_columns]
                value_placeholders = sql.SQL(", ").join(
                    [sql.Placeholder()] * len(data_columns)
                )

                set_parts = [
                    sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(col))
                    for col in data_columns
                    if col not in pk_columns
                ]

                query = sql.SQL(Query.CDC_UPSERT_DATA).format(
                    schema=sql.Identifier(table.target_schema_name),
                    table=sql.Identifier(table.target_table_name),
                    columns=sql.SQL(", ").join(map(sql.Identifier, data_columns)),
                    values=value_placeholders,
                    pk_columns=sql.SQL(", ").join(map(sql.Identifier, pk_columns)),
                    set_clause=sql.SQL(", ").join(set_parts),
                )

                with self.connection_manager.cursor() as cursor:
                    cursor.execute(query, values)

            else:
                self._operation_delete(table, row)
                self._operation_insert(table, row)

        except Exception as e:
            e = UpsertCDCError(
                f"Erro ao realizar UPSERT: {e}",
                f"{table.target_schema_name}.{table.target_table_name}",
                cursor.query.decode("utf-8") if cursor.query else "",
            )
            Utils.log_exception(e)
