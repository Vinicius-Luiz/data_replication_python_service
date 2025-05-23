from trempy.Endpoints.Databases.PostgreSQL.Subclasses.ConnectionManager import (
    ConnectionManager,
)
from trempy.Endpoints.Databases.PostgreSQL.Subclasses.TableManager import (
    TableManager,
)
from trempy.Shared.Queries.QueryPostgreSQL import (
    FullLoadQueries as FullLoadQueriesPostgreSQL,
)  #  TODO eu preciso saber qual é o tipo de endpoint correto
from trempy.Shared.Utils import Utils
from trempy.Loggings.Logging import ReplicationLogger
from trempy.Endpoints.Exceptions.Exception import *
from psycopg2.extras import execute_values
from trempy.Tables.Table import Table
from psycopg2 import sql
from time import time
import polars as pl
import os

logger = ReplicationLogger()


class FullLoadHandler:
    def __init__(
        self,
        connection_manager: ConnectionManager,
        table_manager: TableManager,
    ):
        self.connection_manager = connection_manager
        self.table_manager = table_manager

    def __insert_full_load_data(self, table: Table) -> dict:
        """
        Insere dados completos na tabela de destino.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem e os dados a serem inseridos.

        Returns:
            None

        Raises:
            InsertFullLoadError: Se ocorrer um erro ao inserir os dados.
        """

        try:
            stats = {}

            table_column_names = [
                col.name
                for col in sorted(
                    table.columns.values(), key=lambda col: col.ordinal_position
                )
            ]
            query = sql.SQL(
                FullLoadQueriesPostgreSQL.FULL_LOAD_INSERT_DATA.format(
                    schema=table.target_schema_name,
                    table=table.target_table_name,
                    columns=", ".join(table_column_names),
                )
            )

            records = [tuple(row) for row in table.data.iter_rows()]

            logger.info(
                f"ENDPOINT - Inserindo {len(records)} registros na tabela {table.target_schema_name}.{table.target_table_name}"
            )

            with self.connection_manager.cursor() as cursor:
                execute_values(
                    cursor, query, records, page_size=10000
                )
            
            stats["schema_name"] = table.schema_name
            stats["table_name"] = table.table_name
            stats["records"] = len(records)

            return stats
        
        except Exception as e:
            e = InsertFullLoadError(
                f"Erro ao inserir dados na tabela: {e}",
                table.target_table_name,
            )
            logger.critical(e)

    def get_full_load_from_table(self, table: Table) -> dict:
        """
        Realiza a extra o completa dos dados de uma tabela.

        Args:
            table (Table): Objeto representando a estrutura da tabela.

        Returns:
            dict: Dicionário contendo o log de execução do método

        Raises:
            EndpointError: Se houver um erro ao obter a carga completa.
        """

        try:
            initial_time = time()

            with self.connection_manager.cursor() as cursor:
                cursor.execute(
                    FullLoadQueriesPostgreSQL.GET_FULL_LOAD_FROM_TABLE.format(
                        schema=table.schema_name, table=table.table_name
                    )
                )
                data = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

                df = pl.DataFrame(data, schema=columns, orient="row")
                df.write_parquet(table.path_data)

                return {
                    "schema_name": table.schema_name,
                    "table_name": table.table_name,
                    "rowcount": cursor.rowcount,
                    "statusmessage": cursor.statusmessage,
                    "time_elapsed": Utils.format_time_elapsed(time() - initial_time)
                }
        except Exception as e:
            e = EndpointError(f"Erro ao obter a carga completa: {e}")
            logger.critical(e)

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

        Raises:
            EndpointError: Se houver um erro ao inserir os dados.
        """

        try:
            initial_time = time()

            with self.connection_manager.cursor() as cursor:
                self.table_manager.manage_target_table(
                    table,
                    create_table_if_not_exists,
                    recreate_table_if_exists,
                    truncate_before_insert,
                )

                full_load_stats = self.__insert_full_load_data(table)

                self.connection_manager.commit()
                os.remove(table.path_data)

                return {
                    **full_load_stats,
                    "success": 1,
                    "time_elapsed": Utils.format_time_elapsed(time() - initial_time)
                }
        except Exception as e:
            self.connection_manager.rollback()
            e = EndpointError(f"Erro ao inserir dados no modo full load: {e}")
            logger.critical(e)
