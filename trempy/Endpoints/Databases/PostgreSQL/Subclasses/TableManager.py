from trempy.Endpoints.Databases.PostgreSQL.Subclasses.ConnectionManager import (
    ConnectionManager,
)
from trempy.Endpoints.Databases.PostgreSQL.Subclasses.TableCreator import (
    TableCreator,
)
from trempy.Endpoints.Databases.PostgreSQL.Queries.Query import Query
from trempy.Endpoints.Exceptions.Exception import *
from trempy.Shared.Utils import Utils
from trempy.Tables.Table import Table
import logging


class TableManager:
    """Responsabilidade: Gerenciar a estrutura da tabela de destino."""

    def __init__(
        self,
        connection_manager: ConnectionManager,
        table_creator: TableCreator,
    ):
        self.connection_manager = connection_manager
        self.table_creator = table_creator

    def _manage_target_table(
        self,
        table: Table,
        create_if_not_exists: bool = False,
        recreate_if_exists: bool = False,
        truncate_before_insert: bool = False,
    ) -> None:
        """
        Gerencia a tabela de destino antes de inserir os dados.

        Se for informado que a tabela deve ser recriada, remove a tabela se ela existir.
        Verifica se a tabela existe e a cria caso não exista.
        Trunca a tabela antes de inserir dados se for informado.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.
            create_if_not_exists (bool): Indica se a tabela deve ser criada caso não exista.
            recreate_if_exists (bool): Indica se a tabela deve ser recriada caso já exista.
            truncate_before_insert (bool): Indica se a tabela deve ser truncada antes da inserção dos dados.

        Returns:
            None: Este método não retorna valores.

        Raises:
            ManageTableError: Se ocorrer um erro durante a execução dos comandos SQL.
        """

        try:
            with self.connection_manager.cursor() as cursor:
                step = "recreate_if_exists"
                if recreate_if_exists:
                    create_if_not_exists = True
                    logging.info(
                        f"ENDPOINT - Removendo tabela {table.target_schema_name}.{table.target_table_name}"
                    )
                    cursor.execute(
                        Query.DROP_TABLE.format(
                            schema=table.target_schema_name,
                            table=table.target_table_name,
                        )
                    )

                step = "create_if_not_exists"
                if create_if_not_exists:
                    cursor.execute(
                        Query.CHECK_TABLE_EXISTS,
                        (table.target_schema_name, table.target_table_name),
                    )
                    if cursor.fetchone()[0] == 0:
                        create_table_sql = self.table_creator.mount_create_table(table)
                        logging.info(
                            f"ENDPOINT - Criando tabela {table.target_schema_name}.{table.target_table_name}"
                        )
                        cursor.execute(create_table_sql)

                step = "truncate_before_insert"
                if truncate_before_insert:
                    logging.info(
                        f"ENDPOINT - Truncando tabela {table.target_schema_name}.{table.target_table_name}"
                    )
                    cursor.execute(
                        Query.TRUNCATE_TABLE.format(
                            schema=table.target_schema_name,
                            table=table.target_table_name,
                        )
                    )
        except Exception as e:
            e = ManageTableError(
                f"Erro ao gerenciar tabela {table.target_schema_name}.{table.target_table_name}: {e}",
                step,
            )
            Utils.log_exception_and_exit(e)
