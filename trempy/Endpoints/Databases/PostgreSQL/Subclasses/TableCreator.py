from trempy.Endpoints.Databases.PostgreSQL.Subclasses.ConnectionManager import (
    ConnectionManager,
)
from trempy.Shared.Queries.QueryPostgreSQL import (
    TableQueries as TableQueriesPostgreSQL,
)  #  TODO eu preciso saber qual é o tipo de endpoint correto
from trempy.Loggings.Logging import ReplicationLogger
from trempy.Endpoints.Exceptions.Exception import *
from trempy.Tables.Table import Table

logger = ReplicationLogger()


class TableCreator:
    """Responsabilidade: Criar e gerenciar estruturas de tabelas."""

    def __init__(self, connection_manager: ConnectionManager):
        self.connection_manager = connection_manager

    def __mount_columns_to_create_table(self, table: Table) -> str:
        """
        Monta a string de colunas para criação de uma tabela no banco de dados.

        A string é formada por uma lista de colunas, onde cada coluna é representada por uma string
        no formato adequado para o banco de dados. As colunas são separadas por vírgula
        e seguem a mesma ordem de aparição na lista de colunas do objeto.

        Args:
            table (Table): Objeto representando a estrutura da tabela.

        Returns:
            str: String formatada com as colunas para criação da tabela, pronta para ser usada em comandos SQL.
        """

        columns_sql = []
        for column in sorted(
            table.columns.values(), key=lambda col: col.ordinal_position
        ):
            character_maximum_length = (
                f"({column.character_maximum_length})"
                if column.character_maximum_length
                else ""
            )
            column_str = f'{column.name} {column.data_type}{character_maximum_length} {"NOT NULL" if not column.nullable else "NULL"}'
            columns_sql.append(column_str)

        columns_sql = ", ".join(columns_sql)
        return columns_sql

    def __mount_primary_key_to_create_table(self, table: Table) -> str:
        """
        Monta a string de chave primária para criação de uma tabela no banco de dados.

        Args:
            table (Table): Objeto representando a estrutura da tabela.

        A string é formada por uma lista de colunas, onde cada coluna é representada por uma string
        no formato adequado para o banco de dados. As colunas sao separadas por vírgula
        e seguem a mesma ordem de aparição na lista de colunas do objeto.

        Returns:
            str: String formatada com as colunas para criação da tabela, pronta para ser usada em comandos SQL.
        """

        primary_key_sql = []
        for column in sorted(
            table.columns.values(), key=lambda col: col.ordinal_position
        ):
            if column.is_primary_key:
                primary_key_sql.append(column.name)

        if primary_key_sql:
            primary_key_sql = ", ".join(primary_key_sql)
            primary_key_sql = ", PRIMARY KEY ({})".format(primary_key_sql)
        else:
            primary_key_sql = ""

        return primary_key_sql

    def mount_create_table(self, table: Table) -> str:
        """
        Monta a string para criação da tabela no banco de dados.

        Args:
            table (Table): Objeto representando a estrutura da tabela.

        Returns:
            str: String formatada com as colunas para cria o da tabela, pronta para ser usada em comandos SQL.

        Raises:
            EndpointError: Se houver um erro ao montar a string de criação da tabela.
        """

        try:
            columns_sql = self.__mount_columns_to_create_table(table)
            primary_key_sql = self.__mount_primary_key_to_create_table(table)
            return TableQueriesPostgreSQL.CREATE_TABLE.format(
                schema=table.target_schema_name,
                table=table.target_table_name,
                columns=columns_sql,
                primary_key=primary_key_sql,
            )
        except Exception as e:
            e = EndpointError(f"Erro ao montar a string de criação da tabela: {e}")
            logger.critical(e)
