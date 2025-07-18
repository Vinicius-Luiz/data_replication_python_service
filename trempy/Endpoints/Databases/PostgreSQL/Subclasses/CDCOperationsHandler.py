from trempy.Endpoints.Databases.PostgreSQL.Subclasses.ConnectionManager import (
    ConnectionManager,
)
from trempy.Metadata.MetadataConnectionManager import MetadataConnectionManager
from trempy.Endpoints.Databases.PostgreSQL.Subclasses.TableManager import (
    TableManager,
)
from trempy.Shared.Queries.QueryPostgreSQL import (
    SCD2Queries as SCD2QueriesPostgreSQL,
    CDCQueries as CDCQueriesPostgreSQL,
)  #  TODO eu preciso saber qual é o tipo de endpoint correto
from trempy.Shared.Types import CdcModeType, SCD2ColumnType
from psycopg2 import sql, extensions
from trempy.Loggings.Logging import ReplicationLogger
from trempy.Endpoints.Exceptions.Exception import *
from trempy.Tables.Table import Table
from typing import Dict, List


logger = ReplicationLogger()

class CDCOperationsHandler:
    """Responsabilidade: Executar operações CDC (INSERT, UPDATE, DELETE)."""

    # Variáveis de classe para configurações de erro
    STOP_IF_INSERT_ERROR = 0
    STOP_IF_UPDATE_ERROR = 0
    STOP_IF_DELETE_ERROR = 0
    STOP_IF_UPSERT_ERROR = 0
    STOP_IF_SCD2_ERROR = 0

    def __init__(
        self,
        connection_manager: ConnectionManager,
        table_manager: TableManager,
    ):
        self.connection_manager = connection_manager
        self.table_manager = table_manager
        self._load_error_configurations()

    from psycopg2 import InterfaceError, Error

    def __handle_database_exceptions(
        self,
        schema_name: str,
        table_name: str,
        cursor: extensions.cursor = None,
        exception: Exception = None,
    ) -> dict:
        """
        Trata exceções de banco de dados de forma padronizada.

        Args:
            schema_name: Nome do schema
            table_name: Nome da tabela
            cursor: Objeto cursor do psycopg2 (opcional)
            exception: Exceção capturada (opcional)

        Returns:
            str: Mensagem de erro formatada
        """
        error_info = {
            "schema_name": schema_name,
            "table_name": table_name,
            "message": str(exception),
            "type": type(exception).__name__,
            "code": None,
            "query": None,
        }

        error_info["code"] = getattr(exception, "pgcode", None)

        if cursor and hasattr(cursor, "query") and cursor.query:
            error_info["query"] = (
                cursor.query.decode()
                if isinstance(cursor.query, bytes)
                else cursor.query
            )

        with MetadataConnectionManager() as metadata_manager:
            metadata_manager.insert_apply_exceptions(error_info)

        error_info["error_msg"] = (
            f'{error_info["schema_name"]}.{error_info["table_name"]}: {error_info["type"]} - {error_info["message"]}'
        )
        return error_info

    def __insert_cdc_data(
        self,
        table: Table,
        mode: CdcModeType,
    ) -> dict:
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
                case CdcModeType.DEFAULT:
                    cdc_stats = self.__insert_cdc_data_default(table)
                case CdcModeType.UPSERT:
                    cdc_stats = self.__insert_cdc_data_upsert(table)
                case CdcModeType.SCD2:
                    cdc_stats = self.__insert_cdc_data_scd2(table)

            return cdc_stats
        except Exception as e:
            e = CDCDataError(
                f"Erro ao inserir dados no modo CDC ({mode.name}): {e}",
                f"{table.target_schema_name}.{table.target_table_name}",
            )
            logger.critical(e, required_types=["cdc"])

    def __insert_cdc_data_default(self, table: Table) -> dict:
        """Processa operações CDC (INSERT, UPDATE, DELETE) no modo padrão.

        Itera sobre as linhas da tabela de origem e executa a operação correspondente
        ($TREM_OPERATION) na tabela de destino de forma individual para cada tipo
        de operação.

        Args:
            table (Table): Objeto contendo a estrutura da tabela e os dados a serem processados.

        Raises:
            CDCDataError: Se ocorrer algum erro durante o processamento das operações.
        """

        stats = {
            "schema_name": table.schema_name,
            "table_name": table.table_name,
            "inserts": 0,
            "updates": 0,
            "deletes": 0,
            "errors": 0,
            "total": 0,
        }

        for row in table.data.iter_rows(named=True):
            operation = row["$TREM_OPERATION"]
            if operation == "INSERT":
                operation_stats = self.__operation_insert(table, row)
                stats["inserts"] += 1
                stats["total"] += 1
                stats["errors"] += operation_stats.get("errors", 0)
            elif operation == "UPDATE":
                operation_stats = self.__operation_update(table, row)
                stats["updates"] += 1
                stats["total"] += 1
                stats["errors"] += operation_stats.get("errors", 0)
            elif operation == "DELETE":
                operation_stats = self.__operation_delete(table, row)
                stats["deletes"] += 1
                stats["total"] += 1
                stats["errors"] += operation_stats.get("errors", 0)

            self.connection_manager.commit()

        return stats

    def __insert_cdc_data_upsert(self, table: Table) -> None:
        """Processa operações CDC no modo UPSERT (INSERT + UPDATE combinados).

        Itera sobre as linhas da tabela de origem tratando tanto INSERTs quanto UPDATEs
        como operações de UPSERT. DELETE é tratado de forma separada.

        Args:
            table (Table): Objeto contendo a estrutura da tabela e os dados a serem processados.

        Raises:
            CDCDataError: Se ocorrer algum erro durante o processamento das operações.
        """

        stats = {
            "schema_name": table.schema_name,
            "table_name": table.table_name,
            "inserts": 0,
            "updates": 0,
            "deletes": 0,
            "errors": 0,
            "total": 0,
        }

        for row in table.data.iter_rows(named=True):
            operation = row["$TREM_OPERATION"]
            if operation == "INSERT":
                operation_stats = self.__operation_upsert(table, row)
                stats["inserts"] += 1
                stats["total"] += 1
                stats["errors"] += operation_stats.get("errors", 0)
            elif operation == "UPDATE":
                operation_stats = self.__operation_upsert(table, row)
                stats["updates"] += 1
                stats["total"] += 1
                stats["errors"] += operation_stats.get("errors", 0)
            elif operation == "DELETE":
                operation_stats = self.__operation_delete(table, row)
                stats["deletes"] += 1
                stats["total"] += 1
                stats["errors"] += operation_stats.get("errors", 0)

            self.connection_manager.commit()

        return stats

    def __insert_cdc_data_scd2(self, table: Table) -> None:
        """Processa operações CDC no modo SCD2 (Slow Changing Dimension Type 2).

        Implementa a lógica de dimensões lentas tipo 2, mantendo histórico de alterações
        através de registros com validade temporal. Para INSERT/UPDATE, verifica se o
        registro existe e desativa a versão anterior antes de criar a nova. Para DELETE,
        apenas desativa o registro atual.

        Args:
            table (Table): Objeto contendo a estrutura da tabela e os dados a serem processados.

        Raises:
            CDCDataError: Se ocorrer algum erro durante o processamento das operações.
        """

        stats = {
            "schema_name": table.schema_name,
            "table_name": table.table_name,
            "inserts": 0,
            "updates": 0,
            "deletes": 0,
            "errors": 0,
            "total": 0,
        }

        for row in table.data.iter_rows(named=True):
            operation = row["$TREM_OPERATION"]
            if operation == "INSERT":
                row_exists = self.__scd2_verify_if_row_exists(table, row)
                if row_exists:
                    disable_stats = self.__scd2_disable_current(table, row)
                else:
                    disable_stats = {"errors": 0}
                create_stats = self.__scd2_create_current(table, row)
                stats["inserts"] += 1
                stats["total"] += 1
                stats["errors"] += disable_stats.get("errors", 0) or create_stats.get(
                    "errors", 0
                )

            elif operation == "UPDATE":
                row_exists = self.__scd2_verify_if_row_exists(table, row)
                if row_exists:
                    disable_stats = self.__scd2_disable_current(table, row)
                else:
                    disable_stats = {"errors": 0}
                create_stats = self.__scd2_create_current(table, row)
                stats["updates"] += 1
                stats["total"] += 1
                stats["errors"] += disable_stats.get("errors", 0) or create_stats.get(
                    "errors", 0
                )

            elif operation == "DELETE":
                disable_stats = self.__scd2_disable_current(table, row)
                stats["deletes"] += 1
                stats["total"] += 1
                stats["errors"] += disable_stats.get("errors", 0)

            self.connection_manager.commit()

        return stats

    def __scd2_get_where_clause(self, table: Table, row: dict) -> Dict[str, List[str]]:
        """Constrói a cláusula WHERE para operações SCD2 baseada nas colunas-chave.

        Gera as condições WHERE necessárias para identificar unicamente um registro
        na tabela de destino, excluindo colunas temporais do SCD2. A cláusula é
        construída usando apenas as colunas primárias que não são de controle SCD2.

        Args:
            table (Table): Objeto contendo a estrutura da tabela e metadados.
            row (dict): Dicionário contendo os valores da linha a ser verificada.

        Returns:
            Dict[str, List]: Dicionário com:
                - where_parts: Lista de fragmentos SQL das condições WHERE
                - where_values: Lista de valores para os parâmetros da cláusula

        Example:
            Retorno exemplo:
            {
                "where_parts": ["id = %s", "code = %s"],
                "where_values": [1, "ABC"]
            }
        """
        pk_columns_without_scd2_columns = table.get_pk_columns_without_scd2_columns()
        data_columns = [col for col in row.keys() if not col.startswith("$TREM_")]

        where_parts = []
        where_values = []
        for col in data_columns:
            if col in pk_columns_without_scd2_columns:
                where_parts.append(
                    sql.SQL("{col} = %s").format(col=sql.Identifier(col))
                )
                where_values.append(row[col])

        return {"where_parts": where_parts, "where_values": where_values}

    def __scd2_verify_if_row_exists(self, table: Table, row: dict) -> bool:
        """Verifica se um registro já existe na tabela de destino no modo SCD2.

        Executa uma consulta na tabela de destino para verificar a existência de
        um registro ativo (current = True) com os mesmos valores nas colunas-chave,
        ignorando colunas temporais específicas do SCD2.

        Args:
            table (Table): Objeto contendo a estrutura da tabela e metadados.
            row (dict): Dicionário contendo os valores da linha a ser verificada.

        Returns:
            bool: True se o registro existe e está ativo, False caso contrário.

        Raises:
            CDCDataError: Se ocorrer algum erro durante a verificação.
        """
        scd2_columns = table.get_scd2_columns()
        current = scd2_columns[SCD2ColumnType.CURRENT]

        where_clause = self.__scd2_get_where_clause(table, row)
        where_parts = where_clause["where_parts"]
        where_values = where_clause["where_values"]

        query = sql.SQL(SCD2QueriesPostgreSQL.SQL_VERIFY_ROW_SCD2_EXISTS).format(
            schema=sql.Identifier(table.target_schema_name),
            table=sql.Identifier(table.target_table_name),
            where_clause=sql.SQL(" AND ").join(where_parts),
            current=sql.Identifier(current),
        )

        with self.connection_manager.cursor() as cursor:
            cursor.execute(query, where_values)
            row_exists = cursor.fetchone()

        return True if row_exists else False

    def __scd2_create_current(self, table: Table, row: dict) -> dict:
        """Cria um novo registro ativo no padrão SCD2 (Slow Changing Dimension Type 2).

        Adiciona uma nova versão do registro na tabela de destino, marcando-o como ativo (current=1)
        e configurando as colunas temporais (start_date e end_date) conforme definido na estrutura SCD2.

        Args:
            table (Table): Objeto contendo a estrutura da tabela e metadados SCD2.
            row (dict): Dicionário com os dados do registro a ser criado.

        Raises:
            CreateSCD2Error: Se ocorrer algum erro durante a criação do registro.
                Inclui detalhes do schema, tabela e erro original.

        Note:
            - O registro é criado com current=1 (ativo)
            - As datas de início e fim são extraídas do próprio row
            - Delega a operação de inserção para o método __operation_insert
        """

        try:
            stats = {
                "errors": 0,
            }

            scd2_columns = table.get_scd2_columns()
            current = scd2_columns[SCD2ColumnType.CURRENT]
            start_date = scd2_columns[SCD2ColumnType.START_DATE]
            end_date = scd2_columns[SCD2ColumnType.END_DATE]

            row = {
                **row,
                start_date: row[start_date],
                end_date: row[end_date],
                current: 1,
            }

            self.__operation_insert(table, row)

        except Exception as e:
            stats["errors"] += 1

            e = CreateSCD2Error(
                f"Erro ao criar registro no modo SCD2: {e}",
                f"{table.target_schema_name}.{table.target_table_name}",
                None,
            )
            (
                logger.critical(e, required_types=["cdc"])
                if self.STOP_IF_SCD2_ERROR
                else logger.error(e, required_types=["cdc"])
            )

        return stats

    def __scd2_disable_current(self, table: Table, row: dict) -> dict:
        """Desativa um registro existente no padrão SCD2 (Slow Changing Dimension Type 2).

        Atualiza o registro existente na tabela de destino, marcando-o como inativo (current=0)
        e configurando a data de fim (end_date) conforme definido na estrutura SCD2.

        Args:
            table (Table): Objeto contendo a estrutura da tabela e metadados SCD2.
            row (dict): Dicionário com os dados do registro a ser desativado.

        Raises:
            DisableSCD2Error: Se ocorrer algum erro durante a desativação do registro.
                Inclui detalhes do schema, tabela, query executada e erro original.

        Note:
            - Utiliza a cláusula WHERE gerada por __scd2_get_where_clause
            - O registro é mantido na tabela com current=0 (inativo)
            - A data de fim é atualizada conforme valor fornecido no row
        """

        try:
            stats = {
                "errors": 0,
            }

            scd2_columns = table.get_scd2_columns()
            current = scd2_columns[SCD2ColumnType.CURRENT]
            end_date = scd2_columns[SCD2ColumnType.END_DATE]

            where_clause = self.__scd2_get_where_clause(table, row)
            where_parts = where_clause["where_parts"]
            where_values = where_clause["where_values"]

            update_query = sql.SQL(SCD2QueriesPostgreSQL.SQL_UPDATE_EXISTING).format(
                schema=sql.Identifier(table.target_schema_name),
                table=sql.Identifier(table.target_table_name),
                where_clause=sql.SQL(" AND ").join(where_parts),
                current=sql.Identifier(current),
                end_date=sql.Identifier(end_date),
            )

            with self.connection_manager.cursor() as cursor:
                cursor.execute(update_query, where_values)

        except Exception as e:
            stats["errors"] += 1

            error_info = self.__handle_database_exceptions(
                table.target_schema_name, table.target_table_name, cursor, e
            )

            e = DisableSCD2Error(
                f"Erro ao desativar registro no modo SCD2: {error_info['error_msg']}",
                f"{table.target_schema_name}.{table.target_table_name}",
                error_info["query"],
            )
            (
                logger.critical(e, required_types=["cdc"])
                if self.STOP_IF_SCD2_ERROR
                else logger.error(e, required_types=["cdc"])
            )

        return stats

    def __operation_insert(self, table: Table, row: dict) -> dict:
        """Executa a operação de INSERT na tabela de destino.

        Insere um novo registro na tabela de destino, filtrando colunas de metadados (que começam com "$TREM_").
        Constrói dinamicamente a query SQL com os nomes das colunas e valores a serem inseridos.

        Args:
            table (Table): Objeto contendo a estrutura da tabela de destino (schema e nome).
            row (dict): Dicionário com os dados a serem inseridos (incluindo valores das colunas).

        Raises:
            InsertCDCError: Se ocorrer algum erro durante a inserção.
                Contém detalhes do schema, tabela e query executada.
        """
        try:
            stats = {
                "errors": 0,
            }

            scd2_columns = table.get_scd2_columns()
            scd2_start_date = scd2_columns[SCD2ColumnType.START_DATE]

            data_columns = [col for col in row.keys() if not col.startswith("$TREM_")]
            insert_values = []
            value_placeholders = []
            for col in data_columns:
                if col == scd2_start_date:
                    value_placeholders.append(sql.SQL("NOW()"))
                else:
                    value_placeholders.append(sql.Placeholder())
                    insert_values.append(row[col])

            # Garante que a lista de placeholders seja um Composable
            values_part = sql.SQL(", ").join(value_placeholders)

            query = sql.SQL(CDCQueriesPostgreSQL.CDC_INSERT_DATA).format(
                schema=sql.Identifier(table.target_schema_name),
                table=sql.Identifier(table.target_table_name),
                columns=sql.SQL(", ").join(map(sql.Identifier, data_columns)),
                values=values_part,
            )

            with self.connection_manager.cursor() as cursor:
                cursor.execute(query, insert_values)

        except Exception as e:
            stats["errors"] += 1

            error_info = self.__handle_database_exceptions(
                table.target_schema_name, table.target_table_name, cursor, e
            )

            e = InsertCDCError(
                f"Erro ao inserir dados: {error_info['error_msg']}",
                f"{table.target_schema_name}.{table.target_table_name}",
                error_info["query"],
            )
            (
                logger.critical(e, required_types=["cdc"])
                if self.STOP_IF_INSERT_ERROR
                else logger.error(e, required_types=["cdc"])
            )

        return stats

    def __operation_update(self, table: Table, row: dict) -> dict:
        """Executa a operação de UPDATE na tabela de destino.

        Atualiza um registro existente na tabela de destino, usando as colunas primárias (PK)
        como critério de busca. Atualiza apenas colunas não-PK e filtra metadados ($TREM_).

        Args:
            table (Table): Objeto contendo a estrutura da tabela e colunas primárias.
            row (dict): Dicionário com os dados a serem atualizados (valores novos e PKs).

        Raises:
            UpdateCDCError: Se ocorrer algum erro durante a atualização.
                Contém detalhes do schema, tabela e query executada.

        Note:
            - Colunas primárias são usadas apenas na cláusula WHERE
            - Colunas não-PK são atualizadas na cláusula SET
        """
        try:
            stats = {
                "errors": 0,
            }

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

            query = sql.SQL(CDCQueriesPostgreSQL.CDC_UPDATE_DATA).format(
                schema=sql.Identifier(table.target_schema_name),
                table=sql.Identifier(table.target_table_name),
                set_clause=sql.SQL(", ").join(set_parts),
                where_clause=sql.SQL(" AND ").join(where_parts),
            )

            with self.connection_manager.cursor() as cursor:
                cursor.execute(query, set_values + where_values)

        except Exception as e:
            stats["errors"] += 1

            error_info = self.__handle_database_exceptions(
                table.target_schema_name, table.target_table_name, cursor, e
            )

            e = UpdateCDCError(
                f"Erro ao atualizar dados: {error_info['error_msg']}",
                f"{table.target_schema_name}.{table.target_table_name}",
                error_info["query"],
            )
            (
                logger.critical(e, required_types=["cdc"])
                if self.STOP_IF_UPDATE_ERROR
                else logger.error(e, required_types=["cdc"])
            )

        return stats

    def __operation_delete(self, table: Table, row: dict) -> dict:
        """Executa a operação de DELETE na tabela de destino.

        Remove um registro da tabela de destino usando apenas as colunas primárias (PK)
        como critério de exclusão. Ignora completamente colunas de metadados ($TREM_).

        Args:
            table (Table): Objeto contendo a estrutura da tabela e colunas primárias.
            row (dict): Dicionário contendo os valores das PKs para identificar o registro.

        Raises:
            DeleteCDCError: Se ocorrer algum erro durante a exclusão.
                Contém detalhes do schema, tabela e query executada.

        Security Note:
            - A operação é irreversível
            - A cláusula WHERE é construída apenas com PKs para evitar exclusões acidentais
        """
        try:
            stats = {
                "errors": 0,
            }

            pk_columns = table.get_pk_columns()
            pk_values = [row[col] for col in pk_columns]

            where_parts = [
                sql.SQL("{col} = %s").format(col=sql.Identifier(col))
                for col in pk_columns
            ]

            query = sql.SQL(CDCQueriesPostgreSQL.CDC_DELETE_DATA).format(
                schema=sql.Identifier(table.target_schema_name),
                table=sql.Identifier(table.target_table_name),
                where_clause=sql.SQL(" AND ").join(where_parts),
            )

            with self.connection_manager.cursor() as cursor:
                cursor.execute(query, pk_values)

        except Exception as e:
            stats["errors"] += 1

            error_info = self.__handle_database_exceptions(
                table.target_schema_name, table.target_table_name, cursor, e
            )

            e = DeleteCDCError(
                f"Erro ao remover dados: {error_info['error_msg']}",
                f"{table.target_schema_name}.{table.target_table_name}",
                error_info["query"],
            )
            (
                logger.critical(e, required_types=["cdc"])
                if self.STOP_IF_DELETE_ERROR
                else logger.error(e, required_types=["cdc"])
            )

        return stats

    def __operation_upsert(self, table: Table, row: dict) -> dict:
        """Executa uma operação de UPSERT (INSERT ou UPDATE condicional) na tabela de destino.

        Implementa um padrão "insert or update" utilizando a cláusula ON CONFLICT do PostgreSQL.
        Se a tabela tiver apenas colunas PK (sem colunas adicionais), executa DELETE+INSERT como fallback.

        Args:
            table (Table): Objeto contendo a estrutura da tabela e metadados das colunas.
            row (dict): Dicionário com os dados completos do registro (PKs + valores normais).

        Raises:
            UpsertCDCError: Se ocorrer algum erro durante a operação.
                Inclui detalhes do schema, tabela, query executada e erro original.

        Behavior:
            1. Caso normal (tabela tem colunas além das PKs):
            - Usa INSERT ... ON CONFLICT ... DO UPDATE SET
            - Atualiza apenas colunas não-PK em caso de conflito

            2. Caso especial (tabela contém apenas PKs):
            - Executa DELETE seguido de INSERT (fallback)
            - Mais custoso porém necessário para tabelas sem colunas atualizáveis
        """
        try:
            stats = {
                "errors": 0,
            }

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
                query = sql.SQL(CDCQueriesPostgreSQL.CDC_UPSERT_DATA).format(
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
                self.__operation_delete(table, row)
                self.__operation_insert(table, row)

        except Exception as e:
            stats["errors"] += 1

            error_info = self.__handle_database_exceptions(
                table.target_schema_name, table.target_table_name, cursor, e
            )

            e = UpsertCDCError(
                f"Erro ao realizar UPSERT: {error_info['error_msg']}",
                f"{table.target_schema_name}.{table.target_table_name}",
                error_info["query"],
            )
            (
                logger.critical(e, required_types=["cdc"])
                if self.STOP_IF_UPSERT_ERROR
                else logger.error(e, required_types=["cdc"])
            )

        return stats

    def insert_cdc_into_table(
        self,
        mode: str,
        table: Table,
        create_table_if_not_exists: bool = False,
    ) -> dict:
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
            self.table_manager.manage_target_table(table, create_table_if_not_exists)

            cdc_stats = self.__insert_cdc_data(table, mode)

            return cdc_stats

        except Exception as e:
            self.connection_manager.rollback()
            e = EndpointError(f"Erro ao inserir dados ({mode}): {e}")
            logger.critical(e, required_types=["cdc"])

    @classmethod
    def _load_error_configurations(cls):
        """Carrega as configurações de tratamento de erro do metadata."""
        with MetadataConnectionManager() as metadata_manager:
            cls.STOP_IF_INSERT_ERROR = int(
                metadata_manager.get_metadata_config("STOP_IF_INSERT_ERROR", "0")
            )
            cls.STOP_IF_UPDATE_ERROR = int(
                metadata_manager.get_metadata_config("STOP_IF_UPDATE_ERROR", "0")
            )
            cls.STOP_IF_DELETE_ERROR = int(
                metadata_manager.get_metadata_config("STOP_IF_DELETE_ERROR", "0")
            )
            cls.STOP_IF_UPSERT_ERROR = int(
                metadata_manager.get_metadata_config("STOP_IF_UPSERT_ERROR", "0")
            )
            cls.STOP_IF_SCD2_ERROR = int(
                metadata_manager.get_metadata_config("STOP_IF_SCD2_ERROR", "0")
            )
