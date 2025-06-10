from trempy.Loggings.Logging import ReplicationLogger
from trempy.Metadata.Exceptions.Exception import *
from trempy.Metadata.Query import Query
from typing import Optional, Dict
import polars as pl
import sqlite3

logger = ReplicationLogger()


class MetadataConnectionManager:
    """Gerenciador de conexão com o banco de dados SQLite para metadados de replicação."""

    # Estrutura consolidada de tabelas e suas colunas obrigatórias
    TABLE_SCHEMA = {
        "stats_cdc": {
            "schema": [
                "task_name",
                "schema_name",
                "table_name",
                "inserts",
                "updates",
                "deletes",
                "errors",
                "total",
            ],
            "verify_schema": True,
        },
        "stats_full_load": {
            "schema": [
                "task_name",
                "schema_name",
                "table_name",
                "records",
                "success",
                "time_elapsed",
            ],
            "verify_schema": True,
        },
        "stats_source_tables": {
            "schema": [
                "task_name",
                "schema_name",
                "table_name",
                "rowcount",
                "statusmessage",
                "time_elapsed",
            ],
            "verify_schema": True,
        },
        "stats_message": {
            "schema": [
                "task_name",
                "transaction_id",
                "quantity_operations",
                "published",
                "received",
                "processed",
            ],
            "verify_schema": False,
        },
        "dlx_message": {
            "schema": [
                "task_name",
                "transaction_id",
                "message_id",
                "delivery_tag",
                "routing_key",
                "body",
            ],
            "verify_schema": True,
        },
        "apply_exceptions": {
            "schema": [
                "schema_name",
                "table_name",
                "message",
                "type",
                "code",
                "query",
            ],
            "verify_schema": True,
        },
        "metadata_table": {
            "schema": [],
            "verify_schema": False,
        },
    }

    def __init__(self, db_name: str = "trempy.db"):
        """
        Inicializa a conexão com o banco de dados e cria as tabelas necessárias se não existirem.

        Args:
            db_name: Nome do arquivo do banco de dados SQLite.
        """
        self.connection = sqlite3.connect(db_name)

    def create_tables(self) -> None:
        """Cria todas as tabelas de metadados se não existirem."""
        try:
            cursor = self.connection.cursor()

            # Executa todos os SQLs de criação de tabela
            cursor.execute(Query.SQL_CREATE_METADATA_TABLE)
            cursor.execute(Query.SQL_CREATE_STATS_CDC)
            cursor.execute(Query.SQL_CREATE_STATS_FULL_LOAD)
            cursor.execute(Query.SQL_CREATE_STATS_SOURCE_TABLES)
            cursor.execute(Query.SQL_CREATE_STATS_MESSAGE)
            cursor.execute(Query.SQL_CREATE_DLX_MESSAGE)
            cursor.execute(Query.SQL_CREATE_APPLY_EXCEPTIONS)

            self.connection.commit()
        except Exception as e:
            e = CreateTableError(f"Erro ao criar as tabelas de metadados: {e}")
            logger.critical(e)

    def __validate_data(self, table_name: str, data: Dict) -> None:
        """Valida se o dicionário de dados contém todas as colunas obrigatórias."""
        table_schema = self.TABLE_SCHEMA.get(table_name, {})

        verify_schema = table_schema.get("verify_schema", False)
        if not verify_schema:
            return

        required_columns = table_schema.get("schema", [])
        if required_columns and not all(key in data for key in required_columns):
            raise RequiredColumnsError(
                f"Dados não contém todas as colunas obrigatórias para {table_name}: {required_columns}"
            )

    def __insert_data(self, table_name: str, data: Dict) -> None:
        """
        Método genérico para inserção de dados em qualquer tabela.

        Args:
            table_name: Nome da tabela de destino
            data: Dicionário com os dados a serem inseridos
        """
        try:
            self.__validate_data(table_name, data)

            sql_insert = getattr(Query, f"SQL_INSERT_{table_name.upper()}")

            values = [data[col] for col in self.TABLE_SCHEMA[table_name]["schema"]]

            cursor = self.connection.cursor()
            cursor.execute(sql_insert, values)
            self.connection.commit()

        except Exception as e:
            raise InsertMetadataError(
                f"Erro ao inserir dados na tabela {table_name}: {e}", table_name
            )

    # Métodos específicos para cada tabela (apenas encapsulam o método genérico)
    def insert_stats_cdc(self, data: Dict, **kwargs) -> None:
        """Insere dados na tabela stats_cdc."""
        try:
            self.__insert_data("stats_cdc", {**data, **kwargs})
        except InsertMetadataError as e:
            logger.critical(str(e))

    def insert_stats_full_load(self, data: Dict, **kwargs) -> None:
        """Insere dados na tabela stats_full_load."""
        try:
            self.__insert_data("stats_full_load", {**data, **kwargs})
        except InsertMetadataError as e:
            logger.critical(str(e))

    def insert_stats_source_tables(self, data: Dict, **kwargs) -> None:
        """Insere dados na tabela stats_source_tables."""
        try:
            self.__insert_data("stats_source_tables", {**data, **kwargs})
        except InsertMetadataError as e:
            logger.critical(str(e))

    def insert_stats_message(self, data: Dict, **kwargs) -> None:
        """Insere dados na tabela stats_message."""
        try:
            data = {**data, **kwargs}
            data["published"] = data.get("published", 0)
            data["received"] = data.get("received", 0)
            data["processed"] = data.get("processed", 0)

            self.__insert_data("stats_message", data)
        except InsertMetadataError as e:
            logger.critical(str(e))

    def insert_dlx_message(self, data: Dict, **kwargs) -> None:
        """Insere dados na tabela dlx_message."""
        try:
            self.__insert_data("dlx_message", {**data, **kwargs})
        except InsertMetadataError as e:
            logger.critical(str(e))

    def insert_apply_exceptions(self, data: Dict, **kwargs) -> None:
        """Insere dados na tabela apply_exceptions."""
        try:
            self.__insert_data("apply_exceptions", {**data, **kwargs})
        except InsertMetadataError as e:
            logger.critical(str(e))

    def update_stats_message(self, data: Dict, **kwargs) -> None:
        """Atualiza dados na tabela stats_message."""
        try:
            data = {**data, **kwargs}
            column_set = data.get("column")
            value_set = data.get("value")

            cursor = self.connection.cursor()
            current_value = cursor.execute(
                Query.SQL_GET_STATS_MESSAGE.format(column=column_set),
                (data["transaction_id"],),
            ).fetchone()[0]

            cursor.execute(
                Query.SQL_UPDATE_STATS_MESSSAGE.format(
                    column_set=column_set, value_set=current_value + value_set
                ),
                (data["transaction_id"],),
            )
            self.connection.commit()
        except InsertMetadataError as e:
            logger.critical(f"Erro ao atualizar dados na tabela stats_message: {e}")

    def update_metadata_config(self, config: Dict) -> None:
        """Atualiza configurações na tabela metadata_table."""
        try:
            cursor = self.connection.cursor()
            for key, value in config.items():
                cursor.execute(Query.SQL_UPSERT_METADATA_TABLE, (key, value))
            self.connection.commit()
        except InsertMetadataError as e:
            logger.critical(f"Erro ao atualizar dados na tabela metadata_table: {e}")

    def get_metadata_tables(
        self,
        metadata_table_name: str,
        task_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> pl.DataFrame:
        """
        Obtém estatísticas de uma tabela com filtros opcionais.

        Args:
            metadata_table_name: Nome da tabela de estatísticas
            task_name: Nome da tarefa para filtrar (opcional)
            schema_name: Nome do schema para filtrar (opcional)
            table_name: Nome da tabela para filtrar (opcional)

        Returns:
            DataFrame contendo as estatísticas filtradas.
        """
        try:
            if metadata_table_name not in self.TABLE_SCHEMA:
                raise GetMetadataError(
                    f"table_name must be one of: {', '.join(self.TABLE_SCHEMA.keys())}"
                )

            query = f"SELECT * FROM {metadata_table_name}"
            conditions = []
            params = []

            # Adiciona filtros conforme parâmetros fornecidos
            if task_name:
                conditions.append("task_name = ?")
                params.append(task_name)
            if schema_name:
                conditions.append("schema_name = ?")
                params.append(schema_name)
            if table_name:
                conditions.append("table_name = ?")
                params.append(table_name)

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            cursor = self.connection.cursor()
            cursor.execute(query, params)

            df = pl.DataFrame(
                cursor.fetchall(), schema=[desc[0] for desc in cursor.description], orient="row"
            )
            return df

        except GetMetadataError as e:
            logger.critical(f"Erro ao obter estatísticas: {e}")

    def get_messages_stats(self) -> pl.DataFrame:
        """Obtém estatísticas de mensagens."""

        cursor = self.connection.cursor()

        cursor.execute(Query.SQL_GET_MESSAGES_STATS)

        df = pl.DataFrame(
            cursor.fetchall(), schema=[desc[0] for desc in cursor.description], orient="row"
        )
        return df

    def get_metadata_config(self, key: str) -> str:
        """Obtém um valor de configuração da tabela metadata_table."""
        try:
            cursor = self.connection.cursor()
            cursor.execute(Query.SQL_GET_METADATA_CONFIG, (key,))
            return cursor.fetchone()[0]
        except GetMetadataError as e:
            logger.critical(f"Erro ao obter config: {e}")

    def truncate_tables(self) -> None:
        """Limpa todas as tabelas de metadados."""
        try:
            cursor = self.connection.cursor()
            for table in self.TABLE_SCHEMA.keys():
                try:
                    cursor.execute(Query.SQL_TRUNCATE_TABLE.format(table=table))
                except sqlite3.OperationalError:
                    logger.warning(f"Table {table} does not exist")
                    continue
            self.connection.commit()
        except Exception as e:
            e = MetadataError(f"Erro ao limpar as tabelas de metadados: {e}")
            logger.critical(e)

    def commit(self) -> None:
        """Confirma as transações pendentes."""
        self.connection.commit()

    def rollback(self) -> None:
        """Reverte as transações pendentes."""
        self.connection.rollback()

    def close(self) -> None:
        """Fecha a conexão com o banco de dados."""
        self.connection.close()

    def __enter__(self):
        """Garante que a conexão seja aberta ao entrar no contexto."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Garante que a conexão será fechada ao sair do contexto."""
        self.close()
