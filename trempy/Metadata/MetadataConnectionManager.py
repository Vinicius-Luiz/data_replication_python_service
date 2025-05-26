from trempy.Loggings.Logging import ReplicationLogger
from trempy.Metadata.Exceptions.Exception import *
from trempy.Metadata.Query import Query
from typing import Optional, Dict
import polars as pl
import sqlite3

logger = ReplicationLogger()


class MetadataConnectionManager:
    """Gerenciador de conexão com o banco de dados SQLite para metadados de replicação."""

    TABLES_METADATA = [
        "stats_cdc",
        "stats_full_load",
        "stats_source_tables",
        "stats_message",
        "metadata_table",
    ]

    STATS_CDC_REQUIRED_COLUMNS = [
        "task_name",
        "schema_name",
        "table_name",
        "inserts",
        "updates",
        "deletes",
        "errors",
        "total",
    ]

    STATS_FULL_LOAD_REQUIRED_COLUMNS = [
        "task_name",
        "schema_name",
        "table_name",
        "records",
        "success",
        "time_elapsed",
    ]

    STATS_SOURCE_TABLES_REQUIRED_COLUMNS = [
        "task_name",
        "schema_name",
        "table_name",
        "rowcount",
        "statusmessage",
        "time_elapsed",
    ]

    STATS_MESSAGE_REQUIRED_COLUMNS = [
        "task_name",
        "transaction_id",
        "quantity_operations",
        "published",
        "received",
        "processed",
    ]

    def __init__(self, db_name: str = "trempy.db"):
        """
        Inicializa a conexão com o banco de dados e cria as tabelas necessárias se não existirem.

        Args:
            db_name: Nome do arquivo do banco de dados SQLite.
        """
        self.connection = sqlite3.connect(db_name)

    def create_tables(self) -> None:
        """Cria as tabelas stats_cdc e stats_full_load se não existirem."""
        try:
            cursor = self.connection.cursor()

            cursor.execute(Query.SQL_METADATA_TABLE)

            cursor.execute(Query.SQL_CREATE_STATS_CDC)

            cursor.execute(Query.SQL_CREATE_STATS_FULL_LOAD)

            cursor.execute(Query.SQL_CREATE_STATS_SOURCE_TABLES)

            cursor.execute(Query.SQL_CREATE_STATS_MESSAGE)

            self.connection.commit()
        except Exception as e:
            e = CreateTableError(f"Erro ao criar as tabelas de metadados: {e}")
            logger.critical(e)

    def insert_stats_cdc(self, data: Dict, **kargs) -> None:
        """
        Insere dados na tabela stats_cdc.

        Args:
            data: Dicionário com os dados a serem inseridos.
                Deve conter as chaves: task_name, schema_name, table_name,
                inserts, updates, deletes, errors, total.
        """

        try:
            data = {**data, **kargs}
            if not all(key in data for key in self.STATS_CDC_REQUIRED_COLUMNS):
                e = RequiredColumnsError(
                    f"Colunas devem ser: {self.STATS_CDC_REQUIRED_COLUMNS}"
                )
                logger.critical(e)

            cursor = self.connection.cursor()
            cursor.execute(
                Query.SQL_INSERT_STATS_CDC,
                (
                    data["task_name"],
                    data["schema_name"],
                    data["table_name"],
                    data["inserts"],
                    data["updates"],
                    data["deletes"],
                    data["errors"],
                    data["total"],
                ),
            )
            self.connection.commit()
        except Exception as e:
            e = InsertStatsError(f"Erro ao inserir dados na tabela stats_cdc: {e}")
            logger.critical(e)

    def insert_stats_full_load(self, data: Dict, **kargs) -> None:
        """
        Insere dados na tabela stats_full_load.

        Args:
            data: Dicionário com os dados a serem inseridos.
                Deve conter as chaves: task_name, schema_name, table_name,
                records, success, time_elapsed.
        """

        try:
            data = {**data, **kargs}
            if not all(key in data for key in self.STATS_FULL_LOAD_REQUIRED_COLUMNS):
                e = RequiredColumnsError(
                    f"Data must contain all required keys: {self.STATS_SOURCE_TABLES_REQUIRED_COLUMNS}"
                )
                logger.critical(e)

            cursor = self.connection.cursor()
            cursor.execute(
                Query.SQL_INSERT_STATS_FULL_LOAD,
                (
                    data["task_name"],
                    data["schema_name"],
                    data["table_name"],
                    data["records"],
                    data["success"],
                    data["time_elapsed"],
                ),
            )
            self.connection.commit()

        except InsertStatsError as e:
            logger.critical(f"Erro ao inserir dados na tabela stats_full_load: {e}")

    def insert_stats_source_tables(self, data: Dict, **kargs) -> None:
        """
        Insere dados na tabela stats_source_tables.

        Args:
            data: Dicionário com os dados a serem inseridos.
                Deve conter as chaves: task_name, schema_name, table_name.
        """

        try:
            data = {**data, **kargs}
            if not all(
                key in data for key in self.STATS_SOURCE_TABLES_REQUIRED_COLUMNS
            ):
                e = RequiredColumnsError(
                    f"Data must contain all required keys: {self.STATS_FULL_LOAD_REQUIRED_COLUMNS}"
                )
                logger.critical(e)

            cursor = self.connection.cursor()
            cursor.execute(
                Query.SQL_INSERT_STATS_SOURCE_TABLES,
                (
                    data["task_name"],
                    data["schema_name"],
                    data["table_name"],
                    data["rowcount"],
                    data["statusmessage"],
                    data["time_elapsed"],
                ),
            )
            self.connection.commit()
        except InsertStatsError as e:
            logger.critical(f"Erro ao inserir dados na tabela stats_source_tables: {e}")

    def insert_stats_message(self, data: Dict, **kargs) -> None:
        """
        Insere dados na tabela stats_message.

        Args:
            data: Dicionário com os dados a serem inseridos.
                Deve conter as chaves: task_name, schema_name, table_name.
        """

        try:
            data = {**data, **kargs}

            cursor = self.connection.cursor()
            cursor.execute(
                Query.SQL_INSERT_STATS_MESSAGE,
                (
                    data["task_name"],
                    data["transaction_id"],
                    data["quantity_operations"],
                    data.get("published", 0),
                    data.get("received", 0),
                    data.get("processed", 0),
                ),
            )
            self.connection.commit()
        except InsertStatsError as e:
            logger.critical(f"Erro ao inserir dados na tabela stats_message: {e}")

    def update_stats_message(self, data: Dict, **kargs) -> None:
        """
        Insere dados na tabela stats_message.

        Args:
            data: Dicionário com os dados a serem inseridos.
        """

        try:
            data = {**data, **kargs}

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
        except InsertStatsError as e:
            logger.critical(f"Erro ao atualizar dados na tabela stats_message: {e}")

    def update_metadata_config(self, config: Dict) -> None:
        """
        Insere dados na tabela stats_message.

        Args:
            data: Dicionário com os dados a serem inseridos. Com o formato key: value
        """

        try:
            cursor = self.connection.cursor()
            for key, value in config.items():
                cursor.execute(
                    Query.SQL_UPSERT_METADATA_TABLE,
                    (
                        key,
                        value,
                    ),
                )

            self.connection.commit()
        except InsertStatsError as e:
            logger.critical(f"Erro ao atualizar dados na tabela stats_message: {e}")

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
            metadata_table_name: Nome da tabela de estatísticas ('stats_cdc', 'stats_full_load' ou 'stats_source_tables').
            task_name: Nome da tarefa para filtrar (opcional).
            schema_name: Nome do schema para filtrar (opcional).
            table_name: Nome da tabela para filtrar (opcional).

        Returns:
            DataFrame contendo as estatísticas filtradas.
        """

        try:
            if metadata_table_name not in self.TABLES_METADATA:
                e = GetMetadataError(
                    f"table_name must be either {' OR '.join(self.TABLES_METADATA)}"
                )
                logger.critical(e)

            query = f"SELECT * FROM {metadata_table_name}"
            conditions = []
            params = []

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
                cursor.fetchall(), schema=[desc[0] for desc in cursor.description]
            )

            return df

        except GetMetadataError as e:
            logger.critical(f"Erro ao obter estatísticas: {e}")

    def get_metadata_config(self, key: str) -> str:
        try:
            cursor = self.connection.cursor()
            cursor.execute(Query.SQL_GET_METADATA_CONFIG, (key,))
            return cursor.fetchone()[0]
        except GetMetadataError as e:
            logger.critical(f"Erro ao obter config: {e}")

    def truncate_tables(self) -> None:
        """Limpa as tabelas de metadados."""

        try:
            cursor = self.connection.cursor()

            for table in self.TABLES_METADATA:
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
