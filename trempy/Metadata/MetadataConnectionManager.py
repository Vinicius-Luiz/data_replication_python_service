from trempy.Loggings.Logging import ReplicationLogger
from trempy.Metadata.Exceptions.Exception import *
from trempy.Metadata.Query import Query
from typing import Optional, Dict
import polars as pl
import sqlite3

logger = ReplicationLogger()


class MetadataConnectionManager:
    """Gerenciador de conexão com o banco de dados SQLite para metadados de replicação."""

    TABLES_METADATA = ["stats_cdc", "stats_full_load"]

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

            cursor.execute(Query.SQL_CREATE_STATS_CDC)

            cursor.execute(Query.SQL_CREATE_STATS_FULL_LOAD)

            self.connection.commit()
        except Exception as e:
            e = CreateTableError(f"Erro ao criar as tabelas de metadados: {e}")
            logger.critical(e)

    def insert_cdc_stats(self, data: Dict, **kargs) -> None:
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
        except InsertStatsError as e:
            logger.critical(f"Erro ao inserir dados na tabela stats_cdc: {e}")

    def insert_full_load_stats(self, data: Dict, **kargs) -> None:
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
                    f"Data must contain all required keys: {self.STATS_FULL_LOAD_REQUIRED_COLUMNS}"
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

    def get_stats(
        self,
        metadata_table_name: str,
        task_name: Optional[str] = None,
        table_name: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> pl.DataFrame:
        """
        Obtém estatísticas de uma tabela com filtros opcionais.

        Args:
            table_name: Nome da tabela de estatísticas ('stats_cdc' ou 'stats_full_load').
            task_name: Nome da tarefa para filtrar (opcional).
            schema_name: Nome do schema para filtrar (opcional).
            table: Nome da tabela para filtrar (opcional).

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
