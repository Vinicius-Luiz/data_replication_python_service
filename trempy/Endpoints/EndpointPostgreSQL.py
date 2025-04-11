from trempy.Endpoints.Endpoint import Endpoint, EndpointType, DatabaseType
from trempy.Shared.Queries import PostgreSQLQueries
from trempy.Shared.Utils import Utils
from trempy.Tables.Table import Table
from trempy.Columns.Column import Column
from psycopg2 import sql
from psycopg2.extras import execute_values
from datetime import datetime
from time import time
from typing import Dict, Any
import polars as pl
import psycopg2
import logging
import os
import re
import json


class EndpointPostgreSQL(Endpoint):

    def __init__(
        self,
        endpoint_type: EndpointType,
        endpoint_name: str,
        credentials: dict,
        periodicity_in_seconds_of_reading_from_source: int = 10,
    ):
        """
        Inicializa um endpoint para PostgreSQL, gerenciando a conexão.

        Args:
            endpoint_type (EndpointType): Tipo do endpoint (fonte ou destino).
            endpoint_name (str): Nome do endpoint.
            credentials (dict): Credenciais do banco de dados.
        """
        super().__init__(
            DatabaseType.POSTGRESQL,
            endpoint_type,
            endpoint_name,
            periodicity_in_seconds_of_reading_from_source,
        )

        try:
            temp_credentials = credentials.copy()
            self.connection = self.connect(temp_credentials)
        finally:
            del temp_credentials

        logging.info(f"ENDPOINT - {endpoint_name} conectado")
        logging.debug(self.connection.get_dsn_parameters())

    def connect(self, credentials: dict) -> psycopg2.extensions.connection:
        """
        Estabelece uma conexão com o banco de dados.

        Args:
            credentials (dict): Credenciais do banco de dados.

        Returns:
            psycopg2.extensions.connection: Conexão estabelecida com o banco de dados.

        Raises:
            ValueError: Se houver um erro ao conectar ao banco de dados.
        """

        try:
            return psycopg2.connect(**credentials)
        except Exception as e:
            raise_msg = f"ENDPOINT - Erro ao conectar ao banco de dados: {e}"
            logging.critical(raise_msg)
            raise ValueError(raise_msg)

    def cursor(self) -> psycopg2.extensions.cursor:
        """
        Retorna um cursor para a conexão atual do banco de dados.

        Returns:
            psycopg2.extensions.cursor: Cursor para a conexão atual do banco de dados.
        """

        return self.connection.cursor()

    def close(self) -> None:
        """
        Fecha a conexão com o banco de dados.

        Returns:
            None
        """

        self.connection.close()

    def commit(self) -> None:
        """Faz commit na transa o atual."""

        self.connection.commit()

    def rollback(self) -> None:
        """Desfaz as altera es realizadas na transa o atual."""

        self.connection.rollback()

    def get_schemas(self) -> list:
        """Obtém a lista de esquemas do banco de dados.

        Returns:
            list: Lista de esquemas disponíveis no banco de dados.
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(PostgreSQLQueries.GET_SCHEMAS)
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            raise_msg = f"ENDPOINT - Erro ao obter os esquemas: {e}"
            logging.critical(raise_msg)
            return []

    def get_tables(self, schema: str) -> list:
        """Obtém a lista de tabelas de um esquema específico.

        Args:
            schema (str): Nome do esquema.

        Returns:
            list: Lista de tabelas do esquema.
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(PostgreSQLQueries.GET_TABLES, (schema,))
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            raise_msg = f"ENDPOINT - Erro ao obter as tabelas: {e}"
            logging.critical(raise_msg)
            return []

    def get_table_details(self, schema: str, table: str) -> Table:
        """
        Obtém os detalhes de uma tabela específica.

        Args:
            schema (str): Nome do esquema da tabela.
            table (str): Nome da tabela.

        Returns:
            Table: Objeto representando a estrutura da tabela.
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(PostgreSQLQueries.GET_TABLE_DETAILS, (schema, table))
                metadata_table = cursor.fetchone()
                if not metadata_table:
                    raise ValueError(f"Tabela {schema}.{table} não encontrada.")

                table_obj = Table(
                    schema_name=metadata_table[0],
                    table_name=metadata_table[1],
                    estimated_row_count=metadata_table[2],
                    table_size=metadata_table[3],
                )

                primary_keys = self.get_table_primary_key(table_obj)
                return self.get_table_columns(table_obj, primary_keys)
        except Exception as e:
            raise_msg = f"ENDPOINT - Erro ao obter os detalhes da tabela: {e}"
            logging.critical(raise_msg)
            raise ValueError(raise_msg)

    def get_table_primary_key(self, table: Table) -> list:
        """
        Obtém a lista de colunas que compõem a chave primária de uma tabela específica.

        Args:
            table (Table): Objeto representando a estrutura da tabela.

        Returns:
            list: Lista de colunas que compõem a chave primária da tabela.
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    PostgreSQLQueries.GET_TABLE_PRIMARY_KEY,
                    (table.schema_name, table.table_name),
                )
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            raise_msg = f"ENDPOINT - Erro ao obter as chaves primárias da tabela: {e}"
            logging.critical(raise_msg)
            raise ValueError(raise_msg)

    def get_table_columns(self, table: Table, primary_keys: list) -> Table:
        """
        Obtém as colunas de uma tabela específica e atualiza o objeto Table com
        os detalhes das colunas.

        Args:
            table (Table): Objeto representando a estrutura da tabela.
            primary_keys (list): Lista de colunas que compõem a chave primária da tabela.

        Returns:
            Table: Objeto Table atualizado com a estrutura das colunas da tabela.
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    PostgreSQLQueries.GET_TABLE_COLUMNS,
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
            raise_msg = f"ENDPOINT - Erro ao obter as colunas da tabela: {e}"
            logging.critical(raise_msg)
            raise ValueError(raise_msg)

    def get_full_load_from_table(self, table: Table) -> dict:
        """
        Realiza a extra o completa dos dados de uma tabela.

        Args:
            table (Table): Objeto representando a estrutura da tabela.

        Returns:
            dict: Dicionário contendo o log de execução do método
        """

        try:
            initial_time = time()

            with self.connection.cursor() as cursor:
                cursor.execute(
                    PostgreSQLQueries.GET_FULL_LOAD_FROM_TABLE.format(
                        schema=table.schema_name, table=table.table_name
                    )
                )
                data = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

                df = pl.DataFrame(data, schema=columns, orient="row")
                df.write_parquet(table.path_data)

                return {
                    "success": True,
                    "rowcount": cursor.rowcount,
                    "statusmessage": cursor.statusmessage,
                    "path_data": table.path_data,
                    "time_elapsed": f"{time() - initial_time:.2f}s",
                }
        except Exception as e:
            raise_msg = f"ENDPOINT - Erro ao obter a carga completa: {e}"
            logging.critical(raise_msg)
            raise ValueError(raise_msg)

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
        """

        try:
            initial_time = time()

            with self.connection.cursor() as cursor:
                self._manage_table(
                    cursor,
                    table,
                    create_table_if_not_exists,
                    recreate_table_if_exists,
                    truncate_before_insert,
                )

                self._insert_full_load_data(cursor, table)

                self.commit()
                os.remove(table.path_data)

                return {
                    "message": "Full load data inserted successfully",
                    "success": True,
                    "time_elapsed": f"{time() - initial_time:.2f}s",
                }
        except Exception as e:
            self.rollback()
            raise_msg = f"ENDPOINT - Erro ao inserir dados: {e}"
            logging.critical(raise_msg)
            raise ValueError(raise_msg)

    def insert_cdc_into_table(
        self, table: Table, create_table_if_not_exists: bool = False
    ):
        """
        Insere dados de alterações em uma tabela de destino.

        Args:
            table (Table): Objeto representando a estrutura da tabela.
            create_table_if_not_exists (bool): Se True, cria a tabela caso ela não exista.

        Returns:
            dict: Dicionário contendo o log de execução do método.
        """

        try:
            with self.connection.cursor() as cursor:
                self._manage_table(cursor, table, create_table_if_not_exists)

                self._insert_cdc_data(cursor, table)

                self.commit()

                return {"message": "Cdc data inserted successfully", "success": True}

        except Exception as e:
            self.rollback()
            raise_msg = f"ENDPOINT - Erro ao inserir dados: {e}"
            logging.critical(raise_msg)
            raise ValueError(raise_msg)

    def _manage_table(
        self,
        cursor: psycopg2.extensions.cursor,
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
            cursor (psycopg2.extensions.cursor): Cursor do banco de dados para execução de comandos SQL.
            table (Table): Objeto representando a estrutura da tabela de origem.
            create_if_not_exists (bool): Indica se a tabela deve ser criada caso não exista.
            recreate_if_exists (bool): Indica se a tabela deve ser recriada caso já exista.
            truncate_before_insert (bool): Indica se a tabela deve ser truncada antes da inserção dos dados.

        Returns:
            None: Este método não retorna valores.

        Raises:
            psycopg2.Error: Se ocorrer um erro durante a execução dos comandos SQL.
        """

        try:
            if recreate_if_exists:
                create_if_not_exists = True
                logging.info(
                    f"ENDPOINT - Removendo tabela {table.target_schema_name}.{table.target_table_name}"
                )
                cursor.execute(
                    PostgreSQLQueries.DROP_TABLE.format(
                        schema=table.target_schema_name, table=table.target_table_name
                    )
                )

            if create_if_not_exists:
                cursor.execute(
                    PostgreSQLQueries.CHECK_TABLE_EXISTS,
                    (table.target_schema_name, table.target_table_name),
                )
                if cursor.fetchone()[0] == 0:
                    create_table_sql = table.mount_create_table()
                    logging.info(
                        f"ENDPOINT - Criando tabela {table.target_schema_name}.{table.target_table_name}"
                    )
                    cursor.execute(create_table_sql)

            if truncate_before_insert:
                logging.info(
                    f"ENDPOINT - Truncando tabela {table.target_schema_name}.{table.target_table_name}"
                )
                cursor.execute(
                    PostgreSQLQueries.TRUNCATE_TABLE.format(
                        schema=table.target_schema_name, table=table.target_table_name
                    )
                )
        except Exception as e:
            raise_msg = f"ENDPOINT - Erro ao gerenciar tabela {table.target_schema_name}.{table.target_table_name}: {e}"
            logging.critical(raise_msg)
            raise ValueError(raise_msg)

    def _insert_full_load_data(
        self, cursor: psycopg2.extensions.cursor, table: Table
    ) -> None:
        """
        Insere dados completos na tabela de destino.

        Args:
            cursor (psycopg2.extensions.cursor): Cursor do banco de dados para execução de comandos SQL.
            table (Table): Objeto representando a estrutura da tabela de origem e os dados a serem inseridos.

        Returns:
            None
        """

        try:
            table_column_names = [
                col.name
                for col in sorted(
                    table.columns.values(), key=lambda col: col.ordinal_position
                )
            ]
            query = sql.SQL(
                PostgreSQLQueries.INSERT_FULL_LOAD_DATA.format(
                    schema=table.target_schema_name,
                    table=table.target_table_name,
                    columns=", ".join(table_column_names),
                )
            )

            records = [tuple(row) for row in table.data.iter_rows()]

            logging.info(
                f"ENDPOINT - Inserindo {len(records)} registros na tabela {table.target_schema_name}.{table.target_table_name}"
            )
            execute_values(cursor, query, records, page_size=10000)
        except Exception as e:
            logging.critical(
                f"ENDPOINT - Erro ao inserir dados na tabela {table.target_schema_name}.{table.target_table_name}: {e}"
            )
            raise

    def _insert_cdc_data(
        self, cursor: psycopg2.extensions.cursor, table: Table
    ) -> None:
        raise NotImplementedError

    def capture_changes(self, **kargs) -> pl.DataFrame:
        try:
            slot_name = kargs.get("slot_name")
            with self.connection.cursor() as cursor:
                cursor.execute(
                    PostgreSQLQueries.VERIFY_IF_EXISTS_A_REPLICATION_SLOT, (slot_name,)
                )

                exists_replication_slot = cursor.fetchone()
                exists_replication_slot = exists_replication_slot[0]

                if not exists_replication_slot:
                    logging.info(f"ENDPOINT - Criando slot de replicação {slot_name}")
                    cursor.execute(
                        PostgreSQLQueries.CREATE_REPLICATION_SLOT, (slot_name,)
                    )
        except Exception as e:
            raise_msg = f"ENDPOINT - Erro ao obter/criar slot de replicação: {e}"
            raise ValueError(raise_msg)

        try:
            with self.connection.cursor() as cursor:
                logging.info(f"ENDPOINT - Capturando alterações de dados")
                cursor.execute(PostgreSQLQueries.GET_CHANGES, (slot_name,))

                data = cursor.fetchall()

                df = pl.DataFrame(
                    data, schema=[desc[0] for desc in cursor.description], orient="row"
                )

            return df

        except Exception as e:
            raise_msg = f"ENDPOINT - Erro ao capturar alterações de dados: {e}"
            raise ValueError(raise_msg)

    def structure_capture_changes(
        self, df_changes_captured: pl.DataFrame, save_files: bool = False
    ) -> Dict[str, Any]:
        df_changes_captured = df_changes_captured.sort("lsn")

        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        id = Utils.hash_6_chars()

        changes_structured = []
        for xid, group in df_changes_captured.group_by("xid"):
            transactions = self._process_transaction(group)
            changes_structured.extend(transactions)

        changes_structured = {
            "id": id,
            "creted_at": created_at,
            "data": changes_structured,
        }

        if changes_structured.get("data"):
            if save_files:
                with open(f"task/cdc_log/{id}.json", "w") as f:
                    json.dump(changes_structured, f, indent=4)

                df_changes_captured.write_csv(f"task/cdc_log/{id}.csv")

            return changes_structured

        return None
    
    def structure_capture_changes_to_dataframe(self, changes_strucuted: dict) -> pl.DataFrame:
        raise NotImplementedError

    def _process_transaction(self, group: pl.DataFrame) -> Dict[str, Any]:
        transactions = []
        current_transaction = None

        for row in group.iter_rows(named=True):
            data_info = self._parse_data_line(row["data"])

            if data_info is None:
                continue

            if data_info["operation"] in ("begin", "commit"):
                if data_info["operation"] == "begin":
                    current_transaction = {"xid": data_info["xid"], "operations": []}
                elif data_info["operation"] == "commit" and current_transaction:
                    current_transaction["commit_lsn"] = row["lsn"]
                    transactions.append(current_transaction)
                    current_transaction = None
            else:
                if current_transaction is not None:
                    # Filtra apenas operações DML válidas
                    if data_info["operation"] in ("insert", "update", "delete"):
                        current_transaction["operations"].append(data_info)

        return transactions

    def _parse_data_line(self, line: str) -> Dict[str, Any]:
        if line.startswith(("BEGIN", "COMMIT")):
            return {"operation": line.split()[0].lower(), "xid": int(line.split()[1])}

        # Extrai informações da operação DML
        pattern = r"table\s+([^.]+)\.([^:]+):\s+(INSERT|UPDATE|DELETE):\s+(.+)"
        match = re.match(pattern, line)
        if not match:
            return None

        schema_name, table_name, operation, rest = match.groups()

        result = {
            "schema_name": schema_name,
            "table_name": table_name,
            "operation": operation.lower(),
            "columns": [],
        }

        if operation.upper() == "DELETE" and rest.strip() == "(no-tuple-data)":
            return result

        # Parseia as colunas e valores
        if operation.upper() in ("INSERT", "UPDATE"):
            # Para INSERT e UPDATE, temos valores
            column_pattern = r"([^\s]+)\[([^\]]+)\]:'([^']*)'"
            columns = re.findall(column_pattern, rest)
            for col_name, col_type, col_value in columns:
                result["columns"].append(
                    {"name": col_name, "type": col_type, "value": col_value}
                )
        elif operation.upper() == "DELETE":
            # Para DELETE, só temos a condição (normalmente a PK)
            column_pattern = r"([^\s]+)\[([^\]]+)\]:'([^']*)'"
            columns = re.findall(column_pattern, rest)
            for col_name, col_type, col_value in columns:
                result["columns"].append(
                    {"name": col_name, "type": col_type, "value": col_value}
                )

        return result