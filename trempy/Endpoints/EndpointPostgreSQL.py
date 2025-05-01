from trempy.Endpoints.Endpoint import Endpoint, EndpointType, DatabaseType
from trempy.Endpoints.DataTypes.EndpointDataTypePostgreSQL import DataTypes
from trempy.Endpoints.Exceptions.Exception import *
from trempy.Shared.Queries import PostgreSQLQueries
from trempy.Shared.Utils import Utils
from trempy.Tables.Table import Table
from trempy.Columns.Column import Column
from psycopg2 import sql
from psycopg2.extras import execute_values
from datetime import datetime
from time import time
from typing import Dict, List, Any
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
            EndpointError: Se houver um erro ao conectar ao banco de dados.
        """

        try:
            return psycopg2.connect(**credentials)
        except Exception as e:
            raise EndpointError(f"Erro ao conectar ao banco de dados: {e}")

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

    def get_table_details(self, table: dict) -> Table:
        """
        Obtém os detalhes de uma tabela específica.

        Args:
            table (dict): Informações da tabela.

        Returns:
            Table: Objeto representando a estrutura da tabela.

        Raises:
            TableNotFoundError: Se a tabela nao for encontrada.
            EndpointError: Se houver um erro ao obter os detalhes da tabela.
        """

        try:
            schema_name = table.get("schema_name")
            table_name = table.get("table_name")
            priority = table.get("priority")

            with self.connection.cursor() as cursor:
                cursor.execute(PostgreSQLQueries.GET_TABLE_DETAILS, (schema_name, table_name))
                metadata_table = cursor.fetchone()
                if not metadata_table:
                    raise TableNotFoundError(
                        f"Tabela não encontrada.", f"{schema_name}.{table_name}"
                    )

                table_obj = Table(
                    schema_name=metadata_table[0],
                    table_name=metadata_table[1],
                    priority=priority,
                    estimated_row_count=metadata_table[2],
                    table_size=metadata_table[3],
                )

                primary_keys = self.get_table_primary_key(table_obj)
                return self.get_table_columns(table_obj, primary_keys)
        except Exception as e:
            raise EndpointError(f"Erro ao obter os detalhes da tabela: {e}")

    def get_table_primary_key(self, table: Table) -> list:
        """
        Obtém a lista de colunas que compõem a chave primária de uma tabela específica.

        Args:
            table (Table): Objeto representando a estrutura da tabela.

        Returns:
            list: Lista de colunas que compõem a chave primária da tabela.

        Raises:
            EndpointError: Se houver um erro ao obter as chaves primarias da tabela.
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    PostgreSQLQueries.GET_TABLE_PRIMARY_KEY,
                    (table.schema_name, table.table_name),
                )
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            raise EndpointError(f"Erro ao obter as chaves primárias da tabela: {e}")

    def get_table_columns(self, table: Table, primary_keys: list) -> Table:
        """
        Obtém as colunas de uma tabela específica e atualiza o objeto Table com
        os detalhes das colunas.

        Args:
            table (Table): Objeto representando a estrutura da tabela.
            primary_keys (list): Lista de colunas que compõem a chave primária da tabela.

        Returns:
            Table: Objeto Table atualizado com a estrutura das colunas da tabela.

        Raises:
            EndpointError: Se houver um erro ao obter as colunas da tabela.
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
            raise EndpointError(f"Erro ao obter as colunas da tabela: {e}")

    def _mount_columns_to_create_table(self, table: Table) -> str:
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

    def _mount_primary_key_to_create_table(self, table: Table) -> str:
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
            columns_sql = self._mount_columns_to_create_table(table)
            primary_key_sql = self._mount_primary_key_to_create_table(table)
            return PostgreSQLQueries.CREATE_TABLE.format(
                schema=table.target_schema_name,
                table=table.target_table_name,
                columns=columns_sql,
                primary_key=primary_key_sql,
            )
        except Exception as e:
            raise EndpointError(f"Erro ao montar a string de criação da tabela: {e}")

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
            raise EndpointError(f"Erro ao obter a carga completa: {e}")

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
            raise EndpointError(f"Erro ao inserir dados no modo full load: {e}")

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
            with self.connection.cursor() as cursor:
                self._manage_table(cursor, table, create_table_if_not_exists)

                self._insert_cdc_data(cursor, table, mode)

                self.commit()

                return {"message": "CDC data inserted successfully", "success": True}

        except Exception as e:
            self.rollback()
            raise EndpointError(f"Erro ao inserir dados no modo CDC ({mode}): {e}")

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
            ManageTableError: Se ocorrer um erro durante a execução dos comandos SQL.
        """

        try:
            step = "recreate_if_exists"
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

            step = "create_if_not_exists"
            if create_if_not_exists:
                cursor.execute(
                    PostgreSQLQueries.CHECK_TABLE_EXISTS,
                    (table.target_schema_name, table.target_table_name),
                )
                if cursor.fetchone()[0] == 0:
                    create_table_sql = self.mount_create_table(table)
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
                    PostgreSQLQueries.TRUNCATE_TABLE.format(
                        schema=table.target_schema_name, table=table.target_table_name
                    )
                )
        except Exception as e:
            raise ManageTableError(
                f"Erro ao gerenciar tabela {table.target_schema_name}.{table.target_table_name}: {e}",
                step,
            )

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

        Raises:
            InsertFullLoadError: Se ocorrer um erro ao inserir os dados.
        """

        try:
            table_column_names = [
                col.name
                for col in sorted(
                    table.columns.values(), key=lambda col: col.ordinal_position
                )
            ]
            query = sql.SQL(
                PostgreSQLQueries.FULL_LOAD_INSERT_DATA.format(
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
            raise InsertFullLoadError(
                f"Erro ao inserir dados na tabela: {e}",
                table.target_table_name,
            )

    def _insert_cdc_data(
        self, cursor: psycopg2.extensions.cursor, table: Table, mode: str
    ) -> None:
        """
        Insere dados de altera es em uma tabela de destino.

        Insere dados de altera es em uma tabela de destino, considerando o modo
        de inser o especificado. Atualmente, o nico modo implementado  o
        "default", que simplesmente insere todos os dados da tabela.

        Args:
            cursor (psycopg2.extensions.cursor): Cursor do banco de dados para execu o de comandos SQL.
            table (Table): Objeto representando a estrutura da tabela de origem e os dados a serem inseridos.
            mode (str): Modo de inser o. Atualmente, o nico modo implementado  o "default".

        Raises:
            InsertCDCError: Se ocorrer um erro ao inserir os dados.
        """

        try:
            match mode:
                case "default":
                    self._insert_cdc_data_default(cursor, table)
                case _:
                    raise NotImplementedError  # TODO implementar outros modos
        except Exception as e:
            raise InsertCDCError(f"Erro ao inserir dados no modo CDC ({mode}): {e}")

    def _insert_cdc_data_default(
        self, cursor: psycopg2.extensions.cursor, table: Table
    ) -> None:
        """
        Insere dados de altera es em uma tabela de destino, considerando o modo
        de inser o "default".

        O modo "default" insere todos os dados da tabela, sem considerar o tipo
        de opera o (INSERT, UPDATE, DELETE).

        Args:
            cursor (psycopg2.extensions.cursor): Cursor do banco de dados para execu o de comandos SQL.
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

            execute_values(cursor, query, insert_records, page_size=10000)

        # Processar UPDATEs
        if operations["UPDATE"]:
            # Identificar PKs para cláusula WHERE
            pk_columns = [
                col.name for col in table.columns.values() if col.is_primary_key
            ]
            if not pk_columns:
                raise InsertCDCError(
                    f"Nenhuma PK encontrada para tabela", f"{schema}.{table_name}"
                )

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

                cursor.execute(query, set_values + where_values)

        # Processar DELETEs
        if operations["DELETE"]:
            # Identificar PKs para cláusula WHERE
            pk_columns = [
                col.name for col in table.columns.values() if col.is_primary_key
            ]
            if not pk_columns:
                raise InsertCDCError(
                    f"Nenhuma PK encontrada para tabela", f"{schema}.{table_name}"
                )

            # Para DELETE, podemos agrupar operações com os mesmos critérios
            delete_records = []
            for row in operations["DELETE"]:
                delete_records.append([row[col] for col in pk_columns])

            query = sql.SQL(PostgreSQLQueries.CDC_DELETE_DATA).format(
                schema=sql.Identifier(schema),
                table=sql.Identifier(table_name),
                pk_columns=sql.SQL(", ").join(map(sql.Identifier, pk_columns)),
            )

            execute_values(cursor, query, delete_records, page_size=10000)

    def capture_changes(self, **kargs) -> pl.DataFrame:
        """
        Captura as alterações de dados de um slot de replicação lógico.

        Este método verifica se existe um slot de replicação com o nome fornecido nos argumentos.
        Se o slot não existir, ele cria um novo slot de replicação. Em seguida, captura as
        alterações de dados do slot de replicação e retorna como um DataFrame.

        Args:
            **kargs: Argumentos que incluem:
                - slot_name (str): Nome do slot de replicação.

        Returns:
            pl.DataFrame: DataFrame contendo as alterações capturadas do slot de replicação.

        Raises:
            CaptureChangesError: Se ocorrer um erro ao criar ou ler o slot de replicação.
        """

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
            raise CaptureChangesError(
                f"Erro ao criar slot de replicação: {e}", slot_name
            )

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
            raise CaptureChangesError(f"Erro ao ler slot de replicação: {e}", slot_name)

    def structure_capture_changes_to_json(
        self,
        df_changes_captured: pl.DataFrame,
        task_tables: List[Table],
        save_files: bool = False,
    ) -> Dict[str, Any]:
        """
        Processa as mudanças capturadas em um dataframe e as transforma em um dicionário
        com as alterações de cada tabela separadas por schema_name e table_name.

        Args:
            df_changes_captured (pl.DataFrame): DataFrame com as mudanças capturadas
            task_tables (List[Table]): Lista com as tabelas que devem ser processadas
            save_files (bool, optional): Flag para salvar as mudanças em arquivos. Defaults to False.

        Returns:
            Dict[str, Any]: Dicionário com as alterações de cada tabela separadas por schema_name e table_name

        Raises:
            StructureCaptureChangesToJsonError: Se ocorrer um erro ao estruturar as mudanças capturadas
        """

        try:
            df_changes_captured = df_changes_captured.sort("lsn")

            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            id = Utils.hash_6_chars()

            changes_structured = []
            for xid, group in df_changes_captured.group_by("xid"):
                transactions = self._process_transaction(group)
                changes_structured.extend(transactions)
            changes_structured = changes_structured[0] if changes_structured else []

            filtered_changes_structured = []
            if changes_structured:
                for operation in changes_structured.get("operations"):
                    operation
                    schema_name = operation.get("schema_name")
                    table_name = operation.get("table_name")
                    idx = f"{schema_name}.{table_name}"

                    table_ok = (
                        True if idx in [table.id for table in task_tables] else False
                    )

                    if table_ok:
                        filtered_changes_structured.append(operation)

            if filtered_changes_structured:
                changes_structured = {
                    "id": id,
                    "creted_at": created_at,
                    "operations": filtered_changes_structured,
                }

                if save_files:  # TODO CARATER TEMPORÁRIO
                    with open(f"data/cdc_data/{id}.json", "w") as f:
                        json.dump(changes_structured, f, indent=4)

                    df_changes_captured.write_csv(f"data/cdc_data/{id}.csv")

                return changes_structured

            return None

        except Exception as e:
            raise StructureCaptureChangesToJsonError(
                f"Erro ao estruturar as mudanças para json: {e}"
            )

    def structure_capture_changes_to_dataframe(self, changes_structured: dict) -> dict:
        """
        Processa as mudanças capturadas em um dicionário e as transforma em um dicionário de DataFrames
        com as alterações de cada tabela separadas por schema_name e table_name.

        Args:
            changes_structured (dict): Dicionário com as mudanças capturadas

        Returns:
            Dict[str, pl.DataFrame]: Dicionário com as alterações de cada tabela separadas por schema_name e table_name

        Raises:
            StructureCaptureChangesToDataFrameError: Se ocorrer um erro ao estruturar as mudanças capturadas
        """

        try:
            tables_data = {}

            for op_index, operation in enumerate(
                changes_structured.get("operations", [])
            ):
                schema_name = operation.get("schema_name")
                table_name = operation.get("table_name")
                op_type = operation.get("operation", "").upper()

                # Pular DELETE com colunas vazias
                if op_type == "DELETE" and not operation.get("columns"):
                    continue

                # Chave para agrupamento
                key = f"{schema_name}.{table_name}"

                # Dados da linha
                row_data = {
                    "$TREM_OPERATION": op_type,
                    "$TREM_ROWNUM": op_index,  # Usando o índice do enumerate
                }

                # Processar colunas
                for column in operation.get("columns", []):
                    col_name = column["name"]
                    col_type = column["type"]
                    col_value = column["value"]

                    # Conversão de tipo numérico e data
                    col_value = DataTypes.convert_value(col_value, col_type)

                    row_data[col_name] = col_value

                # Adicionar aos dados da tabela
                if key not in tables_data:
                    tables_data[key] = {
                        "schema_name": schema_name,
                        "table_name": table_name,
                        "rows": [],
                    }

                tables_data[key]["rows"].append(row_data)

            # Converter para DataFrames
            result = dict()
            for key, table_info in tables_data.items():
                if not table_info["rows"]:
                    continue

                # Criar DataFrame
                df = pl.DataFrame(table_info["rows"])

                # Garantir a ordem das colunas
                cols = df.columns
                cols.remove("$TREM_OPERATION")
                cols.remove("$TREM_ROWNUM")
                df = df.select(["$TREM_ROWNUM", "$TREM_OPERATION"] + cols)

                result[f'{table_info["schema_name"]}.{table_info["table_name"]}'] = df

            return result

        except Exception as e:
            raise StructureCaptureChangesToDataFrame(
                f"Erro ao estruturar as mudanças para dataframe: {e}"
            )

    def _process_transaction(self, group: pl.DataFrame) -> Dict[str, Any]:
        """
        Processa um grupo de transações para extrair operações DML.

        Este método percorre as linhas de um DataFrame, interpretando cada linha como parte de uma transação
        lógica, começando com uma operação "begin" e terminando com uma operação "commit". Durante o percurso,
        operações DML válidas ("insert", "update", "delete") são coletadas em uma lista de operações.

        Args:
            group (pl.DataFrame): DataFrame contendo as linhas de dados a serem processadas.

        Returns:
            Dict[str, Any]: Um dicionário contendo transações processadas, cada uma incluindo suas operações DML.
        """

        transactions = []
        current_transaction = None

        for row in group.iter_rows(named=True):
            data_info = self._parse_data_line(row["data"])

            if data_info is None:
                continue

            if data_info["operation"] in ("begin", "commit"):
                if data_info["operation"] == "begin":
                    current_transaction = {"operations": []}
                elif data_info["operation"] == "commit" and current_transaction:
                    transactions.append(current_transaction)
                    current_transaction = None
            else:
                if current_transaction is not None:
                    # Filtra apenas operações DML válidas
                    if data_info["operation"] in ("insert", "update", "delete"):
                        current_transaction["operations"].append(data_info)

        return transactions

    def _parse_data_line(self, line: str) -> Dict[str, Any]:
        """
        Analisa uma linha do log de mudanças e extrai as informações sobre a operação DML

        Args:
            line (str): Linha do log de mudanças

        Returns:
            Dict[str, Any]: Dicionário com as informações sobre a operação DML
        """

        if line.startswith(("BEGIN", "COMMIT")):
            return {"operation": line.split()[0].lower()}

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
        if operation.upper() in ("INSERT", "UPDATE", "DELETE"):
            column_pattern = (
                r"([^\s\[]+)\[([^\]]+)\]:([^'\s]*(?:'[^']*'[^'\s]*)*)(?=\s|$)"
            )
            columns = re.findall(column_pattern, rest)
            for col_name, col_type, col_value in columns:
                # Remove aspas extras do valor se existirem
                if col_value.startswith("'") and col_value.endswith("'"):
                    col_value = col_value[1:-1]
                # Identificando valor nulo
                if col_value == "null":
                    col_value = None
                result["columns"].append(
                    {"name": col_name, "type": col_type, "value": col_value}
                )

        return result
