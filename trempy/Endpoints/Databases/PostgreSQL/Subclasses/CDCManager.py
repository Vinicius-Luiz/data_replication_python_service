from trempy.Endpoints.Databases.PostgreSQL.Subclasses.ConnectionManager import (
    ConnectionManager,
)
from trempy.Endpoints.Databases.PostgreSQL.DataTypes.DataType import DataTypes
from trempy.Endpoints.Exceptions.Exception import *
from trempy.Endpoints.Databases.PostgreSQL.Queries.Query import Query
from trempy.Shared.Utils import Utils
from trempy.Tables.Table import Table
from datetime import datetime
from typing import Dict, List, Any
import polars as pl
import logging
import json
import re

"""
    capture_changes
    structure_capture_changes_to_json
    structure_capture_changes_to_dataframe
    _process_transaction
    _parse_data_line
"""


class CDCManager:
    """Responsabilidade: Gerenciar Change Data Capture (CDC)."""

    def __init__(self, connection_manager: ConnectionManager):
        self.connection_manager = connection_manager

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
            with self.connection_manager.cursor() as cursor:
                cursor.execute(
                    Query.VERIFY_IF_EXISTS_A_REPLICATION_SLOT, (slot_name,)
                )

                exists_replication_slot = cursor.fetchone()
                exists_replication_slot = exists_replication_slot[0]

                if not exists_replication_slot:
                    logging.info(f"ENDPOINT - Criando slot de replicação {slot_name}")
                    cursor.execute(
                        Query.CREATE_REPLICATION_SLOT, (slot_name,)
                    )
        except Exception as e:
            e = CaptureChangesError(f"Erro ao criar slot de replicação: {e}", slot_name)
            Utils.log_exception_and_exit(e)

        try:
            with self.connection_manager.cursor() as cursor:
                logging.info(f"ENDPOINT - Capturando alterações de dados")
                cursor.execute(Query.GET_CHANGES, (slot_name,))

                data = cursor.fetchall()

                df = pl.DataFrame(
                    data, schema=[desc[0] for desc in cursor.description], orient="row"
                )

            return df

        except Exception as e:
            e = CaptureChangesError(f"Erro ao ler slot de replicação: {e}", slot_name)
            Utils.log_exception_and_exit(e)

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
            e = StructureCaptureChangesToJsonError(
                f"Erro ao estruturar as mudanças para json: {e}"
            )
            Utils.log_exception_and_exit(e)

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
            e = StructureCaptureChangesToDataFrameError(
                f"Erro ao estruturar as mudanças para dataframe: {e}"
            )
            Utils.log_exception_and_exit(e)

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
