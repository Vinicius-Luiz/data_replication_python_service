from trempy.Endpoints.Databases.PostgreSQL.Subclasses.ConnectionManager import (
    ConnectionManager,
)
from trempy.Shared.Queries.QueryPostgreSQL import (
    ReplicationQueries as ReplicationQueriesPostgreSQL,
)  #  TODO eu preciso saber qual é o tipo de endpoint correto
from trempy.Loggings.Logging import ReplicationLogger
from trempy.Endpoints.Exceptions.Exception import *
from trempy.Shared.DataTypes import Datatype
from trempy.Shared.Utils import Utils
from trempy.Tables.Table import Table
from typing import Dict, List, Any
from datetime import datetime
import polars as pl
import re

logger = ReplicationLogger()


class CDCManager:
    """Responsabilidade: Gerenciar Change Data Capture (CDC)."""

    def __init__(self, connection_manager: ConnectionManager, batch_cdc_size: int):
        self.connection_manager = connection_manager
        self.batch_cdc_size = batch_cdc_size

    def __process_transaction(self, group: pl.DataFrame) -> Dict[str, Any]:
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
            data_info = self.__parse_data_line(row["data"])

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

    def __parse_data_line(self, line: str) -> Dict[str, Any]:
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

    def capture_changes(self, slot_name: str, database_type: str) -> pl.DataFrame:
        """
        Captura as alterações de dados de um slot de replicação lógico.

        Este método verifica se existe um slot de replicação com o nome fornecido nos argumentos.
        Se o slot não existir, ele cria um novo slot de replicação. Em seguida, captura as
        alterações de dados do slot de replicação e retorna como um DataFrame.

        Args:
            - slot_name (str): Nome do slot de replicação.

        Returns:
            pl.DataFrame: DataFrame contendo as alterações capturadas do slot de replicação.

        Raises:
            CaptureChangesError: Se ocorrer um erro ao criar ou ler o slot de replicação.
        """

        try:
            table_name = slot_name.rsplit('_', 1)[0]

            with self.connection_manager.cursor() as cursor:
                old_replication_slots = cursor.execute(
                    ReplicationQueriesPostgreSQL.VERIFY_OLD_REPLICATION_SLOT,
                    (f"{table_name}%", slot_name),
                )
                old_replication_slots = [slot[0] for slot in cursor.fetchall()]

                if old_replication_slots:
                    for old_replication_slot in old_replication_slots:
                        cursor.execute(
                            ReplicationQueriesPostgreSQL.DROP_REPLICATION_SLOT,
                            (old_replication_slot,),
                        )

                cursor.execute(
                    ReplicationQueriesPostgreSQL.VERIFY_IF_EXISTS_A_REPLICATION_SLOT,
                    (slot_name,),
                )

                exists_replication_slot = cursor.fetchone()
                exists_replication_slot = exists_replication_slot[0]

                if not exists_replication_slot:
                    logger.info(
                        f"ENDPOINT - Criando slot de replicação {slot_name}",
                        required_types=["cdc"],
                    )
                    cursor.execute(
                        ReplicationQueriesPostgreSQL.CREATE_REPLICATION_SLOT,
                        (slot_name,),
                    )
        except Exception as e:
            e = CaptureChangesError(f"Erro ao criar slot de replicação: {e}", slot_name)
            logger.critical(e, required_types=["cdc"])

        try:
            with self.connection_manager.cursor() as cursor:
                cursor.execute(ReplicationQueriesPostgreSQL.GET_CHANGES, (slot_name,))

                data = cursor.fetchall()

                df = pl.DataFrame(
                    data, schema=[desc[0] for desc in cursor.description], orient="row"
                )

            return df

        except Exception as e:
            e = CaptureChangesError(f"Erro ao ler slot de replicação: {e}", slot_name)
            logger.critical(e, required_types=["cdc"])

    def structure_capture_changes_to_json(
        self, df_changes_captured: pl.DataFrame, task_tables: List[Table], **kargs
    ) -> Dict:
        """
        Processa as mudanças capturadas em um dataframe e as transforma em um dicionário
        com as alterações de cada tabela separadas por schema_name e table_name.

        Args:
            df_changes_captured (pl.DataFrame): DataFrame com as mudanças capturadas
            task_tables (List[Table]): Lista com as tabelas que devem ser processadas

        Returns:
            Dict: Dicionário com as alterações de cada tabela

        Raises:
            StructureCaptureChangesToJsonError: Se ocorrer um erro ao estruturar as mudanças capturadas
        """

        try:
            source_database_type = kargs.get("database_type")

            df_changes_captured = df_changes_captured.sort("lsn")

            created_at = int(datetime.now().timestamp())
            id = Utils.hash_6_chars()

            changes_structured = []
            for xid, group in df_changes_captured.sort("xid").group_by("xid"):
                transactions = self.__process_transaction(group)
                changes_structured.extend(transactions)

            filtered_changes_structured = []
            if changes_structured:
                for operations in changes_structured:
                    for row in operations.get("operations"):
                        schema_name = row.get("schema_name")
                        table_name = row.get("table_name")
                        idx = f"{schema_name}.{table_name}"

                        table_ok = (
                            True
                            if idx in [table.id for table in task_tables]
                            else False
                        )

                        if table_ok:
                            filtered_changes_structured.append(row)

            if filtered_changes_structured:
                qtd_changes = len(filtered_changes_structured)
                json_changes_structured = {
                    "source_database_type": source_database_type,
                    "qtd_changes": qtd_changes,
                    "transaction_id": id,
                    "created_at": created_at,
                    "changes": [],
                }

                batch_page = 0
                batch_index_start = 0
                batch_index_end = 0

                while True:
                    batch_index_end = batch_index_start + self.batch_cdc_size
                    batch_changes_structured = filtered_changes_structured[
                        batch_index_start:batch_index_end
                    ]

                    if not batch_changes_structured:
                        break

                    json_changes_structured["changes"].append(
                        {
                            "batch_page": batch_page,
                            "batch_size": len(batch_changes_structured),
                            "operations": batch_changes_structured,
                        }
                    )

                    batch_page += 1
                    batch_index_start = batch_index_end

                return json_changes_structured

            return []

        except Exception as e:
            e = StructureCaptureChangesToJsonError(
                f"Erro ao estruturar as mudanças para json: {e}"
            )
            logger.critical(e, required_types=["cdc"])

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
                    col_value = Datatype.DatatypePostgreSQL.convert_value(
                        col_value, col_type
                    )  # TODO eu preciso saber qual é o tipo de endpoint correto

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
            logger.critical(e, required_types=["cdc"])
