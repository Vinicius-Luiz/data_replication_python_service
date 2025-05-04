from trempy.Shared.Types import TaskType, CdcModeType, EndpointType, DatabaseType
from trempy.Transformations.Transformation import Transformation
from trempy.Tasks.Exceptions.Exception import *
from trempy.Endpoints.Endpoint import Endpoint
from trempy.Filters.Filter import Filter
from trempy.Shared.Utils import Utils
from trempy.Tables.Table import Table
from typing import List, Optional
import polars as pl
import logging
import shutil
import json
import re
import os


class Task:
    """
    Classe que representa uma tarefa de replicação de dados.

    Attributes:
        task_name (str): O nome da tarefa.
        source_endpoint (Endpoint): O ponto de entrada de dados da tarefa.
        target_endpoint (Endpoint): O ponto de saída de dados da tarefa.
        replication_type (TaskType): O tipo de replicação da tarefa.
        create_table_if_not_exists (bool): Indica se a tabela deve ser criada se nao existir.
        recreate_table_if_exists (bool): Indica se a tabela deve ser recriada se ja existir.
        truncate_before_insert (bool): Indica se a tabela deve ser truncada antes de inserir os dados.
        id (str): O identificador da tarefa.
    """

    PATH_FULL_LOAD_STAGING_AREA = "data/full_load_data/"
    PATH_CDC_STAGING_AREA = "data/cdc_data/"

    def __init__(
        self,
        task_name: str,
        replication_type: str,
        interval_seconds: int = 60,
        source_endpoint: Endpoint = None,
        target_endpoint: Endpoint = None,
        full_load_settings: dict = {},
        cdc_settings: dict = {},
        scd2_settings: dict = {},
        create_table_if_not_exists: bool = False,
    ) -> None:
        self.task_name = task_name
        self.replication_type = TaskType(replication_type)
        self.interval_seconds = interval_seconds

        self.source_endpoint = source_endpoint
        self.target_endpoint = target_endpoint

        self.create_table_if_not_exists = create_table_if_not_exists
        self.recreate_table_if_exists: bool = full_load_settings.get(
            "recreate_table_if_exists", False
        )
        self.truncate_before_insert: bool = full_load_settings.get(
            "truncate_before_insert", False
        )

        self.cdc_mode: CdcModeType = CdcModeType(cdc_settings.get("mode", "default"))

        self.scd2_start_date_column_name: str = scd2_settings.get(
            "start_date_column_name", "scd_start_date"
        )
        self.scd2_end_date_column_name: str = scd2_settings.get(
            "end_date_column_name", "scd_end_date"
        )
        self.scd2_current_column_name: str = scd2_settings.get(
            "current_column_name", "scd_current"
        )
        self.scd2_date_type: str = scd2_settings.get("date_type", "timestamp") # "date" or "timestamp"

        self.tables: List[Table] = []

        self.filters = []

        self.validate()

    def validate(self) -> None:
        """
        Realiza a validação do tipo da tarefa associada ao objeto.

        Verifica se o tipo da tarefa está entre os valores permitidos e segue
        as especificações definidas para o sistema.

        Raises:
            InvalidTaskTypeError: Se o tipo da tarefa for inválido ou não estiver entre
                       os valores permitidos.
            InvalidTaskNameError: Se o nome da tarefa for inválido.
        """

        if self.replication_type not in TaskType:
            e = InvalidTaskTypeError("Tipo de tarefa inválido", self.replication_type)
            Utils.log_exception_and_exit(e)

        if self.interval_seconds <= 0:
            e = InvalidIntervalSecondsError(
                "Intervalo de execução da tarefa inválido", self.interval_seconds
            )
            Utils.log_exception_and_exit(e)

        partner = re.compile(r"^[a-z0-9_]+$")
        task_name_valid = bool(partner.match(self.task_name))

        if not task_name_valid:
            e = InvalidTaskNameError("Nome da tarefa inválido", self.task_name)
            Utils.log_exception_and_exit(e)

        logging.info(f"TASK - {self.task_name} válido")

    def clean_endpoints(self) -> None:
        """
        Limpa os endpoints da tarefa.

        Deve ser chamado quando a tarefa terminar de ser executada, para liberar
        recursos, disponibilizar variável para exportação via pickle e evitar problemas de concorrência.
        """
        self.source_endpoint = None
        self.target_endpoint = None

    def add_endpoint(self, endpoint: Endpoint) -> None:
        """
        Adiciona um endpoint à tarefa atual.

        Caso o tipo do endpoint seja SOURCE, ele é adicionado como endpoint de origem.
        Caso o tipo do endpoint seja TARGET, ele é adicionado como endpoint de destino.

        Args:
            endpoint (Endpoint): Endpoint a ser adicionado à tarefa.
        """

        if endpoint.endpoint_type == EndpointType.SOURCE:
            self.source_endpoint = endpoint
        elif endpoint.endpoint_type == EndpointType.TARGET:
            self.target_endpoint = endpoint

    def add_tables(self, table_names: List[dict]) -> dict:
        """
        Adiciona múltiplas tabelas à tarefa atual a partir de uma lista de definições.

        Cada tabela é criada e configurada com base nos dicionários fornecidos, que devem
        conter as informações necessárias para identificação das tabelas no banco de dados.

        Args:
            table_names (List[dict]): Lista de dicionários contendo as especificações das tabelas.
                Cada dicionário deve conter:
                - 'schema_name': str - Nome do esquema da tabela
                - 'table_name': str - Nome da tabela
                - 'columns': List[dict] - Lista de colunas (opcional)

        Returns:
            dict: Resultado da operação com a seguinte estrutura:

        Raises:
            AddTablesError: Se ocorrer algum erro ao adicionar as tabelas.
        """

        try:
            schema_name = None
            table_name = None
            tables_detail = []
            for table in table_names:
                schema_name = table.get("schema_name")
                table_name = table.get("table_name")

                logging.info(
                    f"TASK - Obtendo detalhes da tabela {schema_name}.{table_name}"
                )
                table_detail = self.source_endpoint.get_table_details(table)
                logging.debug(table_detail.to_dict())

                tables_detail.append(table_detail)

            self.tables = [table for table in tables_detail]

            return {"success": True, "tables": self.tables}
        except Exception as e:
            e = AddTablesError(
                f"Erro ao adicionar tabelas: {e}", f"{schema_name}.{table_name}"
            )
            Utils.log_exception_and_exit(e)

    def add_transformation(
        self, schema_name: str, table_name: str, transformation: Transformation
    ) -> None:
        """
        Adiciona uma transformação à tabela especificada.

        A transformação será aplicada à tabela durante o processamento, seguindo a ordem
        de prioridade definida. A tabela deve existir previamente na tarefa.

        Args:
            schema_name (str): Nome do esquema onde a tabela está localizada.
                Deve corresponder a um esquema existente no banco de dados.
            table_name (str): Nome da tabela que receberá a transformação.
                Deve corresponder a uma tabela existente no esquema especificado.
            transformation (Transformation): Objeto contendo a definição completa da transformação,
                incluindo tipo, parâmetros e prioridade.

        Raises:
            AddTransformationError: Se ocorrer algum erro ao adicionar a transformação.
        """

        try:
            table = self.find_table(schema_name, table_name)
            table.transformations.append(transformation)
        except Exception as e:
            e = AddTransformationError(
                f"Erro ao adicionar transformação: {e}", f"{schema_name}.{table_name}"
            )
            Utils.log_exception_and_exit(e)

    def add_filter(self, schema_name: str, table_name: str, filter: Filter) -> None:
        """
        Adiciona um filtro à tabela especificada.

        A tabela deve existir previamente na tarefa.

        Args:
            schema_name (str): Nome do esquema onde a tabela está localizada.
            table_name (str): Nome da tabela que receberá o filtro.
            filter (Filter): Objeto contendo a definição completa do filtro.

        Raises:
            AddFilterError: Se ocorrer algum erro ao adicionar o filtro.
        """

        try:
            table = self.find_table(schema_name, table_name)
            table.filters.append(filter)
        except Exception as e:
            e = AddFilterError(
                f"Erro ao adicionar filtro: {e}", f"{schema_name}.{table_name}"
            )
            Utils.log_exception_and_exit(e)

    def find_table(self, schema_name: str, table_name: str) -> Optional[Table]:
        """
        Busca e retorna uma tabela específica na lista de tabelas da tarefa atual.

        Realiza uma busca exata (case-sensitive) pelo par (schema_name, table_name) na lista
        de tabelas associadas à tarefa. A comparação considera espaços e caracteres especiais.

        Args:
            schema_name (str): Nome do esquema onde a tabela está registrada.
            table_name (str): Nome da tabela a ser localizada.

        Returns:
            Optional[Table]: O objeto Table correspondente se encontrado, None caso a tabela
                           não exista na tarefa.
        """

        for table in sorted(self.tables, key=lambda x: x.priority.value):
            if table.schema_name == schema_name and table.table_name == table_name:
                return table

    def execute_source(self) -> dict:
        """
        Executa a tarefa de extração de dados da fonte.

        Dependendo do tipo de replicação, chama o método específico para extração de
        dados em Full Load ou CDC.

        Returns:
            dict: Resultado da operação com a seguinte estrutura:
        """

        match self.replication_type:
            case TaskType.FULL_LOAD:
                return self._execute_source_full_load()
            case TaskType.CDC:
                return self._execute_source_cdc()

    def execute_target(self) -> dict:
        """
        Executa a tarefa de carregamento de dados no destino.

        Dependendo do tipo de replicação, chama o método específico para carregamento de
        dados em Full Load ou CDC.

        Returns:
            dict: Resultado da operação com a seguinte estrutura:
        """

        match self.replication_type:
            case TaskType.FULL_LOAD:
                return self._execute_target_full_load()
            case TaskType.CDC:
                return self._execute_target_cdc()

    def _execute_source_full_load(self) -> dict:
        """
        Executa a extração completa de dados da fonte em Full Load.

        Percorre todas as tabelas especificadas na tarefa e executa a extração completa
        de dados para cada uma delas. A extração utiliza o método get_full_load_from_table
        da classe Endpoint, que pode ser implementado de acordo com a tecnologia do
        banco de dados.

        A extração completa de dados é realizada em paralelo para todas as tabelas
        especificadas.

        Returns:
            dict: Resultado da operação com a seguinte estrutura:
        """

        for table in sorted(self.tables, key=lambda x: x.priority.value):
            logging.info(
                f"TASK - Obtendo dados da tabela {table.target_schema_name}.{table.target_table_name}"
            )
            table.path_data = f"{self.PATH_FULL_LOAD_STAGING_AREA}{self.task_name}_{table.target_schema_name}_{table.target_table_name}.parquet"
            table_get_full_load = self.source_endpoint.get_full_load_from_table(
                table=table
            )
            logging.debug(table_get_full_load)

    def _execute_target_full_load(self) -> None:
        """
        Executa a carga completa de dados no destino em Full Load.

        Percorre todas as tabelas especificadas na tarefa e executa a carga completa
        de dados para cada uma delas. A carga completa de dados é realizada em
        paralelo para todas as tabelas especificadas.
        """

        for table in sorted(self.tables, key=lambda x: x.priority.value):
            table.data = pl.read_parquet(table.path_data)

            table.execute_filters()
            table.execute_transformations()

            logging.info(
                f"TASK - Realizando carga completa da tabela {table.target_schema_name}.{table.target_table_name}"
            )
            table_full_load = self.target_endpoint.insert_full_load_into_table(
                table=table,
                create_table_if_not_exists=self.create_table_if_not_exists,
                recreate_table_if_exists=self.recreate_table_if_exists,
                truncate_before_insert=self.truncate_before_insert,
            )
            logging.debug(table_full_load)

    def _execute_source_cdc(self) -> dict:
        # TODO enviar mensagem via RabbitMQ
        match self.source_endpoint.database_type:
            case DatabaseType.POSTGRESQL:
                kargs = {
                    "slot_name": self.task_name,
                }
            case _:
                e = DatabaseNotImplementedError(
                    "Banco de dados não implementado",
                    self.source_endpoint.database_type,
                )
                Utils.log_exception_and_exit(e)

        changes_captured = self.source_endpoint.capture_changes(**kargs)

        changes_structured = self.source_endpoint.structure_capture_changes_to_json(
            changes_captured, task_tables=self.tables, save_files=True
        )

        return changes_structured

    def _execute_target_cdc(self) -> bool:
        # TODO receber mensagem via RabbitMQ

        # Cria a pasta de destino se não existir
        processed_dir = r"data\cdc_data\processed"
        os.makedirs(processed_dir, exist_ok=True)

        # Lista todos os arquivos JSON na pasta cdc_log
        cdc_log_dir = r"data\cdc_data"
        for filename in os.listdir(cdc_log_dir):
            if filename.endswith(".json"):
                file_path = os.path.join(cdc_log_dir, filename)

                # Lê o arquivo JSON
                with open(file_path, "r", encoding="utf-8") as f:
                    changes_structured = json.load(f)
                    logging.debug(
                        f'changes_structured: {len(changes_structured.get("operations"))} linhas'
                    )

                # TODO somente isso será necessário no futuro
                logging.info(f"TASK - Estruturando alterações de dados")
                df_changes_structured = (
                    self.target_endpoint.structure_capture_changes_to_dataframe(
                        changes_structured
                    )
                )
                for table in sorted(self.tables, key=lambda x: x.priority.value):
                    data: pl.DataFrame = df_changes_structured.get(table.id)
                    table.data = data

                    try:
                        table.data.write_csv(
                            rf"{cdc_log_dir}\{filename}_before_{table.id}.csv"
                        )  # TODO temporário

                        logging.debug(f"{table.id} - {table.data.schema}")
                    except Exception as e:
                        pass

                    if table.id in df_changes_structured.keys():
                        table.execute_filters()
                        table.execute_transformations()

                        table_cdc = self.target_endpoint.insert_cdc_into_table(
                            mode=self.cdc_mode,
                            table=table,
                            create_table_if_not_exists=self.create_table_if_not_exists,
                        )
                    try:
                        table.data.write_csv(
                            rf"{cdc_log_dir}\{filename}_after_{table.id}.csv"
                        )  # TODO temporário
                    except Exception as e:
                        pass

                # TODO somente isso será necessário no futuro

                # Move o arquivo para a pasta processada
                dest_path = os.path.join(processed_dir, filename)
                shutil.move(file_path, dest_path)

        return True
