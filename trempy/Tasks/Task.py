from trempy.Shared.Types import (
    TaskType,
    CdcModeType,
    EndpointType,
    DatabaseType,
    StartType,
)
from trempy.Metadata.MetadataConnectionManager import MetadataConnectionManager
from trempy.Transformations.Transformation import Transformation
from pika.adapters.blocking_connection import BlockingChannel
from trempy.Messages.MessageProducer import MessageProducer
from trempy.Messages.MessageConsumer import MessageConsumer
from trempy.Loggings.Logging import ReplicationLogger
from trempy.Tasks.Exceptions.Exception import *
from trempy.Endpoints.Endpoint import Endpoint
from trempy.Filters.Filter import Filter
from typing import List, Dict, Optional
from trempy.Tables.Table import Table
import polars as pl
import re


logger = ReplicationLogger()


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
        start_mode: str = "continue",
        full_load_settings: dict = {},
        cdc_settings: dict = {},
        scd2_settings: dict = {},
        create_table_if_not_exists: bool = False,
        error_handling: dict = {},
    ) -> None:
        self.task_name = task_name
        self.replication_type = TaskType(replication_type)
        self.interval_seconds = interval_seconds

        self.source_endpoint = source_endpoint
        self.target_endpoint = target_endpoint

        self.start_mode = StartType(start_mode)

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

        self.stop_if_insert_error = error_handling.get("stop_if_insert_error", False)
        self.stop_if_update_error = error_handling.get("stop_if_update_error", False)
        self.stop_if_delete_error = error_handling.get("stop_if_delete_error", False)
        self.stop_if_upsert_error = error_handling.get("stop_if_upsert_error", False)
        self.stop_if_scd2_error = error_handling.get("stop_if_scd2_error", False)

        self.tables: List[Table] = []

        self.filters = []

        self.source_full_load_already_done = False
        self.target_full_load_already_done = False

        self.__validate()

    def __validate(self) -> None:
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
            logger.critical(e)

        if self.interval_seconds <= 0:
            e = InvalidIntervalSecondsError(
                "Intervalo de execução da tarefa inválido", self.interval_seconds
            )
            logger.critical(e)

        partner = re.compile(r"^[a-z0-9_]+$")
        task_name_valid = bool(partner.match(self.task_name))

        if not task_name_valid:
            e = InvalidTaskNameError("Nome da tarefa inválido", self.task_name)
            logger.critical(e)

        logger.info(f"TASK -  {self.task_name} válido")

    def __find_table(self, schema_name: str, table_name: str) -> Optional[Table]:
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

    def __execute_target_cdc_callback(
        self, changes_structured: dict, channel: BlockingChannel
    ):
        try:
            delivery_tag = changes_structured["delivery_tag"]

            try:
                df_changes_structured: dict = (
                    self.target_endpoint.structure_capture_changes_to_dataframe(
                        changes_structured
                    )
                )
            except TaskError as e:
                channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
                e = TaskError(f"Erro ao estruturar as alterações capturadas: {str(e)}")
                logger.critical(e)

            for table in sorted(self.tables, key=lambda x: x.priority.value):
                data: pl.DataFrame = df_changes_structured.get(table.id)

                if table.id in df_changes_structured.keys():
                    table.add_data(data)
                    table.execute_filters()
                    table.execute_transformations()

                    cdc_stats = self.target_endpoint.insert_cdc_into_table(
                        mode=self.cdc_mode,
                        table=table,
                        create_table_if_not_exists=self.create_table_if_not_exists,
                    )

                    with MetadataConnectionManager() as metadata_manager:
                        metadata_manager.insert_stats_cdc(
                            cdc_stats, task_name=self.task_name
                        )

            channel.basic_ack(delivery_tag=delivery_tag)
            logger.info(f"TASK - Confirmado {delivery_tag}")

        except Exception as e:
            e = TaskError(f"Erro ao realizar carga de alterações: {str(e)}")
            logger.critical(e)

    def execute_source_full_load(self) -> bool:
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

        if not self.source_full_load_already_done and self.replication_type.value in (
            "full_load",
            "full_load_and_cdc",
        ):
            try:
                for table in sorted(self.tables, key=lambda x: x.priority.value):
                    logger.info(
                        f"TASK - Obtendo dados da tabela {table.target_schema_name}.{table.target_table_name}",
                        required_types=["full_load_and_cdc", "full_load"],
                    )
                    table.path_data = f"{self.PATH_FULL_LOAD_STAGING_AREA}{self.task_name}_{table.target_schema_name}_{table.target_table_name}.parquet"
                    table_source_stats = self.source_endpoint.get_full_load_from_table(
                        table=table
                    )
                    logger.debug(table_source_stats)
                    with MetadataConnectionManager() as metadata_manager:
                        metadata_manager.insert_stats_source_tables(
                            table_source_stats, task_name=self.task_name
                        )

                self.source_full_load_already_done = True

            except Exception as e:
                e = TaskError(f"Erro ao executar carga completa da fonte: {e}")
                logger.critical(e)

            return True

        return False

    def execute_target_full_load(self) -> bool:
        """
        Executa a carga completa de dados no destino em Full Load.

        Percorre todas as tabelas especificadas na tarefa e executa a carga completa
        de dados para cada uma delas. A carga completa de dados é realizada em
        paralelo para todas as tabelas especificadas.
        """

        if not self.target_full_load_already_done and self.replication_type.value in (
            "full_load",
            "full_load_and_cdc",
        ):

            try:
                for table in sorted(self.tables, key=lambda x: x.priority.value):
                    table.data = pl.read_parquet(table.path_data)

                    table.execute_filters()
                    table.execute_transformations()

                    logger.info(
                        f"TASK - Realizando carga completa da tabela {table.target_schema_name}.{table.target_table_name}",
                        required_types=["full_load_and_cdc", "full_load"],
                    )
                    full_load_stats = self.target_endpoint.insert_full_load_into_table(
                        table=table,
                        create_table_if_not_exists=self.create_table_if_not_exists,
                        recreate_table_if_exists=self.recreate_table_if_exists,
                        truncate_before_insert=self.truncate_before_insert,
                    )

                    logger.debug(full_load_stats)
                    with MetadataConnectionManager() as metadata_manager:
                        metadata_manager.insert_stats_full_load(
                            full_load_stats, task_name=self.task_name
                        )

                    table.data = pl.DataFrame()

                self.target_full_load_already_done = True

            except Exception as e:
                e = TaskError(f"Erro ao realizar carga completa: {str(e)}")
                logger.critical(e)

            return True

        return False

    def execute_source_cdc(self) -> bool:
        if self.replication_type.value in (
            "cdc",
            "full_load_and_cdc",
        ):

            try:
                kargs = {"database_type": self.source_endpoint.database_type.value}

                match self.source_endpoint.database_type:
                    case DatabaseType.POSTGRESQL:
                        kargs["slot_name"] = self.task_name
                    case _:
                        e = DatabaseNotImplementedError(
                            "Banco de dados não implementado",
                            self.source_endpoint.database_type,
                        )
                        logger.critical(e)

                changes_captured = self.source_endpoint.capture_changes(**kargs)

                changes_structured = (
                    self.source_endpoint.structure_capture_changes_to_json(
                        changes_captured, task_tables=self.tables, **kargs
                    )
                )

                if changes_structured:
                    producer = MessageProducer(task_name=self.task_name)
                    producer.publish_message(messages=changes_structured)

            except Exception as e:
                e = TaskError(f"Erro ao executar captura de alterações: {e}")
                logger.critical(e)

            return True

        return False

    def execute_target_cdc(self) -> bool:
        if self.replication_type.value in (
            "cdc",
            "full_load_and_cdc",
        ):
            try:
                consumer = MessageConsumer(
                    task_name=self.task_name,
                    external_callback=self.__execute_target_cdc_callback,
                )
                consumer.start_consuming()

            except Exception as e:
                e = TaskError(f"Erro ao realizar carga de alterações: {str(e)}")
                logger.critical(e)

            return True

        return False

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

                logger.info(
                    f"TASK - Obtendo detalhes da tabela {schema_name}.{table_name}"
                )
                table_detail = self.source_endpoint.get_table_details(table)
                logger.debug(table_detail.to_dict())

                tables_detail.append(table_detail)

            self.tables = [table for table in tables_detail]

            return {"success": True, "tables": self.tables}
        except Exception as e:
            e = AddTablesError(
                f"Erro ao adicionar tabelas: {e}", f"{schema_name}.{table_name}"
            )
            logger.critical(e)

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
            table = self.__find_table(schema_name, table_name)
            table.transformations.append(transformation)
        except Exception as e:
            e = AddTransformationError(
                f"Erro ao adicionar transformação: {e}", f"{schema_name}.{table_name}"
            )
            logger.critical(e)

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
            table = self.__find_table(schema_name, table_name)
            table.filters.append(filter)
        except Exception as e:
            e = AddFilterError(
                f"Erro ao adicionar filtro: {e}", f"{schema_name}.{table_name}"
            )
            logger.critical(e)
