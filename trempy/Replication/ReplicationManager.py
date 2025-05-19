from trempy.Replication.Factory.ReplicationStrategyFactory import (
    ReplicationStrategyFactory,
)
from trempy.Shared.Types import PriorityType, TaskType, CdcModeType
from trempy.Endpoints.Factory.EndpointFactory import EndpointFactory
from trempy.Transformations.Transformation import Transformation
from trempy.Replication.Exceptions.Exception import *
from trempy.Loggings.Logging import ReplicationLogger
from trempy.Filters.Filter import Filter
from task.credentials import credentials
from trempy.Tables.Table import Table
from trempy.Tasks.Task import Task
from dotenv import load_dotenv
import json
import os


ReplicationLogger.configure_logging()
logger = ReplicationLogger()


class ReplicationManager:
    """
    Classe principal que gerencia todo o processo de replicação de dados,
    incluindo carregamento de configurações, criação da tarefa e execução
    da estratégia de replicação apropriada.
    """

    def __init__(self, settings_file: str = "task/settings.json"):
        self.settings_file = settings_file
        self.task = None

    def __load_settings(self) -> dict:
        """
        Carrega as configurações do arquivo JSON especificado.

        Returns:
            dict: Dicionário com todas as configurações carregadas do arquivo.
        """

        with open(self.settings_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def __create_task(self, task_settings: dict) -> Task:
        """
        Cria e configura um objeto Task com base nas configurações fornecidas.

        Args:
            task_settings (dict): Configurações da tarefa carregadas do arquivo.

        Returns:
            Task: Objeto Task totalmente configurado.
        """

        source_endpoint = EndpointFactory.create_endpoint(
            **credentials.get("source_endpoint")
        )

        task = Task(
            source_endpoint=source_endpoint,
            **task_settings.get("task"),
        )

        os.environ["REPLICATION_TYPE"] = task.replication_type.value

        self.__configure_tables(task, task_settings)
        self.__configure_filters(task, task_settings)
        self.__configure_transformations(task, task_settings)

        task.clean_endpoints()
        return task

    def __configure_tables(self, task: Task, task_settings: dict):
        """
        Configura as tabelas na tarefa com base nas configurações.

        Args:
            task (Task): Objeto Task a ser configurado.
            task_settings (dict): Configurações das tabelas.
        """

        task.add_tables(task_settings.get("tables", []))

    def __configure_filters(self, task: Task, task_settings: dict) -> None:
        """
        Configura os filtros para as tabelas na tarefa.

        Args:
            task (Task): Objeto Task a ser configurado.
            task_settings (dict): Configurações dos filtros.
        """

        for filter_config in task_settings.get("filters", []):
            filter = Filter(**filter_config.get("settings"))
            table_info = filter_config.get("table_info")
            task.add_filter(
                schema_name=table_info.get("schema_name"),
                table_name=table_info.get("table_name"),
                filter=filter,
            )

    def __configure_transformations(self, task: Task, task_settings: dict) -> None:
        """
        Configura as transformações para as tabelas na tarefa.

        Args:
            task (Task): Objeto Task a ser configurado.
            task_settings (dict): Configurações das transformações.
        """

        for transformation_config in task_settings.get("transformations", []):
            transformation = Transformation(**transformation_config.get("settings"))
            table_info: dict = transformation_config.get("table_info")
            task.add_transformation(
                schema_name=table_info.get("schema_name"),
                table_name=table_info.get("table_name"),
                transformation=transformation,
            )

            if (
                transformation.priority == PriorityType.VERY_LOW
                and task.replication_type == TaskType.CDC
                and task.cdc_mode == CdcModeType.SCD2
            ):
                logger.warning(
                    f"REPLICATION - Transformação com prioridade {transformation.priority.name} nao é recomendada com CDC com modo SCD2"
                )

        for table in sorted(task.tables, key=lambda x: x.priority.value):
            self.__prepare_to_scd2(task, table)

    def __prepare_to_scd2(self, task: Task, table: Table) -> None:
        try:
            if task.cdc_mode == CdcModeType.SCD2:
                logger.info(
                    f"REPLICATION - Preparando {table.schema_name}.{table.table_name} para SCD2"
                )
                scd2_transformations = [
                    {
                        "table_info": {
                            "schema_name": table.schema_name,
                            "table_name": table.table_name,
                        },
                        "settings": {
                            "transformation_type": "create_column",
                            "description": "Criando coluna scd_start_date",
                            "contract": {
                                "operation": "literal",
                                "new_column_name": task.scd2_start_date_column_name,
                                "value": None,
                                "value_type": "timestamp",
                                "is_scd2_column": True,
                                "scd2_column_type": "start_date",
                            },
                            "priority": 4,
                        },
                    },
                    {
                        "table_info": {
                            "schema_name": table.schema_name,
                            "table_name": table.table_name,
                        },
                        "settings": {
                            "transformation_type": "create_column",
                            "description": "Criando coluna scd_end_date",
                            "contract": {
                                "operation": "literal",
                                "new_column_name": task.scd2_end_date_column_name,
                                "value": None,
                                "value_type": "timestamp",
                                "is_scd2_column": True,
                                "scd2_column_type": "end_date",
                            },
                            "priority": 4,
                        },
                    },
                    {
                        "table_info": {
                            "schema_name": table.schema_name,
                            "table_name": table.table_name,
                        },
                        "settings": {
                            "transformation_type": "create_column",
                            "description": "Criando coluna scd_current",
                            "contract": {
                                "operation": "literal",
                                "new_column_name": task.scd2_current_column_name,
                                "value": None,
                                "value_type": "integer",
                                "is_scd2_column": True,
                                "scd2_column_type": "current",
                            },
                            "priority": 4,
                        },
                    },
                    {
                        "table_info": {
                            "schema_name": table.schema_name,
                            "table_name": table.table_name,
                        },
                        "settings": {
                            "transformation_type": "add_primary_key",
                            "description": "Adicionando chave primária scd_start_date",
                            "contract": {
                                "column_names": [task.scd2_start_date_column_name],
                            },
                            "priority": 4,
                        },
                    },
                ]
                for transformation_config in scd2_transformations:
                    transformation = Transformation(
                        **transformation_config.get("settings")
                    )
                    table_info: dict = transformation_config.get("table_info")
                    task.add_transformation(
                        schema_name=table_info.get("schema_name"),
                        table_name=table_info.get("table_name"),
                        transformation=transformation,
                    )
        except Exception as e:
            e = PrepareSCD2Error(
                f"Erro ao preparar para SCD2: {e}",
                f"{table.schema_name}.{table.table_name}",
            )
            logger.critical(e)

    def run(self):
        """
        Executa o fluxo completo de replicação:
        1. Configura logging e variáveis de ambiente
        2. Carrega configurações
        3. Cria a tarefa
        4. Seleciona e executa a estratégia de replicação apropriada

        Raises:
            Exception: Qualquer erro ocorrido durante a execução é registrado e relançado.
        """

        load_dotenv()

        try:
            task_settings = self.__load_settings()
            self.task = self.__create_task(task_settings)

            strategy = ReplicationStrategyFactory.create_strategy(
                mode=self.task.replication_type,
                interval_seconds=self.task.interval_seconds,
            )

            strategy.execute(task=self.task)
        except Exception as e:
            e = ReplicationRunError(f"Erro durante a execução: {str(e)}")
            logger.critical(e)
