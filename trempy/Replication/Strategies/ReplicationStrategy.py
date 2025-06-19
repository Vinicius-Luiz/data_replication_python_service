from trempy.Metadata.MetadataConnectionManager import MetadataConnectionManager
from trempy.Endpoints.Factory.EndpointFactory import EndpointFactory
from trempy.Shared.Types import PriorityType, TaskType, CdcModeType
from trempy.Transformations.Transformation import Transformation
from trempy.Messages import Message, MessageDlx, MessageConsumer
from trempy.Loggings.Logging import ReplicationLogger
from trempy.Replication.Exceptions.Exception import *
from trempy.Filters.Filter import Filter
from task.credentials import credentials
from trempy.Tables.Table import Table
from abc import ABC, abstractmethod
from trempy.Tasks.Task import Task
from typing import Optional
import subprocess
import sys
import os

logger = ReplicationLogger()


class ReplicationStrategy(ABC):
    """
    Interface base para estratégias de replicação com métodos utilitários comuns.
    """

    def __create_subprocess(self, script_name: str) -> subprocess.Popen:
        """Cria um subprocesso para o script."""
        return subprocess.Popen(
            [sys.executable, script_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def __monitor_process(self, process: subprocess.Popen, script_name: str) -> int:
        """Monitora a execução do processo e registra logs."""
        exit_code = process.wait()
        output, errors = process.communicate()

        self.__log_output(script_name, exit_code, output, errors)
        return exit_code

    def __log_output(
        self,
        script_name: str,
        exit_code: int,
        output: Optional[str],
        errors: Optional[str],
    ) -> None:
        """Registra os resultados da execução."""

        message_data = {
            "script_name": script_name,
            "exit_code": exit_code,
        }

        if output:
            message_data["message_type"] = "output"
            message_data["output"] = output.strip()
        if errors:
            message_data["message_type"] = "errors"
            message_data["output"] = errors.strip()

        logger.debug(message_data)

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
                transformation.priority == PriorityType.LOWEST
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
                                "operation": "datetime_now",
                                "new_column_name": task.scd2_start_date_column_name,
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
                                "value": 1,
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

    def create_task(self, task_settings: dict) -> Task:
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

        if source_endpoint.batch_cdc_size > 1 and task.cdc_mode.value == "scd2":
            logger.warning(
                f"REPLICATION - CDC com modo SCD2 nao é recomendado para endpoints com batch_cdc_size > 1"
            )

        self.__configure_tables(task, task_settings)
        self.__configure_filters(task, task_settings)
        self.__configure_transformations(task, task_settings)

        task.clean_endpoints()
        return task

    def reload_task(self, task_name: str):

        logger.info("REPLICATION - Recarregando tarefa")

        if os.path.exists(r"task\settings.pickle"):
            os.remove(r"task\settings.pickle")

        with MetadataConnectionManager() as metadata_manager:
            logger.info("REPLICATION - Recriando tabelas de metadata")
            metadata_manager.create_tables()
            metadata_manager.truncate_tables()

        message = Message.Message(task_name)
        message_dlx = MessageDlx.MessageDlx(task_name)
        message_consumer = MessageConsumer.MessageConsumer(
            task_name, external_callback=None
        )

        message.delete_exchange()
        message.delete_dlx_exchange()

        message_dlx.delete_queue()
        message_consumer.delete_queue()

    def run_process(self, script_name: str) -> bool:
        """
        Executa um script Python como subprocesso.

        Args:
            script_name: Nome do script a ser executado.

        Returns:
            True se executou com sucesso, False caso contrário.
        """
        process = self.__create_subprocess(script_name)
        exit_code = self.__monitor_process(process, script_name)
        return exit_code == 0

    @abstractmethod
    def execute(self, task: Task) -> None:
        """Método principal para execução da estratégia."""
        pass
