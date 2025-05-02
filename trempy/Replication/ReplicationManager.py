from trempy.Replication.Factory.ReplicationStrategyFactory import (
    ReplicationStrategyFactory,
)
from trempy.Endpoints.Factory.EndpointFactory import EndpointFactory
from trempy.Transformations.Transformation import Transformation
from trempy.Replication.Exceptions.Exception import *
from trempy.Filters.Filter import Filter
from task.credentials import credentials
from trempy.Shared.Utils import Utils
from trempy.Tasks.Task import Task
from dotenv import load_dotenv
import json


class ReplicationManager:
    """
    Classe principal que gerencia todo o processo de replicação de dados,
    incluindo carregamento de configurações, criação da tarefa e execução
    da estratégia de replicação apropriada.
    """

    def __init__(self, settings_file: str = "task/settings.json"):
        self.settings_file = settings_file
        self.task = None

    def load_settings(self) -> dict:
        """
        Carrega as configurações do arquivo JSON especificado.

        Returns:
            dict: Dicionário com todas as configurações carregadas do arquivo.
        """

        with open(self.settings_file, "r", encoding="utf-8") as f:
            return json.load(f)

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
        target_endpoint = EndpointFactory.create_endpoint(
            **credentials.get("target_endpoint")
        )

        task = Task(
            source_endpoint=source_endpoint,
            target_endpoint=target_endpoint,
            **task_settings.get("task"),
        )

        self._configure_tables(task, task_settings)
        self._configure_filters(task, task_settings)
        self._configure_transformations(task, task_settings)

        task.clean_endpoints()
        return task

    def _configure_tables(self, task: Task, task_settings: dict):
        """
        Configura as tabelas na tarefa com base nas configurações.

        Args:
            task (Task): Objeto Task a ser configurado.
            task_settings (dict): Configurações das tabelas.
        """

        task.add_tables(task_settings.get("tables", []))

    def _configure_filters(self, task: Task, task_settings: dict) -> None:
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

    def _configure_transformations(self, task: Task, task_settings: dict) -> None:
        """
        Configura as transformações para as tabelas na tarefa.

        Args:
            task (Task): Objeto Task a ser configurado.
            task_settings (dict): Configurações das transformações.
        """

        for transformation_config in task_settings.get("transformations", []):
            transformation = Transformation(**transformation_config.get("settings"))
            table_info = transformation_config.get("table_info")
            task.add_transformation(
                schema_name=table_info.get("schema_name"),
                table_name=table_info.get("table_name"),
                transformation=transformation,
            )

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

        Utils.configure_logging()
        load_dotenv()

        try:
            task_settings = self.load_settings()
            self.task = self.create_task(task_settings)

            strategy = ReplicationStrategyFactory.create_strategy(
                mode=self.task.replication_type,
                interval_seconds=self.task.interval_seconds,
            )

            strategy.execute(task=self.task)
        except Exception as e:
            e = ReplicationRunError(f"Erro durante a execução: {str(e)}")
            Utils.log_exception_and_exit(e)