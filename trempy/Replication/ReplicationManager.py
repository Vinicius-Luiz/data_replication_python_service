from trempy.Replication.Factory.ReplicationStrategyFactory import (
    ReplicationStrategyFactory,
)
from trempy.Shared.Types import TaskType
from trempy.Replication.Exceptions.Exception import *
from trempy.Loggings.Logging import ReplicationLogger
from dotenv import load_dotenv
import json


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

            replication_type = task_settings["task"]["replication_type"]
            interval_seconds = task_settings["task"]["interval_seconds"]

            strategy = ReplicationStrategyFactory.create_strategy(
                mode=TaskType(replication_type),
                interval_seconds=interval_seconds,
            )

            strategy.execute(task_settings) # antes era task=self.task
        except Exception as e:
            e = ReplicationRunError(f"Erro durante a execução: {str(e)}")
            logger.critical(e)
