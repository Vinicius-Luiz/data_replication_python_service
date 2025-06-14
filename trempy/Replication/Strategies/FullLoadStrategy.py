from trempy.Replication.Strategies.ReplicationStrategy import ReplicationStrategy
from trempy.Metadata.MetadataConnectionManager import MetadataConnectionManager
from trempy.Loggings.Logging import ReplicationLogger
from trempy.Shared.Utils import Utils
import sys
import os

logger = ReplicationLogger()


class FullLoadStrategy(ReplicationStrategy):
    """
    Estratégia de replicação para Full Load que executa sequencialmente:
    1. producer.py para extração completa dos dados
    2. consumer.py para carregamento no destino
    """

    def __setup_environment(self, task_settings: dict):
        """Configura o ambiente para execução."""
        try:
            task_exists = True
            task = Utils.read_task_pickle()
        except FileNotFoundError:
            task_exists = False

        if not task_exists or task.start_mode.value == "reload":
            task = self.create_task(task_settings)
            Utils.write_task_pickle(task)

        with MetadataConnectionManager() as metadata_manager:
            metadata_manager.update_metadata_config(
                {
                    "STOP_IF_INSERT_ERROR": str(int(task.stop_if_insert_error)),
                    "STOP_IF_UPDATE_ERROR": str(int(task.stop_if_update_error)),
                    "STOP_IF_DELETE_ERROR": str(int(task.stop_if_delete_error)),
                    "STOP_IF_UPSERT_ERROR": str(int(task.stop_if_upsert_error)),
                    "STOP_IF_SCD2_ERROR": str(int(task.stop_if_scd2_error)),
                }
            )

    def __run_extraction(self) -> bool:
        """Executa o producer.py para extração de dados."""

        logger.info("FULL LOAD STRATEGY - Iniciando extração dos dados")
        return self.run_process("producer.py")

    def __run_loading(self) -> bool:
        """Executa o consumer.py para carregamento de dados."""
        logger.info("FULL LOAD STRATEGY - Iniciando carregamento dos dados")
        return self.run_process("consumer.py")

    def execute(self, task_settings: dict) -> None:
        """
        Executa a estratégia Full Load em duas etapas sequenciais.

        Args:
            task_settings (dict): Configuração da tarefa de replicação.

        Raises:
            SystemExit: Se qualquer um dos processos falhar.
        """
        with MetadataConnectionManager() as metadata_manager:
            metadata_manager.update_metadata_config(
                {"CURRENT_REPLICATION_TYPE": "full_load"}
            )
            os.environ["CURRENT_REPLICATION_TYPE"] = "full_load"

        self.__setup_environment(task_settings)

        if not self.__run_extraction():
            sys.exit(1)

        if not self.__run_loading():
            sys.exit(1)

        logger.info("FULL LOAD STRATEGY - Full Load concluído com sucesso")
