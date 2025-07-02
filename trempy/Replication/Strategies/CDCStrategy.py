from trempy.Replication.Strategies.ReplicationStrategy import ReplicationStrategy
from trempy.Metadata.MetadataConnectionManager import MetadataConnectionManager
from trempy.Replication.Exceptions.Exception import *
from trempy.Loggings.Logging import ReplicationLogger
from trempy.Shared.Utils import Utils
from time import sleep
import subprocess
import sys
import os

logger = ReplicationLogger()


class CDCStrategy(ReplicationStrategy):
    """
    Estratégia de replicação para CDC (Change Data Capture) com:
    - Consumer rodando continuamente
    - Producer executando em intervalos regulares
    """

    def __init__(self, interval_seconds: int):
        self.interval_seconds = interval_seconds

    def __setup_environment(self, task_settings: dict):
        """Configura o ambiente para execução."""
        try:
            task_exists = True
            task = Utils.read_task_pickle()
        except FileNotFoundError:
            task_exists = False

        with MetadataConnectionManager() as metadata_manager:
            hash_id = metadata_manager.get_metadata_config("HASH_ID")
            if not hash_id:
                hash_id = Utils.hash_6_chars()
                metadata_manager.update_metadata_config({"HASH_ID": hash_id})

            task_settings["task"]["hash_id"] = hash_id
            if not task_exists or task.start_mode.value == "reload":
                task = self.create_task(task_settings)
                Utils.write_task_pickle(task)
                
            metadata_manager.update_metadata_config(
                {
                    "STOP_IF_INSERT_ERROR": str(int(task.stop_if_insert_error)),
                    "STOP_IF_UPDATE_ERROR": str(int(task.stop_if_update_error)),
                    "STOP_IF_DELETE_ERROR": str(int(task.stop_if_delete_error)),
                    "STOP_IF_UPSERT_ERROR": str(int(task.stop_if_upsert_error)),
                    "STOP_IF_SCD2_ERROR": str(int(task.stop_if_scd2_error)),
                }
            )

        logger.info(
            f"CDC STRATEGY - Iniciando CDC com intervalo de {self.interval_seconds}s"
        )

    def __start_consumer(self) -> None:
        """Inicia o processo do consumer em segundo plano."""
        self.consumer_process = subprocess.Popen(
            [sys.executable, "consumer.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def __start_dlx(self) -> None:
        """Inicia o processo do DLX em segundo plano."""
        subprocess.Popen(
            [sys.executable, "dlx.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def __run_cdc_loop(self) -> None:
        """Executa o loop principal do CDC."""
        while True:
            if not self.__run_producer():
                e = ReplicationRuntimeError("Erro ao executar o producer")
                logger.critical(e)
                logger.critical(e)

            self.__check_consumer_status()
            self.__wait_next_cycle()

    def __run_producer(self) -> bool:
        """Executa o producer e retorna o status."""
        return self.run_process("producer.py")

    def __check_consumer_status(self) -> None:
        """Verifica se o consumer está rodando corretamente."""
        if self.consumer_process.poll() is not None:
            exit_code = self.consumer_process.returncode
            e = ReplicationRuntimeError(
                f"Consumer encerrado inesperadamente (código: {exit_code})"
            )
            logger.critical(e)

    def __wait_next_cycle(self) -> None:
        """Aguarda o próximo ciclo de execução."""
        sleep(self.interval_seconds)

    def __graceful_shutdown(self) -> None:
        """Encerra os processos de forma controlada."""
        logger.info("CDC STRATEGY - Interrompendo processo CDC")
        self.consumer_process.terminate()
        self.consumer_process.wait()
        logger.info("CDC STRATEGY - CDC encerrado")

    def __emergency_shutdown(self) -> None:
        """Encerra os processos em caso de erro."""
        self.consumer_process.kill()
        sys.exit(1)

    def execute(self, task_settings: dict) -> None:
        """
        Executa a estratégia CDC em loop até interrupção.

        Args:
            task_settings (dict): Configuração da tarefa de replicação.

        Raises:
            SystemExit: Em caso de falha nos processos ou interrupção.
        """
        with MetadataConnectionManager() as metadata_manager:
            metadata_manager.update_metadata_config({"CURRENT_REPLICATION_TYPE": "cdc"})
            os.environ["CURRENT_REPLICATION_TYPE"] = "cdc"

        self.__setup_environment(task_settings)

        self.__start_dlx()
        self.__start_consumer()

        try:
            self.__run_cdc_loop()
        except KeyboardInterrupt:
            self.__graceful_shutdown()
        except Exception as e:
            self.__emergency_shutdown()
            e = UnsportedExceptionError(f"Erro inesperado: {str(e)}")
            logger.critical(e)
