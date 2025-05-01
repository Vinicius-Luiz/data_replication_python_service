import logging
import subprocess
import sys
from time import sleep
from trempy.Shared.Utils import Utils
from trempy.Tasks.Task import Task
from trempy.Replication.Strategies.ReplicationStrategy import ReplicationStrategy


class CDCStrategy(ReplicationStrategy):
    """
    Estratégia de replicação para CDC (Change Data Capture) com:
    - Consumer rodando continuamente
    - Producer executando em intervalos regulares
    """

    def __init__(self, interval_seconds: int):
        self.interval_seconds = interval_seconds

    def execute(self, task: Task) -> None:
        """
        Executa a estratégia CDC em loop até interrupção.

        Args:
            task (Task): Configuração da tarefa de replicação.

        Raises:
            SystemExit: Em caso de falha nos processos ou interrupção.
        """
        self._setup_environment(task)
        self._start_consumer()

        try:
            self._run_cdc_loop()
        except KeyboardInterrupt:
            self._graceful_shutdown()
        except Exception as e:
            logging.critical(f"Erro inesperado: {str(e)}")
            self._emergency_shutdown()
            raise

    def _setup_environment(self, task: Task) -> None:
        """Configura o ambiente para execução."""
        Utils.write_task_pickle(task)
        Utils.configure_logging()
        logging.info(f"STRATEGY - Iniciando CDC com intervalo de {self.interval_seconds}s")

    def _start_consumer(self) -> None:
        """Inicia o processo do consumer em segundo plano."""
        self.consumer_process = subprocess.Popen(
            [sys.executable, "consumer.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def _run_cdc_loop(self) -> None:
        """Executa o loop principal do CDC."""
        while True:
            if not self._run_producer():
                raise RuntimeError("Falha no producer")

            self._check_consumer_status()
            self._wait_next_cycle()

    def _run_producer(self) -> bool:
        """Executa o producer e retorna o status."""
        logging.debug("Executando ciclo do producer")
        return self._run_process("producer.py")

    def _check_consumer_status(self) -> None:
        """Verifica se o consumer está rodando corretamente."""
        if self.consumer_process.poll() is not None:
            exit_code = self.consumer_process.returncode
            raise RuntimeError(
                f"Consumer terminou inesperadamente (código: {exit_code})"
            )

    def _wait_next_cycle(self) -> None:
        """Aguarda o próximo ciclo de execução."""
        logging.debug(f"Aguardando {self.interval_seconds} segundos...")
        sleep(self.interval_seconds)

    def _graceful_shutdown(self) -> None:
        """Encerra os processos de forma controlada."""
        logging.info("STRATEGY - Interrompendo processo CDC")
        self.consumer_process.terminate()
        self.consumer_process.wait()
        logging.info("STRATEGY - CDC encerrado com sucesso")

    def _emergency_shutdown(self) -> None:
        """Encerra os processos em caso de erro."""
        self.consumer_process.kill()
        sys.exit(1)
