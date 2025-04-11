import logging
import subprocess
import sys
from time import sleep
from trempy.Shared.Utils import Utils
from trempy.Tasks.Task import Task
from trempy.Replication.Strategies.ReplicationStrategy import ReplicationStrategy


class CDCStrategy(ReplicationStrategy):
    """
    Estratégia de replicação para CDC (Change Data Capture) que mantém o consumer
    rodando continuamente enquanto executa o producer em intervalos regulares.

    Args:
        interval_seconds (int): Intervalo em segundos entre execuções do producer.
    """

    def __init__(self, interval_seconds):
        self.interval_seconds = interval_seconds

    def execute(self, task: Task):
        """
        Executa a estratégia CDC, mantendo o consumer em execução contínua e
        acionando o producer periodicamente.

        Args:
            task (Task): Objeto Task contendo a configuração da tarefa de replicação.

        O método executa indefinidamente até ser interrompido por KeyboardInterrupt.
        """
        Utils.write_task_pickle(task)

        consumer_process = subprocess.Popen(
            [sys.executable, "consumer.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            while True:
                # Executa o producer periodicamente
                self._run_process("producer.py")
                logging.debug(
                    f"Aguardando {self.interval_seconds} segundos para próxima execução..."
                )
                sleep(self.interval_seconds)
        except KeyboardInterrupt:
            logging.info("Interrompendo processo CDC...")
            consumer_process.terminate()
            consumer_process.wait()
