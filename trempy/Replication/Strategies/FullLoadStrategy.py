from trempy.Replication.Strategies.ReplicationStrategy import ReplicationStrategy
from trempy.Loggings.Logging import ReplicationLogger
from trempy.Shared.Utils import Utils
from trempy.Tasks.Task import Task
import sys

logger = ReplicationLogger()


class FullLoadStrategy(ReplicationStrategy):
    """
    Estratégia de replicação para Full Load que executa sequencialmente:
    1. producer.py para extração completa dos dados
    2. consumer.py para carregamento no destino
    """

    def __setup_environment(self, task: Task) -> None:
        """Configura o ambiente para execução."""
        Utils.write_task_pickle(task)

    def __run_extraction(self) -> bool:
        """Executa o producer.py para extração de dados."""

        logger.info("FULL LOAD STRATEGY - Iniciando extração dos dados")
        return self.run_process("producer.py")

    def __run_loading(self) -> bool:
        """Executa o consumer.py para carregamento de dados."""
        logger.info("FULL LOAD STRATEGY - Iniciando carregamento dos dados")
        return self.run_process("consumer.py")

    def execute(self, task: Task) -> None:
        """
        Executa a estratégia Full Load em duas etapas sequenciais.

        Args:
            task (Task): Configuração da tarefa de replicação.

        Raises:
            SystemExit: Se qualquer um dos processos falhar.
        """
        self.__setup_environment(task)

        if not self.__run_extraction():
            sys.exit(1)

        if not self.__run_loading():
            sys.exit(1)

        logger.info("FULL LOAD STRATEGY - Full Load concluído com sucesso")
