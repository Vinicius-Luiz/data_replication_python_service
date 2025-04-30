from trempy.Shared.Utils import Utils
from trempy.Replication.Strategies.ReplicationStrategy import ReplicationStrategy
from trempy.Tasks.Task import Task
import logging
import sys

class FullLoadStrategy(ReplicationStrategy):
    """
    Estratégia de replicação para Full Load que executa sequencialmente:
    1. producer.py para extração completa dos dados
    2. consumer.py para carregamento no destino
    """

    def execute(self, task: Task) -> None:
        """
        Executa a estratégia Full Load em duas etapas sequenciais.

        Args:
            task (Task): Configuração da tarefa de replicação.

        Raises:
            SystemExit: Se qualquer um dos processos falhar.
        """
        self._setup_environment(task)
        
        logging.info("Iniciando processo de Full Load")
        
        if not self._run_extraction():
            logging.critical("Falha na extração de dados")
            sys.exit(1)
            
        if not self._run_loading():
            logging.critical("Falha no carregamento de dados")
            sys.exit(1)
            
        logging.info("Full Load concluído com sucesso")

    def _setup_environment(self, task: Task) -> None:
        """Configura o ambiente para execução."""
        Utils.write_task_pickle(task)
        Utils.configure_logging()

    def _run_extraction(self) -> bool:
        """Executa o producer.py para extração de dados."""
        logging.debug("Executando extração de dados (producer.py)")
        return self._run_process("producer.py")

    def _run_loading(self) -> bool:
        """Executa o consumer.py para carregamento de dados."""
        logging.debug("Executando carregamento de dados (consumer.py)")
        return self._run_process("consumer.py")