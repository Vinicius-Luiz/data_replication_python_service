import logging
from Entities.Shared.Utils import Utils
from Entities.Replication.Strategies.ReplicationStrategy import ReplicationStrategy
from Entities.Tasks.Task import Task


class FullLoadStrategy(ReplicationStrategy):
    """
    Estratégia de replicação para Full Load que executa o producer uma vez para
    extrair todos os dados e depois executa o consumer para carregá-los no destino.
    """

    def execute(self, task: Task):
        """
        Executa a estratégia Full Load, primeiro executando o producer para extração
        completa dos dados e depois o consumer para carregamento.

        Args:
            task (Task): Objeto Task contendo a configuração da tarefa de replicação.
        """
        Utils.write_task_pickle(task)
        Utils.configure_logging() and logging.debug("Executando producer.py")
        self._run_process("producer.py")
        self._run_process("consumer.py")
