import subprocess
import sys
import logging
from Entities.Tasks.Task import Task
from abc import ABC, abstractmethod


class ReplicationStrategy(ABC):
    """
    Classe abstrata que define a interface para todas as estratégias de replicação.
    Fornece métodos utilitários comuns para execução e log de subprocessos.
    """

    @abstractmethod
    def execute(self, task: Task):
        """
        Método abstrato que deve ser implementado pelas subclasses para executar
        a estratégia específica de replicação.

        Args:
            task (Task): Objeto Task contendo a configuração da tarefa de replicação.
        """
        pass

    def _run_process(self, script_name):
        """
        Executa um script Python como subprocesso e monitora sua execução.

        Args:
            script_name (str): Nome do script Python a ser executado.

        Returns:
            bool: True se o processo foi executado com sucesso (código de saída 0), False caso contrário.
        """
        processo = subprocess.Popen(
            [sys.executable, script_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        codigo_saida = processo.wait()
        saida, erros = processo.communicate()

        self._log_process_output(script_name, codigo_saida, saida, erros)
        return codigo_saida == 0

    def _log_process_output(self, script_name, exit_code, output, errors):
        """
        Registra os resultados da execução de um subprocesso no log.

        Args:
            script_name (str): Nome do script executado.
            exit_code (int): Código de saída do processo.
            output (str): Saída padrão do processo.
            errors (str): Saída de erro do processo.
        """
        logging.info(f"{script_name} - Código de saída: {exit_code}")
        if output:
            logging.info(f"{script_name} - Saída: {output}")
        if errors:
            logging.error(f"{script_name} - Erros: {errors}")
