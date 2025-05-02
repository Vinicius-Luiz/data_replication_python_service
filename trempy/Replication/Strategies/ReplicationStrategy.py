from abc import ABC, abstractmethod
from trempy.Tasks.Task import Task
from typing import Optional
import subprocess
import logging
import sys


class ReplicationStrategy(ABC):
    """
    Interface base para estratégias de replicação com métodos utilitários comuns.
    """

    @abstractmethod
    def execute(self, task: Task) -> None:
        """Método principal para execução da estratégia."""
        pass

    def _run_process(self, script_name: str) -> bool:
        """
        Executa um script Python como subprocesso.

        Args:
            script_name: Nome do script a ser executado.

        Returns:
            True se executou com sucesso, False caso contrário.
        """
        try:
            process = self._create_subprocess(script_name)
            exit_code = self._monitor_process(process, script_name)
            return exit_code == 0
        except Exception as e:
            logging.error(f"Erro ao executar {script_name}: {str(e)}")
            return False

    def _create_subprocess(self, script_name: str) -> subprocess.Popen:
        """Cria um subprocesso para o script."""
        return subprocess.Popen(
            [sys.executable, script_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def _monitor_process(self, process: subprocess.Popen, script_name: str) -> int:
        """Monitora a execução do processo e registra logs."""
        exit_code = process.wait()
        output, errors = process.communicate()

        self._log_output(script_name, exit_code, output, errors)
        return exit_code

    def _log_output(
        self,
        script_name: str,
        exit_code: int,
        output: Optional[str],
        errors: Optional[str],
    ) -> None:
        """Registra os resultados da execução."""
        log_message = f"{script_name} - Exit code: {exit_code}"

        if exit_code == 0:
            logging.debug(log_message)
        else:
            logging.error(log_message)

        if output:
            logging.debug(f"{script_name} - Output: {output.strip()}")
        if errors:
            logging.error(f"{script_name} - Errors: {errors.strip()}")
