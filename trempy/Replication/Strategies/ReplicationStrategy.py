from trempy.Loggings.Logging import ReplicationLogger
from abc import ABC, abstractmethod
from trempy.Tasks.Task import Task
from typing import Optional
import subprocess
import sys

logger = ReplicationLogger()


class ReplicationStrategy(ABC):
    """
    Interface base para estratégias de replicação com métodos utilitários comuns.
    """

    def __create_subprocess(self, script_name: str) -> subprocess.Popen:
        """Cria um subprocesso para o script."""
        return subprocess.Popen(
            [sys.executable, script_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def __monitor_process(self, process: subprocess.Popen, script_name: str) -> int:
        """Monitora a execução do processo e registra logs."""
        exit_code = process.wait()
        output, errors = process.communicate()

        self.__log_output(script_name, exit_code, output, errors)
        return exit_code

    def __log_output(
        self,
        script_name: str,
        exit_code: int,
        output: Optional[str],
        errors: Optional[str],
    ) -> None:
        """Registra os resultados da execução."""

        message_data = {
            "script_name": script_name,
            "exit_code": exit_code,
        }

        if output:
            message_data["message_type"] = "output"
            message_data["output"] = output.strip()
        if errors:
            message_data["message_type"] = "errors"
            message_data["output"] = errors.strip()

        logger.debug(message_data)

    def run_process(self, script_name: str) -> bool:
        """
        Executa um script Python como subprocesso.

        Args:
            script_name: Nome do script a ser executado.

        Returns:
            True se executou com sucesso, False caso contrário.
        """
        process = self.__create_subprocess(script_name)
        exit_code = self.__monitor_process(process, script_name)
        return exit_code == 0

    @abstractmethod
    def execute(self, task: Task) -> None:
        """Método principal para execução da estratégia."""
        pass
