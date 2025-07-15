from typing import Dict, Union
import logging
import sys
import os


class ReplicationLogger:
    LIBS_TO_IGNORE = [
        "pika",
        "matplotlib",
        "fonttools",
        "pyarrow",
        "openai",
        "requests",
        "httpcore",
        "httpx",
        "PIL",
        "Pillow",
    ]

    def __init__(self):
        """
        Inicializa o logger com o tipo de replicação.
        Se não for fornecido, obtém da variável de ambiente REPLICATION_TYPE.
        """

        self.replication_type = os.getenv("CURRENT_REPLICATION_TYPE")

    @staticmethod
    def configure_logging(level: str = "DEBUG") -> bool:
        """
        Configura o logging do aplicativo.

        Remove todos os handlers existentes e configura um novo handler
        para gravar mensagens de log no arquivo "app.log"

        Args:
            level: Nível de logging. Pode ser 'DEBUG', 'INFO', 'WARNING', 'ERROR' ou 'CRITICAL'.
        """
        match level:
            case "DEBUG":
                logging_level = logging.DEBUG
            case "INFO":
                logging_level = logging.INFO
            case "WARNING":
                logging_level = logging.WARNING
            case "ERROR":
                logging_level = logging.ERROR
            case "CRITICAL":
                logging_level = logging.CRITICAL
            case _:
                logging_level = logging.DEBUG

        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        logging.basicConfig(
            filename="app.log",
            level=logging_level,
            format="%(asctime)s - %(levelname)s - %(message)s",
            encoding="utf-8",
        )

        for lib in ReplicationLogger.LIBS_TO_IGNORE:
            logging.getLogger(lib).setLevel(logging.WARNING)

        return True

    def _should_log(self, required_types: Union[str, list]) -> bool:
        """
        Verifica se o log deve ser disparado com base no tipo de replicação.

        Args:
            required_types: Pode ser uma string ('full_load') ou lista (['full_load', 'cdc'])
                           Se for None ou vazio, o log é sempre disparado.
        """
        if not required_types:
            return True

        if isinstance(required_types, str):
            required_types = [required_types]

        return self.replication_type in required_types

    def critical(self, exception: Exception, required_types: Union[str, list] = None):
        """Loga uma exceção como crítica e encerra o programa"""
        if not self._should_log(required_types):
            return

        error_type = f"{type(exception).__module__}.{type(exception).__name__}"
        log_message = f"{error_type}: {str(exception)}"
        logging.critical(log_message)
        sys.exit(1)

    def error(self, exception: Exception, required_types: Union[str, list] = None):
        """Loga uma exceção como erro (não encerra o programa)"""
        if not self._should_log(required_types):
            return

        error_type = f"{type(exception).__module__}.{type(exception).__name__}"
        log_message = f"{error_type}: {str(exception)}"
        logging.error(log_message)

    def warning(self, message: str, required_types: Union[str, list] = None):
        """Loga uma mensagem de aviso"""
        if not self._should_log(required_types):
            return

        logging.warning(message)

    def info(self, message: str, required_types: Union[str, list] = None):
        """Loga uma mensagem informativa"""
        if not self._should_log(required_types):
            return

        logging.info(message)

    def debug(self, data: Dict, required_types: Union[str, list] = None):
        """Loga dados de depuração"""
        if not self._should_log(required_types):
            return

        logging.debug(data)
