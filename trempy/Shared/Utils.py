from typing import TYPE_CHECKING
import logging
import pickle
import uuid
import sys

if TYPE_CHECKING:
    from trempy.Tasks.Task import Task

class Utils:
    """
    Classe utilitária para ajudar na configuração e execução das tarefas.
    """

    @staticmethod
    def configure_logging() -> bool:
        """
        Configura o logging do aplicativo.

        Remove todos os handlers existentes e configura um novo handler
        para gravar mensagens de log no arquivo "app.log"
        """
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        logging.basicConfig(
            filename="app.log",
            level=logging.DEBUG,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )

        return True
    
    def log_exception_and_exit(e: Exception):
        """Loga a exceção no formato desejado e encerra o programa"""
        error_type = f"{type(e).__module__}.{type(e).__name__}"
        log_message = f"{error_type}: {str(e)}"
        
        logging.critical(log_message)
        sys.exit(1)

    @staticmethod
    def write_task_pickle(task: 'Task') -> None:
        """
        Salva a configuração da tarefa no arquivo "settings.pickle".
        """

        with open("task/settings.pickle", "wb") as f:
            pickle.dump(task, f)

    @staticmethod
    def read_task_pickle() -> 'Task':
        """
        Carrega a configura o da tarefa salva no arquivo "settings.pickle" e
        retorna um objeto Task.
        """

        with open("task/settings.pickle", "rb") as f:
            task: Task = pickle.load(f)
        return task

    @staticmethod
    def hash_6_chars() -> str:
        """
        Gera um hash MD5 a partir do texto passado e retorna apenas
        os 6 primeiros caracteres do hash em hexadecimal.
        """

        return uuid.uuid4().hex[:6].lower()