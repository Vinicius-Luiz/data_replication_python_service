from typing import TYPE_CHECKING
import pickle
import uuid


if TYPE_CHECKING:
    from trempy.Tasks.Task import Task


class Utils:
    """
    Classe utilitária para ajudar na configuração e execução das tarefas.
    """

    @staticmethod
    def write_task_pickle(task: "Task") -> None:
        """
        Salva a configuração da tarefa no arquivo "settings.pickle".
        """

        with open("task/settings.pickle", "wb") as f:
            pickle.dump(task, f)

    @staticmethod
    def read_task_pickle() -> "Task":
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
    
    from time import time

    @staticmethod
    def format_time_elapsed(seconds_float: float) -> str:
        """Formata um tempo em segundos (float) para HH:MM:SS (string)."""
        seconds = int(round(seconds_float))
        hours = seconds // 3600
        remaining_seconds = seconds % 3600
        minutes = remaining_seconds // 60
        seconds_remaining = remaining_seconds % 60
        return f"{hours:02d}:{minutes:02d}:{seconds_remaining:02d}"
