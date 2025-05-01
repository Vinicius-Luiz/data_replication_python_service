class TaskError(Exception):
    """Exceção lançada quando ocorre um erro na tarefa."""

    def __init__(self, message: str):
        prefixed_message = f"TASK - {message}"
        super().__init__(prefixed_message)


class InvalidTaskTypeError(TaskError):
    """Exceção lançada quando o tipo de tarefa é inválido."""

    def __init__(self, message: str, operation: str):
        super().__init__(f"{message} | {operation}")


class InvalidTaskNameError(TaskError):
    """Exceção lançada quando o nome da tarefa é inválido."""

    def __init__(self, message: str, task_name: str):
        super().__init__(f"{message} | {task_name}")


class AddTablesError(TaskError):
    """Exceção lançada quando ocorre um erro ao adicionar tabelas."""

    def __init__(self, message: str, table_name: str):
        super().__init__(f"{message} | {table_name}")

class AddTransformationError(TaskError):
    """Exceção lançada quando ocorre um erro ao adicionar transformações."""

    def __init__(self, message: str, table_name: str):
        super().__init__(f"{message} | {table_name}")

class AddFilterError(TaskError):
    """Exceção lançada quando ocorre um erro ao adicionar filtros."""

    def __init__(self, message: str, table_name: str):
        super().__init__(f"{message} | {table_name}")

class DatabaseNotImplementedError(TaskError):
    """Exceção lançada quando ocorre um erro ao manipular um banco de dados não implementado."""

    def __init__(self, message: str, database_type: str):
        super().__init__(f"{message} | {database_type}")

class InvalidIntervalSecondsError(TaskError):
    """Exceção lançada quando o intervalo de execução da tarefa é inválido."""

    def __init__(self, message: str, interval_seconds: int):
        super().__init__(f"{message} | {interval_seconds}")