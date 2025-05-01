class ReplicationError(Exception):
    """Exceção lançada quando ocorre um erro na replicação."""

    def __init__(self, message: str):
        prefixed_message = f"REPLICATION - {message}"
        super().__init__(prefixed_message)


class InvalidEndpointTypeError(ReplicationError):
    """Exceção lançada quando o tipo de endpoint é inválido."""

    def __init__(self, message: str, operation: str):
        super().__init__(f"{message} | {operation}")

class ReplicationRunError(ReplicationError):
    """Exceção lançada quando ocorre um erro ao executar a replicação."""

    def __init__(self, message: str):
        super().__init__(f"{message}")

class NotSupportedReplicationTypeError(ReplicationError):
    """Exceção lançada quando o tipo de replicação é inválido."""

    def __init__(self, message: str, operation: str):
        super().__init__(f"{message} | {operation}")

class ReplicationRuntimeError(ReplicationError):
    """Exceção lançada quando ocorre um erro ao executar a replicação."""

    def __init__(self, message: str):
        super().__init__(f"{message}")

class UnsportedExceptionError(ReplicationError):
    """Exceção lançada quando ocorre um erro ao executar a replicação."""

    def __init__(self, message: str):
        super().__init__(f"{message}")