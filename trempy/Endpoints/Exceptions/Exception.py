class EndpointError(Exception):
    """Exceção lançada quando ocorre um erro no endpoint."""

    def __init__(self, message: str):
        prefixed_message = f"ENDPOINT - {message}"
        super().__init__(prefixed_message)


class InvalidEndpointTypeError(EndpointError):
    """Exceção lançada quando o tipo de endpoint é inválido."""

    def __init__(self, message: str, operation: str):
        super().__init__(f"{message} | {operation}")

class InvalidDatabaseTypeError(EndpointError):
    """Exceção lançada quando o tipo de banco de dados é inválido."""

    def __init__(self, message: str, operation: str):
        super().__init__(f"{message} | {operation}")

class TableNotFoundError(EndpointError):
    """Exceção lançada quando a tabela nao é encontrada."""

    def __init__(self, message: str, table_name: str):
        super().__init__(f"{message} | {table_name}")

class ManageTableError(EndpointError):
    """Exceção lançada quando ocorre um erro ao manipular uma tabela."""

    def __init__(self, message: str, step: str):
        super().__init__(f"{message} | {step}")

class InsertFullLoadError(EndpointError):
    """Exceção lançada quando ocorre um erro ao inserir dados de carga completa."""

    def __init__(self, message: str, table_name: str):
        super().__init__(f"{message} | {table_name}")

class InsertCDCError(EndpointError):
    """Exceção lançada quando ocorre um erro ao inserir dados do CDC."""

    def __init__(self, message: str, table_name: str):
        super().__init__(f"{message} | {table_name}")


class CaptureChangesError(EndpointError):
    """Exceção lançada quando ocorre um erro ao capturar as mudanças do banco de dados"""

    def __init__(self, message: str, slot_name: str):
        super().__init__(f"{message} | {slot_name}")

class StructureCaptureChangesToJsonError(EndpointError):
    """Exceção lançada quando ocorre um erro ao estruturar para json as mudanças capturadas do banco de dados"""

    def __init__(self, message: str):
        super().__init__(f"{message}")

class StructureCaptureChangesToDataFrame( EndpointError):
    """Exceção lançada quando ocorre um erro ao estruturar para dataframe as mudanças capturadas do banco de dados"""

    def __init__(self, message: str):
        super().__init__(f"{message}")