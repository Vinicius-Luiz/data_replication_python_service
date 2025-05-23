class MetadataError(Exception):
    """Exceção lançada quando ocorre um erro na estrutura de metadados."""

    def __init__(self, message: str):
        prefixed_message = f"METADATA - {message}"
        super().__init__(prefixed_message)


class CreateTableError(MetadataError):
    """Exceção lançada quando ocorre um erro ao criar uma tabela."""

    def __init__(self, message: str):
        super().__init__(message)


class InsertStatsError(MetadataError):
    """Exceção lançada quando ocorre um erro ao inserir dados na tabela stats_cdc."""

    def __init__(self, message: str, table_name: str):
        super().__init__(f"{message} | {table_name}")

class RequiredColumnsError(MetadataError):
    """Exceção lançada quando ocorre um erro ao criar uma tabela."""

    def __init__(self, message: str):
        super().__init__(message)

class GetMetadataError(MetadataError):
    """Exceção lançada quando ocorre um erro ao criar uma tabela."""

    def __init__(self, message: str):
        super().__init__(message)