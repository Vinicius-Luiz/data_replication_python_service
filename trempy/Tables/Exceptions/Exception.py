class TableError(Exception):
    """Exceção lancada quando ocorre um erro na tabela."""

    def __init__(self, message: str):
        prefixed_message = f"TABLE - {message}"
        super().__init__(prefixed_message)


class PrimaryKeyNotFoundError(TableError):
    """Exceção lançada quando a chave primária nao é encontrada."""

    def __init__(self, message: str, table_name: str):
        super().__init__(f"{message} | {table_name}")
