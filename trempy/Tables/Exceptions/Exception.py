class TableError(Exception):
    """Exceção lancada quando ocorre um erro na tabela."""

    def __init__(self, message: str):
        prefixed_message = f"TABLE - {message}"
        super().__init__(prefixed_message)