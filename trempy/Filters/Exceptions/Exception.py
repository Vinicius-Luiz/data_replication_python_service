class FilterError(Exception):
    """Exceção lançada quando ocorre um erro no filtro."""

    def __init__(self, message: str):
        prefixed_message = f"FILTER - {message}"
        super().__init__(prefixed_message)


class InvalidFilterTypeError(FilterError):
    """Exceção lançada quando o tipo de filtro é inválido."""

    def __init__(self, message: str, operation: str):
        super().__init__(f"{message} | {operation}")

class ColumnNotFoundError(FilterError):
    """Exceção lançada quando a coluna procurada nao foi encontrada."""

    def __init__(self, message: str, column_name: str):
        super().__init__(f"{message} | {column_name}")

class InvalidTypeValueError(FilterError):
    """Exceção lançada quando o tipo do valor do filtro é inválido."""

    def __init__(self, message: str, type_name: str):
        super().__init__(f"{message} | {type_name}")

class InvalidTypeDateError(FilterError):
    """Exceção lançada quando o tipo do valor de data do filtro é inválido."""

    def __init__(self, message: str, type_name: str):
        super().__init__(f"{message} | {type_name}")