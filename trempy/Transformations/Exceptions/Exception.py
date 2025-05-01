class TransformationError(Exception):
    """Exceção lançada quando ocorre um erro na transformação."""

    def __init__(self, message: str):
        prefixed_message = f"TRANSFORMATION - {message}"
        super().__init__(prefixed_message)


class NewColumnNameError(TransformationError):
    """Exceção lançada quando o nome da nova coluna é inválido."""

    def __init__(self, message: str, new_column_name: str):
        super().__init__(f"{message} | {new_column_name}")


class InvalidDependencyError(TransformationError):
    """Exceção lançada quando uma coluna dependente é inválida."""

    def __init__(self, message: str, column_name: str):
        super().__init__(f"{message} | {column_name}")


class InvalidOperationError(TransformationError):
    """Exceção lançada quando uma operação de transformação é inválida."""

    def __init__(self, message: str, operation: str):
        super().__init__(f"{message} | {operation}")


class RequiredParameterError(TransformationError):
    """Exceção lançada quando um parâmetro obrigatório é faltando."""

    def __init__(self, message: str, parameter: str):
        super().__init__(f"{message} | {parameter}")


class InvalidColumnTypeError(TransformationError):
    """Exceção lançada quando o tipo de coluna é inválido."""

    def __init__(self, message: str, column_name: str, col_type: str):
        super().__init__(f"{message} | {column_name} ({col_type})")


class UpdateMetadataError(TransformationError):
    """Exceção lançada quando ocorre um erro ao atualizar os metadados."""

    def __init__(self, message: str, table_name: str):
        super().__init__(f"{message} | {table_name}")


class ColumnNameError(TransformationError):
    """Exceção lançada quando o nome da coluna é inválido."""

    def __init__(self, message: str, column_name: str):
        super().__init__(f"{message} | {column_name}")


class MathExpressionError(TransformationError):
    """Exceção lançada quando uma expressão matemática é inválida."""

    def __init__(self, message: str, expression: str):
        super().__init__(f"{message} | {expression}")


class InvalidTransformationTypeError(TransformationError):
    """Exceção lançada quando o tipo de transformação é inválido."""

    def __init__(self, message: str, operation: str):
        super().__init__(f"{message} | {operation}")
