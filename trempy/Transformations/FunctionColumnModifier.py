from trempy.Shared.TransformationDefinitions import TransformationDefinitions
from trempy.Transformations.FunctionColumn import FunctionColumn
from trempy.Shared.Types import TransformationOperationType
from trempy.Transformations.Exceptions.Exception import *
from trempy.Loggings.Logging import ReplicationLogger
import polars as pl
import re

logger = ReplicationLogger()


class FunctionColumnModifier(FunctionColumn):
    """Classe que representa uma coluna de função de transformação."""

    @staticmethod
    def get_required_column_types(operation_type: TransformationOperationType):
        """Retorna os tipos de coluna requeridos para uma operação específica."""
        return TransformationDefinitions.COLUMN_MODIFIER_TYPES.get(operation_type)

    @staticmethod
    def get_required_params(operation_type: TransformationOperationType):
        """Retorna os parâmetros requeridos para uma operação específica."""
        return TransformationDefinitions.COLUMN_MODIFIER_PARAMS.get(operation_type, [])

    @staticmethod
    def format_date(column_name: str, format: str) -> pl.Expr:
        """Formata uma coluna de data/datetime."""
        return pl.col(column_name).dt.strftime(format)

    @staticmethod
    def uppercase(column_name: str) -> pl.Expr:
        """Converte uma coluna para maiúsculas."""
        return pl.col(column_name).str.to_uppercase()

    @staticmethod
    def lowercase(column_name: str) -> pl.Expr:
        """Converte uma coluna para minúsculas."""
        return pl.col(column_name).str.to_lowercase()

    @staticmethod
    def trim(column_name: str) -> pl.Expr:
        """Remove espaços em branco do início e fim."""
        return pl.col(column_name).str.strip_chars()

    @staticmethod
    def extract_year(column_name: str) -> pl.Expr:
        """Extrai o ano de uma data."""
        return pl.col(column_name).dt.year()

    @staticmethod
    def extract_month(column_name: str) -> pl.Expr:
        """Extrai o mês de uma data."""
        return pl.col(column_name).dt.month()

    @staticmethod
    def extract_day(column_name: str) -> pl.Expr:
        """Extrai o dia de uma data."""
        return pl.col(column_name).dt.day()

    @staticmethod
    def math_expression(column_name: str, expression: str) -> pl.Expr:
        """Avalia uma expressão matemática segura em uma coluna usando Polars.

        Args:
            column_name: Nome da coluna alvo para os cálculos.
            expression: Expressão matemática usando 'value' como placeholder.
                Exemplo válido: "(value * 0.2) + 5"
                Operadores permitidos: +, -, *, /, ^

        Returns:
            Expressão Polars que aplica a transformação matemática.

        Raises:
            MathExpressionError: Em qualquer um destes casos:
                - Expressão vazia ou não string
                - Caracteres não permitidos na expressão
                - Operadores não permitidos
                - Sintaxe inválida
                - Falha na avaliação

        Example:
            >>> math_expression("price", "value * 1.1")  # Aumento de 10%
            # Retorna expressão Polars equivalente a: pl.col("price") * 1.1

        Note:
            - Implementação segura que não usa eval() diretamente na expressão original
            - 'value' é substituído pela referência à coluna Polars
            - Suporta apenas operações matemáticas básicas
        """

        # Dicionário de operadores permitidos
        OPERATORS = {
            "+": "__add__",
            "-": "__sub__",
            "*": "__mul__",
            "/": "__truediv__",
            "^": "__pow__",
        }

        # Validação básica da expressão
        if not isinstance(expression, str) or not expression.strip():
            e = MathExpressionError(
                "Expressão deve ser uma string não vazia", expression
            )
            logger.critical(e)

        # Remove espaços e valida caracteres
        clean_expr = expression.replace(" ", "")
        if not re.match(r"^[\d+\-*/\^().value]+$", clean_expr):
            e = MathExpressionError(
                "Expressão contém caracteres não permitidos", expression
            )
            logger.critical(e)

        # Verifica operadores permitidos
        used_ops = set(re.findall(r"[\+\-\*/^]", clean_expr))
        if not used_ops.issubset(OPERATORS.keys()):
            e = MathExpressionError(
                f"Operadores não permitidos: {used_ops - set(OPERATORS.keys())}",
                expression,
            )
            logger.critical(e)

        # Substitui 'value' pela coluna Polars
        expr_parts = []
        last_pos = 0

        for match in re.finditer(r"\bvalue\b", clean_expr):
            expr_parts.append(clean_expr[last_pos : match.start()])
            expr_parts.append(f"pl.col('{column_name}')")
            last_pos = match.end()

        expr_parts.append(clean_expr[last_pos:])
        parsed_expr = "".join(expr_parts)

        # Converte operadores para métodos Polars
        for op, method in OPERATORS.items():
            parsed_expr = parsed_expr.replace(op, f".{method}(")
            # Fecha os parênteses para operadores binários
            if op in "+-*/^":
                parsed_expr = parsed_expr.replace(op, f").{method}(")

        # Adiciona parênteses de fechamento necessários
        open_count = parsed_expr.count("(")
        close_count = parsed_expr.count(")")
        parsed_expr += ")" * (open_count - close_count)

        # Contexto seguro para avaliação
        safe_globals = {"pl": pl}

        try:
            # Avalia a expressão convertida
            return eval(parsed_expr, safe_globals, {})
        except Exception as e:
            e = MathExpressionError(
                f"Falha na avaliação da expressão: {str(e)}", expression
            )
            logger.critical(e)
