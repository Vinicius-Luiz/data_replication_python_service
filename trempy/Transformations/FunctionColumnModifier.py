from trempy.Transformations.FunctionColumn import FunctionColumn
from trempy.Shared.Types import TransformationOperationType
from trempy.Transformations.Exceptions.Exception import *
from trempy.Shared.Utils import Utils
import polars as pl
import re


class FunctionColumnModifier(FunctionColumn):
    """Classe que representa uma coluna de fun o de transforma o."""

    REQUIRED_COLUMN_TYPES = {
        TransformationOperationType.FORMAT_DATE: [pl.Datetime, pl.Date],
        TransformationOperationType.UPPERCASE: [pl.Utf8],
        TransformationOperationType.LOWERCASE: [pl.Utf8],
        TransformationOperationType.TRIM: [pl.Utf8],
        TransformationOperationType.EXTRACT_YEAR: [pl.Datetime, pl.Date],
        TransformationOperationType.EXTRACT_MONTH: [pl.Datetime, pl.Date],
        TransformationOperationType.EXTRACT_DAY: [pl.Datetime, pl.Date],
    }

    REQUIRED_PARAMS = {
        TransformationOperationType.FORMAT_DATE: ["format"],
        TransformationOperationType.MATH_EXPRESSION: ["expression"],
    }

    @staticmethod
    def format_date(column_name: str, format: str) -> pl.Expr:
        """Formata uma coluna de data/tempo para string usando o formato especificado.

        Args:
            column_name: Nome da coluna contendo valores de data/tempo.
            format: String de formato (ex: "%Y-%m-%d" para "2023-01-01").

        Returns:
            Expressão Polars que converte a coluna para string formatada.
        """

        return pl.col(column_name).dt.strftime(format)

    @staticmethod
    def extract_year(column_name: str) -> pl.Expr:
        """Extrai o componente ano de uma coluna de data/tempo.

        Args:
            column_name: Nome da coluna contendo valores de data/tempo.

        Returns:
            Expressão Polars que retorna o ano como valor numérico.
        """

        return pl.col(column_name).dt.year()

    @staticmethod
    def extract_month(column_name: str) -> pl.Expr:
        """Extrai o componente mês (1-12) de uma coluna de data/tempo.

        Args:
            column_name: Nome da coluna contendo valores de data/tempo.

        Returns:
            Expressão Polars que retorna o mês como valor numérico (1-12).
        """

        return pl.col(column_name).dt.month()

    @staticmethod
    def extract_day(column_name: str) -> pl.Expr:
        """Extrai o componente dia (1-31) de uma coluna de data/tempo.

        Args:
            column_name: Nome da coluna contendo valores de data/tempo.

        Returns:
            Expressão Polars que retorna o dia como valor numérico (1-31).
        """

        return pl.col(column_name).dt.day()

    @staticmethod
    def uppercase(column_name: str) -> pl.Expr:
        """Converte todos os caracteres de uma coluna de texto para maiúsculas.

        Args:
            column_name: Nome da coluna contendo strings.

        Returns:
            Expressão Polars com strings em maiúsculas.
        """

        return pl.col(column_name).str.to_uppercase()

    @staticmethod
    def lowercase(column_name: str) -> pl.Expr:
        """Converte todos os caracteres de uma coluna de texto para minúsculas.

        Args:
            column_name: Nome da coluna contendo strings.

        Returns:
            Expressão Polars com strings em minúsculas.
        """

        return pl.col(column_name).str.to_lowercase()

    @staticmethod
    def trim(column_name: str) -> pl.Expr:
        """Remove espaços em branco do início e fim de cada string na coluna.

        Args:
            column_name: Nome da coluna contendo strings.

        Returns:
            Expressão Polars com strings sem espaços extras nas extremidades.
        """

        return pl.col(column_name).str.strip()

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
            Utils.log_exception_and_exit(e)

        # Remove espaços e valida caracteres
        clean_expr = expression.replace(" ", "")
        if not re.match(r"^[\d+\-*/\^().value]+$", clean_expr):
            e = MathExpressionError(
                "Expressão contém caracteres não permitidos", expression
            )
            Utils.log_exception_and_exit(e)

        # Verifica operadores permitidos
        used_ops = set(re.findall(r"[\+\-\*/^]", clean_expr))
        if not used_ops.issubset(OPERATORS.keys()):
            e = MathExpressionError(
                f"Operadores não permitidos: {used_ops - set(OPERATORS.keys())}",
                expression,
            )
            Utils.log_exception_and_exit(e)

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
            Utils.log_exception_and_exit(e)
