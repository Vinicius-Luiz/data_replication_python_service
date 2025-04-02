from Entities.Transformations.FunctionColumn import FunctionColumn
from Entities.Shared.Types import OperationType
import polars as pl
import re


class FunctionColumnModifier(FunctionColumn):
    REQUIRED_COLUMN_TYPES = {
        OperationType.FORMAT_DATE: [pl.Datetime, pl.Date],
        OperationType.UPPERCASE: [pl.Utf8],
        OperationType.LOWERCASE: [pl.Utf8],
        OperationType.TRIM: [pl.Utf8],
        OperationType.EXTRACT_YEAR: [pl.Datetime, pl.Date],
        OperationType.EXTRACT_MONTH: [pl.Datetime, pl.Date],
        OperationType.EXTRACT_DAY: [pl.Datetime, pl.Date],
    }

    REQUIRED_PARAMS = {
        OperationType.FORMAT_DATE: ["format"],
        OperationType.MATH_EXPRESSION: ["expression"],
    }

    @staticmethod
    def format_date(column_name: str, format: str) -> pl.Expr:
        """
        Formata uma coluna de data para uma string conforme o formato especificado.

        Parameters
        ----------
        column_name : str
            Nome da coluna de data a ser formatada.
        format : str
            Formato de string para a formatação da data.

        Returns
        -------
        polars.Expr
            Expressão Polars que representa a coluna formatada como string.
        """

        return pl.col(column_name).dt.strftime(format)

    @staticmethod
    def extract_year(column_name: str) -> pl.Expr:
        """
        Extrai o ano de uma coluna de data.

        Parameters
        ----------
        column_name : str
            Nome da coluna de data a ser extrai o ano.

        Returns
        -------
        polars.Expr
            Expressão Polars que representa o ano extraido da coluna de data.
        """

        return pl.col(column_name).dt.year()

    @staticmethod
    def extract_month(column_name: str) -> pl.Expr:
        """
        Extrai o mês de uma coluna de data.

        Parameters
        ----------
        column_name : str
            Nome da coluna de data a ser extrai o mês.

        Returns
        -------
        polars.Expr
            Expressão Polars que representa o mês extraido da coluna de data.
        """

        return pl.col(column_name).dt.month()

    @staticmethod
    def extract_day(column_name: str) -> pl.Expr:
        """
        Extrai o dia de uma coluna de data.

        Parameters
        ----------
        column_name : str
            Nome da coluna de data a ser extrai o dia.

        Returns
        -------
        polars.Expr
            Expressão Polars que representa o dia extraido da coluna de data.
        """

        return pl.col(column_name).dt.day()

    @staticmethod
    def uppercase(column_name: str) -> pl.Expr:
        """
        Converte uma coluna de texto para maiúsculas.

        Parameters
        ----------
        column_name : str
            Nome da coluna de texto a ser convertida.

        Returns
        -------
        polars.Expr
            Expressão Polars que representa a coluna convertida para maiúsculas.
        """

        return pl.col(column_name).str.to_uppercase()

    @staticmethod
    def lowercase(column_name: str) -> pl.Expr:
        """
        Converte uma coluna de texto para minúsculas.

        Parameters
        ----------
        column_name : str
            Nome da coluna de texto a ser convertida.

        Returns
        -------
        polars.Expr
            Expressão Polars que representa a coluna convertida para minúsculas.
        """

        return pl.col(column_name).str.to_lowercase()

    @staticmethod
    def trim(column_name: str) -> pl.Expr:
        """
        Remove os espaços em branco de uma coluna de texto.

        Parameters
        ----------
        column_name : str
            Nome da coluna de texto a ser tratada.

        Returns
        -------
        polars.Expr
            Expressão Polars que representa a coluna sem espaços em branco.
        """

        return pl.col(column_name).str.strip()

    @staticmethod
    def math_expression(column_name: str, expression: str) -> pl.Expr:
        """
        Executa uma expressão matemática na coluna especificada de forma segura,
        sem usar map_eval.

        Parameters:
        -----------
        column_name : str
            Nome da coluna alvo
        expression : str
            Expressão matemática usando 'value' como placeholder para o valor da coluna
            Exemplo: "(value * 0.2) + 5"

        Returns:
        --------
        pl.Expr
            Expressão Polars resultante

        Raises:
        -------
        ValueError
            Se a expressão contiver elementos não permitidos
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
            raise ValueError("Expressão deve ser uma string não vazia")

        # Remove espaços e valida caracteres
        clean_expr = expression.replace(" ", "")
        if not re.match(r"^[\d+\-*/\^().value]+$", clean_expr):
            raise ValueError("Expressão contém caracteres não permitidos")

        # Verifica operadores permitidos
        used_ops = set(re.findall(r"[\+\-\*/^]", clean_expr))
        if not used_ops.issubset(OPERATORS.keys()):
            raise ValueError(
                f"Operadores não permitidos: {used_ops - set(OPERATORS.keys())}"
            )

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
            raise ValueError(f"Falha ao processar expressão: {str(e)}")
