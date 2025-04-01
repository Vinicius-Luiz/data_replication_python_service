from datetime import datetime
from typing import List, Dict
import polars as pl
import re


class TransformationFunction:
    REQUIRED_COLUMN_TYPES = {
        "format_date": [pl.Datetime, pl.Date],
        "uppercase": [pl.Utf8],
        "lowercase": [pl.Utf8],
        "trim": [pl.Utf8],
        "extract_day": [pl.Datetime, pl.Date],
        "extract_month": [pl.Datetime, pl.Date],
        "extract_year": [pl.Datetime, pl.Date],
        "date_diff_years": [pl.Datetime, pl.Date],
        "concat": {"depends_on": [pl.Utf8]},
        "date_diff_years": {"depends_on": [pl.Datetime, pl.Date]},
    }

    REQUIRED_PARAMS = {
        "math_expression": ["expression"],
        "format_date": ["format"],
        "literal": ["value"],
        "concat": ["depends_on"],
        "date_diff_years": ["depends_on"],
    }

    @staticmethod
    def get_required_column_types(function_name: str) -> List | Dict:
        """Retorna as tipagens de coluna necess rias para execu o de uma
        fun o de transforma o.

        Parameters
        ----------
        function_name : str
            Nome da fun o de transforma o.

        Returns
        -------
        list
            Tipagens de coluna necess rias para a execu o da fun o.
        """
        return TransformationFunction.REQUIRED_COLUMN_TYPES.get(function_name, [])

    @staticmethod
    def get_required_params(function_name: str) -> list:
        """Retorna os par ametros necess rios para execu o de uma fun o de transforma o.

        Parameters
        ----------
        function_name : str
            Nome da fun o de transforma o.

        Returns
        -------
        list
            Par ametros necess rios para a execu o da fun o.
        """

        return TransformationFunction.REQUIRED_PARAMS.get(function_name, [])

    @staticmethod
    def date_diff_years(start_col: str, end_col: str, round_result: bool) -> pl.Expr:
        """
        Calcula a diferença em anos entre duas colunas de datas.

        Parameters
        ----------
        start_col : str
            Nome da coluna que representa a data de início.
        end_col : str
            Nome da coluna que representa a data de fim.
        round_result : bool
            Se True, o resultado será arredondado para o inteiro mais próximo.
            Caso contrário, o resultado será um float64 com 6 casas decimais.

        Returns
        -------
        polars.Expr
            Expressão Polars que calcula a diferença em anos entre as colunas de datas.
        """

        days_in_year = 365.2425
        return (
            pl.when(pl.col(end_col).is_null() | pl.col(start_col).is_null())
            .then(None)
            .otherwise(
                ((pl.col(end_col) - pl.col(start_col)).dt.total_days() / days_in_year)
                .round(0 if round_result else 6)
                .cast(pl.Int32 if round_result else pl.Float64)
            )
        )

    @staticmethod
    def date_now() -> pl.Expr:
        """
        Retorna a expressão Polars que representa a data e hora atuais.

        Returns
        -------
        polars.Expr
            Expressão Polars que contém a data e hora do momento atual.
        """

        return pl.lit(datetime.now()).cast(pl.Datetime)

    @staticmethod
    def concat(separator: str, depends_on: List[str]) -> pl.Expr:
        """
        Concatena as colunas especificadas na lista `depends_on` com o separador
        especificado.

        Parameters
        ----------
        separator : str
            Separador a ser utilizado para concatenar as colunas.
        depends_on : List[str]
            Lista de nomes das colunas a serem concatenadas.

        Returns
        -------
        polars.Expr
            Express o Polars que concatena as colunas com o separador especificado.
        """

        return pl.concat_str(
            [pl.col(col) for col in depends_on],
            separator=separator,
        )

    @staticmethod
    def literal(value: str) -> pl.Expr:
        """
        Retorna a express o Polars que representa um valor literal.

        Parameters
        ----------
        value : str
            Valor literal a ser representado como express o Polars.

        Returns
        -------
        polars.Expr
            Express o Polars que representa o valor literal.
        """

        return pl.lit(value)

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
