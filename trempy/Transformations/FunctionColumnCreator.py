from trempy.Endpoints.Databases.PostgreSQL.DataTypes import DataType
from trempy.Transformations.FunctionColumn import FunctionColumn
from trempy.Shared.Types import TransformationOperationType
from datetime import datetime
from typing import List
import polars as pl


class FunctionColumnCreator(FunctionColumn):
    """Classe que cria expressões Polars para as funções de transformação."""

    REQUIRED_COLUMN_TYPES = {
        TransformationOperationType.CONCAT: {"depends_on": [pl.Utf8]},
        TransformationOperationType.DATE_DIFF_YEARS: [pl.Datetime, pl.Date],
    }

    REQUIRED_PARAMS = {
        TransformationOperationType.LITERAL: ["value", "value_type"],
        TransformationOperationType.CONCAT: ["depends_on"],
        TransformationOperationType.DATE_DIFF_YEARS: ["depends_on"],
    }

    @staticmethod
    def literal(value: str, value_type: str) -> pl.Expr:
        """Cria uma expressão Polars com um valor literal constante.

        Args:
            value: Valor string que será convertido para expressão Polars.
            value_type: Tipo do valor literal.

        Returns:
            Expressão Polars contendo o valor literal fornecido.
        """

        value_type = DataType.DataTypes.TYPE_DATABASE_TO_POLARS[value_type]
        return pl.lit(value).cast(value_type)

    @staticmethod
    def date_now() -> pl.Expr:
        """Cria uma expressão Polars com a data atual.

        Returns:
            Expressão Polars do tipo Datetime contendo o timestamp atual.
        """

        return pl.lit(datetime.now()).cast(pl.Date)

    @staticmethod
    def datetime_now() -> pl.Expr:
        """Cria uma expressão Polars com a data e hora atuais.

        Returns:
            Expressão Polars do tipo Datetime contendo o timestamp atual.
        """

        return pl.lit(datetime.now()).cast(pl.Datetime)

    @staticmethod
    def concat(separator: str, depends_on: List[str]) -> pl.Expr:
        """Concatena múltiplas colunas em uma única string com separador.

        Args:
            separator: String que será usada para separar os valores.
            depends_on: Lista de nomes de colunas a serem concatenadas.

        Returns:
            Expressão Polars que produz a string concatenada.

        Example:
            >>> concat("-", ["first_name", "last_name"])
            # Retorna expressão que concatena como "first_name-last_name"
        """

        return pl.concat_str(
            [pl.col(col) for col in depends_on],
            separator=separator,
        )

    @staticmethod
    def date_diff_years(start_col: str, end_col: str, round_result: bool) -> pl.Expr:
        """Calcula diferença em anos entre datas com opção de arredondamento.

        Args:
            start_col: Nome da coluna com data inicial.
            end_col: Nome da coluna com data final.
            round_result: Se True, retorna inteiro arredondado,
                         se False, retorna float com 6 decimais.

        Returns:
            Expressão Polars que calcula:
            - None se alguma data for nula
            - Diferença em anos (considerando 365.2425 dias/ano)
            - Tipo Int32 se round_result=True, Float64 caso contrário
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
