from Entities.Transformations.FunctionColumn import FunctionColumn
from Entities.Shared.Types import OperationType
from datetime import datetime
from typing import List
import polars as pl


class FunctionColumnCreator(FunctionColumn):
    REQUIRED_COLUMN_TYPES = {
        OperationType.CONCAT: {"depends_on": [pl.Utf8]},
        OperationType.DATE_DIFF_YEARS: [pl.Datetime, pl.Date],
    }

    REQUIRED_PARAMS = {
        OperationType.LITERAL: ["value"],
        OperationType.CONCAT: ["depends_on"],
        OperationType.DATE_DIFF_YEARS: ["depends_on"],
    }

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
