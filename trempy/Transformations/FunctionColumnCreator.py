from trempy.Shared.TransformationDefinitions import TransformationDefinitions
from trempy.Transformations.FunctionColumn import FunctionColumn
from trempy.Shared.Types import TransformationOperationType
from trempy.Shared.DataTypes import Datatype
from datetime import datetime
import polars as pl


class FunctionColumnCreator(FunctionColumn):
    """Classe que cria expressões Polars para as funções de transformação."""

    @staticmethod
    def get_required_column_types(operation_type: TransformationOperationType):
        """Retorna os tipos de coluna requeridos para uma operação específica."""
        return TransformationDefinitions.COLUMN_CREATOR_TYPES.get(operation_type)

    @staticmethod
    def get_required_params(operation_type: TransformationOperationType):
        """Retorna os parâmetros requeridos para uma operação específica."""
        return TransformationDefinitions.COLUMN_CREATOR_PARAMS.get(operation_type, [])

    @staticmethod
    def literal(value: str, value_type: str) -> pl.Expr:
        """Cria uma expressão Polars para um valor literal."""
        polars_type = Datatype.DatatypePostgreSQL.TYPE_DATABASE_TO_POLARS[value_type]
        if not isinstance(polars_type, (pl.DataType, type)):
            raise ValueError(f"Tipo inválido: {value_type}")
        return pl.lit(value).cast(polars_type)

    @staticmethod
    def date_now() -> pl.Expr:
        """Cria uma expressão Polars para a data atual."""
        return pl.lit(datetime.now().date())

    @staticmethod
    def datetime_now() -> pl.Expr:
        """Cria uma expressão Polars para o datetime atual."""
        return pl.lit(datetime.now())

    @staticmethod
    def concat(separator: str, depends_on: list) -> pl.Expr:
        """Concatena várias colunas em uma única expressão."""
        return pl.concat_str([pl.col(col) for col in depends_on], separator=separator)

    @staticmethod
    def date_diff_years(
        start_col: str, end_col: str, round_result: bool = False
    ) -> pl.Expr:
        """Calcula a diferença em anos entre duas datas."""
        diff = (pl.col(end_col).dt.year() - pl.col(start_col).dt.year()).cast(
            pl.Float64
        )
        if round_result:
            return diff.round(0).cast(pl.Int64)
        return diff
