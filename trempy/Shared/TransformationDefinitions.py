from trempy.Shared.Types import TransformationOperationType
from trempy.Loggings.Logging import ReplicationLogger
from typing import Dict, Any
import polars as pl

logger = ReplicationLogger()


class TransformationDefinitions:
    """Classe que estrutura o mapeamento entre tipos de transformação e seus requisitos."""

    # Mapeamento dos tipos de colunas requeridos para cada operação
    COLUMN_CREATOR_TYPES = {
        TransformationOperationType.CONCAT: {"depends_on": [pl.Utf8]},
        TransformationOperationType.DATE_DIFF_YEARS: [pl.Datetime, pl.Date],
    }

    COLUMN_MODIFIER_TYPES = {
        TransformationOperationType.FORMAT_DATE: [pl.Datetime, pl.Date],
        TransformationOperationType.UPPERCASE: [pl.Utf8],
        TransformationOperationType.LOWERCASE: [pl.Utf8],
        TransformationOperationType.TRIM: [pl.Utf8],
        TransformationOperationType.EXTRACT_YEAR: [pl.Datetime, pl.Date],
        TransformationOperationType.EXTRACT_MONTH: [pl.Datetime, pl.Date],
        TransformationOperationType.EXTRACT_DAY: [pl.Datetime, pl.Date],
    }

    # Mapeamento dos parâmetros requeridos para cada operação
    COLUMN_CREATOR_PARAMS = {
        TransformationOperationType.LITERAL: ["value", "value_type"],
        TransformationOperationType.CONCAT: ["depends_on"],
        TransformationOperationType.DATE_DIFF_YEARS: ["depends_on"],
    }

    COLUMN_MODIFIER_PARAMS = {
        TransformationOperationType.FORMAT_DATE: ["format"],
        TransformationOperationType.MATH_EXPRESSION: ["expression"],
    }

    @classmethod
    def get_creator_operations(
        cls, depends_on: list, contract: dict
    ) -> Dict[TransformationOperationType, Dict[str, Any]]:
        """Retorna um dicionário de operações com base em tipos de operações definidos."""
        return {
            TransformationOperationType.LITERAL: {
                "func": lambda: cls.literal(
                    value=contract["value"], value_type=contract["value_type"]
                ),
                "required_params": cls.COLUMN_CREATOR_PARAMS.get(
                    TransformationOperationType.LITERAL, []
                ),
            },
            TransformationOperationType.DATE_NOW: {"func": lambda: cls.date_now()},
            TransformationOperationType.DATETIME_NOW: {
                "func": lambda: cls.datetime_now()
            },
            TransformationOperationType.CONCAT: {
                "func": lambda: cls.concat(
                    separator=contract.get("separator", ""),
                    depends_on=depends_on,
                ),
                "required_params": cls.COLUMN_CREATOR_PARAMS.get(
                    TransformationOperationType.CONCAT, []
                ),
                "column_type": cls.COLUMN_CREATOR_TYPES.get(
                    TransformationOperationType.CONCAT
                ),
            },
            TransformationOperationType.DATE_DIFF_YEARS: {
                "func": lambda: cls.date_diff_years(
                    start_col=depends_on[0],
                    end_col=depends_on[1],
                    round_result=contract.get("round_result", False),
                ),
                "required_params": cls.COLUMN_CREATOR_PARAMS.get(
                    TransformationOperationType.DATE_DIFF_YEARS, []
                ),
                "column_type": cls.COLUMN_CREATOR_TYPES.get(
                    TransformationOperationType.DATE_DIFF_YEARS
                ),
            },
        }

    @classmethod
    def get_modifier_operations(
        cls, column_name: str, contract: dict
    ) -> Dict[TransformationOperationType, Dict[str, Any]]:
        """Retorna um dicionário de operações de transformação disponíveis para uma coluna."""
        return {
            TransformationOperationType.FORMAT_DATE: {
                "func": lambda: cls.format_date(column_name, contract["format"]),
                "required_params": cls.COLUMN_MODIFIER_PARAMS.get(
                    TransformationOperationType.FORMAT_DATE, []
                ),
                "column_type": cls.COLUMN_MODIFIER_TYPES.get(
                    TransformationOperationType.FORMAT_DATE
                ),
            },
            TransformationOperationType.UPPERCASE: {
                "func": lambda: cls.uppercase(column_name),
                "column_type": cls.COLUMN_MODIFIER_TYPES.get(
                    TransformationOperationType.UPPERCASE
                ),
            },
            TransformationOperationType.LOWERCASE: {
                "func": lambda: cls.lowercase(column_name),
                "column_type": cls.COLUMN_MODIFIER_TYPES.get(
                    TransformationOperationType.LOWERCASE
                ),
            },
            TransformationOperationType.TRIM: {
                "func": lambda: cls.trim(column_name),
                "column_type": cls.COLUMN_MODIFIER_TYPES.get(
                    TransformationOperationType.TRIM
                ),
            },
            TransformationOperationType.EXTRACT_YEAR: {
                "func": lambda: cls.extract_year(column_name),
                "column_type": cls.COLUMN_MODIFIER_TYPES.get(
                    TransformationOperationType.EXTRACT_YEAR
                ),
            },
            TransformationOperationType.EXTRACT_MONTH: {
                "func": lambda: cls.extract_month(column_name),
                "column_type": cls.COLUMN_MODIFIER_TYPES.get(
                    TransformationOperationType.EXTRACT_MONTH
                ),
            },
            TransformationOperationType.EXTRACT_DAY: {
                "func": lambda: cls.extract_day(column_name),
                "column_type": cls.COLUMN_MODIFIER_TYPES.get(
                    TransformationOperationType.EXTRACT_DAY
                ),
            },
            TransformationOperationType.MATH_EXPRESSION: {
                "func": lambda: cls.math_expression(
                    column_name, contract["expression"]
                ),
                "required_params": cls.COLUMN_MODIFIER_PARAMS.get(
                    TransformationOperationType.MATH_EXPRESSION, []
                ),
            },
        }
