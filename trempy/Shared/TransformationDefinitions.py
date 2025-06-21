from trempy.Shared.Types import (
    TransformationOperationType,
    OperationType,
    TransformationType,
)
from trempy.Loggings.Logging import ReplicationLogger
import polars as pl

logger = ReplicationLogger()


class TransformationDefinitions:
    """Classe que estrutura o mapeamento entre tipos de transformação e seus requisitos."""

    STRUCTURE_OPERATION_TYPES = {
        TransformationType.ADD_PRIMARY_KEY: OperationType.MODIFY_STRUCTURE,
        TransformationType.REMOVE_PRIMARY_KEY: OperationType.MODIFY_STRUCTURE,
        TransformationType.MODIFY_SCHEMA_NAME: OperationType.MODIFY_STRUCTURE,
        TransformationType.MODIFY_TABLE_NAME: OperationType.MODIFY_STRUCTURE,
        TransformationType.MODIFY_COLUMN_NAME: OperationType.MODIFY_STRUCTURE,
    }

    COLUMN_OPERATION_TYPES = {
        TransformationOperationType.CONCAT: OperationType.CREATE_COLUMN,
        TransformationOperationType.DATE_DIFF_YEARS: OperationType.CREATE_COLUMN,
        TransformationOperationType.DATE_NOW: OperationType.CREATE_COLUMN,
        TransformationOperationType.DATETIME_NOW: OperationType.CREATE_COLUMN,
        TransformationOperationType.LITERAL: OperationType.CREATE_COLUMN,
        TransformationOperationType.FORMAT_DATE: OperationType.MODIFY_COLUMN,
        TransformationOperationType.MATH_EXPRESSION: OperationType.MODIFY_COLUMN,
        TransformationOperationType.UPPERCASE: OperationType.MODIFY_COLUMN,
        TransformationOperationType.LOWERCASE: OperationType.MODIFY_COLUMN,
        TransformationOperationType.TRIM: OperationType.MODIFY_COLUMN,
        TransformationOperationType.EXTRACT_YEAR: OperationType.MODIFY_COLUMN,
        TransformationOperationType.EXTRACT_MONTH: OperationType.MODIFY_COLUMN,
        TransformationOperationType.EXTRACT_DAY: OperationType.MODIFY_COLUMN,
    }

    STRUCTURE_MODIFIER_TYPES = {
        TransformationType.ADD_PRIMARY_KEY: [pl.Utf8],
        TransformationType.REMOVE_PRIMARY_KEY: [pl.Utf8],
        TransformationType.MODIFY_SCHEMA_NAME: [pl.Utf8],
        TransformationType.MODIFY_TABLE_NAME: [pl.Utf8],
        TransformationType.MODIFY_COLUMN_NAME: [pl.Utf8],
    }

    COLUMN_CREATOR_TYPES = {
        TransformationOperationType.CONCAT: [pl.Utf8],
        TransformationOperationType.DATE_DIFF_YEARS: [pl.Datetime, pl.Date],
        TransformationOperationType.DATE_NOW: [],
        TransformationOperationType.DATETIME_NOW: [],
        TransformationOperationType.LITERAL: [],
    }

    COLUMN_MODIFIER_TYPES = {
        TransformationOperationType.FORMAT_DATE: [pl.Datetime, pl.Date],
        TransformationOperationType.MATH_EXPRESSION: [pl.Utf8],
        TransformationOperationType.UPPERCASE: [pl.Utf8],
        TransformationOperationType.LOWERCASE: [pl.Utf8],
        TransformationOperationType.TRIM: [pl.Utf8],
        TransformationOperationType.EXTRACT_YEAR: [pl.Datetime, pl.Date],
        TransformationOperationType.EXTRACT_MONTH: [pl.Datetime, pl.Date],
        TransformationOperationType.EXTRACT_DAY: [pl.Datetime, pl.Date],
    }

    STRUCTURE_MODIFIER_PARAMS = {
        TransformationType.ADD_PRIMARY_KEY: ["column_names"],
        TransformationType.REMOVE_PRIMARY_KEY: ["column_names"],
        TransformationType.MODIFY_SCHEMA_NAME: ["target_schema_name"],
        TransformationType.MODIFY_TABLE_NAME: ["target_table_name"],
        TransformationType.MODIFY_COLUMN_NAME: ["column_name", "target_column_name"],
    }

    COLUMN_CREATOR_PARAMS = {
        TransformationOperationType.LITERAL: ["new_column_name", "value", "value_type"],
        TransformationOperationType.CONCAT: ["new_column_name", "depends_on"],
        TransformationOperationType.DATE_DIFF_YEARS: ["new_column_name", "depends_on"],
        TransformationOperationType.DATE_NOW: ["new_column_name"],
        TransformationOperationType.DATETIME_NOW: ["new_column_name"],
    }

    COLUMN_MODIFIER_PARAMS = {
        TransformationOperationType.FORMAT_DATE: ["column_name", "format"],
        TransformationOperationType.MATH_EXPRESSION: ["column_name", "expression"],
        TransformationOperationType.UPPERCASE: ["column_name"],
        TransformationOperationType.LOWERCASE: ["column_name"],
        TransformationOperationType.TRIM: ["column_name"],
        TransformationOperationType.EXTRACT_YEAR: ["column_name"],
        TransformationOperationType.EXTRACT_MONTH: ["column_name"],
        TransformationOperationType.EXTRACT_DAY: ["column_name"],
    }
