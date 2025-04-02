from __future__ import annotations
from Entities.Transformations.FunctionColumnCreator import FunctionColumnCreator as FCC
from Entities.Shared.Types import OperationType
from Entities.Columns.Column import Column
from typing import Dict, List, Any, TYPE_CHECKING
import polars as pl
import logging

if TYPE_CHECKING:
    from Entities.Tables.Table import Table


class ColumnCreator:
    """Classe responsável por validar e criar novas colunas."""

    TYPE_MAPPING = {
        pl.Int8: "smallint",
        pl.Int16: "smallint",
        pl.Int32: "integer",
        pl.Int64: "bigint",
        pl.Float32: "real",
        pl.Float64: "double precision",
        pl.Utf8: "character varying",
        pl.Boolean: "boolean",
        pl.Date: "date",
        pl.Datetime: "timestamp",
        pl.Decimal: "numeric",
        pl.Null: "text",
    }

    @staticmethod
    def get_operations(depends_on: list, contract: dict) -> Dict[str, Any]:
        return {
            OperationType.LITERAL: {
                "func": lambda: FCC.literal(value=contract["value"]),
                "required_params": FCC.get_required_params(OperationType.LITERAL),
            },
            OperationType.DATE_NOW: {"func": lambda: FCC.date_now()},
            OperationType.CONCAT: {
                "func": lambda: FCC.concat(
                    separator=contract.get("separator", ""),
                    depends_on=depends_on,
                ),
                "required_params": FCC.get_required_params(OperationType.CONCAT),
                "column_type": FCC.get_required_column_types(OperationType.CONCAT),
            },
            OperationType.DATE_DIFF_YEARS: {
                "func": lambda: FCC.date_diff_years(
                    start_col=depends_on[0],
                    end_col=depends_on[1],
                    round_result=contract.get("round_result", False),
                ),
                "required_params": FCC.get_required_params(
                    OperationType.DATE_DIFF_YEARS
                ),
                "column_type": FCC.get_required_column_types(
                    OperationType.DATE_DIFF_YEARS
                ),
            },
        }

    @staticmethod
    def _validate_basic_contract(new_column_name: str, table: Table) -> None:
        """Validações básicas do contrato."""
        if not new_column_name:
            raise ValueError("O contrato deve conter 'new_column_name'")

        if new_column_name in table.data.columns:
            raise ValueError(f"A coluna '{new_column_name}' já existe no DataFrame")

    @staticmethod
    def _validate_dependent_columns(depends_on: List[str], table: Table) -> None:
        """Valida colunas dependentes."""
        for col in depends_on:
            if col not in table.data.columns:
                available = list(table.data.columns)
                raise ValueError(
                    f"Coluna dependente '{col}' não encontrada. Disponíveis: {available}"
                )

    @staticmethod
    def _validate_operation(
        operation: str, operations: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Valida se a operação é suportada."""
        if operation not in operations:
            valid_ops = list(operations.keys())
            raise ValueError(
                f"Operação '{operation}' não suportada. Válidas: {valid_ops}"
            )
        return operations[operation]

    @staticmethod
    def _validate_required_params(
        op_config: Dict[str, Any], contract: Dict[str, Any]
    ) -> None:
        """Valida parâmetros obrigatórios."""
        for param in op_config.get("required_params", []):
            if param not in contract:
                raise ValueError(f"Parâmetro obrigatório faltando: '{param}'")

    @staticmethod
    def _validate_column_types(
        depends_on: List[str], op_config: Dict[str, Any], table: Table
    ) -> None:
        """Valida tipos das colunas de origem."""
        if (
            "column_type" not in op_config
            or "depends_on" not in op_config["column_type"]
        ):
            return

        for col in depends_on:
            actual_type = table.data.schema[col]
            expected_types = op_config["column_type"]["depends_on"]

            if not any(isinstance(actual_type, t) for t in expected_types):
                expected_names = [t.__name__ for t in expected_types]
                raise ValueError(
                    f"Tipo inválido para coluna '{col}'. "
                    f"Esperado: {expected_names}, Recebido: {type(actual_type).__name__}"
                )

    @classmethod
    def create_column(cls, contract: Dict[str, Any], table: Table) -> Table:
        """Fluxo principal para criação de colunas."""
        try:
            # Extrai parâmetros do contrato
            new_column_name = contract.get("new_column_name")
            operation = contract.get("operation")
            depends_on = contract.get("depends_on", [])

            # Busca operações
            operations = cls.get_operations(depends_on, contract)

            # Validações
            cls._validate_basic_contract(new_column_name, table)
            cls._validate_dependent_columns(depends_on, table)
            op_config = cls._validate_operation(operation, operations)
            cls._validate_required_params(op_config, contract)
            cls._validate_column_types(depends_on, op_config, table)

            # Execução
            table.data = table.data.with_columns(
                op_config["func"]().alias(new_column_name)
            )

            # Atualiza metadados
            new_column_type = table.data.schema[new_column_name]
            sql_type = cls.TYPE_MAPPING.get(type(new_column_type), "text")

            table.columns[new_column_name] = Column(
                name=new_column_name,
                data_type=sql_type,
                nullable=True,
                ordinal_position=len(table.columns) + 1,
                is_primary_key=False,
            )

            return table

        except Exception as e:
            logging.critical(f"Falha ao criar coluna: {str(e)}")
            raise
