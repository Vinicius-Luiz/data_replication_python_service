from __future__ import annotations
from typing import TYPE_CHECKING
from typing import Dict, Any
import logging

if TYPE_CHECKING:
    from Entities.Tables.Table import Table


class ColumnModifier:
    """Classe responsável por validar e modificar colunas existentes."""

    @staticmethod
    def _validate_basic_contract(column_name: str, table: Table) -> None:
        """Validações básicas do contrato."""
        if not column_name:
            raise ValueError("O contrato deve conter 'column_name'")

        if column_name not in table.data.columns:
            available = list(table.data.columns)
            raise ValueError(
                f"Coluna '{column_name}' não encontrada. Disponíveis: {available}"
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
    def _validate_column_type(
        column_name: str, op_config: Dict[str, Any], table: Table
    ) -> None:
        """Valida tipo da coluna."""
        if "column_type" not in op_config:
            return

        actual_type = table.data.schema[column_name]
        expected_types = op_config["column_type"]

        if not isinstance(expected_types, (list, tuple)):
            expected_types = [expected_types]

        if not any(isinstance(actual_type, t) for t in expected_types):
            expected_names = [t.__name__ for t in expected_types]
            raise ValueError(
                f"Tipo inválido para coluna '{column_name}'. "
                f"Esperado: {expected_names}, Recebido: {type(actual_type).__name__}"
            )

    @classmethod
    def modify_column(
        cls, contract: Dict[str, Any], table: Table, operations: Dict[str, Any]
    ) -> Table:
        """Fluxo principal para modificação de colunas."""
        try:
            # Extrai parâmetros do contrato
            column_name = contract.get("column_name")
            operation = contract.get("operation")

            # Validações
            cls._validate_basic_contract(column_name, table)
            op_config = cls._validate_operation(operation, operations)
            cls._validate_required_params(op_config, contract)
            cls._validate_column_type(column_name, op_config, table)

            # Execução
            table.data = table.data.with_columns(op_config["func"]().alias(column_name))
            return table

        except Exception as e:
            logging.critical(f"Falha ao modificar coluna: {str(e)}")
            raise
