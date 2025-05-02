from __future__ import annotations
from trempy.Transformations.FunctionColumnModifier import (
    FunctionColumnModifier as FCM,
)
from trempy.Shared.Types import TransformationOperationType
from trempy.Transformations.Exceptions.Exception import *
from typing import Dict, Any, TYPE_CHECKING
from trempy.Shared.Utils import Utils

if TYPE_CHECKING:
    from trempy.Tables.Table import Table


class ColumnModifier:
    """Classe responsável por validar e modificar colunas existentes."""

    @staticmethod
    def get_operations(column_name: str, contract: dict) -> Dict[str, Any]:
        """Retorna um dicionário de operações de transformação disponíveis para uma coluna.

        Args:
            column_name: Nome da coluna alvo para as operações de transformação.
            contract: Configuração da transformação contendo parâmetros adicionais como:
                - format: Formato de data (para operações de formatação)
                - expression: Expressão matemática (para operações MATH_EXPRESSION)

        Returns:
            Dicionário de operações onde cada entrada contém:
            {
                "func": Função lambda pronta para executar a transformação,
                "required_params": Lista de parâmetros obrigatórios (quando aplicável),
                "column_type": Tipos de coluna suportados (quando aplicável)
            }

        Raises:
            KeyError: Se parâmetros obrigatórios estiverem faltando no contrato
        """

        return {
            TransformationOperationType.FORMAT_DATE: {
                "func": lambda: FCM.format_date(column_name, contract["format"]),
                "required_params": FCM.get_required_params(
                    TransformationOperationType.FORMAT_DATE
                ),
                "column_type": FCM.get_required_column_types(
                    TransformationOperationType.FORMAT_DATE
                ),
            },
            TransformationOperationType.UPPERCASE: {
                "func": lambda: FCM.uppercase(column_name),
                "column_type": FCM.get_required_column_types(
                    TransformationOperationType.UPPERCASE
                ),
            },
            TransformationOperationType.LOWERCASE: {
                "func": lambda: FCM.lowercase(column_name),
                "column_type": FCM.get_required_column_types(
                    TransformationOperationType.LOWERCASE
                ),
            },
            TransformationOperationType.TRIM: {
                "func": lambda: FCM.trim(column_name),
                "column_type": FCM.get_required_column_types(
                    TransformationOperationType.TRIM
                ),
            },
            TransformationOperationType.EXTRACT_YEAR: {
                "func": lambda: FCM.extract_year(column_name),
                "column_type": FCM.get_required_column_types(
                    TransformationOperationType.EXTRACT_YEAR
                ),
            },
            TransformationOperationType.EXTRACT_MONTH: {
                "func": lambda: FCM.extract_month(column_name),
                "column_type": FCM.get_required_column_types(
                    TransformationOperationType.EXTRACT_MONTH
                ),
            },
            TransformationOperationType.EXTRACT_DAY: {
                "func": lambda: FCM.extract_day(column_name),
                "column_type": FCM.get_required_column_types(
                    TransformationOperationType.EXTRACT_DAY
                ),
            },
            TransformationOperationType.MATH_EXPRESSION: {
                "func": lambda: FCM.math_expression(
                    column_name, contract["expression"]
                ),
                "required_params": FCM.get_required_params(
                    TransformationOperationType.MATH_EXPRESSION
                ),
            },
        }

    @staticmethod
    def _validate_basic_contract(column_name: str, table: Table) -> None:
        """Valida se o nome da coluna é válido e existe na tabela especificada.

        Args:
            column_name: Nome da coluna a ser validada. Deve:
                - Ser uma string não vazia
                - Existir na tabela fornecida
            table: Tabela contendo os dados onde a coluna será verificada.
                Deve conter um atributo 'data' com um DataFrame válido.

        Raises:
            ColumnNameError: Com mensagens específicas para cada caso de erro:
                - Quando column_name é vazio
                - Quando a coluna não existe na tabela
        """

        if not column_name:
            e = ColumnNameError("O contrato deve conter 'column_name'", None)
            Utils.log_exception_and_exit(e)

        if column_name not in table.data.columns:
            available = list(table.data.columns)
            e = ColumnNameError(
                f"A coluna não existe no DataFrame. Disponíveis: {available}",
                column_name,
            )
            Utils.log_exception_and_exit(e)

    @staticmethod
    def _validate_operation(
        operation: str, operations: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Valida se uma operação está entre as operações suportadas.

        Args:
            operation: Nome da operação a ser validada (comparação exata).
            operations: Dicionário de operações suportadas onde:
                - Chave: Nome da operação (string)
                - Valor: Configuração da operação (dicionário)

        Returns:
            A configuração completa da operação validada, contendo:
            - func: Função de transformação
            - required_params: Parâmetros obrigatórios (quando aplicável)
            - column_type: Tipos de coluna suportados (quando aplicável)

        Raises:
            InvalidOperationError: Se a operação não for encontrada no dicionário de operações.
                A mensagem inclui a lista de operações disponíveis.
        """

        if operation not in operations:
            valid_ops = list(operations.keys())
            e = InvalidOperationError(
                f"Operação não suportada. Válidas: {valid_ops}", operation
            )
            Utils.log_exception_and_exit(e)
        return operations[operation]

    @staticmethod
    def _validate_required_params(
        op_config: Dict[str, Any], contract: Dict[str, Any]
    ) -> None:
        """Valida a presença de todos os parâmetros obrigatórios no contrato de transformação.

        Args:
            op_config: Configuração da operação contendo:
                - required_params: Lista de parâmetros obrigatórios
            contract: Contrato de transformação contendo os parâmetros fornecidos

        Raises:
            RequiredParameterError: Para cada parâmetro obrigatório ausente no contrato.
                A mensagem especifica qual parâmetro está faltando.
        """

        for param in op_config.get("required_params", []):
            if param not in contract:
                e = RequiredParameterError("Parâmetro obrigatório faltando", param)
                Utils.log_exception_and_exit(e)

    @staticmethod
    def _validate_column_type(
        column_name: str, op_config: Dict[str, Any], table: Table
    ) -> None:
        """Valida se o tipo da coluna é compatível com a operação especificada.

        Args:
            column_name: Nome da coluna a ser validada (deve existir na tabela).
            op_config: Configuração da operação contendo:
                - column_type: Tipo(s) esperado(s) (pode ser um único tipo ou lista)
            table: Tabela contendo a coluna a ser validada, deve ter:
                - data.schema: Dicionário mapeando nomes de colunas para seus tipos

        Raises:
            InvalidColumnTypeError: Quando o tipo real da coluna não corresponde a nenhum dos tipos
                esperados. A mensagem inclui:
                - Nome da coluna
                - Tipos esperados
                - Tipo encontrado

        Notes:
            - Retorna silenciosamente se a operação não especificar tipos esperados
            - Aceita tanto um único tipo quanto uma lista de tipos
            - Usa isinstance() para permitir herança de tipos
        """

        if "column_type" not in op_config:
            return

        actual_type = table.data.schema[column_name]
        expected_types = op_config["column_type"]

        if not isinstance(expected_types, (list, tuple)):
            expected_types = [expected_types]

        if not any(isinstance(actual_type, t) for t in expected_types):
            expected_names = [t.__name__ for t in expected_types]
            e = InvalidColumnTypeError(
                f"Tipo inválido para coluna. "
                f"Esperado: {expected_names}, Recebido: {type(actual_type).__name__}",
                column_name,
                type(actual_type).__name__,
            )
            Utils.log_exception_and_exit(e)

    @classmethod
    def modify_column(cls, contract: Dict[str, Any], table: Table) -> Table:
        """Modifica os valores de uma coluna existente conforme especificado no contrato.

        O processo completo inclui:
        1. Extração dos parâmetros do contrato
        2. Validação dos parâmetros e tipos
        3. Aplicação da transformação especificada
        4. Atualização dos dados da tabela

        Args:
            contract: Dicionário contendo:
                - column_name: Nome da coluna a ser modificada
                - operation: Tipo de operação a ser aplicada
                - Outros parâmetros específicos da operação
            table: Tabela contendo a coluna a ser modificada

        Returns:
            A mesma tabela de entrada com os valores da coluna atualizados
        """

        # Extrai parâmetros do contrato
        column_name = contract.get("column_name")
        operation = TransformationOperationType(contract.get("operation"))

        # Busca operações
        operations = cls.get_operations(column_name, contract)

        # Validações
        cls._validate_basic_contract(column_name, table)
        op_config = cls._validate_operation(operation, operations)
        cls._validate_required_params(op_config, contract)
        cls._validate_column_type(column_name, op_config, table)

        # Execução
        table.data = table.data.with_columns(op_config["func"]().alias(column_name))
        return table
