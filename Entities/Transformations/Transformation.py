from __future__ import annotations
from typing import TYPE_CHECKING
from Entities.Transformations.FunctionColumnModifier import (
    FunctionColumnModifier as FCM,
)
from Entities.Transformations.FunctionColumnCreator import FunctionColumnCreator as FCC
from Entities.Shared.Types import TransformationType, OperationType, PriorityType
from Entities.Transformations.ColumnModifier import ColumnModifier
from Entities.Transformations.ColumnCreator import ColumnCreator
import logging

if TYPE_CHECKING:
    from Entities.Tables.Table import Table


class Transformation:
    def __init__(
        self,
        transformation_type: TransformationType,
        description: str,
        contract: dict,
        priority: PriorityType,
    ) -> None:
        self.transformation_type = transformation_type
        self.description = description
        self.contract = contract
        self.priority = priority

        self.validate()

    def validate(self) -> None:
        """
        Valida se o tipo da transformação é válido.

        Raises:
            ValueError: Se o tipo da transformação não for válido.
        """
        if self.transformation_type not in TransformationType:
            logging.error(
                f"TRANSFORMATION - Tipo de transformação inválido: {self.transformation_type}"
            )
            raise ValueError(
                f"TRANSFORMATION - Tipo de transformação inválido: {self.transformation_type}"
            )

    def execute(self, table: Table = None) -> None:
        """
        Executa a transformação na tabela.

        Dependendo do tipo da transformação, o método executará uma das seguintes ações:
        - Criar uma coluna com um nome e tipo especificados.
        - Modificar o nome do schema da tabela.
        - Modificar o nome da tabela.
        - Modificar o nome de uma coluna.
        - Modificar os valores de uma coluna.

        Args:
            table (Table): Objeto representando a estrutura da tabela que receberá a transformação.

        Returns:
            None
        """
        logging.info(
            f"TRANSFORMATION - Aplicando transformação em {table.schema_name}.{table.table_name}: {self.description}"
        )
        match self.transformation_type:
            case TransformationType.CREATE_COLUMN:
                return self.execute_create_column(table)
            case TransformationType.MODIFY_SCHEMA_NAME:
                return self.execute_modify_schema_name(table)
            case TransformationType.MODIFY_TABLE_NAME:
                return self.execute_modify_table_name(table)
            case TransformationType.MODIFY_COLUMN_NAME:
                return self.execute_modify_column_name(table)
            case TransformationType.MODIFY_COLUMN_VALUE:
                return self.execute_modify_column_value(table)

    def execute_modify_schema_name(self, table: Table) -> Table:
        """
        Modifica o nome do schema da tabela de destino com base no contrato de transformação.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.
        Returns:
            Table: Objeto representando a estrutura da tabela de origem com o nome do schema de destino atualizado.
        """
        table.target_schema_name = self.contract["target_schema_name"]
        return table

    def execute_modify_table_name(self, table: Table) -> Table:
        """
        Modifica o nome da tabela de destino com base no contrato de transformação.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.
        Returns:
            Table: Objeto representando a estrutura da tabela de origem com o nome da tabela de destino atualizado.
        """
        table.target_table_name = self.contract["target_table_name"]
        return table

    def execute_modify_column_name(self, table: Table) -> Table:
        """
        Modifica o nome da coluna de destino com base no contrato de transformação.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.
        Returns:
            Table: Objeto representando a estrutura da tabela de origem com o nome da coluna de destino atualizado.
        """
        table.columns[self.contract["column_name"]].name = self.contract[
            "target_column_name"
        ]
        return table

    def execute_create_column(self, table: Table) -> Table:
        """
        Cria uma nova coluna com base no contrato de transformação.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.

        Returns:
            Table: Objeto representando a estrutura da tabela de origem com a coluna de destino criada.
        """

        depends_on = self.contract.get("depends_on", [])

        operations = {
            OperationType.LITERAL: {
                "func": lambda: FCC.literal(value=self.contract["value"]),
                "required_params": FCC.get_required_params(OperationType.LITERAL),
            },
            OperationType.DATE_NOW: {"func": lambda: FCC.date_now()},
            OperationType.CONCAT: {
                "func": lambda: FCC.concat(
                    separator=self.contract.get("separator", ""),
                    depends_on=depends_on,
                ),
                "required_params": FCC.get_required_params(OperationType.CONCAT),
                "column_type": FCC.get_required_column_types(OperationType.CONCAT),
            },
            OperationType.DATE_DIFF_YEARS: {
                "func": lambda: FCC.date_diff_years(
                    start_col=depends_on[0],
                    end_col=depends_on[1],
                    round_result=self.contract.get("round_result", False),
                ),
                "required_params": FCC.get_required_params(
                    OperationType.DATE_DIFF_YEARS
                ),
                "column_type": FCC.get_required_column_types(
                    OperationType.DATE_DIFF_YEARS
                ),
            },
        }

        return ColumnCreator.create_column(self.contract, table, operations)

    def execute_modify_column_value(self, table: Table) -> Table:
        """
        Modifica o valor de uma coluna com base no contrato de transformação.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.

        Returns:
            Table: Objeto representando a estrutura da tabela de origem com o valor da
                   coluna de destino atualizado.
        """

        column_name = self.contract["column_name"]

        operations = {
            OperationType.FORMAT_DATE: {
                "func": lambda: FCM.format_date(column_name, self.contract["format"]),
                "required_params": FCM.get_required_params(OperationType.FORMAT_DATE),
                "column_type": FCM.get_required_column_types(OperationType.FORMAT_DATE),
            },
            OperationType.UPPERCASE: {
                "func": lambda: FCM.uppercase(column_name),
                "column_type": FCM.get_required_column_types(OperationType.UPPERCASE),
            },
            OperationType.LOWERCASE: {
                "func": lambda: FCM.lowercase(column_name),
                "column_type": FCM.get_required_column_types(OperationType.LOWERCASE),
            },
            OperationType.TRIM: {
                "func": lambda: FCM.trim(column_name),
                "column_type": FCM.get_required_column_types(OperationType.TRIM),
            },
            OperationType.EXTRACT_YEAR: {
                "func": lambda: FCM.extract_year(column_name),
                "column_type": FCM.get_required_column_types(
                    OperationType.EXTRACT_YEAR
                ),
            },
            OperationType.EXTRACT_MONTH: {
                "func": lambda: FCM.extract_month(column_name),
                "column_type": FCM.get_required_column_types(
                    OperationType.EXTRACT_MONTH
                ),
            },
            OperationType.EXTRACT_DAY: {
                "func": lambda: FCM.extract_day(column_name),
                "column_type": FCM.get_required_column_types(OperationType.EXTRACT_DAY),
            },
            OperationType.MATH_EXPRESSION: {
                "func": lambda: FCM.math_expression(
                    column_name, self.contract["expression"]
                ),
                "required_params": FCM.get_required_params(
                    OperationType.MATH_EXPRESSION
                ),
            },
        }

        return ColumnModifier.modify_column(self.contract, table, operations)
