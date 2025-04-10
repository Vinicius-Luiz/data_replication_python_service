from __future__ import annotations
from typing import TYPE_CHECKING
from Entities.Shared.Types import TransformationType, PriorityType
from Entities.Transformations.ColumnModifier import ColumnModifier
from Entities.Transformations.ColumnCreator import ColumnCreator
import logging

if TYPE_CHECKING:
    from Entities.Tables.Table import Table


class Transformation:
    """
    Representa uma transformação de dados em uma tabela.

    Attributes:
        transformation_type (str): O tipo da transformação.
        description (str): A descrição da transformação.
        contract (dict): O contrato de transformação.
        priority (str): A prioridade da transformação.
    """

    def __init__(
        self,
        transformation_type: str,
        description: str,
        contract: dict,
        priority: str,
    ) -> None:
        self.transformation_type = TransformationType(transformation_type)
        self.description = description
        self.contract = contract
        self.priority = PriorityType(priority)

        self.validate()

    def validate(self) -> None:
        """
        Valida se o tipo da transformação é válido.

        Raises:
            ValueError: Se o tipo da transformação não for válido.
        """

        if self.transformation_type not in TransformationType:
            raise_msg = f"TRANSFORMATION - Tipo de transformação inválido: {self.transformation_type}"
            logging.critical(raise_msg)
            raise ValueError(raise_msg)

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
                return self._execute_create_column(table)
            case TransformationType.MODIFY_SCHEMA_NAME:
                return self._execute_modify_schema_name(table)
            case TransformationType.MODIFY_TABLE_NAME:
                return self._execute_modify_table_name(table)
            case TransformationType.MODIFY_COLUMN_NAME:
                return self._execute_modify_column_name(table)
            case TransformationType.MODIFY_COLUMN_VALUE:
                return self._execute_modify_column_value(table)

    def _execute_modify_schema_name(self, table: Table) -> Table:
        """
        Modifica o nome do schema da tabela de destino com base no contrato de transformação.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.
        Returns:
            Table: Objeto representando a estrutura da tabela de origem com o nome do schema de destino atualizado.
        """
        table.target_schema_name = self.contract["target_schema_name"]
        return table

    def _execute_modify_table_name(self, table: Table) -> Table:
        """
        Modifica o nome da tabela de destino com base no contrato de transformação.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.
        Returns:
            Table: Objeto representando a estrutura da tabela de origem com o nome da tabela de destino atualizado.
        """
        table.target_table_name = self.contract["target_table_name"]
        return table

    def _execute_modify_column_name(self, table: Table) -> Table:
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

    def _execute_create_column(self, table: Table) -> Table:
        """
        Cria uma nova coluna com base no contrato de transformação.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.

        Returns:
            Table: Objeto representando a estrutura da tabela de origem com a coluna de destino criada.
        """

        return ColumnCreator.create_column(self.contract, table)

    def _execute_modify_column_value(self, table: Table) -> Table:
        """
        Modifica o valor da coluna de destino com base no contrato de transformação.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.

        Returns:
            Table: Objeto representando a estrutura da tabela de origem com o valor da coluna de destino atualizado.
        """

        return ColumnModifier.modify_column(self.contract, table)
