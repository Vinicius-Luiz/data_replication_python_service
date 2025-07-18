from __future__ import annotations
from typing import TYPE_CHECKING
from trempy.Shared.Types import TransformationType, PriorityType
from trempy.Transformations.ColumnModifier import ColumnModifier
from trempy.Transformations.ColumnCreator import ColumnCreator
from trempy.Transformations.Exceptions.Exception import *
from trempy.Loggings.Logging import ReplicationLogger

if TYPE_CHECKING:
    from trempy.Tables.Table import Table

logger = ReplicationLogger()


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

        self.__validate()

    def __validate(self) -> None:
        """
        Valida se o tipo da transformação é válido.

        Raises:
            InvalidTransformationTypeError: Se o tipo da transformação não for válido.
        """

        if self.transformation_type not in TransformationType:
            e = InvalidTransformationTypeError(
                "Tipo de transformação inválido", str(self.transformation_type.value)
            )
            logger.critical(e)
            raise e

    def __execute_modify_schema_name(self, table: Table) -> Table:
        """
        Modifica o nome do schema da tabela de destino com base no contrato de transformação.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.
        Returns:
            Table: Objeto representando a estrutura da tabela de origem com o nome do schema de destino atualizado.
        """
        table.target_schema_name = self.contract["target_schema_name"]
        return table

    def __execute_modify_table_name(self, table: Table) -> Table:
        """
        Modifica o nome da tabela de destino com base no contrato de transformação.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.
        Returns:
            Table: Objeto representando a estrutura da tabela de origem com o nome da tabela de destino atualizado.
        """
        table.target_table_name = self.contract["target_table_name"]
        return table

    def __execute_modify_column_name(self, table: Table) -> Table:
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
        table.data = table.data.rename(
            {self.contract["column_name"]: self.contract["target_column_name"]}
        )
        return table

    def __execute_add_primary_key(self, table: Table) -> Table:
        """
        Adiciona a chave primária a tabela de destino com base no contrato de transformação.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.

        Returns:
            Table: Objeto representando a estrutura da tabela de origem com a chave primária adicionada.

        """

        column_names = self.contract["column_names"]

        for column_name in column_names:
            table.columns[column_name].is_primary_key = True

        return table

    def __execute_remove_primary_key(self, table: Table) -> Table:
        """
        Remove a chave primária da tabela de destino com base no contrato de transformação.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.

        Returns:
            Table: Objeto representando a estrutura da tabela de origem com a chave primária removida.
        """

        column_names = self.contract["column_names"]

        for column_name in column_names:
            table.columns[column_name].is_primary_key = False

        return table

    def __execute_create_column(self, table: Table) -> Table:
        """
        Cria uma nova coluna com base no contrato de transformação.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.

        Returns:
            Table: Objeto representando a estrutura da tabela de origem com a coluna de destino criada.
        """

        return ColumnCreator.create_column(self.contract, table)

    def __execute_modify_column_value(self, table: Table) -> Table:
        """
        Modifica o valor da coluna de destino com base no contrato de transformação.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.

        Returns:
            Table: Objeto representando a estrutura da tabela de origem com o valor da coluna de destino atualizado.
        """

        return ColumnModifier.modify_column(self.contract, table)

    def execute(self, table: Table) -> Table:
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
            Table: A tabela após a aplicação da transformação.

        Raises:
            InvalidTransformationTypeError: Se o tipo de transformação for inválido.
            TransformationError: Se ocorrer algum erro durante a transformação.
        """

        logger.info(
            f"TRANSFORMATION - Aplicando transformação em {table.schema_name}.{table.table_name}: {self.description}",
            required_types=["full_load"],
        )

        try:
            transformation_functions = {
                TransformationType.CREATE_COLUMN: self.__execute_create_column,
                TransformationType.MODIFY_SCHEMA_NAME: self.__execute_modify_schema_name,
                TransformationType.MODIFY_TABLE_NAME: self.__execute_modify_table_name,
                TransformationType.MODIFY_COLUMN_NAME: self.__execute_modify_column_name,
                TransformationType.MODIFY_COLUMN_VALUE: self.__execute_modify_column_value,
                TransformationType.ADD_PRIMARY_KEY: self.__execute_add_primary_key,
                TransformationType.REMOVE_PRIMARY_KEY: self.__execute_remove_primary_key,
            }

            if self.transformation_type in transformation_functions:
                return transformation_functions[self.transformation_type](table)
            else:
                e = InvalidTransformationTypeError(
                    "Tipo de transformação inválido",
                    str(self.transformation_type.value),
                )
                logger.critical(e)
                raise e

        except Exception as e:
            e = TransformationError(str(e))
            logger.critical(e)
            raise e
