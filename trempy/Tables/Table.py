from __future__ import annotations
from trempy.Transformations.Transformation import Transformation
from trempy.Shared.Types import PriorityType, SCD2ColumnType
from trempy.Loggings.Logging import ReplicationLogger
from trempy.Tables.Exceptions.Exception import *
from trempy.Tables.Exceptions.Exception import *
from trempy.Shared.DataTypes import Datatype
from trempy.Columns.Column import Column
from trempy.Filters.Filter import Filter
from typing import List, Dict, Optional
from typing import TYPE_CHECKING
import polars as pl

if TYPE_CHECKING:
    from trempy.Transformations.Transformation import Transformation

logger = ReplicationLogger()


class Table:
    """Classe que representa uma tabela.

    Attributes:
        schema_name (str): O nome do esquema da tabela.
        table_name (str): O nome da tabela.
        estimated_row_count (int): A estimativa de linhas da tabela.
        table_size (str): O tamanho da tabela.
    """

    def __init__(
        self,
        schema_name: str = None,
        table_name: str = None,
        priority: int = None,
        estimated_row_count: int = None,
        table_size: str = None,
    ):

        self.schema_name = schema_name
        self.table_name = table_name
        self.priority = PriorityType(priority)
        self.estimated_row_count = estimated_row_count
        self.table_size = table_size
        self.id = f"{self.schema_name}.{self.table_name}"

        self.target_schema_name = self.schema_name
        self.target_table_name = self.table_name

        self.path_data: str = None
        self.data: pl.DataFrame = None

        self.columns: Dict[str, Column] = {}
        self.filters: List[Filter] = []
        self.transformations: List[Transformation] = []

    def find_column(self, column_name: str) -> Optional[Column]:
        """
        Busca e retorna uma coluna pelo nome especificado.

        Percorre a lista de colunas da tabela procurando uma correspondência exata
        com o nome fornecido. A comparação é case-sensitive (diferencia maiúsculas
        de minúsculas).

        Args:
            column_name (str): Nome da coluna a ser localizada. Não pode ser vazio.

        Returns:
            Optional[Column]: Objeto Column correspondente se encontrado, None caso contrário.
        """

        return self.columns.get(column_name)

    def add_data(self, data: pl.DataFrame) -> None:
        """
        Adiciona dados a tabela.

        Args:
            data (pl.DataFrame): DataFrame contendo os dados a serem adicionados.

        Returns:
            None
        """
        try:
            for source_column_name, column in sorted(
                self.columns.items(), key=lambda col: col[1].ordinal_position
            ):
                if (
                    source_column_name not in data.columns
                    and not column.is_created_by_trempy
                ):
                    data_type = Datatype.DatatypePostgreSQL.TYPE_DATABASE_TO_POLARS[
                        column.data_type
                    ]  # TODO eu preciso saber qual é o tipo de endpoint correto
                    data = data.with_columns(
                        pl.lit(None, dtype=data_type).alias(source_column_name)
                    )
            self.data = data
        except Exception as e:
            e = AddDataError(f"Erro ao adicionar dados na tabela: {e}", self.id)
            logger.critical(e)

    def get_pk_columns(self, returns: str = "name") -> List[str | Column]:
        """
        Retorna a lista de colunas que compoem a chave primaria da tabela.

        Args:
            returns (str): Tipo de retorno. Pode ser 'name' ou 'object'.

        Returns:
            list: Lista de nomes de colunas que compoem a chave primaria da tabela.

        Raises:
            PrimaryKeyNotFoundError: Se nenhuma chave primaria for encontrada na tabela.
        """

        if returns == "name":
            pk_columns = [
                col.name for col in self.columns.values() if col.is_primary_key
            ]
        elif returns == "object":
            pk_columns = [col for col in self.columns.values() if col.is_primary_key]

        if not pk_columns:
            e = PrimaryKeyNotFoundError(
                f"Nenhuma PK encontrada para tabela",
                f"{self.target_schema_name}.{self.target_table_name}",
            )
            logger.critical(e)

        return pk_columns

    def get_pk_columns_without_scd2_columns(
        self, returns: str = "name"
    ) -> List[str] | List[Column]:
        """
        Retorna a lista de colunas que compoem a chave primaria da tabela sem considerar as colunas SCD2.

        Args:
            returns (str): Tipo de retorno. Pode ser 'name' ou 'object'.

        Returns:
            list: Lista de nomes de colunas que compoem a chave primaria da tabela sem considerar as colunas SCD2.

        Raises:
            PrimaryKeyNotFoundError: Se nenhuma chave primaria for encontrada na tabela.
        """

        if returns == "name":
            pk_columns = [
                col.name
                for col in self.columns.values()
                if col.is_primary_key and not col.is_scd2_column
            ]
        elif returns == "object":
            pk_columns = [
                col
                for col in self.columns.values()
                if col.is_primary_key and not col.is_scd2_column
            ]

        if not pk_columns:
            e = PrimaryKeyNotFoundError(
                f"Nenhuma PK encontrada para tabela",
                f"{self.target_schema_name}.{self.target_table_name}",
            )
            logger.critical(e)

        return pk_columns

    def get_scd2_columns(self) -> Dict[SCD2ColumnType, str]:
        """
        Retorna um dicionário com as colunas SCD2 da tabela.

        O dicionário retornado tem como chave o tipo de coluna SCD2 (start_date ou end_date)
        e como valor o nome da coluna.

        Returns:
            dict: Dicionário com as colunas SCD2 da tabela.
        """

        scd2_columns = dict()
        for col in self.columns.values():
            if col.is_scd2_column:
                scd2_columns[col.scd2_column_type] = col.name

        return scd2_columns

    def to_dict(self) -> dict:
        """
        Converte o objeto Table em um dicionário com suas principais propriedades.

        A conversão inclui todas as informações relevantes da tabela em um formato
        serializável. O dicionário retornado pode ser usado para serialização JSON
        ou outras formas de armazenamento/externação do estado da tabela.

        Returns:
            dict: Dicionário contendo a representação da tabela com a seguinte estrutura:
                {
                    'schema_name': str,
                    'table_name': str,
                    'estimated_row_count': int,
                    'table_size': str,
                    'columns': List[str],
                }
        """

        return {
            "schema_name": self.schema_name,
            "table_name": self.table_name,
            "estimated_row_count": self.estimated_row_count,
            "table_size": self.table_size,
            "columns": list(self.columns.keys()),
        }

    def execute_transformations(self) -> None:
        """
        Executa todas as transformações definidas para a tabela em ordem de prioridade.

        As transformações são processadas seguindo a ordem de prioridade, das mais altas
        para as mais baixas. Todas as alterações são aplicadas diretamente na tabela atual.

        Returns:
            None: Este método não retorna valores, apenas modifica a tabela atual.

        Raises:
            ValueError: Se alguma transformação contiver valores inválidos.
            TypeError: Se alguma transformação for de um tipo não suportado.
            RuntimeError: Se ocorrer um erro durante a execução das transformações.
        """

        for transformation in sorted(
            self.transformations, key=lambda x: x.priority.value
        ):
            transformation.execute(self)

    def execute_filters(self) -> None:
        """
        Executa todos os filtros definidos para a tabela.

        Os filtros são processados em ordem de prioridade, das mais altas para as mais baixas.
        Todas as alterações realizadas pelos filtros são aplicadas diretamente na tabela atual.

        Returns:
            None: Este método não retorna valores, apenas modifica a tabela atual.

        Raises:
            ValueError: Se algum filtro contiver valores inválidos.
            TypeError: Se algum filtro for de um tipo não suportado.
            RuntimeError: Se ocorrer um erro durante a execução dos filtros.
        """

        for filter in self.filters:
            filter.execute(self)
