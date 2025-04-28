from __future__ import annotations
from typing import TYPE_CHECKING
from trempy.Shared.Queries import PostgreSQLQueries
from trempy.Transformations.Transformation import Transformation
from trempy.Filters.Filter import Filter
from trempy.Columns.Column import Column
from typing import List, Dict, Optional
import polars as pl

if TYPE_CHECKING:
    from trempy.Transformations.Transformation import Transformation


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
        estimated_row_count: int = None,
        table_size: str = None,
    ):

        self.schema_name = schema_name
        self.table_name = table_name
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

    def mount_columns_to_create_table(self) -> str:
        """
        Monta a string de colunas para criação de uma tabela no banco de dados.

        A string é formada por uma lista de colunas, onde cada coluna é representada por uma string
        no formato adequado para o banco de dados. As colunas são separadas por vírgula
        e seguem a mesma ordem de aparição na lista de colunas do objeto.

        Args:
            self: A instância atual do objeto contendo a lista de colunas.

        Returns:
            str: String formatada com as colunas para criação da tabela, pronta para ser usada em comandos SQL.

        Raises:
            AttributeError: Se o objeto não possuir a lista de colunas esperada.
            ValueError: Se a lista de colunas estiver vazia.
        """

        columns_sql = []
        for column in sorted(
            self.columns.values(), key=lambda col: col.ordinal_position
        ):
            character_maximum_length = (
                f"({column.character_maximum_length})"
                if column.character_maximum_length
                else ""
            )
            column_str = f'{column.name} {column.data_type}{character_maximum_length} {"NOT NULL" if not column.nullable else "NULL"}'
            columns_sql.append(column_str)

        columns_sql = ", ".join(columns_sql)
        return columns_sql

    def mount_primary_key_to_create_table(self) -> str:
        """
        Monta a string de chave primária para criação de uma tabela no banco de dados.

        A string é formada por uma lista de colunas, onde cada coluna é representada por uma string
        no formato adequado para o banco de dados. As colunas sao separadas por vírgula
        e seguem a mesma ordem de aparição na lista de colunas do objeto.

        Args:
            self: A instância atual do objeto contendo a lista de colunas.

        Returns:
            str: String formatada com as colunas para criação da tabela, pronta para ser usada em comandos SQL.

        Raises:
            AttributeError: Se o objeto não possuir a lista de colunas esperada.
            ValueError: Se a lista de colunas estiver vazia.
        """

        primary_key_sql = []
        for column in sorted(
            self.columns.values(), key=lambda col: col.ordinal_position
        ):
            if column.is_primary_key:
                primary_key_sql.append(column.name)

        if primary_key_sql:
            primary_key_sql = ", ".join(primary_key_sql)
            primary_key_sql = ", PRIMARY KEY ({})".format(primary_key_sql)
        else:
            primary_key_sql = ""
            
        return primary_key_sql

    def mount_create_table(self) -> str:
        """
        Monta o comando SQL completo para criação de uma tabela no banco de dados.

        Combina o template de criação de tabela com as colunas formatadas para gerar
        um comando SQL executável. Utiliza a string de colunas gerada por
        mount_columns_to_create_table() e insere no template de query definido em
        PostgreSQLQueries.CREATE_TABLE.

        Args:
            self: A instância atual do objeto contendo as definições da tabela.

        Returns:
            str: Comando SQL completo para criação da tabela, pronto para execução.

        Raises:
            AttributeError: Se o objeto não possuir os atributos necessários.
            ValueError: Se os componentes para montagem do SQL forem inválidos ou vazios.
            KeyError: Se o template PostgreSQLQueries.CREATE_TABLE não estiver disponível.
        """

        columns_sql = self.mount_columns_to_create_table()
        primary_key_sql = self.mount_primary_key_to_create_table()
        return PostgreSQLQueries.CREATE_TABLE.format(
            schema=self.target_schema_name,
            table=self.target_table_name,
            columns=columns_sql,
            primary_key=primary_key_sql,
        )

    def copy(self) -> "Table":
        """
        Cria uma cópia independente do objeto Table.

        Gera um novo objeto Table com os mesmos atributos e valores do original,
        incluindo uma cópia profunda da lista de colunas para garantir total independência
        entre os objetos.

        Args:
            self: A instância atual do objeto Table a ser copiada.

        Returns:
            Table: Novo objeto Table idêntico ao original, mas completamente independente.

        Raises:
            AttributeError: Se algum atributo obrigatório não estiver presente no objeto original.
            TypeError: Se algum atributo não puder ser copiado corretamente.
        """

        new_table = Table(
            self.schema_name, self.table_name, self.estimated_row_count, self.table_size
        )
        new_table.columns = self.columns.copy()
        return new_table

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

        Raises:
            ValueError: Se column_name for uma string vazia.
            TypeError: Se column_name não for do tipo string.
        """

        return self.columns.get(column_name)

    def execute_transformations(self) -> None:
        """
        Executa todas as transformações definidas para a tabela em ordem de prioridade.

        As transformações são processadas seguindo a ordem de prioridade, das mais altas
        para as mais baixas. Todas as alterações são aplicadas diretamente na tabela atual.

        Args:
            self: Instância da tabela que contém as transformações a serem aplicadas.

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

        Args:
            self: Instância da tabela que contém os filtros a serem aplicados.

        Returns:
            None: Este método não retorna valores, apenas modifica a tabela atual.

        Raises:
            ValueError: Se algum filtro contiver valores inválidos.
            TypeError: Se algum filtro for de um tipo não suportado.
            RuntimeError: Se ocorrer um erro durante a execução dos filtros.
        """

        for filter in self.filters:
            filter.execute(self)

    def to_dict(self) -> dict:
        """
        Converte o objeto Table em um dicionário com suas principais propriedades.

        A conversão inclui todas as informações relevantes da tabela em um formato
        serializável. O dicionário retornado pode ser usado para serialização JSON
        ou outras formas de armazenamento/externação do estado da tabela.

        Returns:
            dict: Dicionário contendo a representação da tabela com a seguinte estrutura:
                {
                    'schema_name': str,         # Nome do esquema da tabela
                    'table_name': str,          # Nome da tabela
                    'estimated_row_count': int,  # Quantidade estimada de linhas
                    'table_size': str,          # Tamanho da tabela em bytes
                    'columns': List[str],       # Lista de nomes de colunas
                    'transformations': List[Dict]  # Lista de transformações serializadas
                }

        Raises:
            AttributeError: Se algum atributo obrigatório não estiver disponível
            ValueError: Se algum valor não puder ser convertido adequadamente
        """

        return {
            "schema_name": self.schema_name,
            "table_name": self.table_name,
            "estimated_row_count": self.estimated_row_count,
            "table_size": self.table_size,
            "columns": self.columns.keys(),
            "transformations": self.transformations,
        }
