from abc import ABC, abstractmethod
from trempy.Endpoints.Decorators.EndpointDecorators import (
    source_method,
    target_method,
)
from trempy.Tables.Table import Table
from trempy.Shared.Types import DatabaseType, EndpointType
import polars as pl
import logging


class Endpoint(ABC):
    """
    Classe abstrata que representa um endpoint de conexão com um banco de dados.

    Essa classe define métodos abstratos para operações comuns como obtenção de esquemas,
    tabelas e carregamento de dados.

    Attributes:
        database_type (DatabaseType): Tipo do banco de dados.
        endpoint_type (EndpointType): Tipo do endpoint (fonte ou destino).
        endpoint_name (str): Nome do endpoint.
        credentials (dict): Credenciais de acesso ao banco de dados.
        id (str): Identificador único do endpoint gerado a partir dos atributos.
    """

    def __init__(
        self,
        database_type: DatabaseType,
        endpoint_type: EndpointType,
        endpoint_name: str,
        periodicity_in_seconds_of_reading_from_source: int,
    ) -> None:
        """
        Inicializa um endpoint com os dados fornecidos.

        Args:
            database_type (DatabaseType): Tipo do banco de dados.
            endpoint_type (EndpointType): Tipo do endpoint (fonte ou destino).
            endpoint_name (str): Nome do endpoint.

        Raises:
            ValueError: Se o tipo do banco de dados ou tipo de endpoint for inválido.
        """
        self.database_type = database_type
        self.endpoint_type = endpoint_type
        self.endpoint_name = endpoint_name
        self.periodicity_in_seconds_of_reading_from_source = (
            periodicity_in_seconds_of_reading_from_source
        )

        self.id = (
            f"{self.database_type.name}_{self.endpoint_type.name}_{self.endpoint_name}"
        )

        self.validate()

    def validate(self) -> None:
        """
        Valida se os tipos de banco de dados e endpoint são válidos.

        Raises:
            ValueError: Se o tipo do banco de dados ou o tipo de endpoint não forem válidos.
        """
        if self.database_type not in DatabaseType:
            raise_msg = f"ENDPOINT - Tipo de banco de dados {self.database_type} inválido."
            logging.critical(raise_msg)
            raise ValueError(raise_msg)

        if self.endpoint_type not in EndpointType:
            raise_msg = f"ENDPOINT - Tipo de endpoint {self.endpoint_type} inválido."
            logging.critical(raise_msg)
            raise ValueError(raise_msg)

        logging.info(f"ENDPOINT - {self.endpoint_name} válido")

    @abstractmethod
    @source_method
    def get_schemas(self) -> list:
        """
        Obtém a lista de esquemas do banco de dados.

        Returns:
            list: Lista de esquemas disponíveis no banco de dados.
        """
        pass

    @abstractmethod
    @source_method
    def get_tables(self) -> list:
        """Obtém a lista de tabelas de um esquema específico.

        Args:
            schema (str): Nome do esquema.

        Returns:
            list: Lista de tabelas do esquema.
        """
        pass

    @abstractmethod
    @source_method
    def get_table_details(self) -> Table:
        """
        Obtém os detalhes de uma tabela específica.

        Args:
            schema (str): Nome do esquema da tabela.
            table (str): Nome da tabela.

        Returns:
            Table: Objeto representando a estrutura da tabela.
        """
        pass

    @abstractmethod
    @source_method
    def get_table_primary_key(self) -> list:
        """
        Obtém a lista de colunas que compõem a chave primária de uma tabela específica.

        Args:
            table (Table): Objeto representando a estrutura da tabela.

        Returns:
            list: Lista de colunas que compõem a chave primária da tabela.
        """
        pass

    @abstractmethod
    @source_method
    def get_table_columns(self) -> Table:
        """
        Obtém as colunas de uma tabela específica e atualiza o objeto Table com
        os detalhes das colunas.

        Args:
            table (Table): Objeto representando a estrutura da tabela.
            primary_keys (list): Lista de colunas que compõem a chave primária da tabela.

        Returns:
            Table: Objeto Table atualizado com a estrutura das colunas da tabela.
        """
        pass

    @abstractmethod
    @source_method
    def get_full_load_from_table(self) -> dict:
        """
        Realiza a extra o completa dos dados de uma tabela.

        Args:
            table (Table): Objeto representando a estrutura da tabela.

        Returns:
            dict: Dicionário contendo o log de execução do método
        """
        pass

    @abstractmethod
    @target_method
    def insert_full_load_into_table(self) -> dict:
        """
        Insere dados completos em uma tabela de destino.

        Args:
            table (Table): Objeto representando a estrutura da tabela.
            create_table_if_not_exists (bool): Se True, cria a tabela caso ela não exista.
            recreate_table_if_exists (bool): Se True, recria a tabela caso ela já exista.
            truncate_before_insert (bool): Se True, trunca a tabela antes da inserção dos dados.

        Returns:
            dict: Dicionário contendo o log de execução do método.
        """
        pass

    @abstractmethod
    @target_method
    def insert_cdc_into_table(self) -> dict:
        """
        Insere dados de alterações em uma tabela de destino.

        Args:
            table (Table): Objeto representando a estrutura da tabela.
            create_table_if_not_exists (bool): Se True, cria a tabela caso ela não exista.
        
        Returns:
            dict: Dicionário contendo o log de execução do método.
        """
    
    @abstractmethod
    @source_method
    def capture_changes(self, **kargs) -> pl.DataFrame:
        pass

    @abstractmethod
    @source_method
    def structure_capture_changes(self) -> dict:
        pass

    @abstractmethod
    @target_method
    def structure_capture_changes_to_dataframe(self) -> pl.DataFrame:
        pass