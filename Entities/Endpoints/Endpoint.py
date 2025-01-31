from abc import ABC, abstractmethod
from Entities.Endpoints.Decorators.EndpointDecorators import source_method, target_method
from Entities.Shared.Types import DatabaseType, EndpointType
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

    PATH_FULL_LOAD_STAGING_AREA = 'data/full_load_data/'
    PATH_CDC_STAGING_AREA = 'data/cdc_data/'

    def __init__(self, database_type: DatabaseType, endpoint_type: EndpointType, endpoint_name: str) -> None:
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

        self.id = f'{self.database_type.name}_{self.endpoint_type.name}_{self.endpoint_name}'
        
        self.validate()

    def validate(self) -> None:
        """
        Valida se os tipos de banco de dados e endpoint são válidos.

        Raises:
            ValueError: Se o tipo do banco de dados ou o tipo de endpoint não forem válidos.
        """
        if self.database_type not in DatabaseType:
            logging.error(f"Tipo de banco de dados inválido: {self.database_type}")
            raise ValueError("Invalid database type.")

        if self.endpoint_type not in EndpointType:
            logging.error(f"Tipo de endpoint inválido: {self.endpoint_type}")
            raise ValueError("Invalid endpoint type.")

        logging.info(f"Endpoint {self.id} validado com sucesso.")

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
        """
        Obtém a lista de tabelas disponíveis no banco de dados.

        Returns:
            list: Lista de tabelas disponíveis no banco de dados.
        """
        pass

    @abstractmethod
    @source_method
    def get_table_details(self) -> list:
        """
        Obtém detalhes sobre as tabelas do banco de dados.

        Returns:
            list: Lista com detalhes das tabelas.
        """
        pass

    @abstractmethod
    @source_method
    def get_table_primary_key(self) -> list:
        """
        Obtém a chave primária de uma tabela específica.

        Returns:
            list: Lista contendo as colunas que compõem a chave primária.
        """
        pass

    @abstractmethod
    @source_method
    def get_table_columns(self) -> list:
        """
        Obtém as colunas de uma tabela específica.

        Returns:
            list: Lista contendo os nomes das colunas da tabela.
        """
        pass

    @abstractmethod
    @source_method
    def get_full_load_from_table(self) -> dict:
        """
        Realiza a extração completa dos dados de uma tabela.

        Returns:
            dict: Dicionário contendo o log de execução do método
        """
        pass

    @abstractmethod
    @target_method
    def insert_full_load_into_table(self) -> dict:
        """
        Insere dados completos em uma tabela de destino.

        Returns:
            dict: Dicionário contendo o log de execução do método
        """
        pass
