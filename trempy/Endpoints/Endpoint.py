from abc import ABC, abstractmethod
from trempy.Endpoints.Decorators.EndpointDecorators import (
    source_method,
    target_method,
)
from trempy.Endpoints.Exceptions.Exception import *
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
    ) -> None:
        """
        Inicializa um endpoint com os dados fornecidos.

        Args:
            database_type (DatabaseType): Tipo do banco de dados.
            endpoint_type (EndpointType): Tipo do endpoint (fonte ou destino).
            endpoint_name (str): Nome do endpoint.
        """
        self.database_type = database_type
        self.endpoint_type = endpoint_type
        self.endpoint_name = endpoint_name

        self.id = (
            f"{self.database_type.name}_{self.endpoint_type.name}_{self.endpoint_name}"
        )

        self.validate()

    def validate(self) -> None:
        """
        Valida se os tipos de banco de dados e endpoint são válidos.

        Raises:
            InvalidDatabaseTypeError: Se o tipo do banco de dados for inválido.
            InvalidEndpointTypeError: Se o tipo do endpoint for inválido.
        """
        if self.database_type not in DatabaseType:
            raise InvalidDatabaseTypeError(
                "Tipo de banco de dados inválido", self.database_type
            )

        if self.endpoint_type not in EndpointType:
            raise InvalidEndpointTypeError(
                "Tipo de endpoint inválido", self.endpoint_type
            )

    @abstractmethod
    @source_method
    def get_schemas(self) -> list:
        pass

    @abstractmethod
    @source_method
    def get_tables(self) -> list:
        pass

    @abstractmethod
    @source_method
    def get_table_details(self) -> Table:
        pass

    @abstractmethod
    @source_method
    def get_table_primary_key(self) -> list:
        pass

    @abstractmethod
    @source_method
    def get_table_columns(self) -> Table:
        pass

    @abstractmethod
    @source_method
    def get_full_load_from_table(self) -> dict:
        pass

    @abstractmethod
    @target_method
    def insert_full_load_into_table(self) -> dict:
        pass

    @abstractmethod
    @source_method
    def capture_changes(self, **kargs) -> pl.DataFrame:
        pass

    @abstractmethod
    @target_method
    def insert_cdc_into_table(self) -> dict:
        pass

    @abstractmethod
    @source_method
    def structure_capture_changes_to_json(self) -> dict:
        pass

    @abstractmethod
    @target_method
    def structure_capture_changes_to_dataframe(self) -> dict:
        pass
