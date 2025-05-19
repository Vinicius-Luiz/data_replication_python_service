from trempy.Endpoints.Endpoint import Endpoint, EndpointType, DatabaseType
from trempy.Endpoints.Databases.PostgreSQL.Subclasses import (
    TableManager,
    CDCManager,
    CDCOperationsHandler,
    ConnectionManager,
    FullLoadHandler,
    MetadataReader,
    TableCreator,
)
from trempy.Endpoints.Exceptions.Exception import *
from trempy.Tables.Table import Table
from typing import Dict, List
import polars as pl


class EndpointPostgreSQL(Endpoint):

    def __init__(
        self,
        endpoint_type: EndpointType,
        endpoint_name: str,
        credentials: dict,
        batch_cdc_size: int,
    ):
        """
        Inicializa um endpoint para PostgreSQL, gerenciando a conexão.

        Args:
            endpoint_type (EndpointType): Tipo do endpoint (fonte ou destino).
            endpoint_name (str): Nome do endpoint.
            credentials (dict): Credenciais do banco de dados.
        """
        super().__init__(
            DatabaseType.POSTGRESQL,
            endpoint_type,
            endpoint_name,
            batch_cdc_size
        )

        self.connection_manager = ConnectionManager.ConnectionManager(credentials)
        self.metadata_reader = MetadataReader.MetadataReader(self.connection_manager)
        self.table_creator = TableCreator.TableCreator(self.connection_manager)
        self.table_manager = TableManager.TableManager(
            self.connection_manager, self.table_creator
        )
        self.cdc_manager = CDCManager.CDCManager(self.connection_manager, batch_cdc_size)
        self.cdc_operations_handler = CDCOperationsHandler.CDCOperationsHandler(
            self.connection_manager, self.table_manager
        )
        self.full_load_handler = FullLoadHandler.FullLoadHandler(
            self.connection_manager, self.table_manager
        )

    def get_schemas(self) -> list:
        return self.metadata_reader.get_schemas()

    def get_tables(self, schema_name: str) -> list:
        return self.metadata_reader.get_tables(schema_name)

    def get_table_details(self, table: dict) -> Table:
        return self.metadata_reader.get_table_details(table)

    def get_table_primary_key(self, table: Table) -> list:
        return self.metadata_reader.get_table_primary_key(table)

    def get_table_columns(self, table: Table, primary_keys: list) -> Table:
        return self.metadata_reader.get_table_columns(table, primary_keys)

    def mount_create_table(self, table: Table) -> str:
        return self.table_creator.mount_create_table(table)

    def get_full_load_from_table(self, table: Table) -> dict:
        return self.full_load_handler.get_full_load_from_table(table)

    def insert_full_load_into_table(
        self,
        table: Table,
        create_table_if_not_exists: bool,
        recreate_table_if_exists: bool,
        truncate_before_insert: bool,
    ) -> dict:
        return self.full_load_handler.insert_full_load_into_table(
            table,
            create_table_if_not_exists,
            recreate_table_if_exists,
            truncate_before_insert,
        )

    def capture_changes(self, **kargs) -> pl.DataFrame:
        return self.cdc_manager.capture_changes(**kargs)

    def insert_cdc_into_table(
        self, mode: str, table: Table, create_table_if_not_exists: bool = False
    ):
        return self.cdc_operations_handler.insert_cdc_into_table(
            mode, table, create_table_if_not_exists
        )

    def structure_capture_changes_to_json(
        self, df_changes_captured: pl.DataFrame, task_tables: List[Table], **kargs
    ) -> List[Dict]:
        return self.cdc_manager.structure_capture_changes_to_json(
            df_changes_captured, task_tables, **kargs
        )

    def structure_capture_changes_to_dataframe(self, changes_structured: dict) -> dict:
        return self.cdc_manager.structure_capture_changes_to_dataframe(
            changes_structured
        )
