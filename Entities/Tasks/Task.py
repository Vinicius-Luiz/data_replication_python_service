from Entities.Endpoints.Endpoint import Endpoint
from Entities.Transformations.Transformation import Transformation
from Entities.Tables.Table import Table
from Entities.Shared.Types import TaskType
from typing import List, Dict
import polars as pl
import logging

class Task:
    PATH_FULL_LOAD_STAGING_AREA = 'data/full_load_data/'
    PATH_CDC_STAGING_AREA = 'data/cdc_data/'

    def __init__(self, task_name: str, source_endpoint: Endpoint, target_endpoint: Endpoint, replication_type: TaskType,
                 create_table_if_not_exists: bool = False, recreate_table_if_exists: bool = False, truncate_before_insert: bool = False) -> None:
        self.task_name = task_name
        self.source_endpoint  = source_endpoint
        self.target_endpoint  = target_endpoint
        self.replication_type = replication_type

        self.create_table_if_not_exists = create_table_if_not_exists
        self.recreate_table_if_exists   = recreate_table_if_exists
        self.truncate_before_insert     = truncate_before_insert

        self.id = f'{self.source_endpoint.id}_{self.target_endpoint.id}_{self.task_name}'

        self.tables: List[Table] = []

        self.filters = []

        self.validate()

    def validate(self) -> None:
        if self.replication_type not in TaskType:
            logging.error(f"TASK - Tipo de tarefa {self.replication_type} inválido")
            raise ValueError(f"TASK - Tipo de tarefa {self.replication_type} inválido")

        logging.info(f"TASK - {self.task_name} válido")

    def add_tables(self, table_names: List[dict]) -> dict:
        try:
            tables_detail = []
            for idx, table in enumerate(table_names):
                schema_name = table.get('schema_name')
                table_name = table.get('table_name')
                priority = table.get('priority') or idx
                
                logging.info(f"TASK - Obtendo detalhes da tabela {schema_name}.{table_name}")
                table_detail = self.source_endpoint.get_table_details(schema=schema_name, table=table_name)
                logging.debug(table_detail.__dict__)
                
                tables_detail.append({'priority': priority, 'table_detail': table_detail})
            
            tables_detail = sorted(tables_detail, key=lambda x: x['priority'])
            self.tables = [table['table_detail'] for table in tables_detail]

            return {'success': True, 'tables': self.tables}
        except Exception as e:
            logging.critical(f"TASK - Erro ao obter detalhes da tabela: {e}")
            raise ValueError(f"TASK - Erro ao obter detalhes da tabela: {e}")
    
    def add_transformation(self, schema_name: str, table_name: str, transformation: Transformation) -> None:
        try:
            table = self.find_table(schema_name, table_name)
            table.transformations.append(transformation)
        except Exception as e:
            logging.critical(f"TASK - Erro ao adicionar transformação: {e}")
            raise ValueError(f"TASK - Erro ao adicionar transformação: {e}")
    
    def find_table(self, schema_name: str, table_name: str) -> Table:
        for table in self.tables:
            if table.schema_name == schema_name and table.table_name == table_name:
                return table
    

    def run(self) -> dict:
        if self.replication_type == TaskType.FULL_LOAD:
            return self._run_full_load()
        if self.replication_type == TaskType.CDC:
            return self._run_cdc()
        if self.replication_type == TaskType.FULL_LOAD_CDC:
            self._run_full_load()
            return self._run_cdc()

    def _run_full_load(self) -> dict:
        try:
            for table in self.tables:
                logging.info(f"TASK - Obtendo dados da tabela {table.target_schema_name}.{table.target_table_name}")
                table.path_data = f'{self.PATH_FULL_LOAD_STAGING_AREA}{self.task_name}_{table.target_schema_name}_{table.target_table_name}.parquet'
                table_get_full_load = self.source_endpoint.get_full_load_from_table(table = table)
                table.data = pl.read_parquet(table.path_data)
                logging.debug(table_get_full_load)

                table.execute_transformations()

                logging.info(f"TASK - Realizando carga completa da tabela {table.target_schema_name}.{table.target_table_name}")
                table_full_load = self.target_endpoint.insert_full_load_into_table(
                    table = table,
                    create_table_if_not_exists = self.create_table_if_not_exists,
                    recreate_table_if_exists   = self.recreate_table_if_exists,
                    truncate_before_insert     = self.truncate_before_insert
                )
                logging.debug(table_full_load)

            return table_full_load
        except Exception as e:
            logging.critical(e)
            raise ValueError(e)

    def _run_cdc(self) -> dict:
        raise NotImplementedError