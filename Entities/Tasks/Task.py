from Entities.Endpoints.Endpoint import Endpoint
from Entities.Transformations.Transformation import Transformation
from Entities.Tables.Table import Table
from Entities.Shared.Types import TaskType
from typing import List, Dict
import logging

class Task:
    def __init__(self, task_name: str, source_endpoint: Endpoint, target_endpoint: Endpoint, replication_type: TaskType) -> None:
        self.task_name = task_name
        self.source_endpoint = source_endpoint
        self.target_endpoint = target_endpoint
        self.replication_type = replication_type

        self.id = f'{self.source_endpoint.id}_{self.target_endpoint.id}_{self.task_name}'

        self.tables: List[Table] = []

        self.filters = []

        self.validate()

    def validate(self) -> None:
        if self.replication_type not in TaskType:
            logging.error(f"Tipo de tarefa inválido: {self.replication_type}")
            raise ValueError("Invalid task type.")

        logging.info(f"Task {self.id} validado com sucesso.")

    def add_tables(self, table_names: List[dict]) -> dict:
        try:
            tables_detail = []
            for idx, table in enumerate(table_names):
                schema_name = table.get('schema_name')
                table_name = table.get('table_name')
                priority = table.get('priority') or idx
                
                logging.info(f"Obtendo detalhes da tabela {schema_name}.{table_name}")
                table_detail = self.source_endpoint.get_table_details(schema=schema_name, table=table_name)
                logging.debug(table_detail.__dict__)
                
                tables_detail.append({'priority': priority, 'table_detail': table_detail})
            
            tables_detail = sorted(tables_detail, key=lambda x: x['priority'])
            self.tables = [table['table_detail'] for table in tables_detail]

            return {'success': True, 'tables': self.tables}
        except Exception as e:
            logging.critical(f"Erro ao obter detalhes da tabela: {e}")
            raise ValueError(f"Erro ao obter detalhes da tabela: {e}")
    
    def find_table(self, schema_name: str, table_name: str) -> Table:
        for table in self.tables:
            if table.schema_name == schema_name and table.table_name == table_name:
                return table
    
    def add_transformation(self, schema_name: str, table_name: str, transformation: Transformation) -> None:
        try:
            table = self.find_table(schema_name, table_name)
            table.transformations.append(transformation)
        except Exception as e:
            logging.critical(f"Erro ao adicionar transformação: {e}")
            raise ValueError(f"Erro ao adicionar transformação: {e}")

    def run(self) -> dict:
        self.apply_transformations()

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
                logging.info(f"Obtendo dados da tabela {table.schema_name}.{table.table_name}")
                table_get_full_load = self.source_endpoint.get_full_load_from_table(schema=table.schema_name, table=table.table_name)
                logging.debug(table_get_full_load)

                for transformation in table.transformations:
                    logging.info(f"Aplicando transformação: {transformation.description}")
                    transformation.execute(table)

                logging.info(f"Realizando carga completa da tabela {table.schema_name}.{table.table_name}")
                table_full_load = self.target_endpoint.insert_full_load_into_table(
                    # target_schema=table.target_schema_name,
                    # target_table=table.target_table_name,
                    table=table,
                    create_table_if_not_exists=True,
                    recreate_table_if_exists=True,
                    truncate_before_insert=True
                )
                logging.debug(table_full_load)

            return {'message': 'Full load data inserted successfully', 'success': True}
        except Exception as e:
            logging.critical(f"Erro ao realizar carga completa: {e}")
            raise ValueError(f"Erro ao realizar carga completa: {e}")

    def _run_cdc(self) -> dict:
        pass