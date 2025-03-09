from Entities.Shared.Queries import PostgreSQLQueries
from Entities.Transformations.Transformation import Transformation
from Entities.Columns.Column import Column
from typing import List
import pandas as pd
import logging

class Table:
    def __init__(self, schema_name: str  = None,  
                 table_name: str  = None,
                 estimated_row_count: int  = None,
                 table_size: str = None):
        
        self.schema_name = schema_name
        self.table_name = table_name
        self.estimated_row_count = estimated_row_count
        self.table_size = table_size
        self.id = f'{self.schema_name}_{self.table_name}'

        self.target_schema_name = self.schema_name
        self.target_table_name = self.table_name

        self.path_data: str = None
        self.data: pd.DataFrame = None

        self.columns: List[Column] = []
        self.transformations: List[Transformation] = []

    def mount_columns_to_create_table(self) -> str:
        columns_sql = []
        for column in self.columns:
            character_maximum_length = f'({column.character_maximum_length})' if column.character_maximum_length else ''
            column_str = f'{column.name} {column.data_type}{character_maximum_length} {"NOT NULL" if not column.nullable else "NULL"}'
            columns_sql.append(column_str)

        columns_sql = ', '.join(columns_sql)
        return columns_sql
    
    def mount_create_table(self) -> str:
        columns_sql = self.mount_columns_to_create_table()
        return PostgreSQLQueries.CREATE_TABLE.format(schema=self.target_schema_name, table=self.target_table_name, columns=columns_sql)      

    def copy(self) -> 'Table':
        new_table = Table(self.schema_name, self.table_name, self.estimated_row_count, self.table_size)
        new_table.columns = self.columns.copy()
        return new_table
    
    def execute_transformations(self) -> None:
        for transformation in self.transformations:
            logging.info(f"Aplicando transformação em {self.schema_name}.{self.table_name}: {transformation.description}")
            transformation.execute(self)