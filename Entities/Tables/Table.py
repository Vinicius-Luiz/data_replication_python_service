from __future__ import annotations
from typing import TYPE_CHECKING
from Entities.Shared.Queries import PostgreSQLQueries
from Entities.Transformations.Transformation import Transformation
from Entities.Columns.Column import Column
from typing import List, Dict
import polars as pl

if TYPE_CHECKING:
    from Entities.Transformations.Transformation import Transformation

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
        self.data: pl.DataFrame = None

        self.columns: Dict[str, Column] = {}
        self.transformations: List[Transformation] = []

    def mount_columns_to_create_table(self) -> str:
        columns_sql = []
        for column in sorted(self.columns.values(), key=lambda col: col.ordinal_position):
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
    
    def find_column(self, column_name: str) -> Column:
        return self.columns.get(column_name)
    
    def execute_transformations(self) -> None:
        for transformation in sorted(self.transformations, key=lambda x: x.priority.value):
            transformation.execute(self)