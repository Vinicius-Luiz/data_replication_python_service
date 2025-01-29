from Entities.Shared.Queries import PostgreSQLQueries
from Entities.Columns.Column import Column
from typing import List

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

        self.columns: List[Column] = []

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
        return PostgreSQLQueries.CREATE_TABLE.format(schema=self.schema_name, table=self.table_name, columns=columns_sql)      

    def copy(self) -> 'Table':
        new_table = Table(self.schema_name, self.table_name, self.estimated_row_count, self.table_size)
        new_table.columns = self.columns.copy()
        return new_table