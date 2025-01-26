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

        self.columns = []