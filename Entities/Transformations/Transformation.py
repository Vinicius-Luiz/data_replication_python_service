from Entities.Shared.Types import TransformationType
from Entities.Tables.Table import Table
import pandas as pd
import logging 

class Transformation:
    def __init__(self, transformation_type: TransformationType, description: str, contract: dict) -> None:
        self.transformation_type = transformation_type
        self.description = description
        self.contract = contract

        self.validate()
    
    def validate(self) -> None:
        if self.transformation_type not in TransformationType:
            logging.error(f"Tipo de transformação inválido: {self.transformation_type}")
            raise ValueError("Invalid transformation type.")
        
    def get_contract_example(self) -> dict:
        match self.transformation_type:
            case TransformationType.CREATE_COLUMN:
                return {'column_name': None, 'column_type': None}
            case TransformationType.MODIFY_SCHEMA_NAME:
                return {'schema_name': None, 'target_schema_name': None}
            case TransformationType.MODIFY_TABLE_NAME:
                return {'table_name': None, 'target_table_name': None}
            case TransformationType.MODIFY_COLUMN_NAME:
                return {'column_name': None, 'target_column_name': None}
            case TransformationType.MODIFY_COLUMN_TYPE:
                return {'column_name': None, 'target_column_type': None}
            case TransformationType.MODIFY_COLUMN_VALUES:
                return {'column_name': None, 'target_column_value': None}
    
    def execute(self, table: Table = None) -> None:
        match self.transformation_type:
            case TransformationType.CREATE_COLUMN:
                return self.execute_create_column(table)
            case TransformationType.MODIFY_SCHEMA_NAME:
                return self.execute_modify_schema_name(table)
            case TransformationType.MODIFY_TABLE_NAME:
                return self.execute_modify_table_name(table)
            case TransformationType.MODIFY_COLUMN_NAME: 
                return self.execute_modify_column_name(table)
            case TransformationType.MODIFY_COLUMN_TYPE:
                return self.execute_modify_column_type(table)
            case TransformationType.MODIFY_COLUMN_VALUES:
                return self.execute_modify_column_values(table)
    
    def execute_create_column(self, table: Table) -> Table:
        raise NotImplementedError

    def execute_modify_schema_name(self, table: Table) -> Table:
        raise NotImplementedError

    def execute_modify_table_name(self, table: Table) -> Table:
        table.target_table_name = self.contract['target_table_name']
        return table

    def execute_modify_column_name(self, table: Table) -> Table:
        raise NotImplementedError

    def execute_modify_column_type(self, table: Table) -> Table:
        raise NotImplementedError

    def execute_modify_column_values(self, table: Table) -> Table:
        raise NotImplementedError