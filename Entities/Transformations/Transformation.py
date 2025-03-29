from Entities.Shared.Types import TransformationType
import polars as pl
import logging 

class Transformation:
    def __init__(self, transformation_type: TransformationType, description: str, contract: dict) -> None:
        self.transformation_type = transformation_type
        self.description = description
        self.contract = contract

        self.validate()
    
    def validate(self) -> None:
        if self.transformation_type not in TransformationType:
            logging.error(f"TRANSFORMATION - Tipo de transformação inválido: {self.transformation_type}")
            raise ValueError(f"TRANSFORMATION - Tipo de transformação inválido: {self.transformation_type}")
        
    def get_contract_example(self) -> dict:
        match self.transformation_type:
            case TransformationType.CREATE_COLUMN:
                return {'column_name': None, 'column_type': None}
            case TransformationType.MODIFY_SCHEMA_NAME:
                return {'target_schema_name': None}
            case TransformationType.MODIFY_TABLE_NAME:
                return {'target_table_name': None}
            case TransformationType.MODIFY_COLUMN_NAME:
                return {'column_name': None, 'target_column_name': None}
            case TransformationType.MODIFY_COLUMN_VALUES:
                return {'column_name': None, 'target_column_value': None}
    
    def execute(self, table = None) -> None:
        match self.transformation_type:
            case TransformationType.CREATE_COLUMN:
                return self.execute_create_column(table)
            case TransformationType.MODIFY_SCHEMA_NAME:
                return self.execute_modify_schema_name(table)
            case TransformationType.MODIFY_TABLE_NAME:
                return self.execute_modify_table_name(table)
            case TransformationType.MODIFY_COLUMN_NAME: 
                return self.execute_modify_column_name(table)
            case TransformationType.MODIFY_COLUMN_VALUES:
                return self.execute_modify_column_values(table)
    
    def execute_create_column(self, table):
        raise NotImplementedError

    def execute_modify_schema_name(self, table):
        """
        Modifica o nome do schema da tabela de destino com base no contrato de transforma o.
        
        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.
        Returns:
            Table: Objeto representando a estrutura da tabela de origem com o nome do schema de destino atualizado.
        """
        table.target_schema_name = self.contract['target_schema_name']
        return table

    def execute_modify_table_name(self, table):
        """
        Modifica o nome da tabela de destino com base no contrato de transforma o.
        
        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.
        Returns:
            Table: Objeto representando a estrutura da tabela de origem com o nome da tabela de destino atualizado.
        """
        table.target_table_name = self.contract['target_table_name']
        return table

    def execute_modify_column_name(self, table):
        """
        Modifica o nome da coluna de destino com base no contrato de transformação.
        
        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.
        Returns:
            Table: Objeto representando a estrutura da tabela de origem com o nome da coluna de destino atualizado.
        """
        table.columns[self.contract['column_name']].name = self.contract['target_column_name']
        return table

    def execute_modify_column_values(self, table):
        raise NotImplementedError