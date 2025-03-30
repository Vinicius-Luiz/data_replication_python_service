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
        """
        Valida se o tipo da transformação é válido.

        Raises:
            ValueError: Se o tipo da transformação não for válido.
        """
        if self.transformation_type not in TransformationType:
            logging.error(f"TRANSFORMATION - Tipo de transformação inválido: {self.transformation_type}")
            raise ValueError(f"TRANSFORMATION - Tipo de transformação inválido: {self.transformation_type}")
        
    def get_contract_example(self) -> dict:
        """
        Retorna um exemplo do contrato para a transformação.

        Retorna um dicionário com as chaves e valores esperados para o contrato
        da transformação. Isso útil para quando você precisa criar um contrato
        para uma transformação, mas não sabe quais são as chaves e valores
        esperados.

        Returns:
            dict: Um dicionário com as chaves e valores esperados para o contrato
                  da transformação.
        """
        match self.transformation_type:
            case TransformationType.CREATE_COLUMN:
                return {'column_name': None, 'column_type': None}
            case TransformationType.MODIFY_SCHEMA_NAME:
                return {'target_schema_name': None}
            case TransformationType.MODIFY_TABLE_NAME:
                return {'target_table_name': None}
            case TransformationType.MODIFY_COLUMN_NAME:
                return {'column_name': None, 'target_column_name': None}
            case TransformationType.MODIFY_COLUMN_VALUE:
                return {'column_name': None, 'target_column_value': None}
    
    def execute(self, table = None) -> None:
        """
        Executa a transformação na tabela.

        Dependendo do tipo da transformação, o método executará uma das seguintes ações:
        - Criar uma coluna com um nome e tipo especificados.
        - Modificar o nome do schema da tabela.
        - Modificar o nome da tabela.
        - Modificar o nome de uma coluna.
        - Modificar os valores de uma coluna.

        Args:
            table (Table): Objeto representando a estrutura da tabela que receberá a transformação.

        Returns:
            None
        """
        match self.transformation_type:
            case TransformationType.CREATE_COLUMN:
                return self.execute_create_column(table)
            case TransformationType.MODIFY_SCHEMA_NAME:
                return self.execute_modify_schema_name(table)
            case TransformationType.MODIFY_TABLE_NAME:
                return self.execute_modify_table_name(table)
            case TransformationType.MODIFY_COLUMN_NAME: 
                return self.execute_modify_column_name(table)
            case TransformationType.MODIFY_COLUMN_VALUE:
                return self.execute_modify_column_value(table)

    def execute_modify_schema_name(self, table):
        """
        Modifica o nome do schema da tabela de destino com base no contrato de transformação.
        
        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.
        Returns:
            Table: Objeto representando a estrutura da tabela de origem com o nome do schema de destino atualizado.
        """
        table.target_schema_name = self.contract['target_schema_name']
        return table

    def execute_modify_table_name(self, table):
        """
        Modifica o nome da tabela de destino com base no contrato de transformação.
        
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

    def execute_create_column(self, table):
        raise NotImplementedError
    
    def execute_modify_column_value(self, table):      
        """
        Modifica os valores de uma coluna de destino com base no contrato de transformação.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.

        Returns:
            Table: Objeto representando a estrutura da tabela de origem com o nome da coluna de destino atualizado.
        """
               
        column_name = self.contract.get('column_name')
        operation = self.contract.get('operation')

        if column_name not in table.data.columns:
            raise_message = f"A coluna '{column_name}' não existe no DataFrame. Colunas disponíveis: {table.data.columns}"
            logging.critical(raise_message)
            raise ValueError(raise_message)
               
        # Mapeamento de operações suportadas (com validações específicas)
        operations = {
            'format_date': {
                'func': lambda: pl.col(column_name).dt.strftime(self.contract['format']),
                'required_params': ['format'],
                'column_type': [pl.Datetime, pl.Date]
            },
            'uppercase': {
                'func': lambda: pl.col(column_name).str.to_uppercase(),
                'column_type': [pl.Utf8]  
            },
            'lowercase': {
                'func': lambda: pl.col(column_name).str.to_lowercase(),
                'column_type': [pl.Utf8]
            },
            'trim': {
                'func': lambda: pl.col(column_name).str.strip(),
                'column_type': [pl.Utf8]
            },
            'extract_year': {
                'func': lambda: pl.col(column_name).dt.year(),
                'column_type': [pl.Datetime, pl.Date]
            },
            'custom_expr': {
                'func': lambda: pl.col(column_name).map_eval(self.contract['expression']),
                'required_params': ['expression']
            }
        }
        
        # Verifica se a operação é suportada
        if operation not in operations:
            raise_message = f"Operação '{operation}' não suportada. Operações válidas: {list(operations.keys())}"
            logging.critical(raise_message)
            raise ValueError(raise_message)
        
        # Valida parâmetros obrigatórios (ex: 'format' para 'format_date')
        op_config = operations[operation]
        for param in op_config.get('required_params', []):
            if param not in self.contract:
                raise_message = f"A operação '{operation}' requer o parâmetro '{param}' no contrato"
                logging.critical(raise_message)
                raise ValueError(raise_message)
        
        # Valida tipo da coluna (se especificado na operação)
        if 'column_type' in op_config:
            actual_type = table.data.schema[column_name]
            expected_types = op_config['column_type']
            
            # Garante que expected_types seja uma lista mesmo se for passado um único tipo
            if not isinstance(expected_types, (list, tuple)):
                expected_types = [expected_types]
            
            if not any(isinstance(actual_type, expected) for expected in expected_types):
                expected_names = [t.__name__ for t in expected_types]
                raise_message = (
                    f"A operação '{operation}' requer que a coluna '{column_name}' "
                    f"seja de um dos tipos: {expected_names}, mas é {actual_type.__class__.__name__}"
                )
                logging.critical(raise_message)
                raise ValueError(raise_message)
        
        # 5. Aplica a transformação
        try:
            table.data = table.data.with_columns(
                op_config['func']().alias(column_name) )
            
            return table
        
        except Exception as e:
            raise_message = f"Falha ao aplicar a transformação '{operation}' na coluna '{column_name}'. Erro: {str(e)}"
            logging.critical(raise_message)
            raise ValueError(raise_message)