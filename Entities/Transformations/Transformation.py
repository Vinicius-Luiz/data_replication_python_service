from __future__ import annotations
from typing import TYPE_CHECKING
from Entities.Shared.Types import TransformationType
from Entities.Columns.Column import Column
from datetime import datetime
import polars as pl
import logging

if TYPE_CHECKING:
    from Entities.Tables.Table import Table


class Transformation:
    def __init__(
        self,
        transformation_type: TransformationType,
        description: str,
        contract: dict,
        priority: int,
    ) -> None:
        self.transformation_type = transformation_type
        self.description = description
        self.contract = contract
        self.priority = priority

        self.validate()

    def validate(self) -> None:
        """
        Valida se o tipo da transformação é válido.

        Raises:
            ValueError: Se o tipo da transformação não for válido.
        """
        if self.transformation_type not in TransformationType:
            logging.error(
                f"TRANSFORMATION - Tipo de transformação inválido: {self.transformation_type}"
            )
            raise ValueError(
                f"TRANSFORMATION - Tipo de transformação inválido: {self.transformation_type}"
            )

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
                return {"column_name": None, "column_type": None}
            case TransformationType.MODIFY_SCHEMA_NAME:
                return {"target_schema_name": None}
            case TransformationType.MODIFY_TABLE_NAME:
                return {"target_table_name": None}
            case TransformationType.MODIFY_COLUMN_NAME:
                return {"column_name": None, "target_column_name": None}
            case TransformationType.MODIFY_COLUMN_VALUE:
                return {"column_name": None, "target_column_value": None}

    def execute(self, table: Table = None) -> None:
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
        logging.info(f"TRANSFORMATION - Aplicando transformação em {table.schema_name}.{table.table_name}: {self.description}")
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

    def execute_modify_schema_name(self, table: Table) -> Table:
        """
        Modifica o nome do schema da tabela de destino com base no contrato de transformação.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.
        Returns:
            Table: Objeto representando a estrutura da tabela de origem com o nome do schema de destino atualizado.
        """
        table.target_schema_name = self.contract["target_schema_name"]
        return table

    def execute_modify_table_name(self, table: Table) -> Table:
        """
        Modifica o nome da tabela de destino com base no contrato de transformação.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.
        Returns:
            Table: Objeto representando a estrutura da tabela de origem com o nome da tabela de destino atualizado.
        """
        table.target_table_name = self.contract["target_table_name"]
        return table

    def execute_modify_column_name(self, table: Table) -> Table:
        """
        Modifica o nome da coluna de destino com base no contrato de transformação.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.
        Returns:
            Table: Objeto representando a estrutura da tabela de origem com o nome da coluna de destino atualizado.
        """
        table.columns[self.contract["column_name"]].name = self.contract[
            "target_column_name"
        ]
        return table

    def execute_create_column(self, table: Table) -> Table:
        """
        Cria uma nova coluna no DataFrame com base no contrato de transformação.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.

        Returns:
            Table: Objeto com a nova coluna adicionada.
        """
        new_column_name = self.contract.get("new_column_name")
        operation = self.contract.get("operation")
        depends_on = self.contract.get(
            "depends_on", []
        )  # Colunas das quais esta depende

        if not new_column_name:
            raise_message = "O contrato deve conter 'new_column_name'"
            logging.critical(raise_message)
            raise ValueError(raise_message)

        if new_column_name in table.data.columns:
            raise_message = f"A coluna '{new_column_name}' já existe no DataFrame"
            logging.critical(raise_message)
            raise ValueError(raise_message)

        # Verifica se as colunas dependentes existem
        for col in depends_on:
            if col not in table.data.columns:
                raise_message = f"Coluna dependente '{col}' não encontrada. Colunas disponíveis: {table.data.columns}"
                logging.critical(raise_message)
                raise ValueError(raise_message)

        # Mapeamento de operações suportadas para criação
        operations = {
            "literal": {
                "func": lambda: pl.lit(self.contract["value"]),
                "required_params": ["value"],
            },
            "derived": {
                "func": lambda: pl.col(depends_on[0]).map_eval(
                    self.contract["expression"]
                ),
                "required_params": ["expression", "depends_on"],
                "column_type": {  # Tipo esperado para colunas de origem
                    "depends_on": [pl.Utf8]  # Exemplo: pode variar por operação
                },
            },
            "concat": {
                "func": lambda: pl.concat_str(
                    [pl.col(col) for col in depends_on],
                    separator=self.contract.get("separator", ""),
                ),
                "required_params": ["depends_on"],
                "column_type": {
                    "depends_on": [pl.Utf8]  # Todas as colunas devem ser strings
                },
            },
            "date_now": {
                "func": lambda: pl.lit(datetime.now()).cast(pl.Datetime),
                "column_type": {"new_column": pl.Datetime},  # Tipo da nova coluna
            },
            "date_diff_years": {
                "func": lambda: self._calculate_years_diff(
                    table.data,
                    self.contract["depends_on"][0],
                    self.contract["depends_on"][1],
                    self.contract.get("round_result", False),
                ),
                "required_params": ["depends_on"],
                "column_type": {
                    "depends_on": [
                        pl.Datetime,
                        pl.Date,
                    ]  # Ambas colunas devem ser datas
                },
            },
        }

        # Verifica se a operação é suportada
        if operation not in operations:
            raise_message = f"Operação '{operation}' não suportada para criação. Operações válidas: {list(operations.keys())}"
            logging.critical(raise_message)
            raise ValueError(raise_message)

        # Valida parâmetros obrigatórios
        op_config = operations[operation]
        for param in op_config.get("required_params", []):
            if param not in self.contract:
                raise_message = (
                    f"A operação '{operation}' requer o parâmetro '{param}' no contrato"
                )
                logging.critical(raise_message)
                raise ValueError(raise_message)

        # Valida tipos das colunas de origem (se especificado)
        if "column_type" in op_config:
            if "depends_on" in op_config["column_type"]:
                for col in depends_on:
                    actual_type = table.data.schema[col]
                    expected_types = op_config["column_type"]["depends_on"]

                    if not any(
                        isinstance(actual_type, expected) for expected in expected_types
                    ):
                        expected_names = [t.__name__ for t in expected_types]
                        raise_message = (
                            f"A operação '{operation}' requer que a coluna '{col}' "
                            f"seja de um dos tipos: {expected_names}, mas é {actual_type.__class__.__name__}"
                        )
                        logging.critical(raise_message)
                        raise ValueError(raise_message)

        # Aplica a transformação
        try:
            # 1. Adiciona a coluna no DataFrame
            table.data = table.data.with_columns(
                op_config["func"]().alias(new_column_name)
            )

            # 2. Determina o tipo de dados da nova coluna
            new_column_type = table.data.schema[new_column_name]

            # 3. Mapeia tipos Polars para tipos SQL
            type_mapping = {
                pl.Int8: "smallint",
                pl.Int16: "smallint",
                pl.Int32: "integer",
                pl.Int64: "bigint",
                pl.Float32: "real",
                pl.Float64: "double precision",
                pl.Utf8: "character varying",
                pl.Boolean: "boolean",
                pl.Date: "date",
                pl.Datetime: "timestamp",
                pl.Decimal: "numeric",
                pl.Null: "text",
            }

            sql_type = type_mapping.get(type(new_column_type), "text")

            # 4. Cria o objeto Column e adiciona ao dicionário
            new_column = Column(
                name=new_column_name,
                data_type=sql_type,
                nullable=True,
                ordinal_position=len(table.columns) + 1,
                is_primary_key=False,
            )

            table.columns[new_column_name] = new_column

            return table

        except Exception as e:
            raise_message = f"Falha ao criar a coluna '{new_column_name}' com a operação '{operation}'. Erro: {str(e)}"
            logging.critical(raise_message)
            raise ValueError(raise_message)

    def execute_modify_column_value(self, table: Table) -> Table:
        """
        Modifica os valores de uma coluna de destino com base no contrato de transformação.

        Args:
            table (Table): Objeto representando a estrutura da tabela de origem.

        Returns:
            Table: Objeto representando a estrutura da tabela de origem com o nome da coluna de destino atualizado.
        """

        column_name = self.contract.get("column_name")
        operation = self.contract.get("operation")

        if column_name not in table.data.columns:
            raise_message = f"A coluna '{column_name}' não existe no DataFrame. Colunas disponíveis: {table.data.columns}"
            logging.critical(raise_message)
            raise ValueError(raise_message)

        # Mapeamento de operações suportadas (com validações específicas)
        operations = {
            "format_date": {
                "func": lambda: pl.col(column_name).dt.strftime(
                    self.contract["format"]
                ),
                "required_params": ["format"],
                "column_type": [pl.Datetime, pl.Date],
            },
            "uppercase": {
                "func": lambda: pl.col(column_name).str.to_uppercase(),
                "column_type": [pl.Utf8],
            },
            "lowercase": {
                "func": lambda: pl.col(column_name).str.to_lowercase(),
                "column_type": [pl.Utf8],
            },
            "trim": {
                "func": lambda: pl.col(column_name).str.strip(),
                "column_type": [pl.Utf8],
            },
            "extract_year": {
                "func": lambda: pl.col(column_name).dt.year(),
                "column_type": [pl.Datetime, pl.Date],
            },
            "custom_expr": {
                "func": lambda: pl.col(column_name).map_eval(
                    self.contract["expression"]
                ),
                "required_params": ["expression"],
            },
        }

        # Verifica se a operação é suportada
        if operation not in operations:
            raise_message = f"Operação '{operation}' não suportada. Operações válidas: {list(operations.keys())}"
            logging.critical(raise_message)
            raise ValueError(raise_message)

        # Valida parâmetros obrigatórios (ex: 'format' para 'format_date')
        op_config = operations[operation]
        for param in op_config.get("required_params", []):
            if param not in self.contract:
                raise_message = (
                    f"A operação '{operation}' requer o parâmetro '{param}' no contrato"
                )
                logging.critical(raise_message)
                raise ValueError(raise_message)

        # Valida tipo da coluna (se especificado na operação)
        if "column_type" in op_config:
            actual_type = table.data.schema[column_name]
            expected_types = op_config["column_type"]

            # Garante que expected_types seja uma lista mesmo se for passado um único tipo
            if not isinstance(expected_types, (list, tuple)):
                expected_types = [expected_types]

            if not any(
                isinstance(actual_type, expected) for expected in expected_types
            ):
                expected_names = [t.__name__ for t in expected_types]
                raise_message = (
                    f"A operação '{operation}' requer que a coluna '{column_name}' "
                    f"seja de um dos tipos: {expected_names}, mas é {actual_type.__class__.__name__}"
                )
                logging.critical(raise_message)
                raise ValueError(raise_message)

        # 5. Aplica a transformação
        try:
            table.data = table.data.with_columns(op_config["func"]().alias(column_name))

            return table

        except Exception as e:
            raise_message = f"Falha ao aplicar a transformação '{operation}' na coluna '{column_name}'. Erro: {str(e)}"
            logging.critical(raise_message)
            raise ValueError(raise_message)

    def _calculate_years_diff(
        self, df: pl.DataFrame, start_col: str, end_col: str, round_result: bool
    ) -> pl.Expr:
        """
        Cálcula a diferença em anos entre duas colunas de datas.

        Parameters
        ----------
        df : polars.DataFrame
            DataFrame que contém as colunas de datas.
        start_col : str
            Nome da coluna que representa a data de início.
        end_col : str
            Nome da coluna que representa a data de fim.
        round_result : bool
            Se True, o resultado será arredondado para o inteiro mais próximo.
            Caso contrário, o resultado será um float64 com 6 casas decimais.

        Returns
        -------
        polars.Expr
            Expressão Polars que calcula a diferença em anos entre as colunas de datas.
        """

        days_in_year = 365.2425
        return (
            pl.when(pl.col(end_col).is_null() | pl.col(start_col).is_null())
            .then(None)
            .otherwise(
                ((pl.col(end_col) - pl.col(start_col)).dt.total_days() / days_in_year)
                .round(0 if round_result else 6)
                .cast(pl.Int32 if round_result else pl.Float64)
            )
        )
