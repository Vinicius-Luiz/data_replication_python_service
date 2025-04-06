from __future__ import annotations
from Entities.Transformations.FunctionColumnCreator import FunctionColumnCreator as FCC
from Entities.Shared.Types import TransformationOperationType
from Entities.Columns.Column import Column
from typing import Dict, List, Any, TYPE_CHECKING
import polars as pl
import logging

if TYPE_CHECKING:
    from Entities.Tables.Table import Table


class ColumnCreator:
    """Classe responsável por validar e criar novas colunas."""

    TYPE_MAPPING = {
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

    @staticmethod
    def get_operations(depends_on: list, contract: dict) -> Dict[str, Any]:
        """Retorna um dicionário de operações com base em tipos de operações definidos.

        O dicionário retornado contém todas as operações disponíveis no sistema, cada uma com:
        - Uma função lambda que executa a operação
        - Parâmetros requeridos
        - Tipos de colunas necessários (quando aplicável)

        Args:
            depends_on (list): Lista de colunas das quais as operações dependem.
            contract (dict): Dicionário contendo detalhes específicos para cada operação,
                como valores literais ou parâmetros de configuração.

        Returns:
            Dict[str, Any]: Dicionário de operações onde:
                - Chave (str): Tipo de operação (valores do enum TransformationOperationType)
                - Valor (dict): Contém:
                    * 'func': Função lambda que executa a operação
                    * 'required_params': Parâmetros obrigatórios (quando aplicável)
                    * 'column_type': Tipos de coluna requeridos (quando aplicável)
        """

        return {
            TransformationOperationType.LITERAL: {
                "func": lambda: FCC.literal(value=contract["value"]),
                "required_params": FCC.get_required_params(
                    TransformationOperationType.LITERAL
                ),
            },
            TransformationOperationType.DATE_NOW: {"func": lambda: FCC.date_now()},
            TransformationOperationType.CONCAT: {
                "func": lambda: FCC.concat(
                    separator=contract.get("separator", ""),
                    depends_on=depends_on,
                ),
                "required_params": FCC.get_required_params(
                    TransformationOperationType.CONCAT
                ),
                "column_type": FCC.get_required_column_types(
                    TransformationOperationType.CONCAT
                ),
            },
            TransformationOperationType.DATE_DIFF_YEARS: {
                "func": lambda: FCC.date_diff_years(
                    start_col=depends_on[0],
                    end_col=depends_on[1],
                    round_result=contract.get("round_result", False),
                ),
                "required_params": FCC.get_required_params(
                    TransformationOperationType.DATE_DIFF_YEARS
                ),
                "column_type": FCC.get_required_column_types(
                    TransformationOperationType.DATE_DIFF_YEARS
                ),
            },
        }

    @staticmethod
    def _validate_basic_contract(new_column_name: str, table: Table) -> None:
        """Valida os requisitos básicos do contrato de transformação.

        Verifica se o nome da nova coluna é válido e único na tabela especificada.

        Args:
            new_column_name: Nome da coluna a ser criada. Não pode ser vazio ou nulo.
            table: Tabela de origem onde a nova coluna será adicionada.

        Raises:
            ValueError: Se ocorrer algum dos seguintes casos:
                - O nome da coluna for vazio ou nulo
                - O nome da coluna já existir na tabela
        """

        if not new_column_name:
            raise ValueError("O contrato deve conter 'new_column_name'")

        if new_column_name in table.data.columns:
            raise ValueError(f"A coluna '{new_column_name}' já existe no DataFrame")

    @staticmethod
    def _validate_dependent_columns(depends_on: List[str], table: Table) -> None:
        """Valida a existência de todas as colunas dependentes na tabela especificada.

        Args:
            depends_on: Lista de nomes das colunas que a operação depende.
                Cada nome deve corresponder a uma coluna existente na tabela.
            table: Tabela de origem contendo os dados a serem validados.

        Raises:
            ValueError: Se alguma coluna da lista `depends_on` não for encontrada
                na tabela. A mensagem de erro inclui a lista de colunas disponíveis.
        """

        for col in depends_on:
            if col not in table.data.columns:
                available = list(table.data.columns)
                raise ValueError(
                    f"Coluna dependente '{col}' não encontrada. Disponíveis: {available}"
                )

    @staticmethod
    def _validate_operation(
        operation: str, operations: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Valida se uma operação está entre as operações suportadas.

        Args:
            operation: Nome da operação a ser validada (case-sensitive).
            operations: Dicionário de operações suportadas, onde as chaves são os nomes
                das operações e os valores são seus respectivos configurations.

        Returns:
            As configurações da operação validada, conforme definido no dicionário
            de operações suportadas.

        Raises:
            ValueError: Se a operação especificada não for encontrada no dicionário
                de operações suportadas. A mensagem de erro inclui a lista de
                operações válidas.
        """

        if operation not in operations:
            valid_ops = list(operations.keys())
            raise ValueError(
                f"Operação '{operation}' não suportada. Válidas: {valid_ops}"
            )
        return operations[operation]

    @staticmethod
    def _validate_required_params(
        op_config: Dict[str, Any], contract: Dict[str, Any]
    ) -> None:
        """Valida a presença de todos os parâmetros obrigatórios no contrato de transformação.

        Args:
            op_config: Configuração da operação contendo a lista de parâmetros obrigatórios
                na chave 'required_params'. Exemplo: {'required_params': ['value', 'format']}
            contract: Contrato de transformação contendo os parâmetros fornecidos pelo usuário.
                Deve incluir todos os parâmetros listados em op_config['required_params'].

        Raises:
            ValueError: Se o contrato não contiver algum dos parâmetros listados como
                obrigatórios na configuração da operação. A mensagem de erro especifica
                qual parâmetro está faltando.

        Note:
            Esta função não valida o tipo ou conteúdo dos parâmetros, apenas sua existência
            no contrato.
        """

        for param in op_config.get("required_params", []):
            if param not in contract:
                raise ValueError(f"Parâmetro obrigatório faltando: '{param}'")

    @staticmethod
    def _validate_column_types(
        depends_on: List[str], op_config: Dict[str, Any], table: Table
    ) -> None:
        """Valida se os tipos das colunas dependentes atendem aos requisitos da operação.

        Args:
            depends_on: Lista de nomes das colunas que serão validadas.
            op_config: Configuração da operação contendo os tipos esperados na estrutura:
                {
                    "column_type": {
                        "depends_on": [list, of, expected, types]  # Classes de tipo esperadas
                    }
                }
            table: Objeto Table contendo os dados e schema a serem validados.

        Raises:
            ValueError: Quando o tipo real de alguma coluna não corresponde a nenhum dos
                tipos esperados definidos na configuração da operação. A mensagem inclui:
                - Nome da coluna com tipo inválido
                - Tipos esperados
                - Tipo recebido

        Note:
            - Retorna silenciosamente se a operação não definir tipos esperados
            - A validação é feita usando isinstance(), permitindo herança de tipos
        """

        if (
            "column_type" not in op_config
            or "depends_on" not in op_config["column_type"]
        ):
            return

        for col in depends_on:
            actual_type = table.data.schema[col]
            expected_types = op_config["column_type"]["depends_on"]

            if not any(isinstance(actual_type, t) for t in expected_types):
                expected_names = [t.__name__ for t in expected_types]
                raise ValueError(
                    f"Tipo inválido para coluna '{col}'. "
                    f"Esperado: {expected_names}, Recebido: {type(actual_type).__name__}"
                )

    @classmethod
    def create_column(cls, contract: Dict[str, Any], table: Table) -> Table:
        """Cria e adiciona uma nova coluna à tabela conforme especificado no contrato.

        O processo completo inclui:
        1. Extração e validação dos parâmetros do contrato
        2. Validação das dependências e tipos
        3. Execução da operação de transformação
        4. Atualização dos metadados da tabela

        Args:
            contract: Contrato de transformação contendo:
                - new_column_name: Nome da nova coluna
                - operation: Tipo de operação a ser aplicada
                - depends_on: Lista de colunas dependentes (opcional)
                - Demais parâmetros específicos da operação
            table: Tabela de destino que receberá a nova coluna

        Returns:
            A tabela modificada com a nova coluna adicionada, incluindo:
            - Os dados transformados
            - Os metadados atualizados (schema)

        Raises:
            ValueError: Para qualquer inconsistência nos parâmetros ou contrato
            TypeError: Se os tipos das colunas forem incompatíveis com a operação
            KeyError: Se parâmetros obrigatórios estiverem faltando no contrato
            Exception: Para erros inesperados durante a execução (registrado em log)
        """

        try:
            # Extrai parâmetros do contrato
            new_column_name = contract.get("new_column_name")
            operation = TransformationOperationType(contract.get("operation"))
            depends_on = contract.get("depends_on", [])

            # Busca operações
            operations = cls.get_operations(depends_on, contract)

            # Validações
            cls._validate_basic_contract(new_column_name, table)
            cls._validate_dependent_columns(depends_on, table)
            op_config = cls._validate_operation(operation, operations)
            cls._validate_required_params(op_config, contract)
            cls._validate_column_types(depends_on, op_config, table)

            # Execução
            table.data = table.data.with_columns(
                op_config["func"]().alias(new_column_name)
            )

            # Atualiza metadados
            new_column_type = table.data.schema[new_column_name]
            sql_type = cls.TYPE_MAPPING.get(type(new_column_type), "text")

            table.columns[new_column_name] = Column(
                name=new_column_name,
                data_type=sql_type,
                nullable=True,
                ordinal_position=len(table.columns) + 1,
                is_primary_key=False,
            )

            return table

        except Exception as e:
            logging.critical(f"Falha ao criar coluna: {str(e)}")
            raise
