from Entities.Endpoints.Endpoint import Endpoint
from Entities.Transformations.Transformation import Transformation
from Entities.Filters.Filter import Filter
from Entities.Tables.Table import Table
from Entities.Shared.Types import TaskType, PriorityType, EndpointType, DatabaseType
from typing import List, Optional
import polars as pl
import logging
import re


class Task:
    """
    Classe que representa uma tarefa de replicação de dados.

    Attributes:
        task_name (str): O nome da tarefa.
        source_endpoint (Endpoint): O ponto de entrada de dados da tarefa.
        target_endpoint (Endpoint): O ponto de saída de dados da tarefa.
        replication_type (TaskType): O tipo de replicação da tarefa.
        create_table_if_not_exists (bool): Indica se a tabela deve ser criada se nao existir.
        recreate_table_if_exists (bool): Indica se a tabela deve ser recriada se ja existir.
        truncate_before_insert (bool): Indica se a tabela deve ser truncada antes de inserir os dados.
        id (str): O identificador da tarefa.
    """

    PATH_FULL_LOAD_STAGING_AREA = "data/full_load_data/"
    PATH_CDC_STAGING_AREA = "data/cdc_data/"

    def __init__(
        self,
        task_name: str,
        replication_type: str,
        interval_seconds: int = 60,
        source_endpoint: Endpoint = None,
        target_endpoint: Endpoint = None,
        create_table_if_not_exists: bool = False,
        recreate_table_if_exists: bool = False,
        truncate_before_insert: bool = False,
    ) -> None:
        self.task_name = task_name
        self.replication_type = TaskType(replication_type)
        self.interval_seconds = interval_seconds

        self.source_endpoint = source_endpoint
        self.target_endpoint = target_endpoint

        self.create_table_if_not_exists = create_table_if_not_exists
        self.recreate_table_if_exists = recreate_table_if_exists
        self.truncate_before_insert = truncate_before_insert

        self.tables: List[Table] = []

        self.filters = []

        self.validate()

    def validate(self) -> None:
        """
        Realiza a validação do tipo da tarefa associada ao objeto.

        Verifica se o tipo da tarefa está entre os valores permitidos e segue
        as especificações definidas para o sistema.

        Raises:
            ValueError: Se o tipo da tarefa for inválido ou não estiver entre
                       os valores permitidos.
            AttributeError: Se o objeto não possuir o atributo de tipo de tarefa.
            TypeError: Se o tipo da tarefa for de um tipo inesperado.
        """

        if self.replication_type not in TaskType:
            raise_msg = f"TASK - Tipo de tarefa {self.replication_type} inválido"
            logging.critical(raise_msg)
            raise ValueError(raise_msg)

        partner = re.compile(r"^[a-z0-9_]+$")
        task_name_valid = bool(partner.match(self.task_name))

        if not task_name_valid:
            raise_msg = f"TASK - Nome da tarefa {self.task_name} inválido"
            logging.critical(raise_msg)
            raise ValueError(raise_msg)

        logging.info(f"TASK - {self.task_name} válido")

    def clean_endpoints(self) -> None:
        """
        Limpa os endpoints da tarefa.

        Deve ser chamado quando a tarefa terminar de ser executada, para liberar
        recursos, disponibilizar variável para exportação via pickle e evitar problemas de concorrência.
        """
        self.source_endpoint = None
        self.target_endpoint = None

    def add_endpoint(self, endpoint: Endpoint) -> None:
        """
        Adiciona um endpoint à tarefa atual.

        Caso o tipo do endpoint seja SOURCE, ele é adicionado como endpoint de origem.
        Caso o tipo do endpoint seja TARGET, ele é adicionado como endpoint de destino.

        Args:
            endpoint (Endpoint): Endpoint a ser adicionado à tarefa.
        """

        if endpoint.endpoint_type == EndpointType.SOURCE:
            self.source_endpoint = endpoint
        elif endpoint.endpoint_type == EndpointType.TARGET:
            self.target_endpoint = endpoint

    def add_tables(self, table_names: List[dict]) -> dict:
        """
        Adiciona múltiplas tabelas à tarefa atual a partir de uma lista de definições.

        Cada tabela é criada e configurada com base nos dicionários fornecidos, que devem
        conter as informações necessárias para identificação das tabelas no banco de dados.

        Args:
            table_names (List[dict]): Lista de dicionários contendo as especificações das tabelas.
                Cada dicionário deve conter:
                - 'schema_name': str - Nome do esquema da tabela
                - 'table_name': str - Nome da tabela
                - 'columns': List[dict] - Lista de colunas (opcional)

        Returns:
            dict: Resultado da operação com a seguinte estrutura:
        """

        try:
            tables_detail = []
            for table in table_names:
                schema_name = table.get("schema_name")
                table_name = table.get("table_name")
                priority = PriorityType(table.get("priority"))

                logging.info(
                    f"TASK - Obtendo detalhes da tabela {schema_name}.{table_name}"
                )
                table_detail = self.source_endpoint.get_table_details(
                    schema=schema_name, table=table_name
                )
                logging.debug(table_detail.to_dict())

                tables_detail.append(
                    {"priority": priority, "table_detail": table_detail}
                )

            tables_detail = sorted(tables_detail, key=lambda x: x["priority"].value)
            self.tables = [table["table_detail"] for table in tables_detail]

            return {"success": True, "tables": self.tables}
        except Exception as e:
            raise_msg = f"TASK - Erro ao obter detalhes da tabela: {e}"
            logging.critical(raise_msg)
            raise ValueError(raise_msg)

    def add_transformation(
        self, schema_name: str, table_name: str, transformation: Transformation
    ) -> None:
        """
        Adiciona uma transformação à tabela especificada.

        A transformação será aplicada à tabela durante o processamento, seguindo a ordem
        de prioridade definida. A tabela deve existir previamente na tarefa.

        Args:
            schema_name (str): Nome do esquema onde a tabela está localizada.
                Deve corresponder a um esquema existente no banco de dados.
            table_name (str): Nome da tabela que receberá a transformação.
                Deve corresponder a uma tabela existente no esquema especificado.
            transformation (Transformation): Objeto contendo a definição completa da transformação,
                incluindo tipo, parâmetros e prioridade.
        """

        try:
            table = self.find_table(schema_name, table_name)
            table.transformations.append(transformation)
        except Exception as e:
            raise_msg = f"TASK - Erro ao adicionar transformação: {e}"
            logging.critical(raise_msg)
            raise ValueError(raise_msg)

    def add_filter(self, schema_name: str, table_name: str, filter: Filter) -> None:
        """
        Adiciona um filtro à tabela especificada.

        A tabela deve existir previamente na tarefa.

        Args:
            schema_name (str): Nome do esquema onde a tabela está localizada.
                Deve corresponder a um esquema existente no banco de dados.
            table_name (str): Nome da tabela que receberá o filtro.
                Deve corresponder a uma tabela existente no esquema especificado.
            filter (Filter): Objeto contendo a definição completa do filtro, incluindo
                coluna, tipo, parâmetros e prioridade.
        """

        try:
            table = self.find_table(schema_name, table_name)
            table.filters.append(filter)
        except Exception as e:
            raise_msg = f"TASK - Erro ao adicionar filtro: {e}"
            logging.critical(raise_msg)
            raise ValueError(raise_msg)

    def find_table(self, schema_name: str, table_name: str) -> Optional[Table]:
        """
        Busca e retorna uma tabela específica na lista de tabelas da tarefa atual.

        Realiza uma busca exata (case-sensitive) pelo par (schema_name, table_name) na lista
        de tabelas associadas à tarefa. A comparação considera espaços e caracteres especiais.

        Args:
            schema_name (str): Nome do esquema onde a tabela está registrada.
                Não pode ser vazio ou conter apenas espaços em branco.
            table_name (str): Nome da tabela a ser localizada.
                Não pode ser vazio ou conter apenas espaços em branco.

        Returns:
            Optional[Table]: O objeto Table correspondente se encontrado, None caso a tabela
                           não exista na tarefa.
        """

        for table in self.tables:
            if table.schema_name == schema_name and table.table_name == table_name:
                return table

    def run(self) -> dict:
        """
        Executa a tarefa de replicação conforme configurado.

        Coordena o fluxo completo de replicação dos dados desde a origem até o destino,
        seguindo o tipo e parâmetros definidos na configuração da tarefa.

        Returns:
            dict: Dicionário contendo informações sobre o resultado da execução:
        """

        if self.replication_type == TaskType.FULL_LOAD:
            return self._run_full_load()
        if self.replication_type == TaskType.CDC:
            return self._run_cdc()
        if self.replication_type == TaskType.FULL_LOAD_CDC:
            self._run_full_load()
            return self._run_cdc()

    def execute_source(self) -> dict:
        if self.replication_type == TaskType.FULL_LOAD:
            return self._execute_source_full_load()
        if self.replication_type == TaskType.CDC:
            return self._execute_source_cdc()

    def execute_target(self) -> dict:
        if self.replication_type == TaskType.FULL_LOAD:
            return self._execute_target_full_load()
        if self.replication_type == TaskType.CDC:
            return self._execute_target_cdc()

    def _execute_source_full_load(self) -> dict:
        try:
            for table in self.tables:
                logging.info(
                    f"TASK - Obtendo dados da tabela {table.target_schema_name}.{table.target_table_name}"
                )
                table.path_data = f"{self.PATH_FULL_LOAD_STAGING_AREA}{self.task_name}_{table.target_schema_name}_{table.target_table_name}.parquet"
                table_get_full_load = self.source_endpoint.get_full_load_from_table(
                    table=table
                )
                logging.debug(table_get_full_load)
        except Exception as e:
            raise ValueError(e)

    def _execute_target_full_load(self) -> dict:
        try:
            for table in self.tables:
                table.data = pl.read_parquet(table.path_data)

                table.execute_filters()
                table.execute_transformations()

                logging.info(
                    f"TASK - Realizando carga completa da tabela {table.target_schema_name}.{table.target_table_name}"
                )
                table_full_load = self.target_endpoint.insert_full_load_into_table(
                    table=table,
                    create_table_if_not_exists=self.create_table_if_not_exists,
                    recreate_table_if_exists=self.recreate_table_if_exists,
                    truncate_before_insert=self.truncate_before_insert,
                )
                logging.debug(table_full_load)

            return table_full_load
        except Exception as e:
            raise ValueError(e)

    def _execute_source_cdc(self) -> dict:
        # TODO enviar mensagem via RabbitMQ
        match self.source_endpoint.database_type:
            case DatabaseType.POSTGRESQL:
                kargs = {
                    "slot_name": self.task_name,
                }
            case _:
                raise ValueError(
                    f"TASK - Banco de dados {self.source_endpoint.database_type} não implementado"
                )

        changes_captured = self.source_endpoint.capture_changes(**kargs)

        changes_structured = self.source_endpoint.structure_capture_changes(
            changes_captured, save_files=True
        )

        return changes_structured

    def _execute_target_cdc(self) -> bool:
        # TODO receber mensagem via RabbitMQ
        # TODO estruturar json em pl.DataFrame
        # TODO executar filtros
        # TODO executar transformações
        # TODO executar carga no destino
        raise False
