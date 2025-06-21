import streamlit as st
import json
from pathlib import Path
from typing import Dict, List, Tuple, Union
from trempy.Shared.Types import (
    TransformationType,
    TransformationOperationType,
    PriorityType,
)
from trempy.Shared.TransformationDefinitions import TransformationDefinitions


class DisplayTransformation:
    """Classe responsável por exibir e gerenciar a interface de transformações."""

    SETTINGS_PATH = Path("task/settings.json")

    def __init__(self):
        """Inicializa o mapeamento de prioridades."""
        self.priority_types = list(PriorityType)
        self.priority_name_to_value = {p.name: p.value for p in self.priority_types}
        self.priority_value_to_name = {p.value: p.name for p in self.priority_types}

    def render(self):
        """Renderiza a interface de transformações."""
        # Carregar configurações existentes
        transformations_settings, all_settings = self.__configure_default_settings()

        # Inicializar estado da sessão
        self.__initialize_session_state(transformations_settings)

        st.header("Configurações de Transformações")
        st.markdown(
            """
            Configure as transformações que serão aplicadas aos dados durante a replicação.
            Você pode criar novas colunas, modificar valores existentes ou alterar a estrutura
            das tabelas (nomes de schema, tabela ou colunas).
            """
        )

        # Botão para adicionar nova transformação
        self.__handle_add_transformation()

        # Exibir transformações existentes
        self.__display_existing_transformations(st.session_state.temp_transformations)

        # Botão para salvar todas as transformações
        col1, _ = st.columns([0.75, 0.25])
        with col1:
            with st.form("main_save_form"):
                if st.form_submit_button("Salvar Configurações", type="primary"):
                    self.__save_settings(
                        all_settings, st.session_state.temp_transformations
                    )

    def __initialize_session_state(self, transformations_settings: List[Dict]) -> None:
        """
        Inicializa o estado da sessão se necessário.

        Args:
            transformations_settings (List[Dict]): Lista de configurações de transformações
        """
        if "temp_transformations" not in st.session_state:
            st.session_state.temp_transformations = transformations_settings.copy()

        if "edit_stage" not in st.session_state:
            st.session_state.edit_stage = {}

    def __handle_add_transformation(self) -> None:
        """Manipula a adição de uma nova transformação."""
        if st.button("➕ Adicionar Transformação", key="add_transformation_button"):
            new_transformation = {
                "table_info": {"schema_name": "", "table_name": ""},
                "settings": {
                    "transformation_type": TransformationType.CREATE_COLUMN.value,
                    "description": "",
                    "contract": {
                        "operation": TransformationOperationType.LITERAL.value
                    },
                    "priority": 0,
                },
            }
            st.session_state.temp_transformations.append(new_transformation)
            new_index = len(st.session_state.temp_transformations) - 1
            st.session_state.edit_stage[new_index] = "select"
            st.rerun()

    def __configure_default_settings(self) -> Tuple[List[Dict], Dict]:
        """
        Carrega as configurações padrão e existentes para transformações.

        Returns:
            Tuple[List[Dict], Dict]: Uma tupla contendo:
                - Lista de configurações de transformações
                - Dicionário com todas as configurações
        """
        # Carregar configurações existentes
        all_settings = {}
        if self.SETTINGS_PATH.exists():
            try:
                with open(self.SETTINGS_PATH, "r", encoding="utf-8") as f:
                    all_settings = json.load(f)
            except Exception as e:
                st.warning(
                    f"Não foi possível carregar as configurações existentes: {e}"
                )

        # Obter transformações existentes ou lista vazia se não existir
        transformations_settings = all_settings.get("transformations", [])

        return transformations_settings, all_settings

    def __save_settings(
        self, all_settings: dict, transformations_settings: List[Dict]
    ) -> bool:
        """
        Salva as configurações de transformações mantendo outras configurações intactas.

        Args:
            all_settings (dict): Todas as configurações do sistema
            transformations_settings (List[Dict]): Lista de configurações de transformações

        Returns:
            bool: True se as configurações foram salvas com sucesso, False caso contrário
        """
        try:
            # Validação das transformações
            valid_transformations = []
            for transformation in transformations_settings:
                if not transformation or transformation.get("__remove__", False):
                    continue

                table_info = transformation.get("table_info", {})
                settings = transformation.get("settings", {})

                # Remove chaves temporárias (com prefixo __)
                settings_clean = {
                    k: v for k, v in settings.items() if not k.startswith("__")
                }

                # Verifica se todos os campos obrigatórios estão presentes
                if (
                    table_info.get("schema_name")
                    and table_info.get("table_name")
                    and settings_clean.get("transformation_type")
                    and settings_clean.get("description")
                    and settings_clean.get("priority") is not None
                ):
                    # Adiciona a transformação limpa
                    valid_transformations.append(
                        {"table_info": table_info, "settings": settings_clean}
                    )

            # Atualiza todas as configurações mantendo outras seções
            updated_settings = {
                **all_settings,
                "transformations": valid_transformations,
            }

            # Garante que o diretório existe
            self.SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)

            # Salva as configurações
            with open(self.SETTINGS_PATH, "w", encoding="utf-8") as f:
                json.dump(updated_settings, f, indent=4, ensure_ascii=False)

            st.success("Configurações de transformações salvas com sucesso!")
            return True

        except Exception as e:
            st.error(f"Erro ao salvar configurações: {str(e)}")
            return False

    def __get_transformation_category(self, transformation_type: str) -> str:
        """
        Determina a categoria da transformação baseado no tipo.

        Args:
            transformation_type (str): Tipo da transformação

        Returns:
            str: Categoria da transformação
        """
        try:
            if transformation_type == TransformationType.CREATE_COLUMN.value:
                return "create_column"
            elif transformation_type == TransformationType.MODIFY_COLUMN_VALUE.value:
                return "modify_column_value"
            else:
                return "modify_structure"
        except ValueError:
            return "create_column"

    def __display_existing_transformations(self, transformations_settings: List[Dict]):
        """
        Exibe e permite edição das transformações existentes.

        Args:
            transformations_settings (List[Dict]): Lista de configurações de transformações
        """
        if not transformations_settings:
            return

        col1, _ = st.columns([0.75, 0.25])
        with col1:
            for idx, transformation in enumerate(transformations_settings):
                if transformation is None:
                    continue
                self.__display_transformation(idx, transformation)

    def __display_transformation(self, idx: int, transformation: Dict):
        """
        Exibe uma transformação específica com suas etapas de edição.

        Args:
            idx (int): Índice da transformação
            transformation (Dict): Dados da transformação
        """
        settings = transformation.get("settings", {})
        unique_key = f"transformation_{idx}"

        # Inicializa o estágio de edição se necessário
        if idx not in st.session_state.edit_stage:
            st.session_state.edit_stage[idx] = "complete"

        with st.expander(
            f"Transformação {idx + 1}: {settings.get('description', 'Sem descrição')}",
            expanded=False,
        ):
            if transformation.get("__remove__"):
                st.warning("Esta transformação será removida ao salvar")

            current_stage = st.session_state.edit_stage.get(idx)
            self.__display_transformation_stage(
                idx, transformation, current_stage, unique_key
            )

    def __display_transformation_stage(
        self, idx: int, transformation: Dict, current_stage: str, unique_key: str
    ):
        """
        Exibe o estágio atual da transformação.

        Args:
            idx (int): Índice da transformação
            transformation (Dict): Dados da transformação
            current_stage (str): Estágio atual da edição
            unique_key (str): Chave única para os componentes
        """
        if current_stage in [None, "select"]:
            self.__display_basic_info_stage(idx, transformation, unique_key)
        elif current_stage == "operation":
            self.__display_operation_stage(idx, transformation, unique_key)
        elif current_stage == "contract":
            self.__display_contract_stage(idx, transformation, unique_key)
        elif current_stage == "complete":
            self.__display_complete_stage(idx, transformation, unique_key)

    def __display_basic_info_stage(
        self, idx: int, transformation: Dict, unique_key: str
    ):
        """
        Exibe o estágio de informações básicas da transformação.

        Args:
            idx (int): Índice da transformação
            transformation (Dict): Dados da transformação
            unique_key (str): Chave única para os componentes
        """
        with st.form(key=f"basic_form_{unique_key}"):
            table_info = transformation.get("table_info", {})
            settings = transformation.get("settings", {})

            # Campos de schema e tabela
            schema_name, table_name = self.__render_table_fields(table_info, unique_key)

            # Categoria e descrição
            transformation_category = self.__render_category_field(settings, unique_key)
            description = self.__render_description_field(settings, unique_key)
            priority = self.__render_priority_field(settings, unique_key)

            if st.form_submit_button("Próxima Etapa"):
                # Atualiza table_info com os valores dos campos
                transformation["table_info"] = {
                    "schema_name": schema_name,
                    "table_name": table_name
                }

                # Atualiza as configurações
                transformation["settings"].update(
                    {
                        "description": description,
                        "priority": self.priority_name_to_value[priority],
                        "__category": transformation_category,
                    }
                )
                st.session_state.edit_stage[idx] = "operation"
                st.rerun()

    def __render_table_fields(
        self, table_info: Dict, unique_key: str
    ) -> Tuple[str, str]:
        """
        Renderiza os campos de schema e tabela.

        Args:
            table_info (Dict): Informações da tabela
            unique_key (str): Chave única para os componentes

        Returns:
            Tuple[str, str]: Schema e nome da tabela
        """
        cols = st.columns([1, 1])
        with cols[0]:
            schema_name = (
                st.text_input(
                    "Schema",
                    value=table_info.get("schema_name", ""),
                    key=f"schema_{unique_key}",
                    help="Nome do schema no banco de dados (ex: public, employees, etc)",
                )
                or ""
            )
        with cols[1]:
            table_name = (
                st.text_input(
                    "Tabela",
                    value=table_info.get("table_name", ""),
                    key=f"table_{unique_key}",
                    help="Nome da tabela no banco de dados (ex: employee, department, etc)",
                )
                or ""
            )
        return schema_name, table_name

    def __render_category_field(self, settings: Dict, unique_key: str) -> str:
        """
        Renderiza o campo de categoria.

        Args:
            settings (Dict): Configurações da transformação
            unique_key (str): Chave única para os componentes

        Returns:
            str: Categoria selecionada
        """
        current_category = self.__get_transformation_category(
            settings.get("transformation_type", "")
        )
        categories = ["create_column", "modify_column_value", "modify_structure"]
        category_labels = {
            "create_column": "Criar Coluna",
            "modify_column_value": "Modificar Valor de Coluna",
            "modify_structure": "Modificar Estrutura",
        }

        return (
            st.selectbox(
                "Categoria de Transformação",
                options=categories,
                index=categories.index(current_category),
                key=f"category_{unique_key}",
                format_func=lambda x: category_labels.get(x, x)
                or x,  # Garante retorno não nulo
                help=(
                    "Escolha o tipo de transformação:\n"
                    "- Criar Coluna: Adiciona uma nova coluna à tabela\n"
                    "- Modificar Valor de Coluna: Altera valores em uma coluna existente\n"
                    "- Modificar Estrutura: Altera nome de schema, tabela ou coluna"
                ),
            )
            or current_category
        )  # Garante retorno não nulo

    def __render_description_field(self, settings: Dict, unique_key: str) -> str:
        """
        Renderiza o campo de descrição.

        Args:
            settings (Dict): Configurações da transformação
            unique_key (str): Chave única para os componentes

        Returns:
            str: Descrição inserida
        """
        return (
            st.text_input(
                "Descrição",
                value=settings.get("description", ""),
                key=f"desc_{unique_key}",
                help="Descrição clara do que esta transformação faz (será usada para identificar a transformação)",
            )
            or ""
        )  # Garante retorno não nulo

    def __render_priority_field(self, settings: Dict, unique_key: str) -> str:
        """
        Renderiza o campo de prioridade.

        Args:
            settings (Dict): Configurações da transformação
            unique_key (str): Chave única para os componentes

        Returns:
            str: Prioridade selecionada
        """
        priority_name = self.priority_value_to_name.get(
            settings.get("priority", 0), "NORMAL"
        )
        return st.selectbox(
            "Prioridade",
            options=list(self.priority_name_to_value.keys()),
            index=list(self.priority_name_to_value.keys()).index(priority_name),
            key=f"priority_{unique_key}",
            help=(
                "Define a ordem de processamento das transformações:\n"
                "- HIGHEST: Processada primeiro\n"
                "- HIGH: Alta prioridade\n"
                "- NORMAL: Prioridade padrão\n"
                "- LOW: Baixa prioridade\n"
                "- LOWEST: Processada por último"
            ),
        )

    def __display_operation_stage(
        self, idx: int, transformation: Dict, unique_key: str
    ):
        """
        Exibe o estágio de seleção de operação.

        Args:
            idx (int): Índice da transformação
            transformation (Dict): Dados da transformação
            unique_key (str): Chave única para os componentes
        """
        with st.form(key=f"operation_form_{unique_key}"):
            settings = transformation.get("settings", {})

            st.write("Selecione o tipo de operação:")
            operations, current_operation = self.__get_available_operations(settings)
            operation = self.__render_operation_selector(
                operations, current_operation, unique_key
            )

            self.__render_operation_buttons(idx, transformation, settings, operation)

    def __render_operation_selector(
        self,
        operations: List[Union[TransformationOperationType, TransformationType]],
        current_operation: Union[TransformationOperationType, TransformationType],
        unique_key: str,
    ) -> Union[TransformationOperationType, TransformationType]:
        """
        Renderiza o seletor de operação.

        Args:
            operations (List): Lista de operações disponíveis
            current_operation: Operação atual
            unique_key (str): Chave única para os componentes

        Returns:
            Union[TransformationOperationType, TransformationType]: Operação selecionada
        """
        try:
            current_index = operations.index(current_operation)
        except (ValueError, KeyError):
            current_index = 0

        return st.selectbox(
            "Tipo de Operação",
            options=operations,
            index=current_index,
            key=f"operation_{unique_key}",
            format_func=lambda x: x.value.replace("_", " ").title(),
            help=(
                "Selecione a operação específica a ser aplicada.\n"
                "Cada operação tem seus próprios parâmetros que serão solicitados na próxima etapa."
            ),
        )

    def __render_operation_buttons(
        self,
        idx: int,
        transformation: Dict,
        settings: Dict,
        operation: Union[TransformationOperationType, TransformationType],
    ):
        """
        Renderiza os botões do estágio de operação.

        Args:
            idx (int): Índice da transformação
            transformation (Dict): Dados da transformação
            settings (Dict): Configurações da transformação
            operation: Operação selecionada
        """
        cols = st.columns([1, 1])
        with cols[0]:
            if st.form_submit_button("Voltar"):
                st.session_state.edit_stage[idx] = "select"
                st.rerun()
        with cols[1]:
            if st.form_submit_button("Próxima Etapa"):
                self.__update_operation(transformation, settings, operation)
                st.session_state.edit_stage[idx] = "contract"
                st.rerun()

    def __display_contract_stage(self, idx: int, transformation: Dict, unique_key: str):
        """
        Exibe o estágio de configuração do contrato.

        Args:
            idx (int): Índice da transformação
            transformation (Dict): Dados da transformação
            unique_key (str): Chave única para os componentes
        """
        with st.form(key=f"contract_form_{unique_key}"):
            settings = transformation.get("settings", {})

            st.write("Configure os detalhes da transformação:")
            required_params = self.__get_required_params(settings)
            contract = settings.get("contract", {})

            new_contract = self.__render_contract_fields(
                contract, required_params, unique_key
            )
            self.__render_contract_buttons(idx, transformation, settings, new_contract)

    def __render_contract_buttons(
        self,
        idx: int,
        transformation: Dict,
        settings: Dict,
        new_contract: Dict,
    ):
        """
        Renderiza os botões do estágio de contrato.

        Args:
            idx (int): Índice da transformação
            transformation (Dict): Dados da transformação
            settings (Dict): Configurações da transformação
            new_contract (Dict): Novo contrato
        """
        cols = st.columns([1, 1, 1])
        with cols[0]:
            if st.form_submit_button("Voltar"):
                st.session_state.edit_stage[idx] = "operation"
                st.rerun()
        with cols[1]:
            if st.form_submit_button("Concluir"):
                transformation["settings"]["contract"] = new_contract
                settings.pop("__category", None)  # remove campo temporário
                st.session_state.edit_stage[idx] = "complete"
                st.rerun()
        with cols[2]:
            if st.form_submit_button("❌ Remover"):
                transformation["__remove__"] = True
                st.session_state.edit_stage.pop(idx, None)
                st.rerun()

    def __display_complete_stage(self, idx: int, transformation: Dict, unique_key: str):
        """
        Exibe o estágio final da transformação.

        Args:
            idx (int): Índice da transformação
            transformation (Dict): Dados da transformação
            unique_key (str): Chave única para os componentes
        """
        st.json(transformation)
        if st.button("Editar", key=f"edit_{unique_key}"):
            st.session_state.edit_stage[idx] = "select"
            st.rerun()

    def __get_available_operations(self, settings: Dict) -> Tuple[
        List[Union[TransformationOperationType, TransformationType]],
        Union[TransformationOperationType, TransformationType],
    ]:
        """
        Obtém as operações disponíveis baseado na categoria.

        Args:
            settings (Dict): Configurações da transformação

        Returns:
            Tuple[List[Union[TransformationOperationType, TransformationType]], Union[TransformationOperationType, TransformationType]]:
                Uma tupla contendo a lista de operações disponíveis e a operação atual
        """
        category = settings.get("__category")
        operations: List[Union[TransformationOperationType, TransformationType]] = []

        if category == "create_column":
            operations.extend(
                list(TransformationDefinitions.COLUMN_CREATOR_TYPES.keys())
            )
            current_operation = TransformationOperationType(
                settings.get("contract", {}).get(
                    "operation",
                    TransformationOperationType.LITERAL.value,
                )
            )
        elif category == "modify_column_value":
            operations.extend(
                list(TransformationDefinitions.COLUMN_MODIFIER_TYPES.keys())
            )
            current_operation = TransformationOperationType(
                settings.get("contract", {}).get(
                    "operation",
                    TransformationOperationType.UPPERCASE.value,
                )
            )
        else:  # modify_structure
            operations.extend(
                [
                    op
                    for op in TransformationType
                    if op in TransformationDefinitions.STRUCTURE_MODIFIER_TYPES
                ]
            )
            current_operation = TransformationType(
                settings.get(
                    "transformation_type",
                    TransformationType.MODIFY_SCHEMA_NAME.value,
                )
            )

        return operations, current_operation

    def __update_operation(
        self,
        transformation: Dict,
        settings: Dict,
        operation: Union[TransformationOperationType, TransformationType],
    ) -> None:
        """
        Atualiza a operação da transformação.

        Args:
            transformation (Dict): Transformação a ser atualizada
            settings (Dict): Configurações da transformação
            operation: Nova operação selecionada
        """
        category = settings.get("__category")
        existing_contract = transformation["settings"].get("contract", {})

        if category == "create_column":
            transformation["settings"].update(
                {
                    "transformation_type": TransformationType.CREATE_COLUMN.value,
                    "contract": {**existing_contract, "operation": operation.value},
                }
            )
        elif category == "modify_column_value":
            transformation["settings"].update(
                {
                    "transformation_type": TransformationType.MODIFY_COLUMN_VALUE.value,
                    "contract": {**existing_contract, "operation": operation.value},
                }
            )
        else:  # modify_structure
            transformation["settings"].update(
                {"transformation_type": operation.value, "contract": existing_contract}
            )

    def __get_required_params(self, settings: Dict) -> List[str]:
        """
        Obtém os parâmetros necessários baseado no tipo de transformação.

        Args:
            settings (Dict): Configurações da transformação

        Returns:
            List[str]: Lista de parâmetros necessários
        """
        transformation_type = settings.get("transformation_type", "")
        operation_type = settings.get("contract", {}).get("operation", "")

        if transformation_type == TransformationType.CREATE_COLUMN.value:
            operation_enum = TransformationOperationType(operation_type)
            return TransformationDefinitions.COLUMN_CREATOR_PARAMS.get(
                operation_enum, []
            )
        elif transformation_type == TransformationType.MODIFY_COLUMN_VALUE.value:
            operation_enum = TransformationOperationType(operation_type)
            return TransformationDefinitions.COLUMN_MODIFIER_PARAMS.get(
                operation_enum, []
            )
        else:  # Modificação de estrutura
            transformation_enum = TransformationType(transformation_type)
            return TransformationDefinitions.STRUCTURE_MODIFIER_PARAMS.get(
                transformation_enum, []
            )

    def __render_contract_fields(
        self, contract: Dict, required_params: List[str], unique_key: str
    ) -> Dict:
        """
        Renderiza os campos do contrato.

        Args:
            contract (Dict): Contrato atual
            required_params (List[str]): Lista de parâmetros necessários
            unique_key (str): Chave única para os componentes

        Returns:
            Dict: Novo contrato com os valores atualizados
        """
        new_contract = contract.copy()

        for param in required_params:
            if param == "depends_on":
                new_contract["depends_on"] = self.__render_depends_on_field(
                    contract, unique_key
                )
            elif param == "value_type":
                new_contract["value_type"] = self.__render_value_type_field(
                    contract, unique_key
                )
            elif param == "round_result":
                new_contract[param] = self.__render_round_result_field(
                    contract, unique_key
                )
            else:
                new_contract[param] = self.__render_text_input_field(
                    param, contract, unique_key
                )

        return new_contract

    def __render_depends_on_field(self, contract: Dict, unique_key: str) -> List[str]:
        """
        Renderiza o campo de colunas dependentes.

        Args:
            contract (Dict): Contrato atual
            unique_key (str): Chave única para os componentes

        Returns:
            List[str]: Lista de colunas dependentes
        """
        num_cols = st.number_input(
            "Número de Colunas Dependentes",
            min_value=1,
            max_value=5,
            value=len(contract.get("depends_on", [])) or 2,
            key=f"num_cols_{unique_key}",
            help="Quantidade de colunas que serão usadas nesta operação",
        )

        depends_on = []
        for i in range(num_cols):
            col = (
                st.text_input(
                    f"Coluna Dependente {i+1}",
                    value=(
                        contract.get("depends_on", [])[i]
                        if i < len(contract.get("depends_on", []))
                        else ""
                    ),
                    key=f"dep_col_{unique_key}_{i}",
                    help="Nome da coluna existente na tabela que será usada na operação",
                )
                or ""
            )  # Garante retorno não nulo
            depends_on.append(col)

        return depends_on

    def __render_value_type_field(self, contract: Dict, unique_key: str) -> str:
        """
        Renderiza o campo de tipo de valor.

        Args:
            contract (Dict): Contrato atual
            unique_key (str): Chave única para os componentes

        Returns:
            str: Tipo de valor selecionado
        """
        value_types = ["varchar", "int", "float", "date", "datetime"]
        return (
            st.selectbox(
                "Tipo do Valor",
                options=value_types,
                index=value_types.index(contract.get("value_type", "varchar")),
                key=f"value_type_{unique_key}",
                help=(
                    "Tipo de dado para o valor:\n"
                    "- varchar: Texto\n"
                    "- int: Número inteiro\n"
                    "- float: Número decimal\n"
                    "- date: Data\n"
                    "- datetime: Data e hora"
                ),
            )
            or "varchar"
        )  # Garante retorno não nulo

    def __render_round_result_field(self, contract: Dict, unique_key: str) -> bool:
        """
        Renderiza o campo de arredondamento.

        Args:
            contract (Dict): Contrato atual
            unique_key (str): Chave única para os componentes

        Returns:
            bool: Se deve arredondar o resultado
        """
        return st.checkbox(
            "Arredondar Resultado",
            value=contract.get("round_result", False),
            key=f"round_result_{unique_key}",
            help="Se marcado, o resultado será arredondado para o número inteiro mais próximo",
        )

    def __render_text_input_field(
        self, param: str, contract: Dict, unique_key: str
    ) -> str:
        """
        Renderiza um campo de texto com label e help apropriados.

        Args:
            param (str): Nome do parâmetro
            contract (Dict): Contrato atual
            unique_key (str): Chave única para os componentes

        Returns:
            str: Valor inserido no campo
        """
        param_value = contract.get(param, "")
        param_label, param_help = self.__get_field_label_and_help(param)

        return (
            st.text_input(
                param_label,
                value=param_value,
                key=f"{param}_{unique_key}",
                help=param_help,
            )
            or ""
        )  # Garante retorno não nulo

    def __get_field_label_and_help(self, param: str) -> Tuple[str, str]:
        """
        Retorna o label e texto de ajuda para um parâmetro.

        Args:
            param (str): Nome do parâmetro

        Returns:
            Tuple[str, str]: Label e texto de ajuda
        """
        param_label = param.replace("_", " ").title()
        param_help = ""

        if param == "new_column_name":
            param_label = "Nome da Nova Coluna"
            param_help = "Nome para a nova coluna que será criada"
        elif param == "column_name":
            param_label = "Nome da Coluna"
            param_help = "Nome da coluna existente que será modificada"
        elif param == "target_column_name":
            param_label = "Novo Nome da Coluna"
            param_help = "Novo nome para a coluna após a modificação"
        elif param == "target_schema_name":
            param_label = "Novo Nome do Schema"
            param_help = "Novo nome para o schema após a modificação"
        elif param == "target_table_name":
            param_label = "Novo Nome da Tabela"
            param_help = "Novo nome para a tabela após a modificação"
        elif param == "expression":
            param_label = "Expressão Matemática"
            param_help = "Expressão matemática a ser aplicada. Use 'value' para referenciar o valor atual da coluna (ex: 'value / 12' para dividir por 12)"
        elif param == "format":
            param_label = "Formato da Data"
            param_help = "Formato para a data usando códigos do Python (ex: %Y-%m-%d para ano-mês-dia)"
        elif param == "separator":
            param_label = "Separador"
            param_help = (
                "Caractere ou texto usado para separar os valores ao concatenar"
            )
        elif param == "value":
            param_label = "Valor"
            param_help = "Valor literal que será usado na nova coluna"
        elif param == "column_names":
            param_label = "Nomes das Colunas"
            param_help = "Lista de nomes de colunas separados por vírgula"

        return param_label, param_help
