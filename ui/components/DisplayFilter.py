from trempy.Shared.FilterDefinitions import FilterDefinitions
from trempy.Shared.Types import FilterType, InputFilterType
from typing import List, Dict, Tuple
from pathlib import Path
import streamlit as st
import json


class DisplayFilter:
    """
    Classe responsável por gerenciar e exibir a interface de configuração de filtros.
    
    Esta classe fornece uma interface gráfica para adicionar, editar e remover filtros
    que serão aplicados nas tabelas do sistema.
    """
    
    SETTINGS_PATH = Path("task/settings.json")

    def __configure_default_settings(self) -> Tuple[List[Dict], Dict]:
        """
        Carrega as configurações padrão e existentes para filtros.
        
        Returns:
            Tuple[List[Dict], Dict]: Uma tupla contendo:
                - Lista de configurações de filtros
                - Dicionário com todas as configurações
        """
        default_filters_settings = []

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

        # Mesclar defaults com configurações existentes
        current_filters_settings = all_settings.get("filters", [])
        filters_settings = current_filters_settings or default_filters_settings

        return filters_settings, all_settings

    def __save_settings(self, all_settings: dict, filters_settings: List[Dict]) -> bool:
        """
        Salva as configurações de filtros mantendo outras configurações intactas.
        
        Args:
            all_settings (dict): Todas as configurações do sistema
            filters_settings (List[Dict]): Lista de configurações de filtros
            
        Returns:
            bool: True se as configurações foram salvas com sucesso, False caso contrário
        """
        try:
            # Validação dos filtros
            valid_filters = []
            for filter_setting in filters_settings:
                if not filter_setting or filter_setting.get("__remove__", False):
                    continue
                    
                table_info = filter_setting.get("table_info", {})
                settings = filter_setting.get("settings", {})
                
                # Verifica se todos os campos obrigatórios estão presentes
                if (table_info.get("schema_name") and 
                    table_info.get("table_name") and 
                    settings.get("column_name")):
                    valid_filters.append(filter_setting)

            # Atualiza todas as configurações mantendo outras seções
            updated_settings = {**all_settings, "filters": valid_filters}

            # Garante que o diretório existe
            self.SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
            
            # Salva as configurações
            with open(self.SETTINGS_PATH, "w", encoding="utf-8") as f:
                json.dump(updated_settings, f, indent=4, ensure_ascii=False)
                
            st.success("Configurações de filtros salvas com sucesso!")
            return True
            
        except PermissionError:
            st.error("Erro de permissão ao salvar o arquivo de configurações. Verifique as permissões da pasta.")
            return False
        except json.JSONDecodeError:
            st.error("Erro ao codificar as configurações em JSON. Verifique se não há caracteres inválidos.")
            return False
        except Exception as e:
            st.error(f"Erro inesperado ao salvar configurações: {str(e)}")
            return False

    def __get_filter_inputs(
        self, filter_type: FilterType, current_settings: Dict
    ) -> Dict[str, str | List[str]]:
        """
        Retorna os inputs necessários para um tipo de filtro específico.
        
        Args:
            filter_type (FilterType): O tipo do filtro
            current_settings (Dict): Configurações atuais do filtro
            
        Returns:
            Dict[str, str | List[str]]: Dicionário com os valores dos inputs
        """
        input_mapping = {
            InputFilterType.VALUE: ("value", ""),
            InputFilterType.VALUES: ("values", []),
            InputFilterType.LOWER: ("lower", ""),
            InputFilterType.UPPER: ("upper", ""),
        }
        
        inputs = {}
        for input_type in FilterDefinitions.get_input_types(filter_type):
            if input_type in input_mapping:
                key, default = input_mapping[input_type]
                inputs[key] = current_settings.get(key, default)
                
        return inputs

    def __render_filter_inputs(
        self, filter_type: FilterType, current_settings: Dict, unique_key: str
    ):
        """Renderiza os inputs específicos para o tipo de filtro."""
        input_types = FilterDefinitions.get_input_types(filter_type)

        if not input_types:
            st.info("Este filtro não requer valores adicionais")
            return {}

        inputs = {}
        for input_type in input_types:
            if input_type == InputFilterType.VALUE:
                inputs["value"] = st.text_input(
                    "Valor",
                    value=current_settings.get("value", ""),
                    key=f"value_{unique_key}",
                )
            elif input_type == InputFilterType.VALUES:
                values_str = ",".join(current_settings.get("values", []))
                new_values_str = st.text_input(
                    "Valores (separados por vírgula)",
                    value=values_str,
                    key=f"values_{unique_key}",
                )
                inputs["values"] = [
                    v.strip() for v in new_values_str.split(",") if v.strip()
                ]
            elif input_type == InputFilterType.LOWER:
                inputs["lower"] = st.text_input(
                    "Valor mínimo",
                    value=current_settings.get("lower", ""),
                    key=f"lower_{unique_key}",
                )
            elif input_type == InputFilterType.UPPER:
                inputs["upper"] = st.text_input(
                    "Valor máximo",
                    value=current_settings.get("upper", ""),
                    key=f"upper_{unique_key}",
                )

        return inputs

    def __initialize_session_state(self, filters_settings: List[Dict]) -> None:
        """Inicializa o estado da sessão se necessário."""
        if "temp_filters" not in st.session_state:
            st.session_state.temp_filters = filters_settings.copy()

        if "edit_stage" not in st.session_state:
            st.session_state.edit_stage = {}
            
    def __render_filter_header(self) -> None:
        """Renderiza o cabeçalho da página de filtros."""
        st.header("Configuração de Filtros")
        st.markdown(
            "Adicione, remova ou edite os filtros que serão aplicados nas tabelas."
        )
        
    def __handle_add_filter(self) -> None:
        """Manipula a adição de um novo filtro."""
        if st.button("➕ Adicionar Filtro", key="add_filter_button"):
            new_filter = {
                "table_info": {"schema_name": "", "table_name": ""},
                "settings": {
                    "filter_type": "equals",
                    "description": "",
                    "column_name": "",
                },
            }
            st.session_state.temp_filters.append(new_filter)
            new_index = len(st.session_state.temp_filters) - 1
            st.session_state.edit_stage[new_index] = "select"
            st.rerun()
            
    def __render_save_all_button(self, all_settings: Dict) -> None:
        """Renderiza o botão para salvar todas as configurações."""
        col1, _ = st.columns([0.75, 0.25])
        with col1:
            with st.form("main_save_form"):
                if st.form_submit_button("Salvar Configurações", type="primary"):
                    # Salva as configurações
                    if self.__save_settings(all_settings, st.session_state.temp_filters):
                        # Atualiza a lista temporária removendo os filtros marcados
                        st.session_state.temp_filters = [
                            f for f in st.session_state.temp_filters
                            if f is not None and not f.get("__remove__", False)
                        ]

                        # Resetar os estágios de edição
                        st.session_state.edit_stage = {
                            i: "complete"
                            for i, f in enumerate(st.session_state.temp_filters)
                            if f is not None
                        }

                        st.rerun()

    def __render_filter_form(
        self,
        index: int,
        filter_data: Dict,
        table_info: Dict,
        settings: Dict,
        filter_type: FilterType,
        unique_key: str
    ) -> None:
        """
        Renderiza o formulário de edição de um filtro específico.
        
        Args:
            index (int): Índice do filtro
            filter_data (Dict): Dados do filtro
            table_info (Dict): Informações da tabela
            settings (Dict): Configurações do filtro
            filter_type (FilterType): Tipo do filtro
            unique_key (str): Chave única para o filtro
        """
        # Etapa 1: Seleção básica (sempre visível)
        with st.form(key=f"basic_form_{unique_key}"):
            if filter_data.get("__remove__"):
                st.warning("Este filtro será removido ao salvar")

            cols = st.columns([1, 1])
            with cols[0]:
                schema_name = st.text_input(
                    "Schema",
                    value=table_info.get("schema_name", ""),
                    key=f"schema_{unique_key}",
                )
            with cols[1]:
                table_name = st.text_input(
                    "Tabela",
                    value=table_info.get("table_name", ""),
                    key=f"table_{unique_key}",
                )

            # Seleção do tipo de filtro
            new_filter_type = st.selectbox(
                "Tipo de Filtro",
                options=[ft.value for ft in FilterType],
                index=list(FilterType).index(filter_type),
                key=f"filter_type_select_{unique_key}",
                format_func=lambda x: x.replace("_", " ").title(),
            )

            column_name = st.text_input(
                "Coluna",
                value=settings.get("column_name", ""),
                key=f"column_{unique_key}",
            )

            description = st.text_input(
                "Descrição",
                value=settings.get("description", ""),
                key=f"desc_{unique_key}",
            )

            # Botão para avançar para inputs específicos
            if st.form_submit_button("Confirmar Tipo de Filtro"):
                # Atualiza os dados básicos
                st.session_state.temp_filters[index]["table_info"] = {
                    "schema_name": schema_name,
                    "table_name": table_name,
                }
                st.session_state.temp_filters[index]["settings"].update(
                    {
                        "filter_type": new_filter_type,
                        "column_name": column_name,
                        "description": description,
                    }
                )

                # Avança para etapa de inputs
                st.session_state.edit_stage[index] = "inputs"
                st.rerun()

        # Etapa 2: Inputs específicos (só aparece após confirmar tipo)
        if st.session_state.edit_stage.get(index) == "inputs":
            with st.form(key=f"inputs_form_{unique_key}"):
                st.write("Configurações específicas do filtro:")

                # Obtém os inputs atuais
                current_inputs = self.__get_filter_inputs(
                    filter_type, settings
                )

                # Renderiza inputs específicos
                new_inputs = self.__render_filter_inputs(
                    filter_type, current_inputs, unique_key=unique_key
                )

                # Botões de ação
                cols = st.columns([1, 1, 1])
                with cols[0]:
                    save_inputs = st.form_submit_button("✅ Salvar Inputs")
                with cols[1]:
                    back_to_select = st.form_submit_button("↩️ Voltar")
                with cols[2]:
                    remove_filter = st.form_submit_button(
                        "❌ Remover Filtro"
                    )

                if save_inputs:
                    # Atualiza os inputs
                    st.session_state.temp_filters[index]["settings"].update(
                        new_inputs
                    )
                    st.session_state.edit_stage[index] = "complete"
                    st.rerun()

                if back_to_select:
                    st.session_state.edit_stage[index] = "select"
                    st.rerun()

            if remove_filter:
                # Marca para remoção mantendo os dados originais
                st.session_state.temp_filters[index]["__remove__"] = True
                st.session_state.edit_stage.pop(index, None)
                st.rerun()

        # Mostrar resumo para filtros completos
        elif st.session_state.edit_stage.get(index) == "complete":
            st.json(settings)

    def render(self) -> None:
        """Exibe a interface para configuração dos filtros."""
        # Carregar configurações
        filters_settings, all_settings = self.__configure_default_settings()
        
        # Inicializar estado da sessão
        self.__initialize_session_state(filters_settings)
        
        # Renderizar cabeçalho
        self.__render_filter_header()
        
        # Botão para adicionar novo filtro
        self.__handle_add_filter()

        # Lista de filtros
        col1, _ = st.columns([0.75, 0.25])
        with col1:
            for i, filter_data in enumerate(st.session_state.temp_filters):
                if filter_data is None:
                    continue

                table_info = filter_data["table_info"]
                settings = filter_data["settings"]
                filter_type = FilterType(settings.get("filter_type", "equals"))
                unique_key = f"filter_{i}"

                with st.expander(
                    f"Filtro {i+1} - {filter_type.value.replace('_', ' ').title()}",
                    expanded=True,
                ):
                    self.__render_filter_form(i, filter_data, table_info, settings, filter_type, unique_key)

        # Botão para salvar todas as configurações
        self.__render_save_all_button(all_settings)
