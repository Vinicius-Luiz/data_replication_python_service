from trempy.Shared.Types import FilterType, InputFilterType
from trempy.Shared.InputFilter import InputFilter
from typing import List, Dict
from pathlib import Path
import streamlit as st
import json


class DisplayFilter:
    SETTINGS_PATH = Path("task/settings.json")

    def __configure_default_settings(self) -> tuple:
        """Carrega as configurações padrão e existentes para filtros."""
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

    def __save_settings(self, all_settings: dict, filters_settings: List[Dict]) -> None:
        """Salva as configurações de filtros mantendo outras configurações intactas."""
        # Filtra apenas os filtros válidos (não marcados para remoção e com estrutura correta)
        valid_filters = [
            f
            for f in filters_settings
            if f is not None
            and not f.get("__remove__", False)
            and f.get("table_info")
            and f["table_info"].get("schema_name")
            and f["table_info"].get("table_name")
            and f.get("settings")
            and f["settings"].get("column_name")
        ]

        # Atualiza todas as configurações mantendo outras seções
        updated_settings = {
            **all_settings,
            "filters": valid_filters,
        }

        try:
            self.SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(self.SETTINGS_PATH, "w", encoding="utf-8") as f:
                json.dump(updated_settings, f, indent=4, ensure_ascii=False)
            st.success("Configurações de filtros salvas com sucesso!")
        except Exception as e:
            st.error(f"Erro ao salvar configurações: {e}")

    def __get_filter_inputs(
        self, filter_type: FilterType, current_settings: Dict
    ) -> Dict:
        """Retorna os inputs necessários para um tipo de filtro específico."""
        inputs = {}
        input_types = InputFilter.get_input_types(filter_type)

        for input_type in input_types:
            if input_type == InputFilterType.VALUE:
                inputs["value"] = current_settings.get("value", "")
            elif input_type == InputFilterType.VALUES:
                inputs["values"] = current_settings.get("values", [])
            elif input_type == InputFilterType.LOWER:
                inputs["lower"] = current_settings.get("lower", "")
            elif input_type == InputFilterType.UPPER:
                inputs["upper"] = current_settings.get("upper", "")

        return inputs

    def __render_filter_inputs(
        self, filter_type: FilterType, current_settings: Dict, unique_key: str
    ):
        """Renderiza os inputs específicos para o tipo de filtro."""
        input_types = InputFilter.get_input_types(filter_type)

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

    def display_filters(self):
        """Exibe a interface para configuração dos filtros."""
        st.header("Configuração de Filtros")
        st.markdown(
            "Adicione, remova ou edite os filtros que serão aplicados nas tabelas."
        )

        # Carregar configurações
        filters_settings, all_settings = self.__configure_default_settings()

        # Inicializar estado da sessão
        if "temp_filters" not in st.session_state:
            st.session_state.temp_filters = filters_settings.copy()

        if "edit_stage" not in st.session_state:
            st.session_state.edit_stage = (
                {}
            )  # {filter_idx: "select"|"inputs"|"complete"}

        # Botão para adicionar novo filtro
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

        # Lista de filtros
        col1, _ = st.columns(2)
        with col1:
            for i, filter_data in enumerate(st.session_state.temp_filters):
                if filter_data is None:
                    continue

                table_info = filter_data["table_info"]
                settings = filter_data["settings"]
                filter_type = FilterType(settings.get("filter_type", "equals"))
                unique_key = f"filter_{i}"  # Chave única para este filtro

                with st.expander(
                    f"Filtro {i+1} - {filter_type.value.replace('_', ' ').title()}",
                    expanded=True,
                ):
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
                        basic_submitted = st.form_submit_button(
                            "Confirmar Tipo de Filtro"
                        )

                        if basic_submitted:
                            # Atualiza os dados básicos
                            st.session_state.temp_filters[i]["table_info"] = {
                                "schema_name": schema_name,
                                "table_name": table_name,
                            }
                            st.session_state.temp_filters[i]["settings"].update(
                                {
                                    "filter_type": new_filter_type,
                                    "column_name": column_name,
                                    "description": description,
                                }
                            )

                            # Avança para etapa de inputs
                            st.session_state.edit_stage[i] = "inputs"
                            st.rerun()

                    # Etapa 2: Inputs específicos (só aparece após confirmar tipo)
                    if st.session_state.edit_stage.get(i) == "inputs":
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
                                st.session_state.temp_filters[i]["settings"].update(
                                    new_inputs
                                )
                                st.session_state.edit_stage[i] = "complete"
                                st.rerun()

                            if back_to_select:
                                st.session_state.edit_stage[i] = "select"
                                st.rerun()

                        if remove_filter:
                            # Marca para remoção mantendo os dados originais
                            st.session_state.temp_filters[i]["__remove__"] = True
                            st.session_state.edit_stage.pop(i, None)
                            st.rerun()

                    # Mostrar resumo para filtros completos
                    elif st.session_state.edit_stage.get(i) == "complete":
                        st.json(settings)

        # Formulário principal para salvar tudo
        with st.form("main_save_form"):
            submitted = st.form_submit_button(
                "💾 Salvar Todas Configurações", type="primary"
            )
            if submitted:
                # Filtrar apenas filtros completos e válidos, excluindo os marcados para remoção
                valid_filters = [
                    f
                    for f in st.session_state.temp_filters
                    if f is not None
                    and not f.get("__remove__", False)
                    and f.get("table_info")
                    and f["table_info"].get("schema_name")
                    and f["table_info"].get("table_name")
                    and f.get("settings")
                    and f["settings"].get("column_name")
                ]
                self.__save_settings(all_settings, valid_filters)
                st.success("Configurações salvas com sucesso!")

                # Atualizar a lista temporária removendo os filtros marcados
                st.session_state.temp_filters = [
                    f
                    for f in st.session_state.temp_filters
                    if f is not None and not f.get("__remove__", False)
                ]

                # Resetar os estágios de edição
                st.session_state.edit_stage = {
                    i: "complete"
                    for i, f in enumerate(st.session_state.temp_filters)
                    if f is not None
                }

                st.rerun()
