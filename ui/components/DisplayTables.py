from trempy.Shared.Types import PriorityType
from typing import List, Dict
from pathlib import Path
import streamlit as st
import json


class DisplayTables:
    SETTINGS_PATH = Path("task/settings.json")

    def __configure_default_settings(self) -> tuple:
        """Carrega as configurações padrão e existentes para tabelas."""
        default_tables_settings = []

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
        current_tables_settings = all_settings.get("tables", [])
        tables_settings = current_tables_settings or default_tables_settings

        return tables_settings, all_settings

    def __save_settings(self, all_settings: dict, tables_settings: List[Dict]) -> None:
        """Salva as configurações de tabelas mantendo outras configurações intactas."""
        updated_settings = {
            **all_settings,
            "tables": tables_settings,
        }

        try:
            self.SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(self.SETTINGS_PATH, "w", encoding="utf-8") as f:
                json.dump(updated_settings, f, indent=4)
            st.success("Configurações de tabelas salvas com sucesso!")
        except Exception as e:
            st.error(f"Erro ao salvar configurações: {e}")

    def display_tables(self):
        """Exibe a interface para configuração das tabelas."""
        st.header("Configuração de Tabelas")
        st.markdown("Adicione, remova ou edite as tabelas que serão replicadas.")

        # Mapeamento de prioridades
        priority_types = list(PriorityType)
        priority_name_to_value = {p.name: p.value for p in priority_types}
        priority_value_to_name = {p.value: p.name for p in priority_types}

        # Carregar configurações
        tables_settings, all_settings = self.__configure_default_settings()

        # Inicializar/atualizar a sessão para as tabelas temporárias
        if "temp_tables" not in st.session_state:
            # Converter valores para nomes ao carregar
            st.session_state.temp_tables = [
                {
                    "schema_name": t["schema_name"],
                    "table_name": t["table_name"],
                    "priority": priority_value_to_name.get(t["priority"], "NORMAL")  # Converte valor para nome
                }
                for t in tables_settings
            ]

        # Botão para adicionar tabela (fora do formulário)
        if st.button("➕ Adicionar Tabela"):
            st.session_state.temp_tables.append(
                {"schema_name": "", "table_name": "", "priority": "NORMAL"}  # Usa nome como padrão
            )
            st.rerun()

        # Container principal para o formulário
        with st.form("tables_settings_form"):
            # Lista editável de tabelas
            tables_to_display = [
                t for t in st.session_state.temp_tables 
                if t is not None
            ]

            for i, table in enumerate(tables_to_display):
                with st.expander(f"Tabela {i+1}", expanded=True):
                    cols = st.columns([1, 1, 1, 0.5])
                    with cols[0]:
                        schema_name = st.text_input(
                            f"Nome do Schema #{i+1}",
                            value=table.get("schema_name", ""),
                            key=f"schema_{i}",
                        )
                    with cols[1]:
                        table_name = st.text_input(
                            f"Nome da Tabela #{i+1}",
                            value=table.get("table_name", ""),
                            key=f"table_{i}",
                        )
                    with cols[2]:
                        current_priority_name = table.get("priority", "NORMAL")
                        priority_name = st.selectbox(
                            f"Prioridade #{i+1}",
                            options=list(priority_name_to_value.keys()),
                            index=list(priority_name_to_value.keys()).index(current_priority_name),
                            key=f"priority_{i}",
                        )
                        # Atualiza com o nome da prioridade
                        st.session_state.temp_tables[i]["priority"] = priority_name
                    with cols[3]:
                        # Checkbox para marcar tabela para remoção
                        remove = st.checkbox("Remover", key=f"remove_{i}")

                    # Atualizar os valores temporários
                    if remove:
                        st.session_state.temp_tables[i] = None
                    else:
                        st.session_state.temp_tables[i] = {
                            "schema_name": schema_name,
                            "table_name": table_name,
                            "priority": priority_name,  # Armazena o nome
                        }

            # Botão de submit
            submitted = st.form_submit_button("Salvar Configurações", type="primary")

            if submitted:
                # Filtrar tabelas não removidas e válidas e converter nomes para valores
                valid_tables = []
                tables_id = []
                for t in st.session_state.temp_tables:
                    if t is None:
                        continue
                        
                    table_id = f"{t['schema_name']}.{t['table_name']}"
                    if t["schema_name"] and t["table_name"] and table_id not in tables_id:
                        tables_id.append(table_id)
                        valid_tables.append({
                            "schema_name": t["schema_name"],
                            "table_name": t["table_name"],
                            "priority": priority_name_to_value[t["priority"]]  # Converte nome para valor
                        })

                self.__save_settings(all_settings, valid_tables)
                # Atualiza as tabelas temporárias após salvar (convertendo valores para nomes)
                st.session_state.temp_tables = [
                    {
                        "schema_name": t["schema_name"],
                        "table_name": t["table_name"],
                        "priority": priority_value_to_name[t["priority"]]  # Converte valor para nome
                    }
                    for t in valid_tables
                ]
                st.rerun()