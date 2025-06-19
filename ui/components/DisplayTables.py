from trempy.Shared.Types import PriorityType
from typing import List, Dict, Any
from pathlib import Path
from time import sleep
import streamlit as st
import json


class DisplayTables:
    """Gerencia a interface de configuração de tabelas."""

    SETTINGS_PATH = Path("task/settings.json")

    def __init__(self):
        """Inicializa o mapeamento de prioridades."""
        self.priority_types = list(PriorityType)
        self.priority_name_to_value = {p.name: p.value for p in self.priority_types}
        self.priority_value_to_name = {p.value: p.name for p in self.priority_types}

    def __load_settings(self) -> List[Dict[str, Any]]:
        """Carrega as configurações do arquivo settings.json."""
        try:
            if self.SETTINGS_PATH.exists():
                with open(self.SETTINGS_PATH, "r", encoding="utf-8") as f:
                    settings = json.load(f)
                    return settings.get("tables", [])
            return []
        except Exception as e:
            st.error(f"Erro ao carregar configurações: {str(e)}")
            return []

    def __save_settings(self, tables: List[Dict[str, Any]]) -> bool:
        """Salva as configurações no arquivo settings.json."""
        try:
            # Carrega configurações existentes para manter outras seções
            with open(self.SETTINGS_PATH, "r", encoding="utf-8") as f:
                settings = json.load(f)

            # Atualiza apenas a seção 'tables'
            settings["tables"] = tables

            # Salva o arquivo
            with open(self.SETTINGS_PATH, "w", encoding="utf-8") as f:
                json.dump(settings, f, indent=4, ensure_ascii=False)

            return True
        except Exception as e:
            st.error(f"Erro ao salvar configurações: {str(e)}")
            return False

    def display_tables(self):
        """Exibe a interface de configuração de tabelas."""
        st.header("Configuração de Tabelas")

        # Inicializa o estado se necessário
        if "tables" not in st.session_state:
            st.session_state.tables = self.__load_settings()

        # Botão para adicionar tabela
        if st.button(
            "➕ Adicionar Tabela",
            help="Clique para adicionar uma nova tabela à configuração",
        ):
            st.session_state.tables.append(
                {
                    "schema_name": "",
                    "table_name": "",
                    "priority": self.priority_name_to_value["NORMAL"],
                }
            )

        # Lista de tabelas a serem exibidas
        tables_to_display = []

        # Formulário de edição
        with st.form("tables_form"):
            for i, table in enumerate(st.session_state.tables):
                with st.expander(f"Tabela {i+1}", expanded=True):
                    cols = st.columns([1, 1, 1, 0.5])

                    # Cria uma cópia da tabela para edição
                    edited_table = table.copy()

                    with cols[0]:
                        edited_table["schema_name"] = st.text_input(
                            "Schema",
                            value=table["schema_name"],
                            key=f"schema_{i}",
                            help="Nome do schema no banco de dados (ex: public, employees, etc)",
                        )

                    with cols[1]:
                        edited_table["table_name"] = st.text_input(
                            "Tabela",
                            value=table["table_name"],
                            key=f"table_{i}",
                            help="Nome da tabela no banco de dados (ex: employee, department, etc)",
                        )

                    with cols[2]:
                        priority_name = self.priority_value_to_name.get(
                            table["priority"], "NORMAL"
                        )
                        selected_priority = st.selectbox(
                            "Prioridade",
                            options=list(self.priority_name_to_value.keys()),
                            index=list(self.priority_name_to_value.keys()).index(
                                priority_name
                            ),
                            key=f"priority_{i}",
                            help=(
                                "Define a ordem de processamento das tabelas:\n"
                                "- HIGHEST: Processada primeiro\n"
                                "- HIGH: Alta prioridade\n"
                                "- NORMAL: Prioridade padrão\n"
                                "- LOW: Baixa prioridade\n"
                                "- LOWEST: Processada por último"
                            ),
                        )
                        edited_table["priority"] = self.priority_name_to_value[
                            selected_priority
                        ]

                    with cols[3]:
                        if not st.checkbox(
                            "Remover",
                            key=f"remove_{i}",
                            help="Marque esta opção para remover a tabela da configuração",
                        ):
                            tables_to_display.append(edited_table)

            # Botão de salvar
            if st.form_submit_button(
                "Salvar Configurações",
                type="primary",
                help="Clique para salvar todas as alterações feitas nas tabelas",
            ):
                st.session_state.tables = tables_to_display
                if self.__save_settings(tables_to_display):
                    st.success("Configurações salvas com sucesso!")
                    sleep(5)
                    st.rerun()
