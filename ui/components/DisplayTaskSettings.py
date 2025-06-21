from trempy.Shared.Types import TaskType, StartType, CdcModeType
from typing import Dict, Any
from pathlib import Path
from time import sleep
import streamlit as st
import json


class DisplayTaskSettings:
    """Gerencia a interface de configuração da tarefa."""
    
    SETTINGS_PATH = Path("task/settings.json")
    
    def __init__(self):
        """Inicializa os tipos de configuração disponíveis."""
        self.task_types = [t.value for t in TaskType]
        self.start_types = [t.value for t in StartType]
        self.cdc_modes = [t.value for t in CdcModeType]

    def __load_settings(self) -> Dict[str, Any]:
        """Carrega as configurações do arquivo settings.json."""
        try:
            if self.SETTINGS_PATH.exists():
                with open(self.SETTINGS_PATH, "r", encoding="utf-8") as f:
                    settings = json.load(f)
                    loaded_settings = settings.get("task", {})
                    # Garante que todas as chaves existam mesclando com as configurações padrão
                    return {**self.__get_default_settings(), **loaded_settings}
            return self.__get_default_settings()
        except Exception as e:
            st.error(f"Erro ao carregar configurações: {str(e)}")
            return self.__get_default_settings()

    def __get_default_settings(self) -> Dict[str, Any]:
        """Retorna as configurações padrão da tarefa."""
        return {
            "task_name": "",
            "replication_type": "full_load",
            "interval_seconds": 1,
            "start_mode": "reload",
            "create_table_if_not_exists": True,
            "full_load_settings": {
                "recreate_table_if_exists": True,
                "truncate_before_insert": True,
            },
            "cdc_settings": {
                "mode": "default",
                "scd2_settings": {
                    "start_date_column_name": "scd_start_date",
                    "end_date_column_name": "scd_end_date",
                    "current_column_name": "scd_current",
                },
            },
        }

    def __save_settings(self, task_settings: Dict[str, Any]) -> bool:
        """Salva as configurações no arquivo settings.json."""
        try:
            # Carrega configurações existentes para manter outras seções
            with open(self.SETTINGS_PATH, "r", encoding="utf-8") as f:
                settings = json.load(f)
            
            # Prepara as configurações da tarefa mantendo a estrutura completa
            new_task_settings = {
                **self.__get_default_settings(),  # Garante a estrutura base
                "task_name": task_settings["task_name"],
                "replication_type": task_settings["replication_type"],
                "start_mode": task_settings["start_mode"],
                "interval_seconds": task_settings["interval_seconds"],
                "create_table_if_not_exists": task_settings["create_table_if_not_exists"],
                "full_load_settings": {
                    "recreate_table_if_exists": task_settings["recreate_table_if_exists"],
                    "truncate_before_insert": task_settings["truncate_before_insert"],
                },
                "cdc_settings": {
                    "mode": task_settings["cdc_mode"],
                    "scd2_settings": {
                        "start_date_column_name": task_settings["start_date_column_name"],
                        "end_date_column_name": task_settings["end_date_column_name"],
                        "current_column_name": task_settings["current_column_name"],
                    }
                }
            }
            
            # Atualiza apenas a seção 'task'
            settings["task"] = new_task_settings
            
            # Salva o arquivo
            with open(self.SETTINGS_PATH, "w", encoding="utf-8") as f:
                json.dump(settings, f, indent=4, ensure_ascii=False)
            
            return True
        except Exception as e:
            st.error(f"Erro ao salvar configurações: {str(e)}")
            return False

    def render(self):
        """Renderiza a interface de configurações da tarefa."""
        st.header("Configurações da Tarefa")
        st.markdown(
            """
            Configure as configurações gerais da tarefa de replicação.
            Defina o tipo de replicação (CDC ou Full Load), o comportamento do SCD2,
            e outras configurações que afetam como os dados serão processados e armazenados.
            """
        )

        # Inicializa o estado se necessário
        if "task_settings" not in st.session_state:
            st.session_state.task_settings = self.__load_settings()
        
        # Formulário de configuração
        col1, _ = st.columns([0.75, 0.25])
        with col1:
            with st.form("task_settings_form"):
                # Configurações básicas
                st.subheader("Básicas")
                task_settings = {}
                
                task_settings["task_name"] = st.text_input(
                    "Nome da Tarefa",
                    value=st.session_state.task_settings["task_name"],
                    help="Nome único para identificar esta tarefa"
                )
                
                task_settings["replication_type"] = st.selectbox(
                    "Tipo de Replicação",
                    options=self.task_types,
                    index=self.task_types.index(st.session_state.task_settings["replication_type"]),
                    help="Escolha o tipo de replicação: full_load, cdc ou ambos"
                )
                
                task_settings["start_mode"] = st.selectbox(
                    "Modo de Início",
                    options=self.start_types,
                    index=self.start_types.index(st.session_state.task_settings["start_mode"]),
                    help="Como a replicação deve começar: do zero (reload) ou continuar do último ponto"
                )
                
                task_settings["interval_seconds"] = st.number_input(
                    "Intervalo em segundos",
                    min_value=1,
                    value=st.session_state.task_settings["interval_seconds"],
                    help="Intervalo entre cada ciclo de replicação"
                )
                
                task_settings["create_table_if_not_exists"] = st.checkbox(
                    "Criar tabela se não existir",
                    value=st.session_state.task_settings["create_table_if_not_exists"],
                    help="Se marcado, cria a tabela de destino caso ela não exista"
                )

                # Configurações específicas
                subcol1, subcol2 = st.columns(2, gap="medium")
                
                with subcol1:
                    st.subheader("CDC")
                    # Garante que cdc_settings existe
                    cdc_settings = st.session_state.task_settings.get("cdc_settings", {"mode": "default"})
                    task_settings["cdc_mode"] = st.selectbox(
                        "Modo CDC",
                        options=self.cdc_modes,
                        index=self.cdc_modes.index(cdc_settings["mode"]),
                        help="Modo de captura de mudanças: padrão ou SCD2"
                    )

                    st.subheader("SCD2")
                    # Obtém as configurações padrão para usar como fallback
                    default_scd2 = self.__get_default_settings()["cdc_settings"]["scd2_settings"]
                    # Usa get() para pegar as configurações SCD2 ou um dicionário vazio se não existir
                    scd2_settings = cdc_settings.get("scd2_settings", default_scd2)
                    
                    task_settings["start_date_column_name"] = st.text_input(
                        "Coluna de data inicial",
                        value=scd2_settings.get("start_date_column_name", default_scd2["start_date_column_name"]),
                        help="Nome da coluna que armazenará a data de início do registro"
                    )
                    
                    task_settings["end_date_column_name"] = st.text_input(
                        "Coluna de data final",
                        value=scd2_settings.get("end_date_column_name", default_scd2["end_date_column_name"]),
                        help="Nome da coluna que armazenará a data de fim do registro"
                    )
                    
                    task_settings["current_column_name"] = st.text_input(
                        "Coluna de registro atual",
                        value=scd2_settings.get("current_column_name", default_scd2["current_column_name"]),
                        help="Nome da coluna que indicará se o registro é o atual"
                    )

                with subcol2:
                    st.subheader("Full Load")
                    # Garante que full_load_settings existe
                    full_load_settings = st.session_state.task_settings.get(
                        "full_load_settings",
                        self.__get_default_settings()["full_load_settings"]
                    )
                    
                    task_settings["recreate_table_if_exists"] = st.checkbox(
                        "Recriar tabela se existir",
                        value=full_load_settings.get("recreate_table_if_exists", True),
                        help="Se marcado, recria a tabela de destino mesmo se ela já existir"
                    )
                    
                    task_settings["truncate_before_insert"] = st.checkbox(
                        "Truncar antes de inserir",
                        value=full_load_settings.get("truncate_before_insert", True),
                        help="Se marcado, limpa todos os dados da tabela antes de inserir"
                    )

                # Botão de salvar
                if st.form_submit_button(
                    "Salvar Configurações",
                    type="primary",
                    help="Clique para salvar todas as alterações"
                ):
                    if self.__save_settings(task_settings):
                        st.session_state.task_settings = self.__load_settings()  # Recarrega as configurações
                        st.success("Configurações salvas com sucesso!")
                        sleep(5)
                        st.rerun()
