from trempy.Shared.Types import TaskType, StartType, CdcModeType
from pathlib import Path
import streamlit as st
import json


class DisplayTaskSettings:
    SETTINGS_PATH = Path("task/settings.json")

    def __configure_default_settings(self):
        default_task_settings = {
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

        # Carregar configurações existentes (se existirem)
        all_settings = {}
        if self.SETTINGS_PATH.exists():
            try:
                with open(self.SETTINGS_PATH, "r") as f:
                    all_settings = json.load(f)
            except Exception as e:
                st.warning(
                    f"Não foi possível carregar as configurações existentes: {e}"
                )

        # Mesclar defaults com configurações existentes (apenas para 'task')
        current_task_settings = all_settings.get("task", {})
        task_settings = {**default_task_settings, **current_task_settings}

        return task_settings, all_settings

    def __save_settings(self, all_settings: dict, task_settings: dict) -> None:
        new_task_settings = {
            "task_name": task_settings.get("task_name"),
            "replication_type": task_settings.get("replication_type"),
            "start_mode": task_settings.get("start_mode"),
            "interval_seconds": task_settings.get("interval_seconds"),
            "create_table_if_not_exists": task_settings.get(
                "create_table_if_not_exists"
            ),
            "full_load_settings": {
                "recreate_table_if_exists": task_settings.get(
                    "recreate_table_if_exists"
                ),
                "truncate_before_insert": task_settings.get("truncate_before_insert"),
            },
        }

        # Adicionar campos condicionais
        if task_settings.get("replication_type") in ["cdc", "full_load_and_cdc"]:

            # Configurações de CDC
            cdc_settings = {"mode": task_settings.get("cdc_mode")}

            if task_settings.get("cdc_mode") == "scd2":
                # Configurações específicas do SCD2
                cdc_settings["scd2_settings"] = {
                    "start_date_column_name": task_settings.get(
                        "start_date_column_name"
                    ),
                    "end_date_column_name": task_settings.get("end_date_column_name"),
                    "current_column_name": task_settings.get("current_column_name"),
                }

            new_task_settings["cdc_settings"] = cdc_settings

        # Atualizar apenas a chave 'task' mantendo as outras intactas
        updated_settings = {
            **all_settings,
            "task": new_task_settings,
        }

        # Salvar no arquivo
        try:
            self.SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(self.SETTINGS_PATH, "w") as f:
                json.dump(updated_settings, f, indent=4)
            st.success("Configurações salvas com sucesso!")
        except Exception as e:
            st.error(f"Erro ao salvar configurações: {e}")

        st.json(updated_settings)

    def display_task_settings(self):
        """Exibe configurações da tarefa, mantendo outras chaves do settings.json intactas"""
        st.header("Configurações da Tarefa")

        task_types = list(map(lambda task: task.value, TaskType))
        start_types = list(map(lambda start: start.value, StartType))
        cdc_modes = list(map(lambda cdc: cdc.value, CdcModeType))

        task_settings, all_settings = self.__configure_default_settings()

        with st.form("task_settings_form"):
            # Seção básica
            col1, _ = st.columns([0.5, 0.5])
            with col1:
                st.subheader("Básicas")
                task_name = st.text_input(
                    "Nome da Tarefa", value=task_settings.get("task_name", "")
                )
                replication_type = st.selectbox(
                    "Tipo de Replicação",
                    options=task_types,
                    index=task_types.index(
                        task_settings.get("replication_type", "full_load")
                    ),
                )
                start_mode = st.selectbox(
                    "Modo de Início",
                    options=start_types,
                    index=start_types.index(task_settings.get("start_mode", "reload")),
                )
                interval_seconds = st.number_input(
                    "Intervalo em segundos",
                    min_value=1,
                    value=task_settings.get("interval_seconds", 1),
                )
                create_table_if_not_exists = st.checkbox(
                    "Criar tabela se não existir",
                    value=task_settings.get("create_table_if_not_exists", True),
                )

                subcol1, subcol2 = st.columns(2, gap="medium")
                with subcol1:
                    st.subheader("CDC")
                    cdc_mode = st.selectbox(
                        "Modo CDC",
                        options=cdc_modes,
                        index=cdc_modes.index(
                            task_settings["cdc_settings"].get("mode", "default")
                        ),
                    )

                    st.subheader("SCD2 - Slowly Changing Dimension Type 2")
                    scd2_defaults = task_settings["cdc_settings"].get(
                        "scd2_settings", {}
                    )
                    start_date_column_name = st.text_input(
                        "Nome da coluna de data de início",
                        value=scd2_defaults.get(
                            "start_date_column_name", "scd_start_date"
                        ),
                    )
                    end_date_column_name = st.text_input(
                        "Nome da coluna de data de fim",
                        value=scd2_defaults.get("end_date_column_name", "scd_end_date"),
                    )
                    current_column_name = st.text_input(
                        "Nome da coluna de indicador atual",
                        value=scd2_defaults.get("current_column_name", "scd_current"),
                    )

                with subcol2:
                    st.subheader("Full Load")
                    recreate_table_if_exists = st.checkbox(
                        "Recriar tabela se existir",
                        value=task_settings["full_load_settings"].get(
                            "recreate_table_if_exists", True
                        ),
                    )
                    truncate_before_insert = st.checkbox(
                        "Truncar antes de inserir",
                        value=task_settings["full_load_settings"].get(
                            "truncate_before_insert", True
                        ),
                    )

            # Botão de submit
            submitted = st.form_submit_button("Salvar Configurações", type="primary")

            if submitted:
                task_settings = {
                    "task_name": task_name,
                    "replication_type": replication_type,
                    "start_mode": start_mode,
                    "interval_seconds": interval_seconds,
                    "create_table_if_not_exists": create_table_if_not_exists,
                    "recreate_table_if_exists": recreate_table_if_exists,
                    "truncate_before_insert": truncate_before_insert,
                    "cdc_mode": cdc_mode,
                    "start_date_column_name": start_date_column_name,
                    "end_date_column_name": end_date_column_name,
                    "current_column_name": current_column_name,
                }
                self.__save_settings(all_settings, task_settings)
