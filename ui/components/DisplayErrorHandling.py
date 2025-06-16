from pathlib import Path
import streamlit as st
import json


class DisplayErrorHandling:
    SETTINGS_PATH = Path("task/settings.json")

    def __configure_default_settings(self):
        default_error_settings = {
            "stop_if_insert_error": False,
            "stop_if_update_error": False,
            "stop_if_delete_error": False,
            "stop_if_upsert_error": False,
            "stop_if_scd2_error": False,
        }

        # Load existing settings (if they exist)
        all_settings = {}
        if self.SETTINGS_PATH.exists():
            try:
                with open(self.SETTINGS_PATH, "r") as f:
                    all_settings = json.load(f)
            except Exception as e:
                st.warning(
                    f"Não foi possível carregar as configurações existentes: {e}"
                )

        # Merge defaults with existing settings (only for 'error_handling')
        current_error_settings = all_settings.get("error_handling", {})
        error_settings = {**default_error_settings, **current_error_settings}

        return error_settings, all_settings

    def __save_settings(self, all_settings: dict, error_settings: dict) -> None:
        new_error_settings = {
            "stop_if_insert_error": error_settings.get("stop_if_insert_error", False),
            "stop_if_update_error": error_settings.get("stop_if_update_error", False),
            "stop_if_delete_error": error_settings.get("stop_if_delete_error", False),
            "stop_if_upsert_error": error_settings.get("stop_if_upsert_error", False),
            "stop_if_scd2_error": error_settings.get("stop_if_scd2_error", False),
        }

        # Update only the 'error_handling' key while keeping others intact
        updated_settings = {
            **all_settings,
            "error_handling": new_error_settings,
        }

        # Save to file
        try:
            self.SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(self.SETTINGS_PATH, "w") as f:
                json.dump(updated_settings, f, indent=4)
            st.success("Configurações de tratamento de erro salvas com sucesso!")
        except Exception as e:
            st.error(f"Erro ao salvar configurações: {e}")

        st.json(updated_settings)

    def display_error_settings(self):
        """Exibe configurações de tratamento de erro, mantendo outras chaves do settings.json intactas"""
        st.header("Configurações de Tratamento de Erro")

        error_settings, all_settings = self.__configure_default_settings()

        with st.form("error_settings_form"):
            
            col1, col2, _ = st.columns(3, gap="medium")
            
            with col1:
                st.subheader("Operações DML")
                stop_if_insert_error = st.checkbox(
                    "Parar se ocorrer erro no INSERT",
                    value=error_settings.get("stop_if_insert_error", False),
                )
                stop_if_update_error = st.checkbox(
                    "Parar se ocorrer erro no UPDATE",
                    value=error_settings.get("stop_if_update_error", False),
                )
                stop_if_delete_error = st.checkbox(
                    "Parar se ocorrer erro no DELETE",
                    value=error_settings.get("stop_if_delete_error", False),
                )
            
            with col2:
                st.subheader("Modos de CDC")
                stop_if_upsert_error = st.checkbox(
                    "Parar se ocorrer erro no UPSERT",
                    value=error_settings.get("stop_if_upsert_error", False),
                )
                stop_if_scd2_error = st.checkbox(
                    "Parar se ocorrer erro no SCD2",
                    value=error_settings.get("stop_if_scd2_error", False),
                )

            # Submit button
            submitted = st.form_submit_button("Salvar Configurações", type="primary")

            if submitted:
                error_settings = {
                    "stop_if_insert_error": stop_if_insert_error,
                    "stop_if_update_error": stop_if_update_error,
                    "stop_if_delete_error": stop_if_delete_error,
                    "stop_if_upsert_error": stop_if_upsert_error,
                    "stop_if_scd2_error": stop_if_scd2_error,
                }
                self.__save_settings(all_settings, error_settings)