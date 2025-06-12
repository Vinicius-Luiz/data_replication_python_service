from ui.components.SessionManager import SessionManager
from ui.components.UIComponents import UIComponents
import streamlit as st


class ReplicationController:
    def __init__(self):
        st.set_page_config(layout="wide", initial_sidebar_state="expanded")
        self.session_manager = SessionManager()
        self.ui_components = UIComponents()
        
    def display_tabs(self):
        
        st.title("🚂 TREMpy - Transactional Replication Engine for Multi-databases")
        
        tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(
            [
                "Página Inicial",
                "Conexões",
                "Configurações da Tarefa",
                "Tabelas",
                "Filtros",
                "Transformações",
                "Error Handling",
            ]
        )

        with tab1:
            self.ui_components.display_home_page()

        with tab2:
            self.ui_components.display_connections()

        with tab3:
            self.ui_components.display_task_settings()

        with tab4:
            self.ui_components.display_tables()

        with tab5:
            self.ui_components.display_filters()

        with tab6:
            self.ui_components.display_transformations()

        with tab7:
            self.ui_components.display_error_handling()
