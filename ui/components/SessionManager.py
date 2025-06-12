import streamlit as st

class SessionManager:
    def __init__(self):
        self.__initialize_session_state()

    def __initialize_session_state(self):
        """Inicializa o estado da sessão"""
        if "process" not in st.session_state:
            st.session_state.process = None
