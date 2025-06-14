from trempy.Loggings.Logging import ReplicationLogger
import streamlit as st 
import subprocess
import platform
import signal
import psutil
import sys
import os

logger = ReplicationLogger()

class ReplicationEngine:
    PYTHON_PATH = sys.executable
    
    def start(self):
        """Inicia o processo de replicação"""
        if st.session_state.process is None:
            try:
                st.session_state.process = subprocess.Popen(
                    [self.PYTHON_PATH, "manager.py"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    creationflags=(
                        subprocess.CREATE_NEW_PROCESS_GROUP
                        if platform.system() == "Windows"
                        else 0
                    ),
                )
                logger.info("UI - Replicação de dados iniciada")
            except Exception as e:
                st.error(f"Erro ao iniciar a replicação: {e}")
                logger.error(f"UI - Erro ao iniciar replicação: {e}")

    def stop(self):
        """Para o processo de replicação"""
        if st.session_state.process is not None:
            try:
                parent = psutil.Process(st.session_state.process.pid)

                if platform.system() == "Windows":
                    os.system(f"taskkill /pid {parent.pid} /f /t")
                else:
                    os.killpg(os.getpgid(parent.pid), signal.SIGINT)

                try:
                    st.session_state.process.wait(timeout=5)
                    logger.info("UI - Replicação de dados finalizada")
                except subprocess.TimeoutExpired:
                    st.error(
                        "Não foi possível parar a replicação graciosamente. Processo foi terminado."
                    )
                    logger.warning("UI - Replicação finalizada forçadamente")
                finally:
                    st.session_state.process = None

            except Exception as e:
                logger.error(f"UI - Erro ao parar replicação: {e}")
                st.session_state.process = None