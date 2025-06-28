from trempy.Loggings.Logging import ReplicationLogger
import streamlit as st 
import subprocess
import platform
import signal
import psutil
import json
import sys
import os
from datetime import datetime

logger = ReplicationLogger()

class ReplicationEngine:
    PYTHON_PATH = sys.executable
    STATE_FILE = "replication_state.json"
    
    def __init__(self):
        """Inicializa o engine e carrega o estado atual"""
        self.load_state()
    
    def save_state(self, pid=None, status="stopped"):
        """Salva o estado atual do processo em um arquivo JSON"""
        state = {
            "pid": pid,
            "status": status,
            "last_update": datetime.now().isoformat()
        }
        with open(self.STATE_FILE, "w") as f:
            json.dump(state, f)
    
    def load_state(self):
        """Carrega o estado do processo do arquivo JSON"""
        try:
            with open(self.STATE_FILE, "r") as f:
                state = json.load(f)
                
            # Verifica se o processo ainda está rodando
            if state.get("pid"):
                try:
                    process = psutil.Process(state["pid"])
                    if not process.is_running():
                        self.save_state()  # Processo não está mais rodando
                        return None
                    return state
                except psutil.NoSuchProcess:
                    self.save_state()  # Processo não existe mais
                    return None
            return state
        except (FileNotFoundError, json.JSONDecodeError):
            self.save_state()  # Cria um novo arquivo de estado
            return None
    
    def is_running(self):
        """Verifica se o processo de replicação está rodando"""
        state = self.load_state()
        return state is not None and state.get("status") == "running" and state.get("pid") is not None
    
    def start(self):
        """Inicia o processo de replicação"""
        if not self.is_running():
            try:
                process = subprocess.Popen(
                    [self.PYTHON_PATH, "manager.py"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    creationflags=(
                        subprocess.CREATE_NEW_PROCESS_GROUP
                        if platform.system() == "Windows"
                        else 0
                    ),
                )
                
                self.save_state(pid=process.pid, status="running")
                logger.info("UI - Replicação de dados iniciada")
            except Exception as e:
                st.error(f"Erro ao iniciar a replicação: {e}")
                self.save_state()

    def stop(self):
        """Para o processo de replicação"""
        if self.is_running():
            try:
                state = self.load_state()
                if state is None:
                    return
                    
                parent = psutil.Process(state["pid"])

                if platform.system() == "Windows":
                    os.system(f"taskkill /pid {parent.pid} /f /t")
                else:
                    os.killpg(os.getpgid(parent.pid), signal.SIGINT)

                try:
                    process = psutil.Process(state["pid"])
                    process.wait(timeout=5)
                    logger.info("UI - Replicação de dados finalizada")
                except (psutil.NoSuchProcess, subprocess.TimeoutExpired) as e:
                    # No Windows, o processo já foi finalizado pelo taskkill
                    if isinstance(e, psutil.NoSuchProcess) and platform.system() == "Windows":
                        logger.info("UI - Replicação de dados finalizada com sucesso")
                    else:
                        st.error(
                            "Não foi possível parar a replicação graciosamente. Processo foi terminado."
                        )
                        logger.warning("UI - Replicação finalizada forçadamente")
                finally:
                    self.save_state()  # Reseta o estado para parado

            except Exception as e:
                st.error(f"Erro ao parar a replicação: {e}")
                self.save_state()  # Garante que o estado seja resetado mesmo em caso de erro