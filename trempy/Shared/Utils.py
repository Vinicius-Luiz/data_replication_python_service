from typing import TYPE_CHECKING
import pickle
import uuid
import os
import json


if TYPE_CHECKING:
    from trempy.Tasks.Task import Task


class Utils:
    """
    Classe utilitária para ajudar na configuração e execução das tarefas.
    """

    @staticmethod
    def write_task_pickle(task: "Task") -> None:
        """
        Salva a configuração da tarefa no arquivo "settings.pickle".
        """

        with open("task/settings.pickle", "wb") as f:
            pickle.dump(task, f)

    @staticmethod
    def read_task_pickle() -> "Task":
        """
        Carrega a configura o da tarefa salva no arquivo "settings.pickle" e
        retorna um objeto Task.
        """

        with open("task/settings.pickle", "rb") as f:
            task: Task = pickle.load(f)
        return task

    @staticmethod
    def hash_6_chars() -> str:
        """
        Gera um hash MD5 a partir do texto passado e retorna apenas
        os 6 primeiros caracteres do hash em hexadecimal.
        """

        return uuid.uuid4().hex[:6].lower()
    
    from time import time

    @staticmethod
    def format_time_elapsed(seconds_float: float) -> str:
        """Formata um tempo em segundos (float) para HH:MM:SS (string)."""
        seconds = int(round(seconds_float))
        hours = seconds // 3600
        remaining_seconds = seconds % 3600
        minutes = remaining_seconds // 60
        seconds_remaining = remaining_seconds % 60
        return f"{hours:02d}:{minutes:02d}:{seconds_remaining:02d}"

    @staticmethod
    def read_credentials() -> dict:
        """
        Lê as credenciais do arquivo credentials.json.
        
        Returns:
            dict: Dicionário com as credenciais dos endpoints
        """
        from trempy.Shared.Crypto import CredentialsCrypto
        
        credentials_path = os.path.join("task", "credentials.json")
        try:
            with open(credentials_path, "r", encoding="utf-8") as f:
                credentials = json.load(f)
                
            # Descriptografa as senhas
            if credentials.get("source_endpoint"):
                source_creds = credentials["source_endpoint"].get("credentials", {})
                if source_creds and source_creds.get("password"):
                    source_creds["password"] = CredentialsCrypto.decrypt(source_creds["password"])
                    
            if credentials.get("target_endpoint"):
                target_creds = credentials["target_endpoint"].get("credentials", {})
                if target_creds and target_creds.get("password"):
                    target_creds["password"] = CredentialsCrypto.decrypt(target_creds["password"])
                    
            return credentials
        except FileNotFoundError:
            raise FileNotFoundError(f"Arquivo de credenciais não encontrado: {credentials_path}")
        except json.JSONDecodeError:
            raise ValueError(f"Arquivo de credenciais inválido: {credentials_path}")
        except Exception as e:
            raise Exception(f"Erro ao ler arquivo de credenciais: {str(e)}")
            
    @staticmethod
    def save_credentials(credentials: dict) -> None:
        """
        Salva as credenciais no arquivo credentials.json com senhas criptografadas.
        
        Args:
            credentials: Dicionário com as credenciais dos endpoints
        """
        from trempy.Shared.Crypto import CredentialsCrypto
        
        credentials_path = os.path.join("task", "credentials.json")
        try:
            # Faz uma cópia para não modificar o original
            encrypted_credentials = json.loads(json.dumps(credentials))
            
            # Criptografa as senhas
            if encrypted_credentials.get("source_endpoint"):
                source_creds = encrypted_credentials["source_endpoint"].get("credentials", {})
                if source_creds and source_creds.get("password"):
                    source_creds["password"] = CredentialsCrypto.encrypt(source_creds["password"])
                    
            if encrypted_credentials.get("target_endpoint"):
                target_creds = encrypted_credentials["target_endpoint"].get("credentials", {})
                if target_creds and target_creds.get("password"):
                    target_creds["password"] = CredentialsCrypto.encrypt(target_creds["password"])
            
            # Garante que o diretório pai existe
            os.makedirs(os.path.dirname(credentials_path), exist_ok=True)
            
            # Salva as configurações
            with open(credentials_path, "w", encoding="utf-8") as f:
                json.dump(encrypted_credentials, f, indent=4)
                
        except Exception as e:
            raise Exception(f"Erro ao salvar arquivo de credenciais: {str(e)}")
