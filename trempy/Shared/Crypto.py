from cryptography.fernet import Fernet
from pathlib import Path
import base64
import os


class CredentialsCrypto:
    """
    Classe responsável por criptografar e descriptografar credenciais.
    Usa a biblioteca cryptography com Fernet (criptografia simétrica).
    """
    
    KEY_ENV_VAR = "TREMPY_CRYPTO_KEY"
    
    @classmethod
    def generate_key(cls) -> bytes:
        """
        Gera uma nova chave de criptografia.
        
        Returns:
            bytes: Nova chave de criptografia
        """
        return Fernet.generate_key()
    
    @classmethod
    def get_crypto_key(cls) -> bytes:
        """
        Obtém a chave de criptografia do ambiente.
        Se não existir, gera uma nova.
        
        Returns:
            bytes: Chave de criptografia
        """
        key = os.getenv(cls.KEY_ENV_VAR)
        if not key:
            raise ValueError(
                f"Chave de criptografia não encontrada. "
                f"Defina a variável de ambiente {cls.KEY_ENV_VAR}"
            )
        return base64.b64decode(key)
    
    @classmethod
    def encrypt(cls, value: str) -> str:
        """
        Criptografa um valor.
        
        Args:
            value: Valor a ser criptografado
            
        Returns:
            str: Valor criptografado em base64
        """
        if not value or value.strip() == "":
            return value
            
        key = cls.get_crypto_key()
        f = Fernet(key)
        encrypted = f.encrypt(value.encode())
        return base64.b64encode(encrypted).decode()
    
    @classmethod
    def decrypt(cls, encrypted_value: str) -> str:
        """
        Descriptografa um valor.
        
        Args:
            encrypted_value: Valor criptografado em base64
            
        Returns:
            str: Valor descriptografado
        """
        if not encrypted_value or encrypted_value.strip() == "":
            return encrypted_value
            
        try:
            key = cls.get_crypto_key()
            f = Fernet(key)
            decrypted = f.decrypt(base64.b64decode(encrypted_value))
            return decrypted.decode()
        except Exception as e:
            raise ValueError(f"Erro ao descriptografar valor: {str(e)}") 