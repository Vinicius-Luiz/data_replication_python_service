from trempy.Shared.Crypto import CredentialsCrypto
import base64
from pathlib import Path
# Gera a chave de criptografia
key = CredentialsCrypto.generate_key()
env_value = f"TREMPY_CRYPTO_KEY={base64.b64encode(key).decode()}"

# Define o caminho do arquivo .env
env_path = Path(".env")

# Se o arquivo existir, lê o conteúdo atual
if env_path.exists():
    with open(env_path, "r", encoding="utf-8") as f:
        env_lines = f.readlines()
else:
    env_lines = []

# Remove a linha antiga se existir
env_lines = [line for line in env_lines if not line.startswith("TREMPY_CRYPTO_KEY=")]

# Adiciona a nova linha
env_lines.append(env_value + "\n")

# Salva o arquivo
with open(env_path, "w", encoding="utf-8") as f:
    f.writelines(env_lines)

print(f"Chave de criptografia gerada e salva em {env_path}:")
print(env_value)