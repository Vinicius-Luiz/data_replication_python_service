FROM python:3.10.11-slim

WORKDIR /app

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copiar todo o código fonte primeiro
COPY . .

# Copiar arquivos template
RUN cp task/settings.template.json task/settings.json && \
    cp task/credentials.template.json task/credentials.json

# Instalar dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Criar arquivo .env e gerar chave
RUN python save_key_crypto.py

# Comando para iniciar o Streamlit
CMD ["streamlit", "run", "app.py"] 