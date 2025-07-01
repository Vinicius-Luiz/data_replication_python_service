# Manual de Uso do Docker

## Pré-requisitos

- Docker instalado
- Docker Compose instalado

## Comandos Básicos

### Construção e Inicialização

```bash
# Construir/Reconstruir a imagem
docker-compose build streamlit

# Iniciar os serviços (primeira vez ou após parar)
docker-compose up -d

# Reconstruir e iniciar (comando único)
docker-compose up -d --build
```

### Gerenciamento de Containers

```bash
# Verificar status dos containers
docker-compose ps

# Parar containers (mantendo dados)
docker-compose stop

# Remover containers
docker-compose down

# Remover containers e volumes (limpeza completa)
docker-compose down -v

# Reiniciar um serviço específico
docker-compose restart rabbitmq
# ou
docker-compose restart streamlit
```

> **Nota sobre Limpeza**: 
> - `docker-compose down`: Remove containers e redes, mantém volumes
> - `docker-compose down -v`: Remove containers, redes E volumes (útil para "começar do zero")
> - Use `-v` quando precisar limpar completamente os dados do RabbitMQ ou outros volumes persistentes

### Pausar e Retomar

```bash
# Pausar todos os containers (mantém estado em memória)
docker-compose pause

# Pausar serviço específico
docker-compose pause rabbitmq
# ou
docker-compose pause streamlit

# Retomar containers pausados
docker-compose unpause

# Retomar serviço específico
docker-compose unpause rabbitmq
# ou
docker-compose unpause streamlit
```

> **Nota sobre Pause vs Stop**:
> - `pause`: congela o estado em memória (para pausas curtas)
> - `stop`: salva o estado em disco (para pausas longas)

### Logs e Monitoramento

```bash
# Ver logs de todos os serviços
docker-compose logs

# Ver logs de um serviço específico
docker-compose logs rabbitmq
# ou
docker-compose logs streamlit

# Acompanhar logs em tempo real
docker-compose logs -f
```

## Executando Múltiplas Instâncias

Para executar várias instâncias simultaneamente:

```bash
# Primeira instância (portas padrão)
docker-compose up -d

# Segunda instância
CONTAINER_NAME=replication2 STREAMLIT_PORT=8502 docker-compose up -d

# Terceira instância
CONTAINER_NAME=replication3 STREAMLIT_PORT=8503 docker-compose up -d
```

## Acessando os Serviços

- Streamlit: http://localhost:${STREAMLIT_PORT:-8501}
- RabbitMQ: http://localhost:15672 (usuário/senha: guest/guest)

## Volumes e Persistência

Os dados são persistidos através de volumes Docker. O código fonte é montado diretamente do host para o container, permitindo desenvolvimento em tempo real. 

## Inspecionando Containers

### Visualizar Arquivos e Diretórios

```bash
# Listar arquivos em um diretório específico do container
docker exec ${CONTAINER_NAME:-data_replication}_streamlit ls -la /app

# Visualizar conteúdo de um arquivo
docker exec ${CONTAINER_NAME:-data_replication}_streamlit cat /app/.env

# Abrir um shell interativo no container
docker exec -it ${CONTAINER_NAME:-data_replication}_streamlit bash

# Copiar arquivo do container para sua máquina
docker cp ${CONTAINER_NAME:-data_replication}_streamlit:/app/.env ./env_backup
```

> **Dica**: Use o nome correto do container ou substitua `${CONTAINER_NAME:-data_replication}` pelo nome que você definiu. 

### Executar Comandos Python

```bash
# Executar um script Python
docker exec data_replication_streamlit python manager.py

> **Dica**: Se você definiu um CONTAINER_NAME personalizado, use:
> ```bash
> docker exec ${CONTAINER_NAME:-data_replication}_streamlit python manager.py
> ``` 