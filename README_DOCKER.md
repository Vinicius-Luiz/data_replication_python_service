# Manual de Uso do Docker

## Pré-requisitos

- Docker instalado
- Docker Compose instalado

## Executando Múltiplas Instâncias

Para rodar múltiplas instâncias do serviço de replicação, utilize SEMPRE o parâmetro `-p <nome_do_projeto>` para isolar cada instância. Assim, cada conjunto de containers, redes e recursos será independente.

### Construção e Inicialização

> **Dica:** Sempre utilize o parâmetro `-p` para build e up, mesmo na primeira vez.

```bash
# Construir/Reconstruir a imagem para um projeto específico
# (Necessário apenas se você alterou o código ou o Dockerfile)
docker-compose -p replication1 build streamlit

docker-compose -p replication2 build streamlit
# ... para cada projeto/instância

# Iniciar os serviços (primeira vez ou após parar)
docker-compose -p replication1 up -d
CONTAINER_NAME=replication2 STREAMLIT_PORT=8502 DEEPSEEK_API_KEY=sua_chave_aqui docker-compose -p replication2 up -d
CONTAINER_NAME=replication3 STREAMLIT_PORT=8503 DEEPSEEK_API_KEY=sua_chave_aqui docker-compose -p replication3 up -d

# Reconstruir e iniciar (comando único)
docker-compose -p replication1 up -d --build
```

> **Importante:**
> - O parâmetro `-p <nome_do_projeto>` garante que cada instância seja totalmente isolada das demais, mesmo usando o mesmo arquivo `docker-compose.yml` e no mesmo diretório.
> - Não é necessário duplicar arquivos ou pastas para rodar múltiplas instâncias.
> - Para parar/remover uma instância específica, use `docker-compose -p <nome_do_projeto> down`.

## Gerenciamento de Containers e Projetos

### Listar todos os containers ativos
```bash
docker ps -a
```

### Verificar status dos containers de um projeto
```bash
docker-compose -p <nome_do_projeto> ps
```

### Parar containers de um projeto (mantendo dados)
```bash
docker-compose -p <nome_do_projeto> stop
```

### Remover containers de um projeto
```bash
docker-compose -p <nome_do_projeto> down
```

### Remover containers e redes de todos os projetos (cuidado!)
```bash
docker rm -f $(docker ps -aq)
```

### Reiniciar um serviço específico de um projeto
```bash
docker-compose -p <nome_do_projeto> restart rabbitmq
# ou
docker-compose -p <nome_do_projeto> restart streamlit
```

### Pausar e Retomar
```bash
# Pausar todos os containers de um projeto
docker-compose -p <nome_do_projeto> pause

# Pausar serviço específico
docker-compose -p <nome_do_projeto> pause rabbitmq
# ou
docker-compose -p <nome_do_projeto> pause streamlit

# Retomar containers pausados
docker-compose -p <nome_do_projeto> unpause

# Retomar serviço específico
docker-compose -p <nome_do_projeto> unpause rabbitmq
# ou
docker-compose -p <nome_do_projeto> unpause streamlit
```

### Logs e Monitoramento
```bash
# Ver logs de todos os serviços de um projeto
docker-compose -p <nome_do_projeto> logs

# Ver logs de um serviço específico
docker-compose -p <nome_do_projeto> logs rabbitmq
# ou
docker-compose -p <nome_do_projeto> logs streamlit

# Acompanhar logs em tempo real
docker-compose -p <nome_do_projeto> logs -f
```

## Acessando os Serviços

- Streamlit: http://localhost:${STREAMLIT_PORT:-8501}
- RabbitMQ: http://localhost:15672 (usuário/senha: guest/guest)

## Volumes e Persistência

- **Removido:** Esta aplicação não utiliza volumes Docker para persistência de dados.
- O código fonte é montado diretamente do host para o container, permitindo desenvolvimento em tempo real.

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
docker exec ${CONTAINER_NAME:-data_replication}_streamlit python manager.py
```

> **Dica**: Se você definiu um CONTAINER_NAME personalizado, use:
> ```bash
> docker exec ${CONTAINER_NAME:-data_replication}_streamlit python manager.py
> ``` 