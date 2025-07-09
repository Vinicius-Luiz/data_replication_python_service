# TREMpy

**Transactional Replication Engine Made in Python**

## Descrição
O TREMpy é um motor de replicação transacional desenvolvido em Python, projetado para facilitar a replicação de dados entre bancos PostgreSQL utilizando técnicas modernas de captura de dados de alteração (CDC) e integração com mensageria via RabbitMQ. Com uma interface intuitiva baseada em Streamlit e suporte a orquestração via Docker, o TREMpy oferece uma solução flexível e extensível para cenários de replicação, monitoramento e automação de fluxos de dados.

## Tabela de Conteúdos
- [Descrição](#descrição)
- [Instalação](#instalação)
- [Configuração](#configuração)
  - [Configuração do PostgreSQL](#configuração-do-postgresql)
  - [Configuração do RabbitMQ](#configuração-do-rabbitmq)
  - [Configuração do Docker](#configuração-do-docker)
  - [Configuração do Streamlit](#configuração-do-streamlit)
- [Como Usar](#como-usar)
- [Uso do PostgreSQL](#uso-do-postgresql)
- [Uso do Streamlit](#uso-do-streamlit)
- [Uso do Docker](#uso-do-docker)
- [Uso de IA (Deepseek API)](#uso-de-ia-deepseek-api)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Licença](#licença)
- [Contato](#contato)

---

## Instalação

Antes de começar, certifique-se de que os seguintes pré-requisitos estão instalados no seu ambiente:

- **Python 3.10 ou superior**
- **Docker** (caso deseje executar o projeto em containers)
- **Docker Compose** (para orquestração dos containers)

## Configuração
Como preparar o ambiente para que o projeto funcione corretamente.

### Configuração do PostgreSQL

Para utilizar o PostgreSQL com o TREMpy, é necessário realizar as seguintes configurações:

Configuração em `postgresql.conf`:
```
wal_level = logical
max_replication_slots = 5 # recomendado > 20
max_wal_senders = 5       # recomendado = max_replication_slots

# Para conexões via Docker
listen_addresses = '*'     # permite conexões de qualquer interface
```

Configuração em `pg_hba.conf`:
```
# Pré-requisito obrigatório para replicação, tanto usando Docker quanto sem Docker:
host replication all 0.0.0.0/0 trust
```

#### Configuração de Rede para Docker

Para garantir que o PostgreSQL aceite conexões do container Docker, adicione as seguintes entradas ao `pg_hba.conf`:

```
# Projeto Python Replicate
host replication all 0.0.0.0/0 trust
host all all 172.17.0.0/16 md5   # rede do Docker
host all all 172.26.64.0/20 md5  # Rede do WSL
host all all 192.168.1.0/24 md5  # Sua rede Wi-Fi
```

Ao configurar a conexão na interface do Streamlit, use o IP do WSL (`172.26.64.1`) como host quando estiver rodando a aplicação via Docker. Este é o IP que o container Docker usará para se comunicar com seu PostgreSQL local.

### Integração com RabbitMQ

O RabbitMQ é utilizado como sistema de mensageria para garantir a entrega confiável dos eventos de replicação capturados no PostgreSQL. O projeto cria automaticamente as filas necessárias para o fluxo de dados, incluindo filas de processamento principal e dead-letter (DLX) para tratamento de falhas. Os scripts produtores enviam mensagens para as filas, enquanto os consumidores processam e aplicam as alterações nos destinos configurados. Essa arquitetura permite desacoplar a captura de dados da aplicação dos destinos, aumentando a escalabilidade e a tolerância a falhas do sistema.

### Configuração do Docker

O projeto utiliza Docker para facilitar a orquestração e o isolamento dos serviços necessários para a replicação de dados. O Dockerfile define o ambiente de execução da aplicação, enquanto o docker-compose permite iniciar múltiplos serviços (como Streamlit e RabbitMQ) de forma integrada e simples.

- **Dockerfile:** Responsável por construir a imagem do serviço principal, garantindo que todas as dependências estejam presentes e a aplicação pronta para execução.
- **docker-compose.yml:** Permite subir todos os serviços necessários com um único comando, além de facilitar o gerenciamento de múltiplas instâncias isoladas do sistema utilizando o parâmetro `-p <nome_do_projeto>`. Isso garante que cada instância tenha seus próprios containers, redes e recursos, sem necessidade de duplicar arquivos.
- **Isolamento:** Cada instância do projeto é totalmente isolada das demais, mesmo usando o mesmo arquivo `docker-compose.yml` e no mesmo diretório.
- **Volumes:** Esta aplicação não utiliza volumes Docker para persistência de dados. O código fonte é montado diretamente do host para o container, permitindo desenvolvimento em tempo real.

Para mais detalhes sobre comandos, dicas de uso, múltiplas instâncias e gerenciamento de containers, consulte o manual completo em [`docker/README.md`](docker/README.md).

---

## Uso do Docker

Após configurar o ambiente Docker, utilize os comandos abaixo para executar e gerenciar o projeto na prática:

### Construção e Inicialização

```bash
# Construir/Reconstruir a imagem para um projeto específico
# (Necessário apenas se você alterou o código ou o Dockerfile)
docker-compose -p replication1 build streamlit

docker-compose -p replication2 build streamlit
# ... para cada projeto/instância

# Iniciar os serviços (primeira vez ou após parar)
docker-compose -p replication1 up -d
CONTAINER_NAME=replication2 STREAMLIT_PORT=8502 docker-compose -p replication2 up -d
CONTAINER_NAME=replication3 STREAMLIT_PORT=8503 docker-compose -p replication3 up -d

# Reconstruir e iniciar (comando único)
docker-compose -p replication1 up -d --build
```

- O parâmetro `-p <nome_do_projeto>` garante que cada instância seja totalmente isolada das demais.
- Não é necessário duplicar arquivos ou pastas para rodar múltiplas instâncias.
- Para parar/remover uma instância específica, use `docker-compose -p <nome_do_projeto> down`.

### Acessando os Serviços

- Streamlit: http://localhost:${STREAMLIT_PORT:-8501}
- RabbitMQ: http://localhost:15672 (usuário/senha: guest/guest)

Para mais detalhes e dicas avançadas, consulte o manual completo em [`docker/README.md`](docker/README.md).

## Como Usar
Explique como executar o projeto, tanto via Docker quanto localmente, e como acessar a interface Streamlit.

## Uso do PostgreSQL
Detalhe como o projeto utiliza o PostgreSQL, especialmente o papel do replication slot.

## Uso do Streamlit
Explique para que serve a interface Streamlit e como ela pode ser utilizada.

## Uso de IA (Deepseek API para criação de tarefas)
Explique a funcionalidade planejada para uso de IA, mesmo que ainda não esteja implementada.

## Estrutura do Projeto
Breve explicação das principais pastas e arquivos do projeto.

## Licença
Informe o tipo de licença do projeto.

## Contato
Como entrar em contato com os responsáveis pelo projeto.

## Agradecimentos (opcional)
Créditos a pessoas, bibliotecas ou projetos que ajudaram. 