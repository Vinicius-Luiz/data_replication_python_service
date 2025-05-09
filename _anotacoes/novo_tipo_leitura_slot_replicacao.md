# Como `start_replication` e `consume_stream` retornam os dados no PostgreSQL

O método `start_replication` em conjunto com `consume_stream` retorna os dados de mudança em um formato específico através de um sistema de callbacks. Vou explicar detalhadamente como isso funciona:

## Mecanismo de Retorno de Dados

1. **`start_replication()`**:
   - Inicia o processo de replicação lógica
   - Não retorna dados diretamente, mas configura o canal
   - Os dados são recebidos assincronamente via callbacks

2. **`consume_stream()`**:
   - Processa o fluxo de dados de replicação
   - Para cada mensagem recebida, chama a função callback fornecida

## Estrutura da Mensagem de Replicação

Quando você implementa o callback (como `ChangeHandler` no exemplo anterior), ele recebe um objeto `msg` que contém:

```python
class ReplicationMessage:
    payload: str       # Os dados da mudança em formato textual
    data_start: int    # LSN (Log Sequence Number) de início
    data_end: int      # LSN de fim
    send_time: int     # Timestamp do envio
    cursor: object     # Referência ao cursor de replicação
```

## Exemplo Detalhado do Fluxo

```python
import psycopg2
from psycopg2.extras import LogicalReplicationConnection

def start_replication_example():
    conn = psycopg2.connect(
        host='localhost',
        database='seu_db',
        user='seu_user',
        password='sua_senha',
        connection_factory=LogicalReplicationConnection
    )
    
    cursor = conn.cursor()
    
    # Classe handler que processa cada mensagem
    class RawMessageHandler:
        def __call__(self, msg):
            print("\n--- Mensagem Recebida ---")
            print(f"LSN: {msg.data_start}")
            print(f"Payload: {msg.payload}")
            print(f"Tipo: {type(msg.payload)}")
            print(f"Tamanho: {len(msg.payload)} bytes")
            
            # Confirma o processamento
            msg.cursor.send_feedback(flush_lsn=msg.data_start)
    
    try:
        # Inicia a replicação
        cursor.start_replication(
            slot_name='meu_slot',
            decode=True,
            options={
                'proto_version': '1',
                'publication_names': 'minha_publicacao'
            }
        )
        
        # Consome o stream com nosso handler
        cursor.consume_stream(RawMessageHandler())
        
    except Exception as e:
        print(f"Erro: {e}")
    finally:
        cursor.close()
        conn.close()
```

## Formato Típico do Payload

O payload retornado varia conforme o plugin de replicação usado (testdecode, pgoutput, wal2json). Com o plugin padrão `pgoutput`, um exemplo seria:

```
BEGIN 1234
table public.employees: INSERT: id[integer]:1 name[text]:'João' salary[numeric]:5000.00
table public.departments: UPDATE: old_id[integer]:1 new_id[integer]:2 name[text]:'TI'
COMMIT 1234
```

## Tipos de Mensagens

1. **Mensagens de Transação**:
   - `BEGIN [XID]` - Início da transação
   - `COMMIT [XID]` - Fim da transação

2. **Mensagens de Dados**:
   - `table [schema].[table]: [OPERATION]: [detalhes]`
   - Operações podem ser INSERT, UPDATE, DELETE

3. **Mensagens Especiais**:
   - Mensagens de relação (descrição de tabelas)
   - Mensagens de tipo (descrição de tipos customizados)

## Como Personalizar o Formato de Saída

Você pode modificar o handler para formatar exatamente como deseja:

```python
class CustomFormatHandler:
    def __call__(self, msg):
        # Formatação personalizada
        output = f"{msg.data_start},{msg.payload.split()[0]},{msg.payload}"
        print(output)
        
        # Ou escreve em arquivo, envia para fila, etc.
        with open('changes.log', 'a') as f:
            f.write(output + '\n')
        
        # Confirma o processamento
        msg.cursor.send_feedback(flush_lsn=msg.data_start)
```

## Considerações Importantes

1. O formato exato depende do plugin de replicação configurado
2. O processamento é assíncrono - mensagens chegam quando ocorrem mudanças
3. Você DEVE enviar feedback periodicamente com `send_feedback()`
4. Para parar a replicação, você precisa interromper o `consume_stream()`

Este é o mecanismo básico como o PostgreSQL retorna os dados de mudança através da API de replicação lógica em Python.

## Outros links

-- https://www.psycopg.org/docs/extras.html