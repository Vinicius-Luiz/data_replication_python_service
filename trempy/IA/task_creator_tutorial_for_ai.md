# Documentação para Geração Automática de Tarefas de Replicação

> **Atenção:** Sempre preencha todos os campos obrigatórios do settings.json, utilizando os valores padrão definidos nesta documentação caso o usuário não especifique. Não crie parâmetros extras além dos explicitamente documentados.

Esta documentação serve como referência para a criação automática de tarefas de replicação de dados a partir de prompts em linguagem natural, utilizando API ou modelos de IA.

O objetivo é retornar o settings.json completo com todas as seções estruturadas corretamente, usando os valores exatos dos enums conforme a documentação. Inclua descrições claras para cada filtro e transformação.

---

## 1. Estrutura Geral do settings.json

O arquivo de configuração é um JSON com a seguinte estrutura:

```json
{
  "task": { ... },
  "error_handling": { ... },
  "tables": [ ... ],
  "filters": [ ... ],
  "transformations": [ ... ]
}
```

---

## 1.1 Parâmetros obrigatórios e valores padrão

Ao gerar o settings.json, SEMPRE inclua os seguintes parâmetros, mesmo que o usuário não os mencione. Use o valor padrão indicado caso o usuário não especifique:

- task.replication_type: "full_load" (se não informado pelo usuário)
- task.interval_seconds: 10 (se não informado pelo usuário)
- task.start_mode: "reload" (se não informado pelo usuário)
- task.create_table_if_not_exists: false (se não informado pelo usuário)
- task.full_load_settings.recreate_table_if_exists: false (se não informado pelo usuário)
- task.full_load_settings.truncate_before_insert: false (se não informado pelo usuário)
- task.cdc_settings.mode: "default" (se não informado pelo usuário e houver CDC)
- task.cdc_settings.scd2_settings.start_date_column_name: "scd_start_date" (se não informado e houver SCD2)
- task.cdc_settings.scd2_settings.end_date_column_name: "scd_end_date" (se não informado e houver SCD2)
- task.cdc_settings.scd2_settings.current_column_name: "scd_current" (se não informado e houver SCD2)

Importante: Mesmo que o usuário não mencione, inclua todos os parâmetros obrigatórios com seus valores padrão.

---

## 2. Seções do Arquivo de Configuração

### 2.1. Seção `task`
- **task_name**: Nome da tarefa.
- **replication_type**: Tipo de replicação (`full_load`, `cdc`, `full_load_and_cdc`).
- **interval_seconds**: Intervalo entre execuções (em segundos).
- **start_mode**: Modo de início (`reload`, `continue`).
- **create_table_if_not_exists**: Cria a tabela de destino se não existir (boolean).
- **full_load_settings**: (objeto)
  - **recreate_table_if_exists**: Recria a tabela se já existir (boolean).
  - **truncate_before_insert**: Trunca a tabela antes de inserir (boolean).
- **cdc_settings**: (objeto, opcional)
  - **mode**: Modo do CDC (`default`, `upsert`, `scd2`).
  - **scd2_settings**: (objeto, apenas se `mode` for `scd2`)
    - **start_date_column_name**: Nome da coluna de início de vigência.
    - **end_date_column_name**: Nome da coluna de fim de vigência.
    - **current_column_name**: Nome da coluna que indica se o registro é o atual.

### 2.2. Seção `error_handling`
- **stop_if_insert_error**: Interrompe a tarefa em erro de insert (boolean).
- **stop_if_update_error**: Interrompe a tarefa em erro de update (boolean).
- **stop_if_delete_error**: Interrompe a tarefa em erro de delete (boolean).
- **stop_if_upsert_error**: Interrompe a tarefa em erro de upsert (boolean).
- **stop_if_scd2_error**: Interrompe a tarefa em erro de SCD2 (boolean, só aparece em tarefas SCD2).

### 2.3. Seção `tables` (lista de objetos)
Cada objeto:
- **schema_name**: Nome do schema.
- **table_name**: Nome da tabela.
- **priority**: Prioridade de replicação (0 a 4).

### 2.4. Seção `filters` (lista de objetos)
Cada objeto:
- **table_info**: (objeto)
  - **schema_name**: Nome do schema.
  - **table_name**: Nome da tabela.
- **settings**: (objeto)
  - **filter_type**: Tipo do filtro (ver enums).
  - **description**: Descrição do filtro.
  - **column_name**: Nome da coluna a ser filtrada.
  - **value**: Valor para filtro (pode ser string, número, data, etc).
  - **values**: Lista de valores (para filtros do tipo IN, NOT_IN, etc).
  - **lower**: Limite inferior (para filtros do tipo BETWEEN).
  - **upper**: Limite superior (para filtros do tipo BETWEEN).

### 2.5. Seção `transformations` (lista de objetos)
Cada objeto:
- **table_info**: (objeto)
  - **schema_name**: Nome do schema.
  - **table_name**: Nome da tabela.
- **settings**: (objeto)
  - **transformation_type**: Tipo da transformação (ver enums).
  - **description**: Descrição da transformação.
  - **contract**: (objeto) Parâmetros específicos da operação (detalhados na seção 4).
  - **priority**: Prioridade da transformação (0 a 4).

---

## 3. Enums e Valores Aceitos

### 3.1. TaskType
- `full_load_and_cdc`, `full_load`, `cdc`

### 3.2. StartType
- `continue`, `reload`

### 3.3. CdcModeType
- `default`, `upsert`, `scd2`

### 3.4. TransformationType
- `create_column`, `modify_schema_name`, `modify_table_name`, `modify_column_name`, `modify_column_value`, `add_primary_key`, `remove_primary_key`

### 3.5. TransformationOperationType
- `format_date`, `uppercase`, `lowercase`, `trim`, `extract_year`, `extract_month`, `extract_day`, `math_expression`, `literal`, `date_now`, `datetime_now`, `concat`, `date_diff_years`

### 3.6. FilterType
- `equals`, `not_equals`, `greater_than`, `greater_than_or_equal`, `less_than`, `less_than_or_equal`, `in`, `not_in`, `is_null`, `is_not_null`, `starts_with`, `ends_with`, `contains`, `not_contains`, `between`, `not_between`, `date_equals`, `date_not_equals`, `date_greater_than`, `date_greater_than_or_equal`, `date_less_than`, `date_less_than_or_equal`, `date_between`, `date_not_between`

### 3.7. PriorityType
- `0` (HIGHEST), `1` (HIGH), `2` (NORMAL), `3` (LOW), `4` (LOWEST)

---

## 4. Transformações: Tipos e Operações

| transformation_type      | operation                | Parâmetros principais no contract           |
|-------------------------|--------------------------|---------------------------------------------|
| modify_column_value     | math_expression          | column_name, expression                     |
|                        | format_date              | column_name, format                         |
|                        | uppercase                | column_name                                 |
|                        | lowercase                | column_name                                 |
|                        | trim                     | column_name                                 |
|                        | extract_year             | column_name                                 |
|                        | extract_month            | column_name                                 |
|                        | extract_day              | column_name                                 |
| create_column           | literal                  | new_column_name, value_type, value          |
|                        | concat                   | new_column_name, depends_on, separator      |
|                        | date_diff_years          | new_column_name, depends_on, round_result   |
|                        | date_now                 | new_column_name                             |
|                        | datetime_now             | new_column_name                             |
| modify_schema_name      | -                        | target_schema_name                          |
| modify_table_name       | -                        | target_table_name                           |
| modify_column_name      | -                        | column_name, target_column_name             |
| add_primary_key         | -                        | column_names                                |
| remove_primary_key      | -                        | column_names                                |

---

## 5. Filtros: Tipos e Inputs Necessários

| filter_type           | Inputs necessários         |
|----------------------|---------------------------|
| equals, not_equals    | value                     |
| greater_than, etc.    | value                     |
| starts_with, etc.     | value                     |
| in, not_in            | values                    |
| between, not_between  | lower, upper              |
| is_null, is_not_null  | (nenhum)                  |

---

## 6. Exemplos Práticos

### Exemplo de settings.json
```json
{
  "task": {
    "task_name": "replicacao_clientes_vip",
    "replication_type": "full_load_and_cdc",
    "interval_seconds": 60,
    "start_mode": "reload",
    "create_table_if_not_exists": true,
    "full_load_settings": {
      "recreate_table_if_exists": false,
      "truncate_before_insert": true
    },
    "cdc_settings": {
      "mode": "upsert"
    }
  },
  "error_handling": {
    "stop_if_insert_error": false,
    "stop_if_upsert_error": true
  },
  "tables": [
    {
      "schema_name": "vendas",
      "table_name": "clientes",
      "priority": 1
    }
  ],
  "filters": [
    {
      "table_info": {
        "schema_name": "vendas",
        "table_name": "clientes"
      },
      "settings": {
        "filter_type": "greater_than",
        "description": "Filtrar clientes com limite de crédito alto",
        "column_name": "limite_credito",
        "value": 10000
      }
    },
    {
      "table_info": {
        "schema_name": "vendas",
        "table_name": "clientes"
      },
      "settings": {
        "filter_type": "in",
        "description": "Filtrar apenas clientes dos estados RJ, SP e MG",
        "column_name": "uf",
        "values": ["RJ", "SP", "MG"]
      }
    },
    {
      "table_info": {
        "schema_name": "vendas",
        "table_name": "clientes"
      },
      "settings": {
        "filter_type": "date_between",
        "description": "Filtrar clientes cadastrados no último ano",
        "column_name": "data_cadastro",
        "lower": "2023-01-01",
        "upper": "2023-12-31"
      }
    }
  ],
  "transformations": [
    {
      "table_info": {
        "schema_name": "vendas",
        "table_name": "clientes"
      },
      "settings": {
        "transformation_type": "modify_column_value",
        "description": "Converter nome para maiúsculas",
        "contract": {
          "operation": "uppercase",
          "column_name": "nome"
        },
        "priority": 2
      }
    },
    {
      "table_info": {
        "schema_name": "vendas",
        "table_name": "clientes"
      },
      "settings": {
        "transformation_type": "create_column",
        "description": "Adicionar data de sincronização",
        "contract": {
          "operation": "datetime_now",
          "new_column_name": "data_sincronizacao"
        },
        "priority": 1
      }
    }
  ]
}
```

### Exemplo de Prompt
```
Crie uma tarefa de replicação para a tabela "produtos" do schema "estoque":
- Replicação completa a cada hora
- Criar tabela se não existir
- Adicionar coluna "ultima_atualizacao" com data/hora atual
- Converter nome do produto para maiúsculas
- Não parar em caso de erro de inserção

// Mesmo que o prompt não mencione, o settings.json gerado deve conter todos os parâmetros obrigatórios com seus valores padrão, conforme a tabela de parâmetros obrigatórios.
```

---

## 7. Dicas para Criação de Prompts

- Especifique claramente tabelas, colunas e operações desejadas
- Indique o comportamento esperado em caso de erro
- Consulte os enums para valores válidos
- Utilize os exemplos como referência

## 7.1 Exemplo de prompt válido

```
Crie uma tarefa de replicação completa com CDC seguindo estas especificações:

1. Configurações Básicas:
- Nome da tarefa: "replicacao_clientes_vip"
- Tipo: full_load_and_cdc
- Intervalo de execução: 10 minutos (600 segundos)
- Criar tabela de destino se não existir
- Modo CDC: upsert

2. Comportamento em Erros:
- Continuar em caso de erro de inserção
- Interromper em caso de erro de upsert

3. Tabela de Origem:
- Schema: "vendas"
- Tabela: "clientes"
- Prioridade: alta (1)

4. Filtros a serem aplicados:
- Filtro 1: Apenas clientes com limite_credito > 10000
- Filtro 2: Somente clientes dos estados RJ, SP ou MG (coluna "uf")
- Filtro 3: Clientes cadastrados em 2023 (data_cadastro entre 01/01/2023 e 31/12/2023)

5. Transformações necessárias:
- Transformação 1: Converter valores da coluna "nome" para MAIÚSCULAS (prioridade 2)
- Transformação 2: Criar nova coluna "data_sincronizacao" com a data/hora atual do sistema (prioridade 1)

6. Configurações de Full Load:
- Não recriar tabela se existir
- Truncar tabela antes da carga completa

Retorne o settings.json completo com todas as seções estruturadas corretamente, usando os valores exatos dos enums conforme a documentação. Inclua descrições claras para cada filtro e transformação.
```

# Regras Adicionais para Geração de settings.json

Além das instruções anteriores, considere as seguintes regras ao gerar o arquivo de configuração:

## 8. Regras Especiais para Transformações de Estrutura de Tabela

- **Prioridade das Transformações de Estrutura:**
  - Transformações que modificam a estrutura da tabela devem obrigatoriamente ter prioridade mais alta (`0`).
  - As demais transformações **não podem** ter prioridade `0`, mas podem ter prioridade alta (`1`).

- **Transformações consideradas de estrutura:**
  - `modify_schema_name`
  - `modify_table_name`
  - `modify_column_name`
  - `add_primary_key`
  - `remove_primary_key`

- **Valores possíveis para `value_type` na transformação `literal`:**
  - `smallint`
  - `integer`
  - `bigint`
  - `real`
  - `double precision`
  - `character varying`
  - `varchar`
  - `boolean`
  - `date`
  - `timestamp`
  - `numeric`
  - `text`

## 9. Observações Específicas sobre math_expression

- Na operação `math_expression`, a expressão deve sempre utilizar o termo `value` para representar o valor da coluna indicada em `column_name`.

## 10. Prioridade para Transformações com depends_on

- Transformações que possuem o campo `depends_on` devem ter prioridade **inferior** (ou seja, um número maior) em relação às transformações das quais dependem, mas **não é obrigatório** que tenham prioridade 4 (a mais baixa). Basta garantir que sua prioridade seja menor do que a das transformações das quais dependem.

- Os valores presentes no campo `depends_on` devem sempre referenciar o nome das colunas após sua transformação do tipo `modify_column_name`.

> **Observação Importante:**
> Caso o usuário solicite alguma configuração, parâmetro ou campo que não esteja previsto nesta documentação, **NÃO** crie nem invente parâmetros ou seções extras. Utilize **apenas** o que está explicitamente documentado aqui.