CONFIGURAÇÃO DA TAREFA: fl_cdc_upsert_employees
===========================================

1. CONFIGURAÇÃO BÁSICA
---------------------
- Nome da Tarefa: fl_cdc_upsert_employees
- Tipo de Replicação: Full Load + CDC
- Intervalo: 3 segundos
- Modo de Início: reload
- Modo CDC: default (upsert)
- Criar tabela se não existir: sim
- Configurações Full Load:
  * Recriar tabela se existir: sim
  * Truncar antes de inserir: sim

2. TABELAS REPLICADAS
--------------------
- employees.employee -> employees.employee_female
  * Prioridade: 0

3. FILTROS APLICADOS
-------------------
- Tabela: employees.employee
  * Tipo: equals
  * Coluna: gender
  * Valor: F
  * Descrição: Filtrando gênero feminino

4. TRANSFORMAÇÕES APLICADAS
-------------------------
- Tabela: employees.employee
  * Tipo: create_column
  * Nova Coluna: full_name
  * Operação: concat
  * Dependências: first_name, last_name
  * Separador: " " (espaço)
  * Prioridade: 2
  * Descrição: Cria coluna 'full_name'

5. TRATAMENTO DE ERROS
---------------------
- Parar se erro de INSERT: não
- Parar se erro de UPDATE: não
- Parar se erro de DELETE: não
- Parar se erro de UPSERT: não

RESUMO DA TAREFA
===============
Esta tarefa realiza uma replicação inicial completa (Full Load) seguida de 
replicação contínua (CDC) no modo upsert da tabela employee. Ela filtra apenas 
funcionárias (gênero feminino) e adiciona uma coluna full_name concatenando 
first_name e last_name. A tarefa não para em caso de erros de operações DML. 