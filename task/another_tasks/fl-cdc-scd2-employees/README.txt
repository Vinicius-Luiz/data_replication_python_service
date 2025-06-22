CONFIGURAÇÃO DA TAREFA: fl_cdc_scd2_employees
=========================================

1. CONFIGURAÇÃO BÁSICA
---------------------
- Nome da Tarefa: fl_cdc_scd2_employees
- Tipo de Replicação: Full Load + CDC
- Intervalo: 3 segundos
- Modo de Início: reload
- Modo CDC: scd2
- Criar tabela se não existir: sim
- Configurações Full Load:
  * Recriar tabela se existir: sim
  * Truncar antes de inserir: sim
- Configurações SCD2:
  * Coluna data início: scd_start_date
  * Coluna data fim: scd_end_date
  * Coluna registro atual: scd_current

2. TABELAS REPLICADAS
--------------------
- employees.salary -> funcionarios.salario_historico
  * Prioridade: 1
- employees.employee -> employees.employee_history
  * Prioridade: 0
- employees.department_manager -> employees.department_manager_history
  * Prioridade: 1
- employees.department -> employees.department_history
  * Prioridade: 0

3. FILTROS APLICADOS
-------------------
Nenhum filtro configurado.

4. TRANSFORMAÇÕES APLICADAS
-------------------------
Modificações de Nomes de Tabelas:
- employee -> employee_history
- salary -> salario_historico
- department_manager -> department_manager_history
- department -> department_history

Modificações de Schema:
- employees.salary -> funcionarios.salario_historico

Modificações de Colunas (Tabela salary):
- employee_id -> funcionario_id
- amount -> quantia_mensal
- from_date -> data_inicio
- to_date -> data_fim

Criação de Colunas:
1. Tabela salary:
   * Nova Coluna: periodo_anos
   * Operação: date_diff_years
   * Dependências: from_date, to_date
   * Arredondar: sim
   * Prioridade: 2

2. Tabela employee:
   * Nova Coluna: full_name
   * Operação: concat
   * Dependências: first_name, last_name
   * Separador: " " (espaço)
   * Prioridade: 2

   * Nova Coluna: updated_by
   * Operação: literal
   * Valor: "PYTHON"
   * Tipo: varchar
   * Prioridade: 2

   * Nova Coluna: sync_date
   * Operação: date_now
   * Prioridade: 2

Modificações de Valores:
1. Tabela salary:
   * Coluna: amount
   * Operação: math_expression
   * Expressão: value / 12
   * Prioridade: 2

2. Tabela employee:
   * Coluna: first_name
   * Operação: uppercase
   * Prioridade: 2

   * Coluna: last_name
   * Operação: uppercase
   * Prioridade: 2

5. TRATAMENTO DE ERROS
---------------------
- Parar se erro de INSERT: não
- Parar se erro de UPDATE: não
- Parar se erro de DELETE: não
- Parar se erro de UPSERT: não
- Parar se erro de SCD2: não

RESUMO DA TAREFA
===============
Esta tarefa realiza uma replicação inicial completa (Full Load) seguida de 
replicação contínua (CDC) no modo SCD2 (Slowly Changing Dimension - Tipo 2) 
de várias tabelas do schema employees. Ela aplica diversas transformações, 
incluindo renomeação de tabelas/colunas, tradução para português, cálculos 
matemáticos e manipulações de texto. A tarefa mantém um histórico completo 
das mudanças com datas de início/fim e indicador de registro atual. A tarefa 
não para em caso de erros de operações DML ou SCD2. 