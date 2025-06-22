CONFIGURAÇÃO DA TAREFA: fl_cdc_employees
=====================================

1. CONFIGURAÇÃO BÁSICA
---------------------
- Nome da Tarefa: fl_cdc_employees
- Tipo de Replicação: Full Load
- Intervalo: 1 segundo
- Criar tabela se não existir: sim
- Configurações Full Load:
  * Recriar tabela se existir: sim
  * Truncar antes de inserir: sim

2. TABELAS REPLICADAS
--------------------
- employees.department_manager
  * Prioridade: 0
- employees.salary -> funcionarios.salario
  * Prioridade: 1
- employees.employee -> employees.employee_female
  * Prioridade: 2

3. FILTROS APLICADOS
-------------------
- Tabela: employees.department_manager
  * Tipo: date_equals
  * Coluna: to_date
  * Valor: 9999-01-01
  * Descrição: Filtrando data de inicio igual a 9999-01-01

- Tabela: employees.employee
  * Tipo: equals
  * Coluna: gender
  * Valor: F
  * Descrição: Filtrando gênero feminino

4. TRANSFORMAÇÕES APLICADAS
-------------------------
Modificações de Nomes de Tabelas:
- employee -> employee_female
- salary -> salario

Modificações de Schema:
- employees.salary -> funcionarios.salario

Modificações de Colunas (Tabela salary):
- employee_id -> funcionario_id
- amount -> quantia
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

RESUMO DA TAREFA
===============
Esta tarefa realiza uma replicação inicial completa (Full Load) das tabelas do 
schema employees, com foco em funcionárias (gênero feminino). Ela aplica diversas 
transformações, incluindo renomeação de tabelas/colunas para português, conversão 
de nomes para maiúsculas, criação de nome completo, cálculo de período em anos, 
conversão de salário anual para mensal e adição de metadados de sincronização. 
A tarefa não para em caso de erros de operações DML. 