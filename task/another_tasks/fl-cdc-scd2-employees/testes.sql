----------------------------------------------
-- CONSULTAS INICIAIS (ANTES DOS TESTES)
----------------------------------------------
SELECT * FROM employees.employee;
SELECT * FROM employees.salary;
SELECT * FROM employees.department;
SELECT * FROM employees.department_manager;

----------------------------------------------
-- TESTES PARA TABELA employees.employee
----------------------------------------------
-- INSERTS
INSERT INTO employees.employee (id, birth_date, first_name, last_name, gender, hire_date)
VALUES 
(500000, '2000-05-14', 'Vinícius', 'Luiz', 'M', '2024-06-01'),
(500001, '2002-05-07', 'Vital', 'Fernando', 'M', '2025-04-04'),
(500002, '1996-03-12', 'Aron', 'Sales', 'M', '2024-04-01');

-- UPDATES (COM AÇÕES REVERSÍVEIS)
-- Atualização 1 
UPDATE employees.employee SET last_name = 'França' WHERE id = 500000;

-- Atualização 2 
UPDATE employees.employee SET last_name = 'Ramos' WHERE id = 500001;

-- Atualização 3 
UPDATE employees.employee SET hire_date = '2024-03-25' WHERE id = 500002;
UPDATE employees.employee SET hire_date = '2024-04-02' WHERE id = 500002;
UPDATE employees.employee SET hire_date = '2024-04-01' WHERE id = 500002;

-- DELETE (LIMPEZA)
DELETE FROM employees.employee WHERE id IN (500000, 500001, 500002);

----------------------------------------------
-- TESTES PARA TABELA employees.department
----------------------------------------------
-- INSERTS
INSERT INTO employees.department (id, dept_name)
VALUES
('d008', 'Logistic'),
('d009', 'Relationship'),
('d010', 'Architecture');

-- UPDATE
UPDATE employees.department SET dept_name = 'Customers Relationship' WHERE id = 'd009';

-- DELETE (LIMPEZA)
DELETE FROM employees.department WHERE id IN ('d008', 'd009', 'd010');

----------------------------------------------
-- TESTES PARA TABELA employees.salary
----------------------------------------------
-- INSERTS (REGISTROS INICIAIS)
INSERT INTO employees.salary (employee_id, amount, from_date, to_date)
VALUES 
(500000, 60000, '2024-06-01', '9999-12-31'),
(500001, 26000, '2025-04-04', '9999-12-31'),
(500002, 80520, '2024-04-01', '9999-12-31');

-- UPDATES (OPERACOES SCD2)
-- Atualização 1 
BEGIN;
UPDATE employees.salary SET to_date = CURRENT_DATE WHERE employee_id = 500000 AND to_date = '9999-12-31';
INSERT INTO employees.salary (employee_id, amount, from_date, to_date) 
VALUES (500000, 66000, CURRENT_DATE, '9999-12-31');
COMMIT;

-- Atualização 2 
BEGIN;
UPDATE employees.salary SET to_date = CURRENT_DATE WHERE employee_id = 500001 AND to_date = '9999-12-31';
INSERT INTO employees.salary (employee_id, amount, from_date, to_date) 
VALUES (500001, 28800, CURRENT_DATE, '9999-12-31');
COMMIT;

-- Atualização 3 
BEGIN;
UPDATE employees.salary SET to_date = CURRENT_DATE WHERE employee_id = 500002 AND to_date = '9999-12-31';
INSERT INTO employees.salary (employee_id, amount, from_date, to_date) 
VALUES (500002, 98520, CURRENT_DATE, '9999-12-31');
COMMIT;

-- DELETE (ENCERRAMENTO DE REGISTROS SCD2)
UPDATE employees.salary SET to_date = CURRENT_DATE 
WHERE employee_id IN (500000, 500001, 500002) AND to_date = '9999-12-31';

-- DELETE (LIMPEZA)
DELETE FROM employees.salary WHERE employee_id IN (500000, 500001, 500002);

----------------------------------------------
-- TESTES PARA TABELA employees.department_manager 
----------------------------------------------
-- INSERTS (REGISTROS INICIAIS)
INSERT INTO employees.department_manager (employee_id, department_id, from_date, to_date)
VALUES 
(500001, 'd010', '2025-04-04', '9999-01-01'),
(500002, 'd008', '2024-04-01', '9999-01-01');

-- UPDATE (OPERACAO SCD2)
BEGIN;
UPDATE employees.department_manager SET to_date = CURRENT_DATE WHERE employee_id = 500002 AND to_date = '9999-01-01';
INSERT INTO employees.department_manager (employee_id, department_id, from_date, to_date)
VALUES (500002, 'd009', CURRENT_DATE, '9999-01-01');
COMMIT;

-- DELETE (ENCERRAMENTO DE REGISTROS SCD2)
UPDATE employees.department_manager SET to_date = CURRENT_DATE 
WHERE employee_id IN (500001, 500002) AND to_date = '9999-01-01';

-- DELETE (LIMPEZA)
DELETE FROM employees.department_manager WHERE employee_id IN (500000, 500001, 500002);

----------------------------------------------
-- DESTINO
----------------------------------------------
SELECT * FROM employees.employee_history order by id, scd_start_date;
SELECT * FROM employees.department_history order by id, scd_start_date;
SELECT * FROM funcionarios.salario_historico order by funcionario_id, scd_start_date;
SELECT * FROM employees.department_manager_history order by employee_id, department_id, scd_start_date;


SELECT * FROM employees.employee_history WHERE id IN (500000, 500001, 500002) order by id, scd_start_date;
SELECT * FROM employees.department_history WHERE id IN ('d008', 'd009', 'd010') order by id, scd_start_date;
SELECT * FROM funcionarios.salario_historico WHERE funcionario_id IN (500000, 500001, 500002) order by funcionario_id, scd_start_date;
SELECT * FROM employees.department_manager_history WHERE employee_id IN (500000, 500001, 500002) order by employee_id, department_id, scd_start_date;

-- DROP TABLE employees.department_history;
-- DROP TABLE employees.employee_history;
-- DROP TABLE funcionarios.salario_historico;
-- DROP TABLE employees.department_manager_history;




----------------------------------------------
-- TESTE DE CARGA
----------------------------------------------
-- UPDATE employees.salary set employee_id = employee_id;

-- select 2844047  as qtd_total,
--        count(*) as qtd_target,
-- 	   2844047-count(*) as qtd_restante
--   from funcionarios.salario_historico;


-- select min(scd_start_date), max(scd_start_date), max(scd_start_date)  - min(scd_start_date)
--   from funcionarios.salario_historico

