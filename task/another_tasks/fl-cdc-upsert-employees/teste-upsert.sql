----------------------------------------------
-- CONSULTAS INICIAIS (ANTES DOS TESTES)
----------------------------------------------
SELECT * FROM employees.employee;

----------------------------------------------
-- TESTES PARA CDC UPSERT (employees.employee)
----------------------------------------------
-- INSERTS (Apenas funcionárias)
INSERT INTO employees.employee (id, birth_date, first_name, last_name, gender, hire_date)
VALUES 
(500000, '2002-09-16', 'Maria', 'Luiza', 'F', '2024-06-01'),
(500001, '1979-09-19', 'Hermione', 'Granger', 'F', '2025-04-04'),
(500002, '2000-05-14', 'Vinicius', 'Luiz', 'M', '2024-04-01'); -- Este não deve aparecer no destino (é masculino)

-- UPDATES (Testando upsert)
-- Atualização 1 - Deve atualizar o registro existente
UPDATE employees.employee SET last_name = 'Astreia' WHERE id = 500000;

-- Atualização 2 - Deve atualizar o registro existente
UPDATE employees.employee 
SET first_name = 'Hermione', 
    last_name = 'Weasley' 
WHERE id = 500001;

-- Atualização 3 - Alterando gênero (não deve refletir no destino)
UPDATE employees.employee SET gender = 'M', hire_date = '2024-04-01' WHERE id = 500000;

-- Atualização 4 - Alterando gênero (deve adicionar ao destino)
UPDATE employees.employee SET gender = 'F' WHERE id = 500002;

-- -- DELETE (LIMPEZA)
-- DELETE FROM employees.employee WHERE id IN (500000, 500001, 500002);


----------------------------------------------
-- TESTE DE CARGA
----------------------------------------------
-- UPDATE employees.employee set id = id


----------------------------------------------
-- TARGET
----------------------------------------------
SELECT * FROM employees.employee_female; -- Tabela de destino


-- Verificar se apenas as funcionárias foram replicadas
SELECT * FROM employees.employee_female WHERE id IN (500000, 500001, 500002);

-- Verificar resultados após updates
SELECT * FROM employees.employee_female WHERE id IN (500000, 500001, 500002);


-- Verificar se a transformação full_name está funcionando
SELECT id, first_name, last_name, full_name, gender 
FROM employees.employee_female 
WHERE id IN (500000, 500001, 500002);


-- -- Verificar se os registros foram removidos do destino
-- SELECT * FROM employees.employee_female WHERE id IN (500000, 500001, 500002); 