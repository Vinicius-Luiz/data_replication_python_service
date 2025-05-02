-- Criar um Slot de Replicação Lógica
SELECT pg_create_logical_replication_slot('audit_slot', 'test_decoding');

-- Testar a Captura de Eventos
SELECT * FROM pg_logical_slot_get_changes('audit_slot', NULL, NULL);
--- Formato: table public.employees_salary_target: INSERT: employee_id[integer]:42 amount[integer]:5000 from_date[timestamp]:2024-01-01 to_date[timestamp]:2025-01-01

-- Limpar o Slot de Replicação
SELECT pg_drop_replication_slot('audit_slot');

-- Consultar os Slots de Replicação
SELECT * FROM pg_replication_slots;
/*
slot_name → Nome do slot de replicação.
plugin → Plugin usado (test_decoding, wal2json, etc.).
slot_type → Tipo do slot (physical ou logical).
database → Nome do banco de dados associado ao slot.
active → Indica se o slot está ativo (true ou false).
restart_lsn → Última posição LSN do WAL que pode ser reproduzida pelo slot.
*/


UPDATE employees.department_manager
SET
  to_date = from_date + INTERVAL '2 years 2 days'
WHERE
  EXTRACT(
    YEAR
    FROM
      to_date
  ) = 9999;

------------- UPDATE [2]
UPDATE employees.department_manager
SET
  to_date = '9999-01-01'
WHERE
  to_date = from_date + INTERVAL '2 years 2 days';

SELECT * FROM employees.department_manager
------------------------------------------------------------


INSERT INTO
  employees.department (id, dept_name)
VALUES
  ('d008', 'Logistic'),
  ('d009', 'Relationship'),
  ('d010', 'Architecture');

------------- UPDATE
UPDATE employees.department 
   SET dept_name = 'Customers Relationship'
 WHERE id = 'd009'

------------- DELETE 
DELETE FROM employees.department
WHERE
  id in ('d008', 'd009', 'd010');
  
SELECT * FROM employees.department
------------------------------------------------------------
CREATE TABLE employees.the_office_season (
    id     INT,
    season_number   INT,
    description varchar(255),
    PRIMARY KEY (id, season_number)
);

INSERT INTO employees.the_office_season (id, season_number, description)
VALUES
  (0, 1, 'Temporada sofrível'),
  (1, 2, 'Melhor desenvolvimento do Michael Scott'),
  (2, 3, 'Melhor desenvolvimento do casal Jim e Pam');

DELETE FROM employees.the_office_season
WHERE id = 0;

DELETE FROM employees.the_office_season
WHERE id in (1, 2);

  
SELECT * FROM employees.the_office_season
------------------------------------------------------------
CREATE TABLE employees.eletronic_history (
    id     INT,
    date_acquisition   DATE,
    PRIMARY KEY (id, date_acquisition)
)

INSERT INTO employees.eletronic_history (id, date_acquisition)
VALUES
  (0, '2022-05-14'),
  (1, '2023-05-13'),
  (2, '2025-04-28');

DELETE FROM employees.eletronic_history
WHERE id = 0;

  
SELECT * FROM employees.eletronic_history
------------------------------------------------------------

------------- CRIAR TABELA SEM PK
create table
  employees.the_office (
    id integer,
    name varchar(60),
    age integer,
    created_date timestamp
  );

insert into
  employees.the_office (id, name, age, created_date)
values
  (1, '[Michael Scott', 40, current_date),
  (2, '[Pam Beesly]', 24, current_date),
  (3, 'Jim Halpert]', 26, current_date);

------------- DELETE TABELA SEM PK
delete from employees.the_office
where
  id in (1, 2, 3);

------------ UPDASET TABELA SEM PK
update employees.the_office
set
  name = '[Dwight Schrute]'
where
  id = 1;

SELECT * FROM employees.the_office