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