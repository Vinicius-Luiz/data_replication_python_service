class SchemaQueries:
    GET_SCHEMAS = "SELECT schema_name FROM information_schema.schemata"


class TableQueries:
    GET_TABLES = """
  SELECT table_name 
    FROM information_schema.tables 
    WHERE table_type   = 'BASE TABLE'
      AND table_schema = %s"""

    GET_TABLE_DETAILS = """
  SELECT schemaname                           AS schema_name,
          relname                              AS table_name,
          n_live_tup                           AS estimated_row_count,
          pg_size_pretty(pg_table_size(relid)) AS table_size
    FROM  pg_stat_user_tables
    WHERE schemaname = %s
      AND relname = %s
    ORDER BY schemaname, relname
    """

    CHECK_TABLE_EXISTS = """
  SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_schema = %s
      AND table_name   = %s
  """

    CREATE_TABLE = """
  CREATE TABLE {schema}.{table}
  (
      {columns}
      {primary_key}
  )
  """

    DROP_TABLE = """
  DROP TABLE IF EXISTS {schema}.{table}
  """

    TRUNCATE_TABLE = """
  TRUNCATE TABLE {schema}.{table}
  """


class ColumnQueries:
    GET_TABLE_COLUMNS = """
  SELECT table_schema AS schema_name,
          table_name,
          column_name,
          data_type,
          udt_name,
          character_maximum_length,
          is_nullable,
          ordinal_position
    FROM information_schema.columns
    WHERE table_schema = %s
      AND table_name = %s
    ORDER BY table_schema,
            table_name,
            ordinal_position
    """

    GET_TABLE_PRIMARY_KEY = """
  SELECT kcu.column_name AS primary_key_column
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu 
      ON tc.constraint_name = kcu.constraint_name AND
          tc.table_schema = kcu.table_schema
    WHERE tc.constraint_type = 'PRIMARY KEY' AND
          tc.table_schema = %s      AND
          tc.table_name   = %s
  """


class FullLoadQueries:
    GET_FULL_LOAD_FROM_TABLE = """
  SELECT *
    FROM {schema}.{table}
  """

    FULL_LOAD_INSERT_DATA = """
  INSERT INTO {schema}.{table}
  ({columns})
  VALUES %s
  """


class CDCQueries:
    CDC_INSERT_DATA = """
  INSERT INTO {schema}.{table} ({columns})
  VALUES ({values})
  """

    CDC_UPDATE_DATA = """
  UPDATE {schema}.{table}
      SET {set_clause}
    WHERE {where_clause}
  """

    CDC_DELETE_DATA = """
  DELETE FROM {schema}.{table}
   WHERE {where_clause}
  """

    CDC_UPSERT_DATA = """
    INSERT INTO {schema}.{table} ({columns})
    VALUES ({values})
        ON CONFLICT ({pk_columns}) 
        DO UPDATE SET {set_clause}
  """


class ReplicationQueries:
    CREATE_REPLICATION_SLOT = """
  SELECT pg_create_logical_replication_slot(%s, 'test_decoding')
  """

    VERIFY_IF_EXISTS_A_REPLICATION_SLOT = """
  SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = %s
  """
  
    VERIFY_OLD_REPLICATION_SLOT = """
  SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE %s AND slot_name <> %s AND active = 'f'
  """

    DROP_REPLICATION_SLOT = """
  SELECT pg_drop_replication_slot(%s)
  """

    GET_CHANGES = """
  SELECT *
    FROM pg_logical_slot_get_changes(%s, NULL, NULL);
  """


class SCD2Queries:
    SQL_VERIFY_ROW_SCD2_EXISTS = """
      SELECT 1
        FROM {schema}.{table}
       WHERE {where_clause}
         AND {current} = 1
  """

    SQL_UPDATE_EXISTING = """
    UPDATE {schema}.{table}
       SET {end_date} = NOW(),
           {current} = 0
     WHERE {where_clause}
       AND {current} = 1
  """