from trempy.Metadata.MetadataConnectionManager import MetadataConnectionManager
import polars as pl

# with MetadataConnectionManager() as metadata_manager:
#     metadata_manager.truncate_tables()

with MetadataConnectionManager() as metadata_manager:
    df = metadata_manager.get_metadata_tables("stats_cdc")
    df = (
        df.group_by(["task_name", "schema_name", "table_name"])
        .agg(
            [
                pl.sum("inserts"),
                pl.sum("updates"),
                pl.sum("deletes"),
                pl.sum("errors"),
                pl.sum("total"),
            ]
        )
        .sort(["task_name", "schema_name", "table_name"])
    )
    print(df.head(100000))

with MetadataConnectionManager() as metadata_manager:
    df = metadata_manager.get_metadata_tables("stats_full_load")
    print(df.head(100000))


with MetadataConnectionManager() as metadata_manager:
    df = metadata_manager.get_metadata_tables("stats_source_tables")
    print(df.head(100000))

with MetadataConnectionManager() as metadata_manager:
    df = metadata_manager.get_metadata_tables("metadata_table")
    print(df.head(100000))

with MetadataConnectionManager() as metadata_manager:
    df = metadata_manager.get_metadata_tables("stats_message")
    print(df.head(100000))

with MetadataConnectionManager() as metadata_manager:
    df = metadata_manager.get_metadata_tables("apply_exceptions")
    print(df.head(100000))

with MetadataConnectionManager() as metadata_manager:
    df = metadata_manager.get_messages_stats()
    print(df.head(100000))