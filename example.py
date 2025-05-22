import polars as pl

path = rf"data\cdc_data\stats\*.parquet"

df = pl.scan_parquet(path)
print(df.head(6000).collect())


resultado = df.group_by(["schema_name", "table_name"]).agg(
    [
        pl.col("inserts").sum().alias("sum_inserts"),
        pl.col("updates").sum().alias("sum_updates"),
        pl.col("deletes").sum().alias("sum_deletes"),
        pl.col("errors").sum().alias("sum_errors"),
        pl.col("total").sum().alias("sum_total"),
    ]
)

print(resultado.head(6000).collect())
