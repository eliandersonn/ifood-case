# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingestão Bronze
# MAGIC
# MAGIC Valida os dez arquivos esperados na Landing Zone, padroniza os tipos
# MAGIC necessários e publica tabelas Delta Bronze com rastreabilidade.

# COMMAND ----------

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

CATALOG = "workspace"
BRONZE_SCHEMA = "nyc_taxi_bronze"
LANDING_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/landing"
MONTHS = [f"2023-{month:02d}" for month in range(1, 6)]

EXPECTED_FILES = {
    taxi_type: [f"{taxi_type}_tripdata_{month}.parquet" for month in MONTHS]
    for taxi_type in ("yellow", "green")
}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validação da Landing Zone
# MAGIC
# MAGIC A execução é interrompida quando algum arquivo não está disponível.
# MAGIC Isso evita publicar uma tabela parcial sem sinalizar o problema.

# COMMAND ----------

available_files = {
    file_info.name
    for file_info in dbutils.fs.ls(LANDING_PATH)
    if file_info.name.endswith(".parquet")
}
expected_files = {
    file_name
    for files_by_type in EXPECTED_FILES.values()
    for file_name in files_by_type
}

missing_files = sorted(expected_files - available_files)
unexpected_files = sorted(available_files - expected_files)

if missing_files:
    raise FileNotFoundError(
        "Arquivos obrigatórios ausentes na Landing Zone: "
        + ", ".join(missing_files)
    )

print(f"Arquivos esperados encontrados: {len(expected_files)}")
print(f"Arquivos adicionais ignorados: {unexpected_files}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Leitura e padronização técnica
# MAGIC
# MAGIC Cada Parquet é lido individualmente antes da união. Essa estratégia
# MAGIC trata explicitamente possíveis diferenças de tipo entre os meses.

# COMMAND ----------

def read_month_file(taxi_type: str, file_name: str) -> DataFrame:
    """Lê um arquivo mensal e aplica o contrato técnico da camada Bronze."""

    pickup_column = (
        "tpep_pickup_datetime"
        if taxi_type == "yellow"
        else "lpep_pickup_datetime"
    )
    dropoff_column = (
        "tpep_dropoff_datetime"
        if taxi_type == "yellow"
        else "lpep_dropoff_datetime"
    )
    source_month = file_name.split('_')[-1].removesuffix('.parquet')
    source_path = f"{LANDING_PATH}/{file_name}"

    source_df = spark.read.parquet(source_path)
    required_columns = {
        "VendorID",
        "passenger_count",
        "total_amount",
        pickup_column,
        dropoff_column,
    }
    missing_columns = sorted(required_columns - set(source_df.columns))

    if missing_columns:
        raise ValueError(
            f"Schema inválido em {file_name}. Colunas ausentes: {missing_columns}"
        )

    return source_df.select(
        F.col("VendorID").cast(IntegerType()).alias("VendorID"),
        F.col("passenger_count").cast(IntegerType()).alias("passenger_count"),
        F.col("total_amount").cast(DoubleType()).alias("total_amount"),
        F.col(pickup_column).cast(TimestampType()).alias(pickup_column),
        F.col(dropoff_column).cast(TimestampType()).alias(dropoff_column),
        F.lit(file_name).alias("source_file"),
        F.lit(source_month).alias("source_month"),
        F.current_timestamp().alias("ingested_at"),
    )


def build_bronze_dataframe(taxi_type: str) -> DataFrame:
    """Une os cinco meses de um tipo de táxi com schema consistente."""

    monthly_dataframes = [
        read_month_file(taxi_type, file_name)
        for file_name in EXPECTED_FILES[taxi_type]
    ]
    return reduce(
        lambda current, next_df: current.unionByName(next_df),
        monthly_dataframes,
    )


yellow_bronze_df = build_bronze_dataframe("yellow")
green_bronze_df = build_bronze_dataframe("green")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Publicação Delta
# MAGIC
# MAGIC O `overwrite` implementa um full refresh idempotente: executar novamente
# MAGIC substitui a versão anterior e não duplica registros.

# COMMAND ----------

BRONZE_TABLES = {
    "yellow": f"{CATALOG}.{BRONZE_SCHEMA}.yellow_taxi_trips_raw",
    "green": f"{CATALOG}.{BRONZE_SCHEMA}.green_taxi_trips_raw",
}

for taxi_type, bronze_df in {
    "yellow": yellow_bronze_df,
    "green": green_bronze_df,
}.items():
    (
        bronze_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("source_month")
        .saveAsTable(BRONZE_TABLES[taxi_type])
    )

print("Tabelas Bronze publicadas com sucesso.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validação de volumetria

# COMMAND ----------

for table_name in BRONZE_TABLES.values():
    print(f"Tabela: {table_name}")
    (
        spark.read.table(table_name)
        .groupBy("source_month", "source_file")
        .count()
        .orderBy("source_month")
        .show(truncate=False)
    )
