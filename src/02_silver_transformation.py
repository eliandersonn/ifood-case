# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Transformação Silver
# MAGIC
# MAGIC Padroniza Yellow e Green Taxi, aplica regras de qualidade, registra os
# MAGIC dados rejeitados em quarentena e publica tabelas Delta para consumo SQL.

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType

CATALOG = "workspace"
BRONZE_SCHEMA = "nyc_taxi_bronze"
SILVER_SCHEMA = "nyc_taxi_silver"

YELLOW_BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.yellow_taxi_trips_raw"
GREEN_BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.green_taxi_trips_raw"

YELLOW_SILVER_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.yellow_taxi_trips"
GREEN_SILVER_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.green_taxi_trips"
UNIFIED_SILVER_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.taxi_trips_unified"
QUARANTINE_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.taxi_trips_quarantine"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Padronização entre Yellow e Green Taxi
# MAGIC
# MAGIC Os nomes `tpep_*` e `lpep_*` são normalizados somente na tabela unificada.
# MAGIC A tabela Yellow preserva os nomes obrigatórios definidos pelo case.

# COMMAND ----------

def standardize_trips(
    source_df: DataFrame,
    taxi_type: str,
    pickup_column: str,
    dropoff_column: str,
) -> DataFrame:
    """Converte as duas fontes para um contrato comum de processamento."""

    return source_df.select(
        F.col("VendorID").cast(IntegerType()).alias("VendorID"),
        F.col("passenger_count").cast(IntegerType()).alias("passenger_count"),
        F.col("total_amount")
        .cast(DecimalType(12, 2))
        .alias("total_amount"),
        F.col(pickup_column).alias("pickup_datetime"),
        F.col(dropoff_column).alias("dropoff_datetime"),
        F.lit(taxi_type).alias("taxi_type"),
        "source_file",
        "source_month",
        "ingested_at",
    )


yellow_standard_df = standardize_trips(
    spark.read.table(YELLOW_BRONZE_TABLE),
    taxi_type="yellow",
    pickup_column="tpep_pickup_datetime",
    dropoff_column="tpep_dropoff_datetime",
)
green_standard_df = standardize_trips(
    spark.read.table(GREEN_BRONZE_TABLE),
    taxi_type="green",
    pickup_column="lpep_pickup_datetime",
    dropoff_column="lpep_dropoff_datetime",
)

all_trips_df = yellow_standard_df.unionByName(green_standard_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Qualidade e deduplicação
# MAGIC
# MAGIC São avaliadas completude dos campos essenciais, validade dos valores e
# MAGIC horários, consistência com o mês de origem e unicidade por chave composta.
# MAGIC Registros reprovados são preservados com os motivos na quarentena.
# MAGIC Na ausência de um identificador único de corrida, registros coincidentes
# MAGIC em `VendorID`, `passenger_count`, `total_amount`, pickup, dropoff e
# MAGIC `taxi_type` são tratados como duplicidades.
# MAGIC
# MAGIC Uma corrida é enviada para a quarentena quando possui `VendorID`,
# MAGIC `total_amount`, pickup ou dropoff nulo; `total_amount` não positivo;
# MAGIC duração não positiva ou superior a 24 horas; ou pickup fora do mês
# MAGIC declarado pelo arquivo. Uma corrida pode registrar mais de um motivo.
# MAGIC
# MAGIC `passenger_count` nulo ou não positivo não invalida a corrida para a
# MAGIC análise financeira. Esses registros permanecem na Silver e são excluídos
# MAGIC apenas da métrica de passageiros na Gold.

# COMMAND ----------

month_start = F.to_date(F.concat(F.col("source_month"), F.lit("-01")))
month_end = F.add_months(month_start, 1)
trip_duration_seconds = (
    F.col("dropoff_datetime").cast("long")
    - F.col("pickup_datetime").cast("long")
)

quality_checked_df = (
    all_trips_df.withColumn(
        "dq_failures",
        F.array_compact(
            F.array(
                F.when(F.col("VendorID").isNull(), F.lit("null_vendor_id")),
                F.when(
                    F.col("total_amount").isNull(),
                    F.lit("null_total_amount"),
                ),
                F.when(
                    F.col("total_amount") <= 0,
                    F.lit("non_positive_total_amount"),
                ),
                F.when(
                    F.col("pickup_datetime").isNull(),
                    F.lit("null_pickup_datetime"),
                ),
                F.when(
                    F.col("dropoff_datetime").isNull(),
                    F.lit("null_dropoff_datetime"),
                ),
                F.when(
                    trip_duration_seconds <= 0,
                    F.lit("non_positive_trip_duration"),
                ),
                F.when(
                    trip_duration_seconds > 24 * 60 * 60,
                    F.lit("trip_duration_over_24h"),
                ),
                F.when(
                    (F.col("pickup_datetime") < month_start)
                    | (F.col("pickup_datetime") >= month_end),
                    F.lit("pickup_outside_source_month"),
                ),
            )
        ),
    )
    .dropDuplicates(
        [
            "VendorID",
            "passenger_count",
            "total_amount",
            "pickup_datetime",
            "dropoff_datetime",
            "taxi_type",
        ]
    )
)

valid_trips_df = quality_checked_df.filter(F.size("dq_failures") == 0).drop(
    "dq_failures"
)
quarantine_df = quality_checked_df.filter(F.size("dq_failures") > 0)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Tabelas de consumo
# MAGIC
# MAGIC Publica tabelas válidas por categoria, uma visão unificada das corridas
# MAGIC válidas e a quarentena com os motivos de reprovação. As colunas `tpep_*`
# MAGIC obrigatórias do case são preservadas na tabela Yellow.

# COMMAND ----------

yellow_silver_df = valid_trips_df.filter(F.col("taxi_type") == "yellow").select(
    "VendorID",
    "passenger_count",
    "total_amount",
    F.col("pickup_datetime").alias("tpep_pickup_datetime"),
    F.col("dropoff_datetime").alias("tpep_dropoff_datetime"),
    "source_file",
    "source_month",
    "ingested_at",
)

green_silver_df = valid_trips_df.filter(F.col("taxi_type") == "green").select(
    "VendorID",
    "passenger_count",
    "total_amount",
    F.col("pickup_datetime").alias("lpep_pickup_datetime"),
    F.col("dropoff_datetime").alias("lpep_dropoff_datetime"),
    "source_file",
    "source_month",
    "ingested_at",
)

silver_outputs = {
    YELLOW_SILVER_TABLE: yellow_silver_df,
    GREEN_SILVER_TABLE: green_silver_df,
    UNIFIED_SILVER_TABLE: valid_trips_df,
    QUARANTINE_TABLE: quarantine_df,
}

for table_name, output_df in silver_outputs.items():
    (
        output_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("source_month")
        .saveAsTable(table_name)
    )

print("Tabelas Silver e quarentena publicadas com sucesso.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validação do contrato e reconciliação das camadas
# MAGIC
# MAGIC A execução falha se o contrato obrigatório estiver incompleto, se Yellow
# MAGIC e Green não fecharem com a Silver unificada ou se as saídas superarem a
# MAGIC Bronze. O saldo entre Bronze, Silver e quarentena representa as
# MAGIC duplicidades removidas antes da separação entre registros válidos e rejeitados.

# COMMAND ----------

required_consumption_columns = {
    "VendorID",
    "passenger_count",
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
}
published_columns = set(spark.read.table(YELLOW_SILVER_TABLE).columns)
missing_contract_columns = sorted(
    required_consumption_columns - published_columns
)

if missing_contract_columns:
    raise AssertionError(
        f"Contrato de consumo incompleto: {missing_contract_columns}"
    )

print(
    "Contrato Yellow validado: "
    + ", ".join(sorted(required_consumption_columns))
)

reconciliation_row = spark.sql(
    f"""
    WITH layer_counts AS (
        SELECT
            (SELECT COUNT(*) FROM {YELLOW_BRONZE_TABLE})
                + (SELECT COUNT(*) FROM {GREEN_BRONZE_TABLE})
                AS bronze_total,
            (SELECT COUNT(*) FROM {YELLOW_SILVER_TABLE}) AS yellow_valid,
            (SELECT COUNT(*) FROM {GREEN_SILVER_TABLE}) AS green_valid,
            (SELECT COUNT(*) FROM {UNIFIED_SILVER_TABLE}) AS silver_valid,
            (SELECT COUNT(*) FROM {QUARANTINE_TABLE}) AS quarantine
    )
    SELECT
        *,
        bronze_total - silver_valid - quarantine AS duplicates_removed
    FROM layer_counts
    """
).first()

if reconciliation_row.yellow_valid + reconciliation_row.green_valid != (
    reconciliation_row.silver_valid
):
    raise AssertionError(
        "Reconciliação inválida: Yellow + Green difere da Silver unificada."
    )

if reconciliation_row.duplicates_removed < 0:
    raise AssertionError(
        "Reconciliação inválida: as saídas superam o volume da Bronze."
    )

if reconciliation_row.bronze_total != (
    reconciliation_row.silver_valid
    + reconciliation_row.quarantine
    + reconciliation_row.duplicates_removed
):
    raise AssertionError(
        "Reconciliação inválida entre Bronze, Silver e quarentena."
    )

print("Reconciliação automática validada com sucesso.")
spark.createDataFrame([reconciliation_row]).show(truncate=False)

(
    spark.read.table(QUARANTINE_TABLE)
    .select(F.explode("dq_failures").alias("dq_failure"))
    .groupBy("dq_failure")
    .count()
    .orderBy(F.desc("count"))
    .show(truncate=False)
)
