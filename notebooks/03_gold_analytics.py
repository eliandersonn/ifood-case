# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold e Análises
# MAGIC
# MAGIC Materializa as respostas das duas perguntas do case em tabelas Delta
# MAGIC prontas para consulta SQL.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SILVER_SCHEMA = "nyc_taxi_silver"
GOLD_SCHEMA = "nyc_taxi_gold"

YELLOW_SILVER_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.yellow_taxi_trips"
UNIFIED_SILVER_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.taxi_trips_unified"

MONTHLY_AMOUNT_TABLE = (
    f"{CATALOG}.{GOLD_SCHEMA}.yellow_monthly_avg_total_amount"
)
MAY_PASSENGERS_TABLE = (
    f"{CATALOG}.{GOLD_SCHEMA}.all_taxis_may_hourly_avg_passenger_count"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pergunta 1
# MAGIC
# MAGIC Qual a média de `total_amount` recebida em cada mês, considerando todos
# MAGIC os Yellow Taxis? A soma mensal é mantida como informação complementar
# MAGIC para eliminar a ambiguidade entre ticket médio e receita total.

# COMMAND ----------

yellow_silver_df = spark.read.table(YELLOW_SILVER_TABLE)

monthly_amount_df = (
    yellow_silver_df.withColumn(
        "pickup_month",
        F.date_format("tpep_pickup_datetime", "yyyy-MM"),
    )
    .groupBy("pickup_month")
    .agg(
        F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
        F.round(F.sum("total_amount"), 2).alias("sum_total_amount"),
        F.count(F.lit(1)).alias("trip_count"),
    )
    .orderBy("pickup_month")
)

(
    monthly_amount_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(MONTHLY_AMOUNT_TABLE)
)

display(monthly_amount_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pergunta 2
# MAGIC
# MAGIC Qual a média de `passenger_count` por hora do dia em maio, considerando
# MAGIC Yellow e Green Taxi? Valores nulos, zero ou negativos são excluídos
# MAGIC somente desta métrica para não distorcer a média de passageiros.

# COMMAND ----------

all_taxis_df = spark.read.table(UNIFIED_SILVER_TABLE)

may_passengers_df = (
    all_taxis_df.filter(
        (F.col("pickup_datetime") >= F.lit("2023-05-01").cast("timestamp"))
        & (F.col("pickup_datetime") < F.lit("2023-06-01").cast("timestamp"))
        & (F.col("passenger_count") > 0)
    )
    .withColumn("pickup_hour", F.hour("pickup_datetime"))
    .groupBy("pickup_hour")
    .agg(
        F.round(F.avg("passenger_count"), 2).alias(
            "avg_passenger_count"
        ),
        F.count(F.lit(1)).alias("trip_count"),
    )
    .orderBy("pickup_hour")
)

(
    may_passengers_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(MAY_PASSENGERS_TABLE)
)

display(may_passengers_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validação das tabelas Gold

# COMMAND ----------

spark.sql(
    f"""
    SELECT *
    FROM {MONTHLY_AMOUNT_TABLE}
    ORDER BY pickup_month
    """
).show(5, truncate=False)

spark.sql(
    f"""
    SELECT *
    FROM {MAY_PASSENGERS_TABLE}
    ORDER BY pickup_hour
    """
).show(24, truncate=False)
