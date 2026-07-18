# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Transformação Gold
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
# MAGIC Calcula a média de `total_amount` entre as corridas Yellow válidas
# MAGIC iniciadas em cada mês. O resultado representa o valor total médio por
# MAGIC corrida, não o gasto médio por passageiro. A soma mensal é mantida como
# MAGIC informação complementar para diferenciar média e valor total do mês.

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
# MAGIC Calcula a quantidade média de passageiros por corrida Yellow ou Green,
# MAGIC agrupada pela hora de início da corrida durante maio. Valores nulos,
# MAGIC zero ou negativos são excluídos somente desta métrica para não reduzir
# MAGIC artificialmente a média de passageiros por corrida.

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
# MAGIC ## Validação automática das tabelas Gold
# MAGIC
# MAGIC A primeira resposta deve conter todos os meses disponíveis na Silver e
# MAGIC a segunda deve cobrir as 24 horas do dia. A execução falha quando essa
# MAGIC cardinalidade não é atendida.

# COMMAND ----------

monthly_result_df = spark.sql(
    f"""
    SELECT *
    FROM {MONTHLY_AMOUNT_TABLE}
    ORDER BY pickup_month
    """
)

hourly_result_df = spark.sql(
    f"""
    SELECT *
    FROM {MAY_PASSENGERS_TABLE}
    ORDER BY pickup_hour
    """
)

expected_months = {
    row.source_month
    for row in yellow_silver_df.select("source_month").distinct().collect()
}
published_months = {
    row.pickup_month
    for row in monthly_result_df.select("pickup_month").collect()
}
expected_hours = set(range(24))
published_hours = {
    row.pickup_hour
    for row in hourly_result_df.select("pickup_hour").collect()
}

if published_months != expected_months:
    raise AssertionError(
        "Meses inesperados na Gold: "
        f"esperado={sorted(expected_months)}, "
        f"publicado={sorted(published_months)}"
    )

if published_hours != expected_hours:
    raise AssertionError(
        "Horas inesperadas na Gold: "
        f"esperado={sorted(expected_hours)}, "
        f"publicado={sorted(published_hours)}"
    )

print(
    "Cardinalidade Gold validada: "
    f"{len(expected_months)} meses e 24 horas."
)
monthly_result_df.show(5, truncate=False)
hourly_result_df.show(24, truncate=False)
