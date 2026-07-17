# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup do Lakehouse
# MAGIC
# MAGIC Cria e valida as camadas Bronze, Silver e Gold do pipeline de táxis da NYC TLC.

# COMMAND ----------

print(f"Spark version: {spark.version}")

spark.sql(
    """
    SELECT
        current_catalog() AS catalog,
        current_schema() AS schema
    """
).show()

# COMMAND ----------

CATALOG = "workspace"

schemas = {
    "nyc_taxi_bronze": "Dados brutos e imutáveis da fonte NYC TLC.",
    "nyc_taxi_silver": "Dados tratados, validados e prontos para consumo SQL.",
    "nyc_taxi_gold": "Agregações analíticas orientadas ao negócio.",
}

for schema_name, comment in schemas.items():
    spark.sql(
        f"""
        CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema_name}
        COMMENT '{comment}'
        """
    )

spark.sql(
    f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG}.nyc_taxi_bronze.landing
    COMMENT 'Landing Zone com arquivos Parquet originais da NYC TLC.'
    """
)

print("Estrutura Lakehouse criada com sucesso.")

# COMMAND ----------

spark.sql(
    f"""
    SHOW SCHEMAS IN {CATALOG} LIKE 'nyc_taxi_*'
    """
).show()

display(dbutils.fs.ls("/Volumes/workspace/nyc_taxi_bronze/landing"))
