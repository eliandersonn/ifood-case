-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Análise exploratória e qualidade
-- MAGIC
-- MAGIC Verifica volumetria, distribuição mensal, completude, validade e os
-- MAGIC motivos de quarentena antes da interpretação das métricas de negócio.

-- COMMAND ----------

-- Reconciliação da Bronze por categoria e mês de origem.
SELECT
    'yellow' AS taxi_type,
    source_month,
    COUNT(*) AS row_count
FROM workspace.nyc_taxi_bronze.yellow_taxi_trips_raw
GROUP BY source_month

UNION ALL

SELECT
    'green' AS taxi_type,
    source_month,
    COUNT(*) AS row_count
FROM workspace.nyc_taxi_bronze.green_taxi_trips_raw
GROUP BY source_month

ORDER BY taxi_type, source_month;

-- COMMAND ----------

-- Perfil da Silver: volume, completude de passageiros e intervalos observados.
SELECT
    taxi_type,
    COUNT(*) AS row_count,
    COUNT_IF(passenger_count IS NULL) AS null_passenger_count,
    COUNT_IF(passenger_count <= 0) AS non_positive_passenger_count,
    MIN(total_amount) AS min_total_amount,
    MAX(total_amount) AS max_total_amount,
    MIN(pickup_datetime) AS min_pickup_datetime,
    MAX(pickup_datetime) AS max_pickup_datetime
FROM workspace.nyc_taxi_silver.taxi_trips_unified
GROUP BY taxi_type
ORDER BY taxi_type;

-- COMMAND ----------

-- Distribuição dos registros rejeitados por regra de qualidade.
SELECT
    dq_failure,
    COUNT(*) AS failure_count
FROM (
    SELECT EXPLODE(dq_failures) AS dq_failure
    FROM workspace.nyc_taxi_silver.taxi_trips_quarantine
)
GROUP BY dq_failure
ORDER BY failure_count DESC;

-- COMMAND ----------

-- Reconciliação final entre dados aprovados e quarentena.
SELECT 'silver_valid' AS dataset, COUNT(*) AS row_count
FROM workspace.nyc_taxi_silver.taxi_trips_unified

UNION ALL

SELECT 'quarantine' AS dataset, COUNT(*) AS row_count
FROM workspace.nyc_taxi_silver.taxi_trips_quarantine;
