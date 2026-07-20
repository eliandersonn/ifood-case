-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Análise exploratória e qualidade
-- MAGIC
-- MAGIC Verifica volumetria, distribuição mensal, completude, validade e os
-- MAGIC motivos de quarentena antes da interpretação das métricas de negócio.

-- COMMAND ----------

-- Controle de volumetria da Bronze por categoria e mês de origem.
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

-- Volumetria das saídas após a deduplicação: Silver válida e quarentena.
SELECT 'silver_valid' AS dataset, COUNT(*) AS row_count
FROM workspace.nyc_taxi_silver.taxi_trips_unified

UNION ALL

SELECT 'quarantine' AS dataset, COUNT(*) AS row_count
FROM workspace.nyc_taxi_silver.taxi_trips_quarantine;

-- COMMAND ----------

-- Reconciliação consolidada entre Bronze, duplicidades, Silver e quarentena.
-- As taxas usam como base os registros após a remoção de duplicidades.
WITH layer_counts AS (
    SELECT
        (SELECT COUNT(*)
         FROM workspace.nyc_taxi_bronze.yellow_taxi_trips_raw)
        +
        (SELECT COUNT(*)
         FROM workspace.nyc_taxi_bronze.green_taxi_trips_raw)
            AS bronze_total,
        (SELECT COUNT(*)
         FROM workspace.nyc_taxi_silver.taxi_trips_unified)
            AS silver_valid,
        (SELECT COUNT(*)
         FROM workspace.nyc_taxi_silver.taxi_trips_quarantine)
            AS quarantine
), reconciled AS (
    SELECT
        *,
        bronze_total - silver_valid - quarantine AS duplicates_removed,
        silver_valid + quarantine AS rows_after_deduplication
    FROM layer_counts
)
SELECT
    bronze_total,
    duplicates_removed,
    silver_valid,
    quarantine,
    ROUND(
        100.0 * silver_valid / rows_after_deduplication,
        2
    ) AS silver_acceptance_rate_pct,
    ROUND(
        100.0 * quarantine / rows_after_deduplication,
        2
    ) AS quarantine_rate_pct
FROM reconciled;
