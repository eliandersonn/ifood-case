-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Pergunta 1 — Valor total médio por corrida Yellow em cada mês
-- MAGIC
-- MAGIC Calcula a média de `total_amount` entre as corridas Yellow válidas
-- MAGIC iniciadas em cada mês. A métrica representa o valor total médio por
-- MAGIC corrida, não o gasto médio por passageiro. Soma mensal e quantidade de
-- MAGIC corridas são informações auxiliares.

-- COMMAND ----------

SELECT
    DATE_FORMAT(tpep_pickup_datetime, 'yyyy-MM') AS pickup_month,
    ROUND(AVG(total_amount), 2) AS avg_total_amount,
    ROUND(SUM(total_amount), 2) AS sum_total_amount,
    COUNT(*) AS trip_count
FROM workspace.nyc_taxi_silver.yellow_taxi_trips
WHERE tpep_pickup_datetime >= TIMESTAMP '2023-01-01 00:00:00'
  AND tpep_pickup_datetime < TIMESTAMP '2023-06-01 00:00:00'
GROUP BY DATE_FORMAT(tpep_pickup_datetime, 'yyyy-MM')
ORDER BY pickup_month;
