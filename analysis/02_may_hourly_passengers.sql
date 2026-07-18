-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Pergunta 2 — Passageiros por corrida e hora de início em maio
-- MAGIC
-- MAGIC Calcula a quantidade média de passageiros por corrida Yellow ou Green,
-- MAGIC agrupada pela hora em que a corrida começou durante maio. Somente
-- MAGIC corridas com `passenger_count` positivo participam da média.

-- COMMAND ----------

SELECT
    HOUR(pickup_datetime) AS pickup_hour,
    ROUND(AVG(passenger_count), 2) AS avg_passenger_count,
    COUNT(*) AS trip_count
FROM workspace.nyc_taxi_silver.taxi_trips_unified
WHERE pickup_datetime >= TIMESTAMP '2023-05-01 00:00:00'
  AND pickup_datetime < TIMESTAMP '2023-06-01 00:00:00'
  AND passenger_count > 0
GROUP BY HOUR(pickup_datetime)
ORDER BY pickup_hour;
