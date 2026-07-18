-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Pergunta 2 — Média de passageiros por hora em maio
-- MAGIC
-- MAGIC Considera Yellow e Green Taxi por meio da tabela unificada. A hora é
-- MAGIC extraída do pickup, momento em que o passageiro iniciou a corrida.

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
