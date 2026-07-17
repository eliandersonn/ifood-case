-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Pergunta 2 — Média de passageiros por hora em maio
-- MAGIC
-- MAGIC Retorna as 24 horas do dia considerando Yellow e Green Taxi. Registros
-- MAGIC com passenger_count nulo, zero ou negativo já foram excluídos somente
-- MAGIC desta métrica durante a materialização Gold.

-- COMMAND ----------

SELECT
    pickup_hour,
    avg_passenger_count,
    trip_count
FROM workspace.nyc_taxi_gold.all_taxis_may_hourly_avg_passenger_count
ORDER BY pickup_hour;
