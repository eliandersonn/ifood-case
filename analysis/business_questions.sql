-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Consultas de negócio
-- MAGIC
-- MAGIC As respostas são consumidas diretamente das tabelas Gold.

-- COMMAND ----------

-- Pergunta 1: média mensal de total_amount para Yellow Taxi.
SELECT
    pickup_month,
    avg_total_amount,
    sum_total_amount,
    trip_count
FROM workspace.nyc_taxi_gold.yellow_monthly_avg_total_amount
ORDER BY pickup_month;

-- COMMAND ----------

-- Pergunta 2: média de passageiros por hora em maio para Yellow + Green Taxi.
SELECT
    pickup_hour,
    avg_passenger_count,
    trip_count
FROM workspace.nyc_taxi_gold.all_taxis_may_hourly_avg_passenger_count
ORDER BY pickup_hour;
