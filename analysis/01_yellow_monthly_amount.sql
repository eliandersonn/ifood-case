-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Pergunta 1 — Média mensal de total_amount
-- MAGIC
-- MAGIC Calcula o ticket médio mensal das corridas Yellow válidas. A soma e a
-- MAGIC quantidade de corridas são mantidas para contextualizar a resposta e
-- MAGIC eliminar a ambiguidade entre média por corrida e receita mensal.

-- COMMAND ----------

SELECT
    pickup_month,
    avg_total_amount,
    sum_total_amount,
    trip_count
FROM workspace.nyc_taxi_gold.yellow_monthly_avg_total_amount
ORDER BY pickup_month;
