-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Insights executivos complementares
-- MAGIC
-- MAGIC Relaciona as métricas já solicitadas pelo case sem criar novas tabelas
-- MAGIC ou ampliar o escopo do pipeline. As consultas usam as camadas de
-- MAGIC consumo para evidenciar tendência, volume e representatividade.

-- COMMAND ----------

-- Evolução mensal de Yellow Taxi.
-- avg_total_amount continua sendo o valor total médio por corrida, não por
-- passageiro. As variações comparam cada mês ao mês imediatamente anterior.
WITH monthly_with_previous AS (
    SELECT
        pickup_month,
        avg_total_amount,
        sum_total_amount,
        trip_count,
        LAG(avg_total_amount) OVER (
            ORDER BY pickup_month
        ) AS previous_avg_total_amount,
        LAG(sum_total_amount) OVER (
            ORDER BY pickup_month
        ) AS previous_sum_total_amount,
        LAG(trip_count) OVER (
            ORDER BY pickup_month
        ) AS previous_trip_count
    FROM workspace.nyc_taxi_gold.yellow_monthly_avg_total_amount
)
SELECT
    pickup_month,
    avg_total_amount,
    sum_total_amount,
    trip_count,
    ROUND(
        100.0 * (
            avg_total_amount / previous_avg_total_amount - 1
        ),
        2
    ) AS avg_total_amount_mom_pct,
    ROUND(
        100.0 * (
            sum_total_amount / previous_sum_total_amount - 1
        ),
        2
    ) AS sum_total_amount_mom_pct,
    ROUND(
        100.0 * (
            trip_count / previous_trip_count - 1
        ),
        2
    ) AS trip_count_mom_pct
FROM monthly_with_previous
ORDER BY pickup_month;

-- COMMAND ----------

-- Demanda versus ocupação em maio.
-- trip_volume_rank ordena as horas por número de corridas; occupancy_rank
-- ordena pela quantidade média de passageiros por corrida.
SELECT
    pickup_hour,
    avg_passenger_count,
    trip_count,
    DENSE_RANK() OVER (
        ORDER BY trip_count DESC
    ) AS trip_volume_rank,
    DENSE_RANK() OVER (
        ORDER BY avg_passenger_count DESC
    ) AS occupancy_rank
FROM workspace.nyc_taxi_gold.all_taxis_may_hourly_avg_passenger_count
ORDER BY pickup_hour;

-- COMMAND ----------

-- Participação de cada categoria na Silver válida.
-- Essa distribuição contextualiza quanto Yellow e Green influenciam a média
-- combinada de passageiros por corrida.
WITH trips_by_type AS (
    SELECT
        taxi_type,
        COUNT(*) AS trip_count
    FROM workspace.nyc_taxi_silver.taxi_trips_unified
    GROUP BY taxi_type
)
SELECT
    taxi_type,
    trip_count,
    ROUND(
        100.0 * trip_count / SUM(trip_count) OVER (),
        2
    ) AS valid_trip_share_pct
FROM trips_by_type
ORDER BY trip_count DESC;
