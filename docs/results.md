# Resultados do Case

## Volumetria e qualidade

| Etapa | Registros |
|---|---:|
| Bronze Yellow | 16.186.386 |
| Bronze Green | 339.630 |
| Bronze total | 16.526.016 |
| Duplicados removidos | 138 |
| Silver válida | 16.373.639 |
| Quarentena | 152.239 |

A quarentena representa aproximadamente 0,92% dos registros após a
deduplicação. Os dados rejeitados continuam disponíveis com os motivos em
`workspace.nyc_taxi_silver.taxi_trips_quarantine`.

## Pergunta 1 — Média mensal de total_amount

Resultado calculado sobre as corridas Yellow válidas da camada Silver.

| Mês | Média | Soma mensal | Corridas |
|---|---:|---:|---:|
| 2023-01 | 27,45 | 83.439.216,07 | 3.039.843 |
| 2023-02 | 27,34 | 78.931.217,30 | 2.887.195 |
| 2023-03 | 28,27 | 95.310.384,64 | 3.371.777 |
| 2023-04 | 28,76 | 93.670.549,73 | 3.256.737 |
| 2023-05 | 29,46 | 102.536.709,05 | 3.480.198 |

Entre janeiro e maio, o ticket médio aumentou aproximadamente 7,3%. Maio
apresentou a maior média, a maior soma mensal e o maior volume de corridas
válidas do período.

## Pergunta 2 — Média de passageiros por hora em maio

Resultado calculado sobre Yellow e Green combinados. Registros com
`passenger_count` nulo, zero ou negativo são excluídos somente desta métrica.

| Hora | Média de passageiros | Corridas |
|---:|---:|---:|
| 0 | 1,43 | 89.602 |
| 1 | 1,44 | 58.245 |
| 2 | 1,45 | 37.559 |
| 3 | 1,45 | 24.545 |
| 4 | 1,40 | 16.067 |
| 5 | 1,28 | 18.555 |
| 6 | 1,26 | 46.466 |
| 7 | 1,28 | 94.115 |
| 8 | 1,29 | 128.331 |
| 9 | 1,31 | 144.038 |
| 10 | 1,35 | 156.609 |
| 11 | 1,36 | 170.637 |
| 12 | 1,37 | 183.820 |
| 13 | 1,38 | 187.745 |
| 14 | 1,39 | 204.538 |
| 15 | 1,40 | 209.179 |
| 16 | 1,40 | 209.682 |
| 17 | 1,39 | 228.791 |
| 18 | 1,38 | 242.975 |
| 19 | 1,39 | 217.714 |
| 20 | 1,40 | 193.072 |
| 21 | 1,42 | 196.852 |
| 22 | 1,43 | 181.622 |
| 23 | 1,42 | 141.608 |

Foram consideradas 3.382.367 corridas de maio com contagem de passageiros
positiva. A maior média ocorreu às 2h e 3h (1,45), enquanto a menor ocorreu às
6h (1,26). O maior volume de corridas da métrica ocorreu às 18h.
