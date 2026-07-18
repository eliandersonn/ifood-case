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

### Motivos de quarentena

| Regra | Interpretação | Ocorrências |
|---|---|---:|
| `non_positive_total_amount` | valor total da corrida menor ou igual a zero | 145.444 |
| `non_positive_trip_duration` | término da corrida anterior ou igual ao início | 6.596 |
| `pickup_outside_source_month` | início da corrida fora do mês indicado pelo arquivo | 485 |
| `trip_duration_over_24h` | duração da corrida superior a 24 horas | 94 |

As regras de campos essenciais nulos também são avaliadas, mas não tiveram
ocorrências neste processamento. Uma corrida pode possuir mais de um motivo;
por isso, a soma das ocorrências não representa a quantidade de registros em
quarentena. As 138 duplicidades foram removidas em um controle separado.

### Perfil exploratório da Silver válida

| Categoria | Corridas válidas | Passageiros nulos | Passageiros ≤ 0 | Menor total_amount | Maior total_amount |
|---|---:|---:|---:|---:|---:|
| Green | 337.889 | 22.857 | 2.391 | 0,13 | 744,95 |
| Yellow | 16.035.750 | 427.577 | 272.831 | 0,01 | 6.304,90 |

As datas de início observadas permanecem dentro do recorte esperado, de 1º de
janeiro a 31 de maio de 2023. `passenger_count` nulo ou não positivo foi
mantido na Silver porque não invalida o valor financeiro da corrida, mas não
participa da média de passageiros por corrida.

## Pergunta 1 — Valor total médio por corrida Yellow em cada mês

Resultado calculado sobre as corridas Yellow válidas da camada Silver,
agrupadas pelo mês de início. A média representa `AVG(total_amount)` por
corrida; não representa gasto médio por passageiro.

| Mês de início | Valor total médio por corrida | Soma de total_amount | Corridas válidas |
|---|---:|---:|---:|
| 2023-01 | 27,45 | 83.439.216,07 | 3.039.843 |
| 2023-02 | 27,34 | 78.931.217,30 | 2.887.195 |
| 2023-03 | 28,27 | 95.310.384,64 | 3.371.777 |
| 2023-04 | 28,76 | 93.670.549,73 | 3.256.737 |
| 2023-05 | 29,46 | 102.536.709,05 | 3.480.198 |

Entre janeiro e maio, o valor total médio por corrida aumentou aproximadamente
7,3%. Maio apresentou a maior média, a maior soma mensal e o maior volume de
corridas válidas do período.

## Pergunta 2 — Passageiros por corrida e hora de início em maio

Resultado calculado sobre corridas Yellow e Green válidas iniciadas em maio.
Para cada hora do dia, a métrica calcula `AVG(passenger_count)` por corrida.
Registros com `passenger_count` nulo, zero ou negativo são excluídos somente
deste cálculo.

| Hora de início | Passageiros médios por corrida | Corridas consideradas |
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
positiva. A maior quantidade média de passageiros por corrida ocorreu às 2h e
3h (1,45), enquanto a menor ocorreu às 6h (1,26). O maior volume de corridas
consideradas ocorreu às 18h.

## Insights complementares

- entre janeiro e maio, o valor total médio por corrida Yellow aumentou 7,3%,
  o volume de corridas válidas aumentou 14,5% e a soma de `total_amount`
  aumentou 22,9%;
- a maior quantidade média de passageiros por corrida ocorreu às 2h e 3h,
  mas o maior volume de corridas ocorreu às 18h: ocupação e demanda não atingem
  seus máximos no mesmo horário;
- Yellow representa 97,94% das corridas válidas, contra 2,06% de Green. Logo,
  a média combinada de passageiros é predominantemente influenciada por
  Yellow;
- após a deduplicação, 99,08% dos registros foram aprovados na Silver e 0,92%
  foram preservados em quarentena.

Esses cruzamentos são reproduzíveis em `analysis/03_executive_insights.sql` e
não criam novas métricas Gold nem alteram as respostas obrigatórias do case.
