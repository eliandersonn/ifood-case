# Arquitetura Lakehouse

## Tecnologia escolhida

- **Databricks Free Edition** para processamento serverless com Apache Spark.
- **Unity Catalog** como catálogo de metadados.
- **Unity Catalog Volume** como Landing Zone.
- **Delta Lake** para tabelas Bronze, Silver e Gold.

Essa escolha entrega armazenamento governado, tabelas transacionais e consumo por SQL sem introduzir infraestrutura de nuvem adicional para o case.

O enunciado recomenda a antiga Community Edition. A Databricks Free Edition
é sua substituta atual e mantém a proposta de uso gratuito, acrescentando
compute Serverless e recursos modernos da plataforma. O Unity Catalog foi
adotado para centralizar metadados e governar tanto as tabelas quanto os
arquivos da Landing por meio de um Volume gerenciado. Para este case, isso
simula uma prática corporativa sem exigir configuração de S3 ou credenciais de
nuvem.

## Camadas

| Camada | Objeto | Responsabilidade |
|---|---|---|
| Landing | `workspace.nyc_taxi_bronze.landing` | Download idempotente e preservação dos Parquets originais. |
| Bronze | tabelas Delta por tipo de táxi | Padronizar tipos e registrar rastreabilidade, sem regras de negócio. |
| Silver | tabelas Delta limpas | Aplicar regras de qualidade e disponibilizar dados estruturados. |
| Gold | tabelas Delta agregadas | Materializar respostas analíticas prontas para SQL. |

## Modelo físico proposto

### Bronze

- `workspace.nyc_taxi_bronze.yellow_taxi_trips_raw`
- `workspace.nyc_taxi_bronze.green_taxi_trips_raw`

Além das colunas de origem, ambas recebem `source_file`, `source_month` e `ingested_at`. Os arquivos são lidos individualmente e têm o schema convertido antes do `unionByName`, prevenindo incompatibilidades de tipo entre meses.

O notebook Bronze mantém duas responsabilidades separadas em seções: primeiro
baixa da fonte oficial para a Landing, com streaming, retry e escrita temporária;
depois lê a Landing com Spark e publica Delta. Os widgets `start_month` e
`end_month` permitem ampliar o período sem alterar a lógica.

### Silver

- `workspace.nyc_taxi_silver.yellow_taxi_trips`
- `workspace.nyc_taxi_silver.green_taxi_trips`
- `workspace.nyc_taxi_silver.taxi_trips_unified`

A tabela Yellow mantém o contrato `tpep_*` exigido pelo case. A tabela unificada normaliza os horários como `pickup_datetime` e `dropoff_datetime`, inclui `taxi_type` e é a fonte da análise de todos os táxis.

### Gold

- `workspace.nyc_taxi_gold.yellow_monthly_avg_total_amount`
- `workspace.nyc_taxi_gold.all_taxis_may_hourly_avg_passenger_count`

## Evolução para produção

O recorte do case é fixo e pequeno para Spark, portanto o pipeline usa full refresh com `overwrite`. Essa estratégia é simples e idempotente: uma nova execução substitui as tabelas sem duplicar registros. Na Landing, arquivos existentes são ignorados e somente meses novos são baixados. A coluna `source_month` e o particionamento mensal deixam explícita a evolução para `replaceWhere` ou `MERGE` em cargas incrementais de produção.

Para um histórico de vários anos, a evolução seria incremental:

1. identificar somente os meses novos ou alterados;
2. escrever Bronze e Silver com `replaceWhere` por `source_month` ou usar
   `MERGE INTO` quando houver atualização de registros;
3. recalcular na Gold apenas as partições mensais afetadas;
4. orquestrar as etapas em um Job com alertas de falha, volumetria e qualidade.

O desenho atual preserva `source_month`, metadados de origem e partições para
permitir essa evolução sem remodelar as tabelas.
