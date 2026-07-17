# NYC Taxi Data Pipeline

Solução para o case técnico de Data Architect do iFood. O pipeline ingere os dados públicos de **Yellow** e **Green Taxi** da NYC TLC entre janeiro e maio de 2023, aplica tratamento com PySpark e disponibiliza dados em Delta Lake para consumo SQL e análises.

## Objetivos do case

- Preservar os arquivos de origem em uma Landing Zone.
- Modelar e criar as tabelas necessárias no Data Lake.
- Usar PySpark no processamento.
- Disponibilizar dados estruturados para consumo.
- Responder às duas perguntas analíticas solicitadas.

## Arquitetura

```text
NYC TLC Parquet
      |
      v
Landing Volume (arquivos originais)
      |
      v
Bronze Delta (dados brutos padronizados + rastreabilidade)
      |
      v
Silver Delta (dados limpos e consultáveis)
      |
      v
Gold Delta (agregações analíticas)
```

Detalhes em [docs/architecture.md](docs/architecture.md).

## Apresentação

A evolução do case é registrada em uma apresentação HTML curta e navegável:

- [Apresentação do case](docs/presentation.html)
- Use as setas do teclado para navegar.
- Use a impressão do navegador para gerar uma versão em PDF.

## Execução no Databricks Free Edition

Os notebooks devem ser executados nesta ordem:

1. `00_setup.py` — cria schemas e o Volume de Landing.
2. `01_bronze_ingestion.py` — valida os arquivos e publica as tabelas Bronze.
3. `02_silver_transformation.py` — aplica qualidade e publica tabelas Silver.
4. `03_gold_analytics.py` — cria agregações Gold e mostra as respostas.

> Os arquivos Parquet não são versionados. Eles devem ser baixados da NYC TLC e enviados ao Volume de Landing antes da etapa Bronze.

## Estrutura

```text
├── analysis/      # consultas SQL das perguntas do case
├── docs/          # requisitos, arquitetura e decisões técnicas
└── notebooks/     # notebooks PySpark para execução no Databricks
```

## Fonte de dados

- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- Período: janeiro a maio de 2023.
- Tipos: Yellow Taxi e Green Taxi.

## Arquivos esperados na Landing

O pipeline valida a presença dos dez arquivos antes de iniciar a Bronze:

```text
yellow_tripdata_2023-01.parquet até yellow_tripdata_2023-05.parquet
green_tripdata_2023-01.parquet até green_tripdata_2023-05.parquet
```
