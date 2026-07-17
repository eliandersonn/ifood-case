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
2. `01_bronze_ingestion.py` — baixa os arquivos para a Landing e publica as tabelas Bronze.
3. `02_silver_transformation.py` — aplica qualidade e publica tabelas Silver.
4. `03_gold_analytics.py` — cria agregações Gold e mostra as respostas.

> Os arquivos Parquet não são versionados. O notebook Bronze baixa os arquivos
> oficiais diretamente para o Volume de Landing e ignora os que já existem.

## Estrutura

```text
├── analysis/      # consultas SQL das perguntas do case
├── docs/          # requisitos, arquitetura e decisões técnicas
└── notebooks/     # notebooks PySpark para execução no Databricks
```

## Fonte de dados

- Catálogo oficial: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
- Endpoint dos Parquets: `https://d37ci6vzurychx.cloudfront.net/trip-data`.
- Período: janeiro a maio de 2023.
- Tipos: Yellow Taxi e Green Taxi.

A página da NYC TLC é a referência institucional. Os links de download
publicados nela apontam para o CloudFront, utilizado pelo pipeline para obter
os arquivos Parquet.

## Aquisição dos arquivos

Por padrão, o pipeline baixa e valida os dez arquivos do período do case:

```text
yellow_tripdata_2023-01.parquet até yellow_tripdata_2023-05.parquet
green_tripdata_2023-01.parquet até green_tripdata_2023-05.parquet
```

Os widgets `start_month` e `end_month` controlam o período sem exigir
alteração do código. Para incluir junho, por exemplo, execute o notebook com
`end_month=2023-06`; somente os arquivos ausentes serão baixados.

A carga Bronze usa full refresh para manter o recorte do case simples e
idempotente. Em uma evolução incremental, a escrita mensal pode usar
`replaceWhere` ou `MERGE` para reprocessar somente as partições afetadas.
