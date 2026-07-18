# NYC Taxi Data Pipeline — Case iFood

Solução do case técnico de Data Architect do iFood. O pipeline ingere os
dados públicos de **Yellow** e **Green Taxi** da NYC TLC entre janeiro e maio
de 2023, aplica tratamento com PySpark e disponibiliza tabelas Delta para
consumo SQL.

## O que a solução entrega

- aquisição automática dos Parquets oficiais da NYC TLC;
- preservação dos arquivos originais em uma Landing Zone;
- tabelas Bronze com schema técnico e rastreabilidade;
- tabelas Silver validadas, deduplicadas e prontas para consumo;
- quarentena com os registros rejeitados e seus motivos;
- tabelas Gold e consultas SQL para as duas perguntas do case;
- análise exploratória com volumetria e indicadores de qualidade.

## Arquitetura

```text
NYC TLC Parquet
      |
      v
Landing Volume (arquivos originais)
      |
      v
Bronze Delta (contrato técnico + rastreabilidade)
      |
      v
Silver Delta (qualidade + deduplicação + quarentena)
      |
      v
Gold Delta (agregações do case)
      |
      v
Databricks SQL (consumo e apresentação dos resultados)
```

Detalhes em [docs/architecture.md](docs/architecture.md) e
[docs/decisions.md](docs/decisions.md).

## Pré-requisitos

- uma conta no Databricks Free Edition;
- acesso a este repositório no GitHub;
- acesso à internet no workspace para baixar os arquivos da NYC TLC;
- compute Serverless disponível.

Não é necessário instalar Python, PySpark ou Databricks Connect para executar
o case. O Databricks já fornece Spark, Delta Lake e `dbutils`.

> O enunciado menciona a antiga Community Edition. A Free Edition é sua
> substituta atual e permite executar o case com Serverless, Unity Catalog e
> Volumes, sem exigir uma conta de nuvem externa.

## Execução completa no Databricks

### 1. Adicionar o repositório ao workspace

1. No Databricks, abra **Workspace**.
2. Selecione **Create > Git folder**.
3. Informe a URL deste repositório:
   `https://github.com/eliandersonn/ifood-case`.
4. Se o repositório estiver privado, conecte sua conta GitHub em
   **Settings > Linked accounts** e autorize o acesso ao repositório.
5. Abra a pasta Git criada e confirme que `src/`, `analysis/` e `docs/`
   estão disponíveis.

### 2. Executar o pipeline

Abra cada arquivo e use **Run all**, selecionando o compute **Serverless**.
Execute exatamente nesta ordem:

1. `src/00_setup.py`
   - cria os schemas Bronze, Silver e Gold;
   - cria o Volume gerenciado usado como Landing Zone.
2. `src/01_bronze_ingestion.py`
   - baixa os dez Parquets oficiais;
   - valida os arquivos e as colunas obrigatórias;
   - publica as tabelas Delta Bronze.
3. `src/02_silver_transformation.py`
   - padroniza Yellow e Green Taxi;
   - aplica qualidade e deduplicação;
   - publica Silver e quarentena.
4. `src/03_gold_analytics.py`
   - calcula e publica as duas respostas analíticas.

O notebook Bronze possui os parâmetros:

| Parâmetro | Padrão | Finalidade |
|---|---|---|
| `start_month` | `2023-01` | primeiro mês processado |
| `end_month` | `2023-05` | último mês processado |

Para reproduzir o case, mantenha os valores padrão. Para acrescentar junho,
por exemplo, altere apenas `end_month` para `2023-06`.

### 3. Validar a execução

Ao final dos notebooks, confirme no **Catalog** a existência dos schemas:

```text
workspace.nyc_taxi_bronze
workspace.nyc_taxi_silver
workspace.nyc_taxi_gold
```

O notebook Silver valida automaticamente o contrato da tabela Yellow após a
publicação e interrompe a execução se qualquer coluna obrigatória estiver
ausente: `VendorID`, `passenger_count`, `total_amount`,
`tpep_pickup_datetime` ou `tpep_dropoff_datetime`.

Para o recorte original do case, as principais volumetrias esperadas são:

| Validação | Registros |
|---|---:|
| Bronze Yellow | 16.186.386 |
| Bronze Green | 339.630 |
| Silver válida | 16.373.639 |
| Quarentena | 152.239 |

Uma diferença de volumetria deve ser investigada antes de interpretar as
métricas, pois a fonte oficial pode publicar uma nova versão dos arquivos.

### 4. Executar as análises SQL

Execute os arquivos da pasta `analysis/`:

1. `00_data_quality_overview.sql` — perfil e qualidade dos dados;
2. `01_yellow_monthly_amount.sql` — média mensal de `total_amount`;
3. `02_may_hourly_passengers.sql` — média de passageiros por hora em maio.

Os dois SQLs de negócio calculam as respostas diretamente a partir da Silver.
As tabelas Gold materializam os mesmos resultados para consumo recorrente.

## Qualidade dos dados

O pipeline aplica controles objetivos e mantém os registros rejeitados em
quarentena, sem apagá-los silenciosamente:

| Dimensão | Controle aplicado |
|---|---|
| Conformidade | validação das colunas obrigatórias e conversão explícita de tipos |
| Completude | campos essenciais não podem ser nulos |
| Validade | `total_amount` deve ser positivo e a duração deve ser maior que zero e de no máximo 24 horas |
| Consistência | pickup deve pertencer ao mês indicado pelo arquivo de origem |
| Unicidade | remoção de corridas duplicadas por seus principais atributos |
| Rastreabilidade | `source_file`, `source_month` e `ingested_at` na Bronze |
| Reconciliação | contagens por tipo, mês e camada ao final de cada etapa |

`passenger_count` nulo ou não positivo não invalida uma corrida financeira.
Esses registros permanecem na Silver, mas são excluídos da métrica de média de
passageiros. A análise exploratória identificou essas ocorrências como dados
inadequados para representar indivíduos transportados; incluí-las reduziria a
média artificialmente.

## Idempotência e novos meses

- arquivos válidos já presentes na Landing não são baixados novamente;
- as tabelas usam full refresh para tornar o recorte pequeno do case simples e
  determinístico;
- uma nova execução substitui as tabelas e não duplica registros.

## Próximos passos — evolução para produção

Para um histórico de vários anos, o pipeline deixaria de reconstruir todo o
período:

1. detectar somente os meses novos ou alterados;
2. usar `replaceWhere` por `source_month` ou `MERGE INTO` para atualizações;
3. recalcular somente as partições Gold afetadas;
4. executar as etapas em um Job com alertas de falha, volumetria e qualidade.

O particionamento e os metadados atuais permitem essa evolução sem alterar o
modelo de dados.

## Fonte de dados

- Catálogo oficial: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
- Endpoint físico dos Parquets:
  `https://d37ci6vzurychx.cloudfront.net/trip-data`.
- Período: janeiro a maio de 2023.
- Categorias utilizadas: Yellow Taxi e Green Taxi.

A página da NYC TLC é a referência institucional. Seus links de download
apontam para o endpoint CloudFront usado pelo pipeline.

## Resultados

- 16.526.016 corridas processadas na Bronze;
- 16.373.639 corridas válidas na Silver;
- 152.239 registros preservados na quarentena;
- média mensal de `total_amount` dos Yellow Taxis de 27,45 em janeiro e 29,46
  em maio;
- média horária de passageiros em maio entre 1,26 e 1,45.

Resultados completos: [docs/results.md](docs/results.md).

Apresentação navegável: [docs/presentation.html](docs/presentation.html).

## Estrutura do repositório

```text
ifood-case/
├── src/
│   ├── 00_setup.py
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   └── 03_gold_analytics.py
├── analysis/
│   ├── 00_data_quality_overview.sql
│   ├── 01_yellow_monthly_amount.sql
│   └── 02_may_hourly_passengers.sql
├── docs/
├── README.md
└── requirements.txt
```

## Dependências

O `requirements.txt` registra a versão do PySpark usada no projeto apenas
para referência e validações locais. A execução oficial deve ser realizada no
Databricks.
