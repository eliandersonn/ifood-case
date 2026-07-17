# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingestão Bronze
# MAGIC
# MAGIC Automatiza a aquisição dos dez Parquets oficiais da NYC TLC, preserva
# MAGIC os arquivos originais na Landing Zone e publica tabelas Delta Bronze
# MAGIC com schema técnico consistente e rastreabilidade.

# COMMAND ----------

import os
import time
import urllib.error
import urllib.request
from datetime import datetime
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

CATALOG = "workspace"
BRONZE_SCHEMA = "nyc_taxi_bronze"
LANDING_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/landing"

# O período pode ser ampliado sem alterar a lógica do pipeline.
dbutils.widgets.text('start_month', '2023-01', 'Mês inicial (AAAA-MM)')
dbutils.widgets.text('end_month', '2023-05', 'Mês final (AAAA-MM)')

START_MONTH = dbutils.widgets.get('start_month')
END_MONTH = dbutils.widgets.get('end_month')

# A página da TLC documenta os datasets; o CloudFront entrega os Parquets.
TLC_CATALOG_URL = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
TLC_DATA_BASE_URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data'


def build_month_range(start_month: str, end_month: str) -> list[str]:
    '''Gera uma lista mensal inclusiva no formato AAAA-MM.'''

    start = datetime.strptime(start_month, '%Y-%m').date().replace(day=1)
    end = datetime.strptime(end_month, '%Y-%m').date().replace(day=1)
    if start > end:
        raise ValueError('O mês inicial não pode ser posterior ao mês final.')

    months = []
    current = start
    while current <= end:
        months.append(current.strftime('%Y-%m'))
        next_year = current.year + (current.month == 12)
        next_month = 1 if current.month == 12 else current.month + 1
        current = current.replace(year=next_year, month=next_month)
    return months


MONTHS = build_month_range(START_MONTH, END_MONTH)
EXPECTED_FILES = {
    taxi_type: [f"{taxi_type}_tripdata_{month}.parquet" for month in MONTHS]
    for taxi_type in ("yellow", "green")
}

print(f'Catálogo oficial: {TLC_CATALOG_URL}')
print(f'Endpoint dos arquivos: {TLC_DATA_BASE_URL}')
print(f'Período configurado: {START_MONTH} até {END_MONTH}')
print(f'Arquivos esperados: {sum(map(len, EXPECTED_FILES.values()))}')

# COMMAND ----------
# MAGIC %md
# MAGIC ## Fonte → Landing
# MAGIC
# MAGIC Os arquivos são baixados da URL oficial somente quando ainda não existem.
# MAGIC A escrita ocorre em chunks e usa um arquivo temporário, evitando consumo
# MAGIC excessivo de memória e arquivos finais incompletos em caso de falha.

# COMMAND ----------

DOWNLOAD_CHUNK_BYTES = 1024 * 1024
DOWNLOAD_TIMEOUT_SECONDS = 300
DOWNLOAD_RETRIES = 3


def download_to_landing(file_name: str) -> dict:
    '''Baixa um arquivo de forma idempotente, com retry e escrita atômica.'''

    source_url = f'{TLC_DATA_BASE_URL}/{file_name}'
    destination = f'{LANDING_PATH}/{file_name}'
    temporary_destination = f'{destination}.part'

    if os.path.exists(destination) and os.path.getsize(destination) > 0:
        return {
            'file_name': file_name,
            'status': 'skipped',
            'size_bytes': os.path.getsize(destination),
        }

    request = urllib.request.Request(
        source_url,
        headers={'User-Agent': 'ifood-case/1.0'},
    )

    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            with urllib.request.urlopen(
                request,
                timeout=DOWNLOAD_TIMEOUT_SECONDS,
            ) as response, open(temporary_destination, 'wb') as target_file:
                while chunk := response.read(DOWNLOAD_CHUNK_BYTES):
                    target_file.write(chunk)

            if os.path.getsize(temporary_destination) == 0:
                raise OSError(f'Download vazio: {file_name}')

            os.replace(temporary_destination, destination)
            return {
                'file_name': file_name,
                'status': 'downloaded',
                'size_bytes': os.path.getsize(destination),
            }
        except (OSError, TimeoutError, urllib.error.URLError) as error:
            if os.path.exists(temporary_destination):
                os.remove(temporary_destination)
            if attempt == DOWNLOAD_RETRIES:
                raise RuntimeError(
                    f'Falha ao baixar {file_name} após {attempt} tentativas.'
                ) from error
            time.sleep(2**attempt)

    raise RuntimeError(f'Estado inesperado durante o download de {file_name}.')


download_results = [
    download_to_landing(file_name)
    for files_by_type in EXPECTED_FILES.values()
    for file_name in files_by_type
]

for result in download_results:
    size_mb = result['size_bytes'] / (1024**2)
    print(
        '{status:>10} | {file_name} | {size_mb:.1f} MB'.format(
            size_mb=size_mb,
            **result,
        )
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validação da Landing Zone
# MAGIC
# MAGIC A execução é interrompida quando algum arquivo do período configurado
# MAGIC não está disponível. Isso evita publicar uma tabela Bronze parcial.

# COMMAND ----------

available_files = {
    file_info.name
    for file_info in dbutils.fs.ls(LANDING_PATH)
    if file_info.name.endswith(".parquet")
}
expected_files = {
    file_name
    for files_by_type in EXPECTED_FILES.values()
    for file_name in files_by_type
}

missing_files = sorted(expected_files - available_files)
unexpected_files = sorted(available_files - expected_files)

if missing_files:
    raise FileNotFoundError(
        "Arquivos obrigatórios ausentes na Landing Zone: "
        + ", ".join(missing_files)
    )

print(f"Arquivos esperados encontrados: {len(expected_files)}")
print(f"Arquivos adicionais ignorados: {unexpected_files}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Landing → Bronze
# MAGIC
# MAGIC Cada Parquet é lido individualmente antes da união. Essa estratégia
# MAGIC trata explicitamente possíveis diferenças de tipo entre os meses.

# COMMAND ----------

def read_month_file(taxi_type: str, file_name: str) -> DataFrame:
    """Lê um arquivo mensal e aplica o contrato técnico da camada Bronze."""

    pickup_column = (
        "tpep_pickup_datetime"
        if taxi_type == "yellow"
        else "lpep_pickup_datetime"
    )
    dropoff_column = (
        "tpep_dropoff_datetime"
        if taxi_type == "yellow"
        else "lpep_dropoff_datetime"
    )
    source_month = file_name.split('_')[-1].removesuffix('.parquet')
    source_path = f"{LANDING_PATH}/{file_name}"

    source_df = spark.read.parquet(source_path)
    required_columns = {
        "VendorID",
        "passenger_count",
        "total_amount",
        pickup_column,
        dropoff_column,
    }
    missing_columns = sorted(required_columns - set(source_df.columns))

    if missing_columns:
        raise ValueError(
            f"Schema inválido em {file_name}. Colunas ausentes: {missing_columns}"
        )

    return source_df.select(
        F.col("VendorID").cast(IntegerType()).alias("VendorID"),
        F.col("passenger_count").cast(IntegerType()).alias("passenger_count"),
        F.col("total_amount").cast(DoubleType()).alias("total_amount"),
        F.col(pickup_column).cast(TimestampType()).alias(pickup_column),
        F.col(dropoff_column).cast(TimestampType()).alias(dropoff_column),
        F.lit(file_name).alias("source_file"),
        F.lit(source_month).alias("source_month"),
        F.current_timestamp().alias("ingested_at"),
    )


def build_bronze_dataframe(taxi_type: str) -> DataFrame:
    """Une os meses configurados de um tipo de táxi com schema consistente."""

    monthly_dataframes = [
        read_month_file(taxi_type, file_name)
        for file_name in EXPECTED_FILES[taxi_type]
    ]
    return reduce(
        lambda current, next_df: current.unionByName(next_df),
        monthly_dataframes,
    )


yellow_bronze_df = build_bronze_dataframe("yellow")
green_bronze_df = build_bronze_dataframe("green")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Publicação Delta
# MAGIC
# MAGIC O `overwrite` implementa um full refresh idempotente: executar novamente
# MAGIC substitui a versão anterior e não duplica registros.

# COMMAND ----------

BRONZE_TABLES = {
    "yellow": f"{CATALOG}.{BRONZE_SCHEMA}.yellow_taxi_trips_raw",
    "green": f"{CATALOG}.{BRONZE_SCHEMA}.green_taxi_trips_raw",
}

for taxi_type, bronze_df in {
    "yellow": yellow_bronze_df,
    "green": green_bronze_df,
}.items():
    (
        bronze_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("source_month")
        .saveAsTable(BRONZE_TABLES[taxi_type])
    )

print("Tabelas Bronze publicadas com sucesso.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validação de volumetria

# COMMAND ----------

for table_name in BRONZE_TABLES.values():
    print(f"Tabela: {table_name}")
    (
        spark.read.table(table_name)
        .groupBy("source_month", "source_file")
        .count()
        .orderBy("source_month")
        .show(truncate=False)
    )
