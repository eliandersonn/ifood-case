# Decisões Técnicas

## Por que Databricks Free Edition?

O case recomenda a Community Edition, que era a oferta gratuita disponível
quando o enunciado foi elaborado. A Free Edition é a substituta atual. Ela foi
usada com Serverless, Unity Catalog e Volumes para representar uma arquitetura
Lakehouse governada e reproduzível sem exigir uma conta de nuvem externa.

O Unity Catalog centraliza os metadados das tabelas e governa a Landing. Essa
escolha aproxima o case de um ambiente corporativo atual, mas mantém a execução
simples para quem clonar o repositório.

## Por que Yellow e Green Taxi?

A primeira pergunta exige Yellow Taxi. A segunda pede todos os táxis da frota; por isso, o pipeline processa Yellow e Green Taxi. FHV e HVFHV não são incluídos, pois representam veículos de aluguel, não as duas categorias de táxi reguladas pela TLC.

## Por que Landing + Bronze?

A Landing preserva o Parquet exatamente como publicado, possibilitando auditoria e reprocessamento. A Bronze cria uma representação Delta com schema uniforme e metadados técnicos. Isso separa a cópia da fonte do processamento estruturado.

Para reduzir passos manuais, o mesmo notebook de ingestão contém duas seções
independentes: Fonte → Landing e Landing → Bronze. O download ocorre em chunks,
com retry, arquivo temporário e verificação de tamanho. Arquivos válidos já
presentes são ignorados, tornando a aquisição idempotente.

A URL da NYC TLC é mantida como catálogo oficial e referência documental. O
download utiliza o endpoint CloudFront para o qual os links da própria página
apontam; ele é o endereço físico dos Parquets, não uma fonte alternativa.

## Como incluir novos meses?

O período é recebido pelos widgets `start_month` e `end_month`. Para incluir
junho, o usuário amplia `end_month` para `2023-06` e reexecuta o notebook.
Somente arquivos ausentes são baixados. O case usa full refresh das tabelas;
em produção, a partição `source_month` permite evoluir para `replaceWhere`
ou `MERGE` sem reprocessar todo o histórico.

## Por que Delta Lake?

Delta oferece schema enforcement, escrita transacional e tabelas consultáveis em Spark SQL. É a opção nativa e simples para o Databricks Free Edition.

## Regras de qualidade

Os controles seguem dimensões usuais de qualidade sem adicionar ferramentas
desnecessárias ao escopo do case:

- **conformidade:** presença das colunas obrigatórias e tipos explícitos;
- **completude:** `VendorID`, `total_amount`, pickup e dropoff obrigatórios;
- **validade:** `total_amount` positivo e duração maior que zero e de no
  máximo 24 horas;
- **consistência:** pickup dentro do mês indicado pelo arquivo de origem;
- **unicidade:** deduplicação pelos principais atributos da corrida;
- **rastreabilidade:** arquivo, mês de origem e instante de ingestão;
- **reconciliação:** contagens por arquivo, mês, camada e regra de rejeição.

Registros reprovados são preservados com um ou mais motivos na tabela de
quarentena. `passenger_count` nulo ou não positivo não invalida a corrida para
a análise financeira; ele é excluído somente da métrica de passageiros.

Os códigos de quarentena refletem condições objetivas:

| Código | Condição |
|---|---|
| `null_vendor_id` | `VendorID` nulo |
| `null_total_amount` | `total_amount` nulo |
| `non_positive_total_amount` | `total_amount` menor ou igual a zero |
| `null_pickup_datetime` | início da corrida nulo |
| `null_dropoff_datetime` | término da corrida nulo |
| `non_positive_trip_duration` | término anterior ou igual ao início |
| `trip_duration_over_24h` | duração superior a 24 horas |
| `pickup_outside_source_month` | início fora do mês declarado pelo arquivo |

Uma corrida pode registrar mais de um código. Duplicidades são removidas por
um controle próprio e não são enviadas para quarentena.

Essas regras são verificadas durante a execução dos notebooks e resumidas em
`analysis/00_data_quality_overview.sql`.

### Quality gates

Os controles críticos não são apenas exibidos. A execução é interrompida
quando o contrato Yellow está incompleto, quando as tabelas Yellow e Green não
fecham com a Silver unificada, quando as saídas superam a Bronze ou quando a
Gold não cobre todos os meses disponíveis e as 24 horas esperadas. O objetivo
é impedir que um resultado estruturalmente incompleto seja apresentado como
sucesso.

## Evolução para produção

O `overwrite` foi escolhido para o recorte fixo de cinco meses por ser simples,
idempotente e fácil de reproduzir. Para vários anos de histórico, o pipeline
processaria somente meses novos ou alterados: `replaceWhere` para substituir
partições completas e `MERGE INTO` quando fosse necessário atualizar registros
específicos. A Gold seria recalculada apenas para os meses afetados.

As partições e a coluna `source_month` já preparam o modelo para essa evolução.
