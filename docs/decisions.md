# Decisões Técnicas

## Por que Yellow e Green Taxi?

A primeira pergunta exige Yellow Taxi. A segunda pede todos os táxis da frota; por isso, o pipeline processa Yellow e Green Taxi. FHV e HVFHV não são incluídos, pois representam veículos de aluguel, não as duas categorias de táxi reguladas pela TLC.

## Por que Landing + Bronze?

A Landing preserva o Parquet exatamente como publicado, possibilitando auditoria e reprocessamento. A Bronze cria uma representação Delta com schema uniforme e metadados técnicos. Isso separa a cópia da fonte do processamento estruturado.

Para reduzir passos manuais, o mesmo notebook de ingestão contém duas seções
independentes: Fonte → Landing e Landing → Bronze. O download ocorre em chunks,
com retry, arquivo temporário e verificação de tamanho. Arquivos válidos já
presentes são ignorados, tornando a aquisição idempotente.

## Como incluir novos meses?

O período é recebido pelos widgets `start_month` e `end_month`. Para incluir
junho, o usuário amplia `end_month` para `2023-06` e reexecuta o notebook.
Somente arquivos ausentes são baixados. O case usa full refresh das tabelas;
em produção, a partição `source_month` permite evoluir para `replaceWhere`
ou `MERGE` sem reprocessar todo o histórico.

## Por que Delta Lake?

Delta oferece schema enforcement, escrita transacional e tabelas consultáveis em Spark SQL. É a opção nativa e simples para o Databricks Free Edition.

## Regras de qualidade

Na Silver, registros devem ter horários válidos, duração positiva e estar dentro do período processado. Valores não positivos de `total_amount` e contagens de passageiros não positivas serão tratados explicitamente e reportados nas validações. A decisão final de manter ou excluir cada caso será documentada junto às métricas observadas, para não esconder impacto analítico.
