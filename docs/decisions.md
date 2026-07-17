# Decisões Técnicas

## Por que Yellow e Green Taxi?

A primeira pergunta exige Yellow Taxi. A segunda pede todos os táxis da frota; por isso, o pipeline processa Yellow e Green Taxi. FHV e HVFHV não são incluídos, pois representam veículos de aluguel, não as duas categorias de táxi reguladas pela TLC.

## Por que Landing + Bronze?

A Landing preserva o Parquet exatamente como publicado, possibilitando auditoria e reprocessamento. A Bronze cria uma representação Delta com schema uniforme e metadados técnicos. Isso separa a cópia da fonte do processamento estruturado.

## Por que Delta Lake?

Delta oferece schema enforcement, escrita transacional e tabelas consultáveis em Spark SQL. É a opção nativa e simples para o Databricks Free Edition.

## Regras de qualidade

Na Silver, registros devem ter horários válidos, duração positiva e estar dentro do período processado. Valores não positivos de `total_amount` e contagens de passageiros não positivas serão tratados explicitamente e reportados nas validações. A decisão final de manter ou excluir cada caso será documentada junto às métricas observadas, para não esconder impacto analítico.
