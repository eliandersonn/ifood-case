# Requisitos do Case

## Entregáveis obrigatórios

1. Solução para ler dados originais, ingeri-los no Data Lake e disponibilizá-los para usuários finais.
2. Uso de PySpark em pelo menos uma etapa.
3. Código SQL ou PySpark com as respostas analíticas.
4. Repositório GitHub com README e instruções de execução.

## Dados e período

- Fonte: NYC Taxi & Limousine Commission (NYC TLC).
- Período: janeiro a maio de 2023.
- Yellow Taxi: necessário para a pergunta de receita e para as colunas `tpep_*`.
- Green Taxi: necessário junto ao Yellow Taxi para a análise de todos os táxis.

## Contrato mínimo da camada de consumo

A tabela Silver de Yellow Taxi deve conter, no mínimo:

- `VendorID`
- `passenger_count`
- `total_amount`
- `tpep_pickup_datetime`
- `tpep_dropoff_datetime`

## Perguntas analíticas

1. Qual a média de `total_amount` recebida em cada mês, considerando todos os Yellow Taxis da frota?
2. Qual a média de `passenger_count` por hora do dia, em maio, considerando todos os táxis da frota?

## Critérios de avaliação

- Qualidade e organização do código.
- Processo de análise exploratória.
- Justificativa das escolhas técnicas.
- Criatividade na solução.
- Clareza na comunicação dos resultados.
