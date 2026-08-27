# Tech Challenge Fase 3 — State of Data Brasil

Pipeline de Big Data & Analytics para diagnosticar o mercado brasileiro de
Dados, Analytics e IA, usando as 3 últimas edições da pesquisa **State of
Data Brasil** (Data Hackers + Bain). Projeto desenvolvido na AWS Academy Lab
(S3, Glue, Athena) para o Tech Challenge Fase 3 da POSTECH.

## Grupo

- [nomes do grupo aqui]

## Estrutura do repositório

```
.
├── docs/
│   ├── State_of_Data_Brasil_Executivo.pptx   # Material executivo (entrega principal)
│   └── arquitetura_aws_state_of_data.drawio  # Diagrama da arquitetura AWS
├── pipeline/
│   └── pipeline_bronze_silver_gold.py        # Script PySpark rodado no AWS Glue Job
├── sql/
│   ├── criar_tabelas_gold_athena.sql         # DDL das 8 tabelas Gold no Athena
│   └── queries_athena.sql                    # Queries que respondem às 7 perguntas de negócio
├── analise/
│   ├── analise_eda_pandas.py                 # EDA em Pandas usado para validar a lógica antes do PySpark
│   └── gold_data.json                        # Métricas agregadas finais (usadas no PPTX)
└── data/
    └── silver_unificado_3edicoes.csv         # Base tratada e unificada (14.005 respondentes)
│── bases
    └── Final Dataset - State of Data 2024 - Kaggle - df_survey_2024.csv
    └── Final Dataset - State of Data 2025-2026 - Kaggle.csv
    └──State_of_data_BR_2023_Kaggle - df_survey_2023.csv
```


## Arquitetura da solução

Pipeline em camadas Bronze → Silver → Gold, 100% em AWS (diagrama completo
em `docs/arquitetura_aws_state_of_data.drawio` — abra em
[app.diagrams.net](https://app.diagrams.net), *File > Open From > Device*):

1. **Ingestão**: os 3 CSVs sobem para `s3://<bucket>/raw/`
2. **Bronze**: AWS Glue Job (PySpark) lê os CSVs e grava em Parquet, particionado por `ano_pesquisa`
3. **Silver**: limpeza, padronização de categorias (gênero, uso de IA, cargo) e unificação das 3 edições em schema comum
4. **Gold**: agregações de negócio que respondem às 7 perguntas do desafio
5. **Catálogo**: as 8 tabelas Gold são criadas no Glue Data Catalog (`database state_of_data_gold`) via `sql/criar_tabelas_gold_athena.sql`
6. **Consumo**: consultas SQL no Amazon Athena (`sql/queries_athena.sql`) + DataViz no material executivo

## Como reproduzir

1. Crie um bucket S3 com as pastas `raw/`, `bronze/`, `silver/`, `gold/`.
2. Baixe os 3 datasets do Kaggle e suba para `raw/` (mantenha os nomes de
   arquivo exatamente como no Kaggle — o dicionário `ARQUIVOS` em
   `pipeline_bronze_silver_gold.py` já está calibrado para os nomes reais
   usados nesta execução).
3. No AWS Glue Studio, crie um Job (Script editor, engine Spark), cole o
   conteúdo de `pipeline/pipeline_bronze_silver_gold.py`, troque a variável
   `BUCKET` pelo nome do seu bucket, selecione a `LabRole` como IAM Role e
   rode.
4. No Athena, rode `sql/criar_tabelas_gold_athena.sql` (uma instrução por
   vez — o Athena não aceita múltiplos `CREATE TABLE` na mesma execução) para
   catalogar as 8 tabelas Gold. **Não recomendamos usar um Glue Crawler
   apontando direto para `gold/`**: como as 8 subpastas têm nomes parecidos
   (`q1_...`, `q2_...`), o crawler tende a agrupá-las como partições de uma
   única tabela em vez de 8 tabelas separadas — foi o que aconteceu na nossa
   primeira tentativa.
5. Rode as queries de `sql/queries_athena.sql` para obter as respostas às 7
   perguntas de negócio do desafio.


## Principais insights (ver material executivo para detalhes)

- Analista de Dados e Cientista de Dados lideram o mercado (~30% da base)
- Adoção de IA no trabalho saltou de 78% (2023) para 98% (2025)
- Participação feminina oscilou entre 25-28% ao longo das 3 edições
- Sudeste concentra 60% dos respondentes
- Python e SQL dominam como linguagens de trabalho
