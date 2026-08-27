-- ============================================================================
-- Criação manual das tabelas GOLD no Athena (alternativa ao Crawler)
-- Rode esses comandos no Athena Query Editor, um de cada vez (ou todos juntos).
-- Troque techchallenge-grupo3-2026 pelo nome real do seu bucket em TODAS as linhas LOCATION.
-- ============================================================================

CREATE DATABASE IF NOT EXISTS state_of_data_gold;

-- Se a tabela "gold" errada (criada pelo crawler) ainda existir, apague antes:
-- DROP TABLE IF EXISTS state_of_data_gold.gold;

CREATE EXTERNAL TABLE IF NOT EXISTS state_of_data_gold.q1_top_cargos (
    cargo string,
    count bigint
)
STORED AS PARQUET
LOCATION 's3://techchallenge-grupo3-2026/gold/q1_top_cargos/';

CREATE EXTERNAL TABLE IF NOT EXISTS state_of_data_gold.q1_respondentes_por_ano (
    ano_pesquisa int,
    count bigint
)
STORED AS PARQUET
LOCATION 's3://techchallenge-grupo3-2026/gold/q1_respondentes_por_ano/';

CREATE EXTERNAL TABLE IF NOT EXISTS state_of_data_gold.q2_senioridade_salario (
    ano_pesquisa int,
    senioridade string,
    faixa_salarial string,
    count bigint
)
STORED AS PARQUET
LOCATION 's3://techchallenge-grupo3-2026/gold/q2_senioridade_salario/';

CREATE EXTERNAL TABLE IF NOT EXISTS state_of_data_gold.q3_genero_por_ano (
    ano_pesquisa int,
    genero_padrao string,
    count bigint
)
STORED AS PARQUET
LOCATION 's3://techchallenge-grupo3-2026/gold/q3_genero_por_ano/';

CREATE EXTERNAL TABLE IF NOT EXISTS state_of_data_gold.q4_linguagens_por_ano (
    ano_pesquisa int,
    linguagem string,
    count bigint
)
STORED AS PARQUET
LOCATION 's3://techchallenge-grupo3-2026/gold/q4_linguagens_por_ano/';

CREATE EXTERNAL TABLE IF NOT EXISTS state_of_data_gold.q5_ia_por_ano (
    ano_pesquisa int,
    usa_ia_padrao string,
    count bigint
)
STORED AS PARQUET
LOCATION 's3://techchallenge-grupo3-2026/gold/q5_ia_por_ano/';

CREATE EXTERNAL TABLE IF NOT EXISTS state_of_data_gold.q6_regiao (
    ano_pesquisa int,
    regiao string,
    count bigint
)
STORED AS PARQUET
LOCATION 's3://techchallenge-grupo3-2026/gold/q6_regiao/';

CREATE EXTERNAL TABLE IF NOT EXISTS state_of_data_gold.q6_senioridade_regiao (
    ano_pesquisa int,
    regiao string,
    senioridade string,
    count bigint
)
STORED AS PARQUET
LOCATION 's3://techchallenge-grupo3-2026/gold/q6_senioridade_regiao/';
