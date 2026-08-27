-- ============================================================================
-- Tech Challenge Fase 3 - Consultas Athena (camada GOLD)
-- Nomes de tabela batem com as saídas de pipeline_bronze_silver_gold.py
-- Database esperado no Glue Catalog: state_of_data_gold
-- ============================================================================

-- 1) Como está estruturado o mercado brasileiro de Dados?
SELECT cargo, "count" AS qtd_profissionais
FROM state_of_data_gold.q1_top_cargos
ORDER BY qtd_profissionais DESC
LIMIT 10;

SELECT ano_pesquisa, "count" AS respondentes
FROM state_of_data_gold.q1_respondentes_por_ano
ORDER BY ano_pesquisa;


-- 2) Quais perfis profissionais são mais valorizados pelo mercado?
--    (senioridade x faixa salarial, por ano)
SELECT ano_pesquisa, senioridade, faixa_salarial, "count" AS qtd
FROM state_of_data_gold.q2_senioridade_salario
WHERE ano_pesquisa = 2025
ORDER BY senioridade, qtd DESC;


-- 3) Qual é o cenário de diversidade de gênero nas carreiras de dados?
SELECT
    ano_pesquisa,
    genero_padrao,
    "count" AS qtd,
    ROUND(100.0 * "count" / SUM("count") OVER (PARTITION BY ano_pesquisa), 1) AS pct_no_ano
FROM state_of_data_gold.q3_genero_por_ano
ORDER BY ano_pesquisa, pct_no_ano DESC;


-- 4) Quais tecnologias apresentam maior adoção entre os profissionais?
SELECT ano_pesquisa, linguagem, "count" AS qtd
FROM state_of_data_gold.q4_linguagens_por_ano
WHERE ano_pesquisa = 2025
ORDER BY qtd DESC
LIMIT 10;


-- 5) Qual é o índice de adoção de Inteligência Artificial e seu impacto?
SELECT
    ano_pesquisa,
    usa_ia_padrao,
    "count" AS qtd,
    ROUND(100.0 * "count" / SUM("count") OVER (PARTITION BY ano_pesquisa), 1) AS pct_no_ano
FROM state_of_data_gold.q5_ia_por_ano
ORDER BY ano_pesquisa, pct_no_ano DESC;

-- taxa geral de adoção (usa vs não usa), simplificada
SELECT
    ano_pesquisa,
    CASE WHEN usa_ia_padrao = 'Não usa IA' THEN 'Não usa' ELSE 'Usa algum tipo de IA' END AS grupo,
    SUM("count") AS qtd
FROM state_of_data_gold.q5_ia_por_ano
GROUP BY ano_pesquisa, CASE WHEN usa_ia_padrao = 'Não usa IA' THEN 'Não usa' ELSE 'Usa algum tipo de IA' END
ORDER BY ano_pesquisa;


-- 6) Existem diferenças relevantes entre regiões, senioridades ou modelos de trabalho?

-- 6a) Por região
SELECT ano_pesquisa, regiao, "count" AS qtd
FROM state_of_data_gold.q6_regiao
ORDER BY ano_pesquisa, qtd DESC;

-- 6b) Senioridade por região
SELECT regiao, senioridade, SUM("count") AS qtd
FROM state_of_data_gold.q6_senioridade_regiao
WHERE ano_pesquisa = 2025 AND regiao != 'Não informado'
GROUP BY regiao, senioridade
ORDER BY regiao, qtd DESC;


-- 7) Quais oportunidades e desafios para empresas que desejam investir em
--    Dados e IA? -> combine os resultados das queries 1, 3 e 5 acima para
--    construir a narrativa (crescimento de vagas + baixa diversidade +
--    alta adoção de IA = oportunidades de capacitação e contratação).
-- Ver slide 10 do executivo (State_of_Data_Brasil_Executivo.pptx) para a
-- síntese já pronta dessas recomendações.
