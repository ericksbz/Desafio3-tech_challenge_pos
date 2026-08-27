"""
Tech Challenge Fase 3 - Pipeline State of Data Brasil (AWS Glue / PySpark)
============================================================================
Este pipeline já foi validado contra os 3 arquivos reais baixados do Kaggle:
  - State of Data Brazil 2023      (df_survey_2023.csv)
  - State of Data Brazil 2024      (df_survey_2024.csv)
  - State of Data Brazil 2025-2026 (df_survey_2025.csv)

A mesma lógica de limpeza abaixo foi testada em Pandas sobre os arquivos reais
(ver analise_eda_pandas.py) e gerou os números usados no PPTX executivo.
Aqui ela está reescrita em PySpark para rodar como AWS Glue Job.

Camadas:
  RAW    (S3) -> CSVs originais, sem alteração
  BRONZE (S3) -> Parquet, 1 partição por ano_pesquisa, schema bruto preservado
  SILVER (S3) -> schema comum entre as 3 edições, categorias limpas e
                 padronizadas (gênero, uso de IA, região a partir da UF)
  GOLD   (S3) -> tabelas agregadas que respondem às 7 perguntas de negócio
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("state_of_data_brasil_pipeline").getOrCreate()

# ---------------------------------------------------------------------------
# 0. CONFIGURAÇÃO
# ---------------------------------------------------------------------------

BUCKET = "s3://SEU-BUCKET-AQUI"  # troque pelo nome do bucket criado no S3

PATHS = {
    "raw": f"{BUCKET}/raw",
    "bronze": f"{BUCKET}/bronze",
    "silver": f"{BUCKET}/silver",
    "gold": f"{BUCKET}/gold",
}

# nomes dos arquivos exatamente como estão no S3 (raw/)
ARQUIVOS = {
    2023: "State_of_data_BR_2023_Kaggle - df_survey_2023.csv",
    2024: "Final Dataset - State of Data 2024 - Kaggle - df_survey_2024.csv",
    2025: "Final Dataset - State of Data 2025-2026 - Kaggle.csv",
}

# ---------------------------------------------------------------------------
# 1. CAMADA BRONZE
# ---------------------------------------------------------------------------
# O arquivo de 2023 tem cabeçalho em formato de tupla-string, ex:
#   "('P2_f ', 'Cargo Atual')"
# Os arquivos 2024/2025 já vêm com nomes legíveis, ex: "2.f_cargo_atual"
# Na Bronze mantemos o cabeçalho ORIGINAL de cada edição (schema bruto) — a
# padronização de nomes só acontece na Silver, coluna a coluna, de forma
# explícita (mais seguro do que renomear tudo automaticamente).


def gera_bronze():
    for ano, arquivo in ARQUIVOS.items():
        caminho = f"{PATHS['raw']}/{arquivo}"
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .option("multiLine", True)
            .option("escape", '"')
            .csv(caminho)
        )
        df = df.withColumn("ano_pesquisa", F.lit(ano))
        destino = f"{PATHS['bronze']}/ano_pesquisa={ano}"
        df.write.mode("overwrite").parquet(destino)
        print(f"[BRONZE] {ano}: {df.count()} linhas / {len(df.columns)} colunas -> {destino}")


# ---------------------------------------------------------------------------
# 2. CAMADA SILVER
# ---------------------------------------------------------------------------
# Mapeamento REAL validado por edição. Campo -> nome da coluna na Bronze.
# 2023 usa códigos "P0"-style pois o header original virou tupla-string;
# ao ler o Parquet da Bronze, o Spark já usa esse nome de coluna tal como
# veio do CSV — então mapeamos por essa string literal.

COLMAP = {
    2023: {
        "genero": "('P1_b ', 'Genero')",
        "uf": "('P1_i_1 ', 'uf onde mora')",
        "regiao": "('P1_i_2 ', 'Regiao onde mora')",
        "cargo": "('P2_f ', 'Cargo Atual')",
        "senioridade": "('P2_g ', 'Nivel')",
        "faixa_salarial": "('P2_h ', 'Faixa salarial')",
        "usa_ia": "('P4_m ', 'Utiliza ChatGPT ou LLMs no trabalho?')",
    },
    2024: {
        "genero": "1.b_genero",
        "uf": "1.i.1_uf_onde_mora",
        "regiao": "1.i.2_regiao_onde_mora",
        "cargo": "2.f_cargo_atual",
        "senioridade": "2.g_nivel",
        "faixa_salarial": "2.h_faixa_salarial",
        "usa_ia": "4.m_usa_chatgpt_ou_copilot_no_trabalho?",
    },
    2025: {
        "genero": "1.b_genero",
        "uf": "1.i.1_uf_onde_mora",
        "regiao": "1.i.2_regiao_onde_mora",
        "cargo": "2.f_cargo_atual",
        "senioridade": "2.g_nivel",
        "faixa_salarial": "2.h_faixa_salarial",
        "usa_ia": "4.j_usa_chatgpt_ou_copilot_no_trabalho?",
    },
}

# Nota sobre P4_m em 2023: o nome exato da coluna no arquivo real pode variar
# ligeiramente em acentuação/espaços -- valide com
#   [c for c in df.columns if c.startswith("('P4_m")]
# antes de rodar em produção, e ajuste a string acima se necessário.

REGIAO_POR_UF = {
    "AC": "Norte", "AP": "Norte", "AM": "Norte", "PA": "Norte", "RO": "Norte", "RR": "Norte", "TO": "Norte",
    "AL": "Nordeste", "BA": "Nordeste", "CE": "Nordeste", "MA": "Nordeste", "PB": "Nordeste",
    "PE": "Nordeste", "PI": "Nordeste", "RN": "Nordeste", "SE": "Nordeste",
    "DF": "Centro-Oeste", "GO": "Centro-Oeste", "MT": "Centro-Oeste", "MS": "Centro-Oeste",
    "ES": "Sudeste", "MG": "Sudeste", "RJ": "Sudeste", "SP": "Sudeste",
    "PR": "Sul", "RS": "Sul", "SC": "Sul",
}


def extrai_edicao(ano: int) -> DataFrame:
    """Lê a partição Bronze do ano e extrai só as colunas-chave, já renomeadas."""
    df = spark.read.parquet(f"{PATHS['bronze']}/ano_pesquisa={ano}")
    m = COLMAP[ano]
    cols = [F.col(f"`{m[campo]}`").alias(campo) for campo in
            ["genero", "uf", "regiao", "cargo", "senioridade", "faixa_salarial", "usa_ia"]
            if m[campo] in df.columns]
    return df.select(F.lit(ano).alias("ano_pesquisa"), *cols)


def classifica_genero(col):
    c = F.lower(F.trim(col))
    return (
        F.when(c.contains("masc"), "Masculino")
         .when(c.contains("femin"), "Feminino")
         .when(c.contains("prefiro") | c.contains("não"), "Prefiro não informar")
         .when(col.isNull(), "Não informado")
         .otherwise("Outros")
    )


def classifica_uso_ia(col):
    """Resposta é multi-select (categorias separadas por vírgula) -> escolhe
    a categoria de maior investimento/prioridade presente na resposta."""
    c = F.lower(col)
    return (
        F.when(col.isNull(), "Não respondeu")
         .when(c.contains("não utilizo nenhum tipo"), "Não usa IA")
         .when(c.contains("empresa em que trabalho paga"), "Usa - empresa paga")
         .when(c.contains("pago do meu próprio bolso"), "Usa - paga do bolso")
         .when(c.contains("ai para código") | c.contains("copilot"), "Usa - copilot/código")
         .when(c.contains("apenas soluções gratuitas"), "Usa - versão gratuita")
         .otherwise("Usa - outro")
    )


def gera_silver():
    dfs = [extrai_edicao(ano) for ano in ARQUIVOS]
    df = dfs[0]
    for d in dfs[1:]:
        df = df.unionByName(d, allowMissingColumns=True)

    df = df.withColumn("genero_padrao", classifica_genero(F.col("genero")))
    df = df.withColumn("usa_ia_padrao", classifica_uso_ia(F.col("usa_ia")))
    df = df.withColumn("cargo", F.trim(F.col("cargo")))
    # 2024 usa uma grafia ligeiramente diferente de 2023/2025 para o mesmo cargo
    df = df.withColumn(
        "cargo",
        F.when(
            F.col("cargo") == "Engenheiro de Dados/Arquiteto de Dados/Data Engineer/Data Architect",
            F.lit("Engenheiro de Dados/Data Engineer/Data Architect"),
        ).otherwise(F.col("cargo")),
    )
    df = df.withColumn("senioridade", F.trim(F.col("senioridade")))

    # região a partir da UF quando o campo região vier vazio
    mapping_expr = F.create_map([F.lit(x) for pair in REGIAO_POR_UF.items() for x in pair])
    df = df.withColumn(
        "regiao",
        F.coalesce(F.col("regiao"), mapping_expr.getItem(F.upper(F.col("uf"))), F.lit("Não informado")),
    )

    df = df.dropDuplicates()
    df.write.mode("overwrite").partitionBy("ano_pesquisa").parquet(PATHS["silver"])
    print(f"[SILVER] {df.count()} linhas gravadas em {PATHS['silver']}")


# ---------------------------------------------------------------------------
# 3. CAMADA GOLD — respostas às 7 perguntas de negócio
# ---------------------------------------------------------------------------

# Colunas de linguagem são "dummies" (uma coluna por linguagem, valor 1/0),
# com prefixo e nomes diferentes por edição. Mapeamos o prefixo de cada ano;
# o sufixo da coluna já é o próprio nome da linguagem.
LINGUAGEM_PREFIXO = {
    2023: "('P4_d_",   # colunas como "('P4_d_1 ', 'SQL')"
    2024: "4.d.",       # colunas como "4.d.1_SQL"
    2025: "4.c.",       # colunas como "4.c.1_SQL"
}


def extrai_linguagens(ano: int) -> DataFrame:
    """Long-format: uma linha por (respondente, linguagem utilizada)."""
    df = spark.read.parquet(f"{PATHS['bronze']}/ano_pesquisa={ano}")
    prefixo = LINGUAGEM_PREFIXO[ano]
    lang_cols = [c for c in df.columns if c.startswith(prefixo) and "não utilizo" not in c.lower()]
    if not lang_cols:
        return spark.createDataFrame([], "ano_pesquisa int, linguagem string")

    exprs = [F.when(F.col(f"`{c}`") == 1, F.lit(c)).otherwise(F.lit(None)) for c in lang_cols]
    arr = F.array(*exprs)
    out = (
        df.withColumn("linguagem_raw", F.explode(arr))
          .filter(F.col("linguagem_raw").isNotNull())
          .withColumn("ano_pesquisa", F.lit(ano))
          .select("ano_pesquisa", "linguagem_raw")
    )
    if ano == 2023:
        # coluna vem como "('P4_d_1 ', 'SQL')" -> extrai o texto entre as
        # últimas aspas simples (a descrição da linguagem)
        out = out.withColumn("linguagem", F.regexp_extract(F.col("linguagem_raw"), r"'([^']+)'\)$", 1))
    else:
        # coluna vem como "4.d.14_JavaScript" -> extrai o texto após o
        # último "_" (o nome da linguagem)
        out = out.withColumn("linguagem", F.element_at(F.split(F.col("linguagem_raw"), "_"), -1))
    return out.select("ano_pesquisa", "linguagem")


def gera_gold():
    df = spark.read.parquet(PATHS["silver"])

    # Q1 - estrutura do mercado: cargos mais frequentes + respondentes/ano
    g1a = df.filter(F.col("cargo").isNotNull()).groupBy("cargo").count().orderBy(F.desc("count"))
    g1a.write.mode("overwrite").parquet(f"{PATHS['gold']}/q1_top_cargos")

    g1b = df.groupBy("ano_pesquisa").count()
    g1b.write.mode("overwrite").parquet(f"{PATHS['gold']}/q1_respondentes_por_ano")

    # Q2 - perfis mais valorizados: senioridade x faixa salarial
    g2 = df.groupBy("ano_pesquisa", "senioridade", "faixa_salarial").count()
    g2.write.mode("overwrite").parquet(f"{PATHS['gold']}/q2_senioridade_salario")

    # Q3 - diversidade de gênero por ano
    g3 = df.groupBy("ano_pesquisa", "genero_padrao").count()
    g3.write.mode("overwrite").parquet(f"{PATHS['gold']}/q3_genero_por_ano")

    # Q5 - adoção de IA por ano
    g5 = df.filter(F.col("usa_ia_padrao") != "Não respondeu").groupBy("ano_pesquisa", "usa_ia_padrao").count()
    g5.write.mode("overwrite").parquet(f"{PATHS['gold']}/q5_ia_por_ano")

    # Q6 - diferenças regionais
    g6a = df.filter(F.col("regiao") != "Não informado").groupBy("ano_pesquisa", "regiao").count()
    g6a.write.mode("overwrite").parquet(f"{PATHS['gold']}/q6_regiao")

    g6b = df.groupBy("ano_pesquisa", "regiao", "senioridade").count()
    g6b.write.mode("overwrite").parquet(f"{PATHS['gold']}/q6_senioridade_regiao")

    # Q4 - tecnologias/linguagens mais adotadas (todas as edições)
    lang_dfs = [extrai_linguagens(ano) for ano in ARQUIVOS]
    g4 = lang_dfs[0]
    for d in lang_dfs[1:]:
        g4 = g4.unionByName(d)
    g4 = g4.groupBy("ano_pesquisa", "linguagem").count().orderBy(F.desc("count"))
    g4.write.mode("overwrite").parquet(f"{PATHS['gold']}/q4_linguagens_por_ano")

    print("[GOLD] Tabelas agregadas geradas com sucesso.")


# ---------------------------------------------------------------------------
# EXECUÇÃO
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    gera_bronze()
    gera_silver()
    gera_gold()
    spark.stop()
