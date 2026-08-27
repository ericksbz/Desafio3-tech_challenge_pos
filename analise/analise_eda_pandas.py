"""
Unifica as 3 edições da pesquisa State of Data Brasil (2023, 2024, 2025-2026)
em uma base "Silver" comum e gera as tabelas "Gold" que respondem às 7
perguntas de negócio do Tech Challenge Fase 3.

Saída: gold_data.json (usado para montar o PPTX) + CSVs de apoio.
"""
import pandas as pd
import numpy as np
import ast
import json
import re

pd.set_option("future.no_silent_downcasting", True)

FILES = {
    2023: "State_of_data_BR_2023_Kaggle_-_df_survey_2023.csv",
    2024: "Final_Dataset_-_State_of_Data_2024_-_Kaggle_-_df_survey_2024.csv",
    2025: "Final_Dataset_-_State_of_Data_2025-2026_-_Kaggle.csv",
}

# ---------------------------------------------------------------------------
# 1) Carrega cada edição e extrai apenas as colunas-chave para um schema comum
# ---------------------------------------------------------------------------

def load_2023(path):
    df = pd.read_csv(path, low_memory=False)
    # colunas vêm como string de tupla "('P2_f ', 'Cargo Atual')" -> pega o código
    code_map = {}
    for c in df.columns:
        try:
            code, desc = ast.literal_eval(c)
            code_map[code.strip()] = c
        except Exception:
            pass

    def col(code):
        return df[code_map[code]] if code in code_map else pd.Series([np.nan] * len(df))

    out = pd.DataFrame({
        "ano_pesquisa": 2023,
        "genero": col("P1_b"),
        "uf": col("P1_i_1"),
        "regiao": col("P1_i_2"),
        "cargo": col("P2_f"),
        "senioridade": col("P2_g"),
        "faixa_salarial": col("P2_h"),
        "usa_ia": col("P4_m"),
    })

    # linguagens: flags binários P4_d_1..P4_d_15
    lang_cols = {c: desc.strip() for c, desc in
                 [(k, k) for k in code_map] if False}
    lang_map = {}
    for c in df.columns:
        try:
            code, desc = ast.literal_eval(c)
        except Exception:
            continue
        code = code.strip()
        if re.match(r"^P4_d_\d+$", code) and "não utilizo" not in desc.lower():
            lang_map[desc.strip()] = df[c]
    linguagens = pd.DataFrame(lang_map).fillna(0).astype(float)
    linguagens.columns = [f"lang__{c}" for c in linguagens.columns]

    return pd.concat([out.reset_index(drop=True), linguagens.reset_index(drop=True)], axis=1)


def load_2024(path):
    df = pd.read_csv(path, low_memory=False)
    out = pd.DataFrame({
        "ano_pesquisa": 2024,
        "genero": df.get("1.b_genero"),
        "uf": df.get("1.i.1_uf_onde_mora"),
        "regiao": df.get("1.i.2_regiao_onde_mora"),
        "cargo": df.get("2.f_cargo_atual"),
        "senioridade": df.get("2.g_nivel"),
        "faixa_salarial": df.get("2.h_faixa_salarial"),
        "usa_ia": df.get("4.m_usa_chatgpt_ou_copilot_no_trabalho?"),
    })
    lang_map = {}
    for c in df.columns:
        m = re.match(r"^4\.d\.(\d+)_(.+)$", c)
        if m and "não utilizo" not in m.group(2).lower():
            lang_map[m.group(2).strip()] = df[c]
    linguagens = pd.DataFrame(lang_map).fillna(0).astype(float)
    linguagens.columns = [f"lang__{c}" for c in linguagens.columns]
    return pd.concat([out.reset_index(drop=True), linguagens.reset_index(drop=True)], axis=1)


def load_2025(path):
    df = pd.read_csv(path, low_memory=False)
    out = pd.DataFrame({
        "ano_pesquisa": 2025,
        "genero": df.get("1.b_genero"),
        "uf": df.get("1.i.1_uf_onde_mora"),
        "regiao": df.get("1.i.2_regiao_onde_mora"),
        "cargo": df.get("2.f_cargo_atual"),
        "senioridade": df.get("2.g_nivel"),
        "faixa_salarial": df.get("2.h_faixa_salarial"),
        "usa_ia": df.get("4.j_usa_chatgpt_ou_copilot_no_trabalho?"),
    })
    lang_map = {}
    for c in df.columns:
        m = re.match(r"^4\.c\.(\d+)_(.+)$", c)
        if m and "não utilizo" not in m.group(2).lower():
            lang_map[m.group(2).strip()] = df[c]
    linguagens = pd.DataFrame(lang_map).fillna(0).astype(float)
    linguagens.columns = [f"lang__{c}" for c in linguagens.columns]
    return pd.concat([out.reset_index(drop=True), linguagens.reset_index(drop=True)], axis=1)


df23 = load_2023(FILES[2023])
df24 = load_2024(FILES[2024])
df25 = load_2025(FILES[2025])

silver = pd.concat([df23, df24, df25], ignore_index=True, sort=False)
lang_cols = [c for c in silver.columns if c.startswith("lang__")]
silver[lang_cols] = silver[lang_cols].fillna(0)

# --- limpeza de categorias -------------------------------------------------

def clean_genero(x):
    if pd.isna(x):
        return "Não informado"
    x = str(x).strip().lower()
    if "masc" in x:
        return "Masculino"
    if "femin" in x:
        return "Feminino"
    if "prefiro" in x or "não" in x:
        return "Prefiro não informar"
    return "Outros"


def clean_ia(x):
    """Classifica a resposta (multi-select) em uma categoria única de adoção de IA."""
    if pd.isna(x):
        return "Não respondeu"
    x = str(x).lower()
    if "não utilizo nenhum tipo" in x:
        return "Não usa IA"
    if "empresa em que trabalho paga" in x:
        return "Usa - empresa paga"
    if "pago do meu próprio bolso" in x:
        return "Usa - paga do bolso"
    if "ai para código" in x or "copilot" in x:
        return "Usa - copilot/código"
    if "apenas soluções gratuitas" in x:
        return "Usa - versão gratuita"
    return "Usa - outro"


silver["genero_padrao"] = silver["genero"].apply(clean_genero)
silver["usa_ia_padrao"] = silver["usa_ia"].apply(clean_ia)
silver["cargo"] = silver["cargo"].astype(str).str.strip()
silver["cargo"] = silver["cargo"].replace({
    "Engenheiro de Dados/Arquiteto de Dados/Data Engineer/Data Architect": "Engenheiro de Dados/Data Engineer/Data Architect",
})
silver["senioridade"] = silver["senioridade"].astype(str).str.strip()
silver["regiao"] = silver["regiao"].astype(str).str.strip()

silver.to_csv("silver_unificado.csv", index=False)
print("Silver:", silver.shape)

# ---------------------------------------------------------------------------
# 2) GOLD — respostas às 7 perguntas de negócio
# ---------------------------------------------------------------------------

gold = {}

# Q1 — Estrutura do mercado: top cargos por ano
top_cargos_geral = (
    silver[~silver["cargo"].isin(["nan", "None"])]
    .groupby("cargo").size().sort_values(ascending=False).head(8)
)
gold["q1_top_cargos"] = {
    "labels": top_cargos_geral.index.tolist(),
    "values": [int(v) for v in top_cargos_geral.values],
}

respondentes_por_ano = silver.groupby("ano_pesquisa").size()
gold["q1_respondentes_por_ano"] = {
    "labels": [str(a) for a in respondentes_por_ano.index],
    "values": [int(v) for v in respondentes_por_ano.values],
}

# Q2 — Perfis mais valorizados: senioridade x faixa salarial (2025, cargos-chave)
sal_ordem = [
    "Menos de R$ 1.000/mês", "de R$ 1.001/mês a R$ 2.000/mês",
    "de R$ 2.001/mês a R$ 3000/mês", "de R$ 3.001/mês a R$ 4.000/mês",
    "de R$ 4.001/mês a R$ 6.000/mês", "de R$ 6.001/mês a R$ 8.000/mês",
    "de R$ 8.001/mês a R$ 12.000/mês", "de R$ 12.001/mês a R$ 16.000/mês",
    "de R$ 16.001/mês a R$ 20.000/mês", "de R$ 20.001/mês a R$ 25.000/mês",
    "Acima de R$ 25.000/mês", "Acima de 25.001/mês",
]
senioridade_ordem = ["Júnior", "Pleno", "Sênior", "Nível Gerencial", "Nível Direção"]

sen_counts = (
    silver[silver["ano_pesquisa"] == 2025]
    .groupby("senioridade").size()
)
sen_counts = sen_counts.reindex([s for s in senioridade_ordem if s in sen_counts.index]).dropna()
gold["q2_senioridade_2025"] = {
    "labels": sen_counts.index.tolist(),
    "values": [int(v) for v in sen_counts.values],
}

# Q3 — Diversidade de gênero por ano
genero_ano = (
    silver.groupby(["ano_pesquisa", "genero_padrao"]).size().unstack(fill_value=0)
)
genero_pct = genero_ano.div(genero_ano.sum(axis=1), axis=0) * 100
gold["q3_genero_por_ano"] = {
    "anos": [str(a) for a in genero_pct.index],
    "series": {g: [round(v, 1) for v in genero_pct[g].values] for g in genero_pct.columns},
}

# Q4 — Tecnologias/linguagens mais usadas (2025)
lang_cols_2025 = [c for c in silver.columns if c.startswith("lang__")]
uso_lang_2025 = silver[silver["ano_pesquisa"] == 2025][lang_cols_2025].sum().sort_values(ascending=False).head(8)
gold["q4_linguagens_2025"] = {
    "labels": [c.replace("lang__", "") for c in uso_lang_2025.index],
    "values": [int(v) for v in uso_lang_2025.values],
}

# Q5 — Adoção de IA por ano (só entre quem respondeu à pergunta)
respondeu = silver[silver["usa_ia_padrao"] != "Não respondeu"]
ia_ano = respondeu.groupby(["ano_pesquisa", "usa_ia_padrao"]).size().unstack(fill_value=0)
ia_pct = ia_ano.div(ia_ano.sum(axis=1), axis=0) * 100
gold["q5_ia_por_ano"] = {
    "anos": [str(a) for a in ia_pct.index],
    "series": {g: [round(v, 1) for v in ia_pct[g].values] for g in ia_pct.columns},
}

# taxa geral de adoção (qualquer uso vs não usa), entre quem respondeu
adota = respondeu.assign(usa_bin=np.where(respondeu["usa_ia_padrao"] == "Não usa IA", "Não usa", "Usa algum tipo de IA"))
adota_ano = adota.groupby(["ano_pesquisa", "usa_bin"]).size().unstack(fill_value=0)
adota_pct = adota_ano.div(adota_ano.sum(axis=1), axis=0) * 100
gold["q5_taxa_adocao_ia"] = {
    "anos": [str(a) for a in adota_pct.index],
    "series": {g: [round(v, 1) for v in adota_pct[g].values] for g in adota_pct.columns},
}

# Q6a — Distribuição por região (geral, 3 edições)
regiao_counts = (
    silver[~silver["regiao"].isin(["nan", "None", ""])]
    .groupby("regiao").size().sort_values(ascending=False)
)
gold["q6_regiao"] = {
    "labels": regiao_counts.index.tolist(),
    "values": [int(v) for v in regiao_counts.values],
}

# Q6b — Senioridade por região (Sudeste vs demais) 2025
sen_regiao = (
    silver[(silver["ano_pesquisa"] == 2025) & (~silver["regiao"].isin(["nan", "None", ""]))]
    .groupby(["regiao", "senioridade"]).size().unstack(fill_value=0)
)
gold["q6_senioridade_regiao"] = {
    "regioes": sen_regiao.index.tolist(),
    "series": {s: [int(v) for v in sen_regiao[s].values] for s in sen_regiao.columns if s in senioridade_ordem},
}

with open("gold_data.json", "w", encoding="utf-8") as f:
    json.dump(gold, f, ensure_ascii=False, indent=2)

print(json.dumps(gold, ensure_ascii=False, indent=2)[:3000])
