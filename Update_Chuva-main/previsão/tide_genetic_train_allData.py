#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Treino GA - contínuo – mantém sempre o TOP-3 DNAs em best_dna.csv
Se o CSV local não existir é baixado do BigQuery; dados são limpos
(dedup, colunas 100 % vazias, linhas incompletas).  Ctrl-C salva checkpoint.
"""

import os, signal, sys, random
from datetime import datetime
import numpy as np
import pandas as pd
from google.cloud import bigquery                     # ⚡️

# ─── CONFIG ──────────────────────────────────────────
LOCAL_CSV  = "All-data - update.csv"
CACHE_CSV  = "alldata_cached.csv"
POP        = 300
P_MUT      = .15
DX         = .02
ELITE      = .20
K_HOF      = 3
BEST       = "best_dna.csv"
GENS_PER_SAVE = 1

# BigQuery fallback ⚡️
BQ_PROJECT   = "local-bliss-359814"
BQ_DATASET   = "wherehouse_tratado"
BQ_TABLE_SRC = f"{BQ_PROJECT}.{BQ_DATASET}.mestre_5min_tratada"

N_FEATS = 15                                          # ⚡️ número fixo de features

# ─── Utilidades ─────────────────────────────────────
def moon_phase(dt):
    lunation, ref = 29.53058867, datetime(2000, 1, 6, 18, 14)
    dt = pd.Timestamp(dt) if not isinstance(dt, pd.Timestamp) else dt
    return ((dt - ref).total_seconds()/86400) % lunation / lunation

# ─── Carrega histórico  (cache → CSV → BigQuery) ───
def load_history():
    if os.path.exists(CACHE_CSV):
        print("📄  Usando cache local", CACHE_CSV)
        return pd.read_csv(CACHE_CSV, parse_dates=["timestamp_br"])

    if os.path.exists(LOCAL_CSV):
        print("⏬  Lendo CSV", LOCAL_CSV)
        df = pd.read_csv(LOCAL_CSV)
    else:
        print("🌐  CSV não achado — baixando do BigQuery…")
        client = bigquery.Client(project=BQ_PROJECT)
        df = client.query(f"SELECT * FROM `{BQ_TABLE_SRC}`").to_dataframe()
        if df.empty:
            raise RuntimeError("Tabela BigQuery vazia ou inexistente.")

    df = df.rename(columns={
        "timestamp"             : "timestamp_br",
        "ventointensidade"      : "ventonum",
        "Intensidade Superfície": "intensidade_superficie",
        "Altura Prev"           : "altura_prev_getmare",
        "Altura medida"         : "altura_real_getmare",
    })
    if "timestamp_br" in df.columns:
        df["timestamp_br"] = pd.to_datetime(df["timestamp_br"], utc=True,
                                            errors="coerce", dayfirst=True)

    df = (df.dropna(axis=1, how="all")                       # remove col 100 % vazias
            .dropna(subset=["timestamp_br", "altura_real_getmare"])
            .drop_duplicates(subset=["timestamp_br"])
            .sort_values("timestamp_br").reset_index(drop=True))

    df.to_csv(CACHE_CSV, index=False)
    print("📌  Cache salvo em", CACHE_CSV)
    return df

# ─── Monta X, y ─────────────────────────────────────
def build_dataset(df):
    df = df.sort_values("timestamp_br").reset_index(drop=True)

    # cria colunas obrigatórias ausentes
    for col in ["altura_prev_getmare", "altura_real_getmare", "temperatura", "ventonum"]:
        if col not in df.columns:
            print(f"⚠️  Coluna {col} ausente — preenchendo zeros.")
            df[col] = 0.0

    # garante numérico
    for col in ["altura_prev_getmare", "altura_real_getmare", "temperatura", "ventonum"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    Cl, M  = df["altura_prev_getmare"].values, df["altura_real_getmare"].values
    Temp   = df["temperatura"].values
    Vent   = df["ventonum"].values
    Ts     = df["timestamp_br"].values

    feats, targets = [], []
    for i in range(2, len(df) - 3):
        Mi_2, Mi_1, Mi = M[i-2], M[i-1], M[i]
        phase = moon_phase(Ts[i])
        feats.append([
            Cl[i], Temp[i] - Vent[i], Mi - Cl[i+2], Cl[i+2] - Cl[i+1], Mi - Mi_1,
            (Mi - Mi_1)*(Cl[i+2] - Cl[i+1]), Cl[i+2] - Cl[i+1], Mi, Cl[i+2] - Cl[i],
            Mi - Mi_2, Cl[i+3] - Mi_1, Cl[i+2] - Mi,
            (Mi - Mi_1)*(Cl[i+2] - Cl[i+1]), Mi * Temp[i], phase
        ])
        targets.append(M[i+1])

    X = np.asarray(feats, dtype=np.float32)
    y = np.asarray(targets, dtype=np.float32).reshape(-1, 1)

    # ⚡️ remove linhas com NaN/Inf
    good = ~np.isnan(X).any(1) & np.isfinite(X).all(1) & ~np.isnan(y).ravel()
    X, y = X[good], y[good]
    return X, y

# ─── GA helpers ─────────────────────────────────────
def fitness(ind, X, y):
    pred = X @ ind
    return np.sqrt(np.nanmean((pred - y.ravel())**2))        # ⚡️ nan-safe

def mutate(ind):
    mask = np.random.rand(ind.size) < P_MUT
    ind[mask] += DX * (np.random.rand(mask.sum()) - .5)      # ⚡️ tamanho correto

def crossover(a, b):
    m = np.random.rand(a.size) < .5
    return np.where(m, a, b)

def hall_of_fame(best, scores, pop):
    best.extend(zip(scores, pop))
    best.sort(key=lambda x: x[0])
    return best[:K_HOF]

# ─── Persistência de DNAs ───────────────────────────
def load_best_dnas(X, y):
    if not os.path.exists(BEST):
        return []
    try:
        arr = np.genfromtxt(BEST, delimiter=",")
        if arr.size == 0:
            return []
        if arr.ndim == 1:
            arr = arr.reshape(1, -1)
        return sorted(
            [(fitness(row[:X.shape[1]], X, y), row[:X.shape[1]]) for row in arr],
            key=lambda x: x[0]
        )[:K_HOF]
    except Exception as e:
        print("⚠️  Erro lendo best_dna:", e)
        return []

def save_hof(best, X, y):
    if not best:
        return
    lines = []
    for score, dna in best:
        err = np.abs((X @ dna) - y.ravel())
        lines.append(
            f"# Acc: {100*np.mean(err<=.15):.1f}% | MAE: {err.mean():.4f} | σ: {err.std():.4f}"
        )
        lines.append(",".join(f"{g:.16g}" for g in np.append(dna, score)))
    open(BEST, "w").write("\n".join(lines))
    print("💾  best_dna.csv atualizado")

# ─── Loop evolutivo ─────────────────────────────────
def train_forever(X, y):
    pop  = np.random.randn(POP, N_FEATS) * .1
    best = load_best_dnas(X, y)

    for i, (_, dna) in enumerate(best):
        if i < POP:
            pop[i] = dna

    if not os.path.exists(BEST):
        save_hof(best, X, y)

    def sigint(sig, frm):
        print("\n🛑  Ctrl-C — salvando Hall-of-Fame…")
        save_hof(best, X, y); sys.exit(0)
    signal.signal(signal.SIGINT, sigint)

    gen, t0 = 0, datetime.now()
    while True:
        gen += 1
        scores = np.array([fitness(ind, X, y) for ind in pop])
        order  = scores.argsort()
        pop, scores = pop[order], scores[order]
        best = hall_of_fame(best, scores[:POP//5], pop[:POP//5])

        if gen % GENS_PER_SAVE == 0:
            print(f"🧬 Gen {gen:>6}  RMSE={scores[0]:.5f}  elapsed={datetime.now()-t0}")
            save_hof(best, X, y)

        elite = pop[:int(POP*ELITE)]
        kids  = []
        while len(kids) < POP - elite.shape[0]:
            p1, p2 = elite[random.randrange(elite.shape[0])], pop[random.randrange(POP)]
            child  = crossover(p1, p2)
            mutate(child)
            kids.append(child)
        pop = np.vstack([elite] + kids)

# ─── Main ───────────────────────────────────────────
if __name__ == "__main__":
    df_hist = load_history()
    X, y    = build_dataset(df_hist)
    print("⚙️  Dataset:", X.shape, "— GA rodando (Ctrl-C para sair).")
    train_forever(X, y)
