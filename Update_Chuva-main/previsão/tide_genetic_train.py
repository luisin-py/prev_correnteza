#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Treino GA - contínuo ─ mantém sempre o TOP-3 DNAs em best_dna.csv
Se o CSV local não existir, faz download do BigQuery, higieniza
(os mesmos tratamentos: dedup, drop de colunas/linhas vazias,
coerção numérica e remoção de NaN/Inf).  Ctrl-C gera checkpoint.
"""

# ─── IMPORTAÇÃO DE MÓDULOS ──────────────────────────────
import os, signal, sys, random
from datetime import datetime
import numpy as np
import pandas as pd
from google.cloud import bigquery                         # BigQuery

# ─── CONFIGURAÇÕES GERAIS ───────────────────────────────
PROJECT   = "local-bliss-359814"
DATASET   = "wherehouse_tratado"
TABLE_SRC = f"{PROJECT}.{DATASET}.mestre_hour_tratada"
CACHE_CSV = "mestre_hour_cached.csv"

POP   = 300
P_MUT = .15
DX    = .02
ELITE = .20
K_HOF = 3
BEST  = "best_dna.csv"
GENS_PER_SAVE = 1

N_FEATS = 15                                               # ⚡️ número fixo de features

# ─── FUNÇÃO AUXILIAR: fase da lua ───────────────────────
def moon_phase(dt):
    y, m, d = dt.year, dt.month, dt.day
    if m < 3: y, m = y - 1, m + 12
    k1 = int(365.25 * (y + 4712))
    k2 = int(30.6 * (m + 1))
    k3 = int(((y // 100) + 49) * 0.75) - 38
    jd = k1 + k2 + d + 59 - k3
    return ((jd - 2451550.1) / 29.53058867) % 1

# ─── Carregamento / higienização dos dados ──────────────
def load_history():
    if os.path.exists(CACHE_CSV):
        print("📄  Usando cache local", CACHE_CSV)
        return pd.read_csv(CACHE_CSV, parse_dates=["timestamp_br"])

    print("⏬  Baixando do BigQuery…")
    q = f"""
      SELECT timestamp_br, temperatura, ventonum, intensidade_superficie,
             pressao, altura_prev_getmare, altura_real_getmare
      FROM `{TABLE_SRC}`
      WHERE altura_real_getmare IS NOT NULL
      ORDER BY timestamp_br
    """
    df = bigquery.Client(project=PROJECT).query(q).to_dataframe()

    # ⚡️ Higiene: nomes coerentes, conversão, dedup, drop cols vazias
    df["timestamp_br"] = pd.to_datetime(df["timestamp_br"], utc=True, errors="coerce")
    cols_need = ["altura_prev_getmare", "altura_real_getmare",
                 "temperatura", "ventonum"]
    for c in cols_need:
        if c not in df.columns:
            print(f"⚠️  Coluna {c} ausente — preenchendo zeros.")
            df[c] = 0.0
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = (df.dropna(axis=1, how="all")                # remove colunas 100 % vazias
            .dropna(subset=["timestamp_br", "altura_real_getmare"])
            .drop_duplicates(subset=["timestamp_br"])
            .sort_values("timestamp_br")
            .reset_index(drop=True))

    df.to_csv(CACHE_CSV, index=False)
    print("📌  Cache salvo em", CACHE_CSV)
    return df

# ─── Feature engineering ────────────────────────────────
def build_dataset(df: pd.DataFrame):
    df = df.sort_values("timestamp_br").reset_index(drop=True)

    Cl   = df["altura_prev_getmare"].values
    M    = df["altura_real_getmare"].values
    Temp = df["temperatura"].values
    Vent = df["ventonum"].values
    Time = df["timestamp_br"]

    feats, target = [], []
    for i in range(2, len(df) - 3):
        dt2mare = M[i] - M[i-2]
        dt2prev = Cl[i+2] - Cl[i]
        dtmare  = M[i] - M[i-1]
        dtprev  = Cl[i+2] - Cl[i+1]
        proddt  = dt2mare * dt2prev
        proddt2 = dtmare * dtprev
        phase   = moon_phase(Time.iloc[i])

        feats.append([
            Cl[i], Temp[i] - Vent[i], M[i] - Cl[i+2],
            dtprev, dtmare, proddt, dtprev, M[i],
            dt2prev, dt2mare, Cl[i+3] - M[i-1], Cl[i+2] - M[i],
            proddt2, M[i] * Temp[i], phase
        ])
        target.append(M[i+1])

    X = np.asarray(feats, dtype=np.float32)
    y = np.asarray(target, dtype=np.float32).reshape(-1, 1)

    # ⚡️ Remove linhas que contêm NaN ou Inf
    good = (~np.isnan(X).any(1)) & np.isfinite(X).all(1) & (~np.isnan(y).ravel())
    return X[good], y[good]

# ─── GA helpers ─────────────────────────────────────────
def fitness(ind, X, y):
    pred = X @ ind
    return np.sqrt(np.nanmean((pred - y.ravel())**2))       # ⚡️ nan-safe

def mutate(ind):
    mask = np.random.rand(ind.size) < P_MUT                 # ⚡️ usa ind.size
    ind[mask] += DX * (np.random.rand(mask.sum()) - .5)

def crossover(a, b):
    m = np.random.rand(a.size) < .5                         # ⚡️ usa a.size
    return np.where(m, a, b)

def hall_of_fame(best, scores, pop):
    best.extend(zip(scores, pop))
    best.sort(key=lambda x: x[0])
    return best[:K_HOF]

# ─── Persistência de DNAs ───────────────────────────────
def load_best_dnas(X, y):
    if not os.path.exists(BEST):
        return []
    try:
        arr = np.genfromtxt(BEST, delimiter=",")
        if arr.size == 0:
            return []
        if arr.ndim == 1:
            arr = arr.reshape(1, -1)
        dnas = [
            row[:N_FEATS] for row in arr
            if len(row) >= N_FEATS and np.isfinite(row[:N_FEATS]).all()
        ]
        # ⚡️ Ordena pelos scores reais
        return sorted([(fitness(d, X, y), d) for d in dnas], key=lambda x: x[0])[:K_HOF]
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
    print("💾  best_dna.csv atualizado (top-3)")

# ─── Loop evolutivo ────────────────────────────────────
def train_forever(X, y):
    pop  = np.random.randn(POP, N_FEATS) * .1
    best = load_best_dnas(X, y)

    for i, (_, dna) in enumerate(best):
        if i < POP:
            pop[i] = dna

    # salva HOF inicial se não existir
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

# ─── Execução principal ────────────────────────────────
if __name__ == "__main__":
    df_hist = load_history()
    X, y    = build_dataset(df_hist)
    print("⚙️  Dataset:", X.shape, "— GA rodando (Ctrl-C para sair).")
    train_forever(X, y)
