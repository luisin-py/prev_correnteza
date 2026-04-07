#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prevê maré para as próximas 6 horas usando algoritmo genético (DNA de 15 genes) + fase da lua,
buscando o valor da Marinha para cada horário previsto e preenchendo tabelas de granularidade horária e 5min.
Inclui suavização de bordas ("blend") para evitar saltos bruscos ao transicionar do histórico real para previsão.
"""

import sys, os, time
from datetime import timedelta, datetime
import numpy as np
import pandas as pd
from google.cloud import bigquery

sys.path.append('..')
from update_table_5minAndHour_tratada import BQTableInterpolator

from correnteza.forecast_ga_all_depths import main as prev_correnteza_main


# ------------------------ CONFIG ------------------------
PROJECT    = "local-bliss-359814"
DATASET    = "wherehouse_tratado"
TABLE_5MIN = f"{PROJECT}.{DATASET}.mestre_5min_tratada"
TABLE_HOUR = f"{PROJECT}.{DATASET}.mestre_hour_tratada"
TABLE_OUT  = f"{PROJECT}.wherehouse_previsoes.previsao_mare"
MARINHA_TB = "local-bliss-359814.horarios.base_marinha_cast_altura_mare"
BEST_CSV   = "best_dna_prev.csv"
HORIZON_H  = 6  # horas de previsão

# ---------- Parâmetros de suavização ----------
BRIDGE_MINUTES_5M = 20  # Ponte de blend (minutos) na 5-min
BRIDGE_HOURS_H1   = 1   # Nº de horas previstas "blendadas" com último valor real (horária)
LINEAR_ROLLING_5M = 3   # Janela rolling mean (5-min) após ponte
LINEAR_ROLLING_H1 = 1   # Janela rolling na série horária (normalmente não precisa)

# Instancia cliente do BigQuery
BQ = bigquery.Client(project=PROJECT)
# --------------------------------------------------------

def moon_phase(dt):
    """
    Calcula a fase da lua para a data/hora fornecida, em [0,1].
    """
    lunation = 29.53058867
    ref = datetime(2000, 1, 6, 18, 14)
    return ((pd.Timestamp(dt) - ref).total_seconds() / 86_400) % lunation / lunation

def backup_table_as_csv(table, filename=None):
    """
    Salva um backup local da tabela SQL consultada em CSV.
    """
    df = BQ.query(f"SELECT * FROM `{table}`").to_dataframe()
    if filename is None:
        filename = f"backup_{table.replace('.', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(filename, index=False)
    print(f"🗂️  Backup da tabela `{table}` salvo em {filename}")

def atualiza_tabelas():
    """
    Chama classe externa para atualizar (interpolar) as tabelas tratadas.
    """
    BQTableInterpolator().run()
from correnteza.forecast_ga_all_depths import main as prev_correnteza_main

def prev_correntezas():
    """Chama a função principal da previsão de correnteza."""
    prev_correnteza_main()


def get_base_for_prediction():
    """
    Busca as últimas 24 linhas da série horária tratada.
    Essa base é usada para gerar features (input do modelo).
    """
    q_hour = f"""
      SELECT timestamp_br, temperatura, ventonum, intensidade_superficie,
             pressao, altura_prev_getmare, altura_real_getmare
      FROM `{TABLE_HOUR}`
      ORDER BY timestamp_br DESC
      LIMIT 24
    """
    df_hour = BQ.query(q_hour).to_dataframe().sort_values("timestamp_br")
    if df_hour.empty:
        sys.exit("❌ Tabela horária vazia!")
    return df_hour.reset_index(drop=True)

def get_mare_marinha_para_horarios(horarios):
    """
    Busca a maré da Marinha para cada horário previsto (hora cheia).
    Retorna dataframe com timestamp e altura_mare.
    """
    if not horarios:
        return pd.DataFrame(columns=["timestamp_br", "altura_mare"])
    lista = ",".join(f"DATETIME('{h}')" for h in horarios)
    q = f"""
      SELECT CAST(date2 AS STRING) AS timestamp_br, altura_mare
      FROM `{MARINHA_TB}`
      WHERE date2 IN ({lista})
    """
    return BQ.query(q).to_dataframe()

def garante_colunas(tabela):
    """
    Garante que a tabela tratada possui todas as colunas de previsão.
    """
    for col, typ in [
        ("timestamp_prev", "DATETIME"),
        ("altura_prevista", "NUMERIC"),
        ("altura_prev_getmare", "NUMERIC"),
    ]:
        BQ.query(
            f"ALTER TABLE `{tabela}` ADD COLUMN IF NOT EXISTS {col} {typ}"
        ).result()

def delete_previstos(df_ins, tabela):
    """
    Remove, na tabela tratada, os registros cujos timestamps já serão atualizados.
    (Evita duplicidade ou sobreposição.)
    """
    if df_ins.empty:
        return
    ts_list = pd.to_datetime(df_ins['timestamp_prev']).dt.strftime('%Y-%m-%d %H:%M:%S').tolist()
    if not ts_list:
        return
    lista = ",".join(f"DATETIME('{ts}')" for ts in ts_list)
    delete_query = f"DELETE FROM `{tabela}` WHERE timestamp_prev IN ({lista})"
    BQ.query(delete_query).result()

def insert_previstos(df_ins, tabela):
    """
    Insere previsões via SQL manual, convertendo NaN -> NULL.
    Usado tanto para série horária quanto 5-min.
    """
    if df_ins.empty:
        return

    def val_or_null(x):
        return 'NULL' if pd.isna(x) else f"{float(x):.6f}"

    values = ",".join(
        f"(DATETIME('{pd.to_datetime(r.timestamp_prev).strftime('%Y-%m-%d %H:%M:%S')}'), "
        f"{val_or_null(r.altura_prevista)}, {val_or_null(r.altura_prev_getmare)})"
        for r in df_ins.itertuples()
    )

    insert_query = f"""
      INSERT INTO `{tabela}` (timestamp_prev, altura_prevista, altura_prev_getmare)
      VALUES {values}
    """
    BQ.query(insert_query).result()

# ------------------ BLENDS / SUAVIZAÇÕES -----------------------

def smooth_hourly_boundary(df_hour_pred, table_hour, blend_hours=1, rolling_window=LINEAR_ROLLING_H1):
    """
    Mistura as primeiras 'blend_hours' horas previstas com o último valor real da tabela horária.
    Deixa a transição mais suave entre o histórico real e a previsão.
    """
    if df_hour_pred.empty or blend_hours <= 0:
        return df_hour_pred

    df_out = df_hour_pred.copy().sort_values("timestamp_prev").reset_index(drop=True)
    first_new_ts = pd.to_datetime(df_out["timestamp_prev"].min())

    # Busca o último valor real antes do início da previsão
    q_last = f"""
      SELECT timestamp_br, altura_real_getmare, altura_prev_getmare
      FROM `{table_hour}`
      WHERE timestamp_br < DATETIME('{first_new_ts:%Y-%m-%d %H:%M:%S}')
      ORDER BY timestamp_br DESC
      LIMIT 1
    """
    df_last = BQ.query(q_last).to_dataframe()

    if df_last.empty:
        return df_out

    last_real = df_last.iloc[0]
    y_real = last_real["altura_real_getmare"]

    # Calcula pesos de blend (quanto puxar do real)
    base_w_start, base_w_end = 0.7, 0.2
    if blend_hours == 1:
        weights = [base_w_start]
    else:
        weights = np.linspace(base_w_start, base_w_end, blend_hours)

    for i in range(min(blend_hours, len(df_out))):
        w = weights[i]
        y_pred = df_out.loc[i, "altura_prevista"]
        if pd.notna(y_pred) and pd.notna(y_real):
            df_out.loc[i, "altura_prevista"] = (1 - w) * y_pred + w * y_real

    # (Opcional) Rolling na série blendada
    if rolling_window and rolling_window > 1:
        df_out["altura_prevista"] = (
            pd.Series(df_out["altura_prevista"].values, index=df_out.index)
              .rolling(window=rolling_window, center=True, min_periods=1)
              .mean()
              .values
        )

    return df_out

def blend_boundary_5min(df5_new, table_5min, bridge_minutes=20, rolling_window=3):
    """
    Cria ponte linear (blend) entre o último ponto 5min já existente e o primeiro novo previsto.
    Suaviza a transição, evitando salto abrupto entre histórico e previsão.
    """
    if df5_new.empty:
        return df5_new

    df5_new = df5_new.sort_values("timestamp_prev").reset_index(drop=True)
    first_new_ts = pd.to_datetime(df5_new["timestamp_prev"].iloc[0])

    # Busca último ponto existente anterior
    q_last = f"""
      SELECT timestamp_prev, altura_prevista, altura_prev_getmare
      FROM `{table_5min}`
      WHERE timestamp_prev < DATETIME('{first_new_ts:%Y-%m-%d %H:%M:%S}')
      ORDER BY timestamp_prev DESC
      LIMIT 1
    """
    df_last = BQ.query(q_last).to_dataframe()

    if df_last.empty:
        return df5_new  # nada pra blend

    last_row = df_last.iloc[0]
    last_ts  = pd.to_datetime(last_row["timestamp_prev"])

    # Só faz blend se os pontos não são contíguos (gap > 5min)
    if (first_new_ts - last_ts) <= pd.Timedelta(minutes=5):
        return df5_new

    # Cria ponte linear entre last_ts e first_new_ts (de 5 em 5 min)
    n_pts = max(int(bridge_minutes / 5), 1)
    bridge_index = pd.date_range(
        start=last_ts + pd.Timedelta(minutes=5),
        end=first_new_ts - pd.Timedelta(minutes=5),
        freq="5min"
    )
    if bridge_index.empty:
        return df5_new

    first_new = df5_new.iloc[0]
    cols = ["altura_prevista", "altura_prev_getmare"]

    bridge_data = {"timestamp_prev": bridge_index}
    for col in cols:
        y0 = float(last_row[col])  if pd.notna(last_row[col])  else np.nan
        y1 = float(first_new[col]) if pd.notna(first_new[col]) else np.nan
        if np.isnan(y0):
            y0 = y1
        if np.isnan(y1):
            y1 = y0
        bridge_vals = np.linspace(y0, y1, len(bridge_index))
        bridge_data[col] = bridge_vals

    df_bridge = pd.DataFrame(bridge_data)

    # Junta ponte + pontos novos
    df_out = pd.concat([df_bridge, df5_new], ignore_index=True).sort_values("timestamp_prev")

    # Rolling mean opcional para suavizar pós-blend
    if rolling_window and rolling_window > 1:
        for col in cols:
            df_out[col] = (
                pd.Series(df_out[col].values, index=df_out.index)
                  .rolling(window=rolling_window, center=True, min_periods=1)
                  .mean()
                  .values
            )

    return df_out

# ------------------ FORECAST CORE -----------------------

def interpolar_e_inserir_5min(df_hour_pred):
    """
    Preenche mestre_5min_tratada com previsão da maré para cada 5 minutos:
      - altura_prevista: interpolada com pchip (fallback linear).
      - altura_prev_getmare: igual à lógica horária (valor exato só nos horários cheios, interpolação linear entre eles).
      - Blend opcional para suavização de borda de previsão.
    """
    if df_hour_pred.empty:
        return

    # Cria range de 5min alinhado, sem segundos
    t0 = pd.to_datetime(df_hour_pred["timestamp_prev"].min()).replace(second=0, microsecond=0)
    t1 = pd.to_datetime(df_hour_pred["timestamp_prev"].max()).replace(second=0, microsecond=0)
    rng_5m = pd.date_range(start=t0, end=t1, freq="5min")

    df5 = pd.DataFrame({"timestamp_prev": rng_5m})

    # ----- Previsão (azul, GA) -----
    df_h = df_hour_pred.copy()
    df_h["timestamp_prev"] = pd.to_datetime(df_h["timestamp_prev"]).dt.floor("5min")
    df5 = df5.merge(df_h[["timestamp_prev", "altura_prevista"]], on="timestamp_prev", how="left")
    try:
        df5["altura_prevista"] = df5["altura_prevista"].interpolate(method="pchip")
    except Exception:
        df5["altura_prevista"] = df5["altura_prevista"].interpolate(method="linear")
    df5["altura_prevista"] = df5["altura_prevista"].ffill().bfill()

    # ----- Marinha (vermelho, valor por hora cheia/interpolação) -----
    # 1. Busca todos horários cheios e valores originais da Marinha
    marinha_q = f"""
        SELECT date2 AS timestamp_prev, altura_mare
        FROM `{MARINHA_TB}`
        WHERE date2 >= DATETIME('{t0:%Y-%m-%d %H:%M:%S}') AND date2 <= DATETIME('{t1:%Y-%m-%d %H:%M:%S}')
        ORDER BY date2
    """
    marinha_raw = BQ.query(marinha_q).to_dataframe()
    marinha_raw["timestamp_prev"] = pd.to_datetime(marinha_raw["timestamp_prev"]).dt.floor("H")
    marinha_raw = marinha_raw.drop_duplicates("timestamp_prev").sort_values("timestamp_prev")
    
    # 2. Monta dict de horários cheios e valores
    horarios_cheios = marinha_raw["timestamp_prev"]
    valores_cheios = marinha_raw["altura_mare"].astype(float)
    dict_mare = dict(zip(horarios_cheios, valores_cheios))
    
    # 3. Interpola entre horários cheios (ou repete valor)
    ts_all = df5["timestamp_prev"].sort_values().unique()
    y_interp = []
    for ts in ts_all:
        hora = ts.replace(minute=0, second=0, microsecond=0)
        if ts == hora and hora in dict_mare:
            y_interp.append(dict_mare[hora])  # valor exato da marinha
        else:
            # Busca as horas cheia anterior e posterior
            prev = max([h for h in dict_mare if h <= ts], default=None)
            nxt = min([h for h in dict_mare if h >= ts], default=None)
            if prev is not None and nxt is not None and prev != nxt:
                # Interpola linearmente
                frac = (ts - prev) / (nxt - prev)
                val = dict_mare[prev] + frac * (dict_mare[nxt] - dict_mare[prev])
                y_interp.append(val)
            elif prev is not None:
                y_interp.append(dict_mare[prev])
            elif nxt is not None:
                y_interp.append(dict_mare[nxt])
            else:
                y_interp.append(np.nan)
    df5["altura_prev_getmare"] = y_interp

    # ---------- Blend opcional (para previsão GA) ----------
    df5 = blend_boundary_5min(
        df5_new=df5,
        table_5min=TABLE_5MIN,
        bridge_minutes=BRIDGE_MINUTES_5M,
        rolling_window=LINEAR_ROLLING_5M
    )

    # Remove duplicatas de timestamp (só 1 por 5min)
    df5["timestamp_prev"] = pd.to_datetime(df5["timestamp_prev"]).dt.floor("5min")
    df5 = df5.drop_duplicates(subset=["timestamp_prev"])

    # Apaga existentes e insere os novos valores previstos
    delete_previstos(df5, TABLE_5MIN)
    insert_previstos(df5, TABLE_5MIN)
    print("✅ mestre_5min_tratada atualizada.\n")


def inserir_mestre_hour(df_ins):
    """
    Insere previsão na tabela horária:
      - Alinha timestamps para hora cheia,
      - Remove duplicatas,
      - Apaga registros antigos e insere novos.
    """
    df_ins["timestamp_prev"] = pd.to_datetime(df_ins["timestamp_prev"]).dt.floor("H")
    df_ins = df_ins.drop_duplicates(subset=["timestamp_prev"])

    delete_previstos(df_ins, TABLE_HOUR)
    insert_previstos(df_ins, TABLE_HOUR)
    print("✅ mestre_hour_tratada atualizada.\n")

def make_features(hist: pd.DataFrame, ts_next: datetime) -> np.ndarray:
    """
    Gera vetor de features (input do DNA/algoritmo genético) para a próxima previsão:
      - Usa as 6 últimas linhas do histórico para montar os 15 features.
      - Sempre calcula deltas/diferenças para capturar dinâmica temporal.
    """
    if len(hist) < 6:
        raise ValueError("Histórico insuficiente (<6 linhas) para gerar features.")

    h   = hist.tail(6).reset_index(drop=True)
    Cl  = h['altura_prev_getmare'].iloc[-1]
    M   = h['altura_real_getmare'].iloc[-1]
    Temp= h['temperatura'].iloc[-1]
    Vent= h['ventonum'].iloc[-1]

    dtprev_1 = Cl - h['altura_prev_getmare'].iloc[-2]
    dtmare_1 = M  - h['altura_real_getmare'].iloc[-2]
    dtprev_2 = Cl - h['altura_prev_getmare'].iloc[-3]
    dtmare_2 = M  - h['altura_real_getmare'].iloc[-3]

    feat = np.array([
        Cl,
        Temp - Vent,
        M - h['altura_prev_getmare'].iloc[-2],
        dtprev_1,
        dtmare_1,
        dtmare_2 * dtprev_2,
        dtprev_2,
        M,
        dtprev_2,
        dtmare_2,
        h['altura_prev_getmare'].iloc[-3] - h['altura_real_getmare'].iloc[-5],
        h['altura_prev_getmare'].iloc[-2] - M,
        dtmare_1 * dtprev_1,
        M * Temp,
        moon_phase(ts_next)
    ], dtype=np.float32)
    return feat

def run_forecast():
    """
    Pipeline principal:
      - Lê DNA médio (top 3) e histórico da tabela horária,
      - Busca previsões da Marinha para cada hora futura,
      - Gera previsões passo a passo (auto-alimentando histórico),
      - Salva previsão na tabela de previsão,
      - Atualiza as tabelas tratadas (hora e 5min).
    """
    if not os.path.exists(BEST_CSV):
        sys.exit("❌ best_dna_prev.csv não encontrado!")

    # Carrega média dos 3 melhores DNAs
    dnas    = pd.read_csv(BEST_CSV, header=None, comment="#").iloc[:, :-1].values
    dna_vec = dnas.mean(axis=0).astype(np.float32)

    # Histórico da tabela horária tratada
    df_hist   = get_base_for_prediction()
    last_ts   = pd.to_datetime(df_hist["timestamp_br"].max())
    hora_base = last_ts.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

    # Monta lista de horários a prever (hora cheia)
    horarios = [(hora_base + timedelta(hours=i)).strftime('%Y-%m-%d %H:%M:%S')
                for i in range(HORIZON_H)]

    # Busca valores previstos da Marinha para esses horários
    marinha_df = get_mare_marinha_para_horarios(horarios)
    marinha_dict = {
        pd.to_datetime(row['timestamp_br']).strftime('%Y-%m-%d %H:%M:%S'): float(row['altura_mare'])
        for _, row in marinha_df.iterrows()
    }

    preds   = []
    df_work = df_hist.copy()

    # Gera previsão passo a passo, autoalimentando histórico
    for i in range(HORIZON_H):
        ts_next = hora_base + timedelta(hours=i)
        key     = ts_next.strftime('%Y-%m-%d %H:%M:%S')
        cl_next = marinha_dict.get(key, df_work['altura_prev_getmare'].iloc[-1])

        # Linha temporária para features e previsão
        feat_row = df_work.iloc[-1:].copy()
        feat_row['timestamp_br']        = ts_next
        feat_row['altura_prev_getmare'] = cl_next
        feat_row['altura_real_getmare'] = df_work['altura_real_getmare'].iloc[-1]

        hist_features = pd.concat([df_work, feat_row], ignore_index=True)
        feat   = make_features(hist_features, ts_next)
        y_next = float(feat @ dna_vec)

        # Linha final (com previsão) adicionada ao histórico de trabalho
        pred_row = feat_row.copy()
        pred_row['altura_real_getmare'] = y_next
        df_work = pd.concat([df_work, pred_row], ignore_index=True)

        preds.append({
            "timestamp_br": ts_next,
            "altura_prevista": y_next,
            "altura_prev_getmare": cl_next
        })

    # Cria dataframe final de previsão para gravação
    df_save = pd.DataFrame(preds)
    df_save["timestamp_br"]        = pd.to_datetime(df_save["timestamp_br"]).dt.tz_localize(None)
    df_save["altura_prevista"]     = pd.to_numeric(df_save["altura_prevista"], errors="coerce")
    df_save["altura_prev_getmare"] = pd.to_numeric(df_save["altura_prev_getmare"], errors="coerce")

    # Remove previsões antigas da tabela de previsões
    lista_ts = ",".join(f"DATETIME('{t}')" for t in df_save["timestamp_br"].dt.strftime('%Y-%m-%d %H:%M:%S'))
    BQ.query(f"DELETE FROM `{TABLE_OUT}` WHERE timestamp_br IN ({lista_ts})").result()

    # Upload previsao_mare
    BQ.load_table_from_dataframe(
        df_save[["timestamp_br", "altura_prevista", "altura_prev_getmare"]],
        TABLE_OUT,
        bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    ).result()
    print("✅ previsao_mare atualizada.")

    # Atualiza série horária tratada
    garante_colunas(TABLE_HOUR)
    df_ins = df_save.rename(columns={"timestamp_br": "timestamp_prev"})[
        ["timestamp_prev", "altura_prevista", "altura_prev_getmare"]
    ].copy()
    df_ins["timestamp_prev"] = pd.to_datetime(df_ins["timestamp_prev"])
    df_ins = smooth_hourly_boundary(
        df_hour_pred=df_ins,
        table_hour=TABLE_HOUR,
        blend_hours=BRIDGE_HOURS_H1,
        rolling_window=LINEAR_ROLLING_H1
    )
    df_ins["timestamp_prev"] = df_ins["timestamp_prev"].dt.strftime('%Y-%m-%d %H:%M:%S')
    delete_previstos(df_ins, TABLE_HOUR)
    insert_previstos(df_ins, TABLE_HOUR)
    print("✅ mestre_hour_tratada atualizada.\n")

    # Atualiza série 5min tratada (interpola marinha e previsão)
    garante_colunas(TABLE_5MIN)
    interpolar_e_inserir_5min(df_ins)

# ------------------------ MAIN LOOP ---------------------

if __name__ == "__main__":
    while True:
        print("\n⏳ Atualizando tabelas tratadas...")
        atualiza_tabelas()
        print("✅ Campos de previsão atualizados!")
        run_forecast()
        prev_correntezas()
        time.sleep(300)  # 5 min
