#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interpolador 5-min ↔︎ 1-hora + campos derivados (atualização incremental)
Atualiza sempre só as últimas 12 h em cada tabela tratada,
e atualiza os campos de previsão via UPDATE direto no BigQuery.
"""

from __future__ import annotations
import sys
from typing import List, Set
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from pandas.api.types import is_numeric_dtype

# ========== BigQuery Config ========== #
BQ_PROJECT        = "local-bliss-359814"
BQ_DATASET        = "wherehouse_tratado"
BQ_TABLE_SRC      = f"{BQ_PROJECT}.{BQ_DATASET}.mestre_5min"
BQ_TABLE_DEST_5M  = f"{BQ_PROJECT}.{BQ_DATASET}.mestre_5min_tratada"
BQ_TABLE_DEST_H   = f"{BQ_PROJECT}.{BQ_DATASET}.mestre_hour_tratada"
BQ_TABLE_PREV     = "local-bliss-359814.wherehouse_previsoes.previsao_mare"

QUADRANTES_VAZANTE = {"SE", "SSE", "S", "SSW", "SW", "WSW", "E"}

PROFUNDIDADES = [
    "15m", "13_5m", "12m", "10_5m",
    "9m", "7_5m", "6m", "3m", "1_5m"
]

EXCLUDE_NUMERIC: Set[str] = {
    "ow_city_id", "ow_coord_lon", "ow_coord_lat",
    "fc_lat", "fc_lon", "ow_sys_id", "ow_sys_type",
    "ow_timezone_offset", "air_aqi"
}

NUMERIC_FORCE = {"ventonum", "fase_lua", "intensidade_3m_kt"}

_DIRS16 = np.array(
    ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
     "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
)
def deg_to_compass(angle) -> str | None:
    if pd.isna(angle):
        return None
    try:
        ang = float(angle)
    except Exception:
        return None
    i = int((ang % 360) / 22.5 + 0.5) % 16
    return _DIRS16[i]

class BQTableInterpolator:
    def __init__(self) -> None:
        self.client = bigquery.Client()
        self.tbl_tipo = 3
        self.max_hours = 6
        self.log_lvl = 2
        self.max_gap_5m   = self.max_hours * 12  # para 5 min
        self.max_gap_hour = self.max_hours       # para 1 h

    def _log(self, msg: str, lvl: int = 2) -> None:
        if self.log_lvl >= lvl:
            print(msg)

    # ────────────── campos derivados ────────────── #
    def _add_derived(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for prof in PROFUNDIDADES:
            deg_src = f"direcao_{prof}"
            deg_dst = f"direcao_{prof}_deg"
            if deg_src in df.columns:
                df[deg_dst] = df[deg_src].map(deg_to_compass)

        for prof in PROFUNDIDADES:
            dir_deg = f"direcao_{prof}_deg"
            ev_col  = f"enchente_vazante_{prof}"
            int_col = f"intensidade_{prof}"
            adj_col = f"intensidade_{prof}_ajustada"
            df[ev_col] = (
                df[dir_deg].str.upper().map(lambda x: -1 if x in QUADRANTES_VAZANTE else 1)
                if dir_deg in df.columns else np.nan
            )
            df[adj_col] = (
                pd.to_numeric(df[int_col], errors="coerce") * df[ev_col]
                if int_col in df.columns else np.nan
            )

        if "direcao_3m_deg" in df.columns:
            df["enchente_vazante"] = df["direcao_3m_deg"].str.upper().map(
                lambda x: -1 if x in QUADRANTES_VAZANTE else 1
            )
            if "intensidade_3m_kt" in df.columns:
                df["intensidade_3m_ajustada"] = (
                    pd.to_numeric(df["intensidade_3m_kt"], errors="coerce") * df["enchente_vazante"]
                )
        else:
            df["enchente_vazante"] = np.nan
            df["intensidade_3m_ajustada"] = np.nan

        for col in NUMERIC_FORCE:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    # ────────────── utilidades ────────────── #
    def _find_numeric_cols(self, df: pd.DataFrame) -> List[str]:
        numeric_cols: List[str] = []
        sample_size = 2000
        for col in df.columns:
            if col in EXCLUDE_NUMERIC | {"timestamp_br"}:
                continue
            s = df[col].dropna()
            if len(s) < 2:
                continue
            if is_numeric_dtype(s) or \
               pd.to_numeric(s.sample(min(len(s), sample_size)), errors="coerce").notna().mean() >= 0.6:
                numeric_cols.append(col)
        self._log(f"🔍 {len(numeric_cols)} colunas numéricas detectadas", 3)
        return numeric_cols

    @staticmethod
    def _reindex_regular(df: pd.DataFrame, freq: str) -> pd.DataFrame:
        df2 = df.copy()
        df2["timestamp_br"] = pd.to_datetime(df2["timestamp_br"])
        df2 = df2.sort_values("timestamp_br").drop_duplicates("timestamp_br")
        rng = pd.date_range(
            df2["timestamp_br"].min().floor(freq),
            df2["timestamp_br"].max().floor(freq),
            freq=freq,
            tz=df2["timestamp_br"].dt.tz
        )
        return (df2.set_index("timestamp_br")
                    .reindex(rng)
                    .rename_axis("timestamp_br")
                    .reset_index())

    @staticmethod
    def _interp(s: pd.Series, limit: int) -> pd.Series:
        return s.interpolate("linear", limit=limit, limit_direction="both")

    def _process(self, df: pd.DataFrame, freq: str, limit: int,
                 numeric_cols: List[str]) -> pd.DataFrame:
        df_reg = self._reindex_regular(df, freq)
        for col in numeric_cols:
            df_reg[col] = pd.to_numeric(df_reg[col], errors="coerce")
        for col in numeric_cols:
            n_before = df_reg[col].isna().sum()
            if n_before:
                df_reg[col] = self._interp(df_reg[col], limit)
                if is_numeric_dtype(df_reg[col]):
                    df_reg[col] = df_reg[col].round(4)
        return df_reg

    # ────────────── carga incremental ────────────── #
    def _update_last_hours(self, df: pd.DataFrame, table_id: str, hours: int = 12):
        now = pd.Timestamp.now(tz="America/Sao_Paulo")
        cutoff = (now - pd.Timedelta(hours=hours)).tz_localize(None)

        df_update = (
            df[pd.to_datetime(df["timestamp_br"]).dt.tz_localize(None) >= cutoff]
              .copy()
              .drop_duplicates("timestamp_br", keep="last")
        )
        if df_update.empty:
            self._log(f"Nenhum dado novo para atualizar em {table_id}", 1)
            return

        delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE timestamp_br >= DATETIME('{cutoff:%Y-%m-%d %H:%M:%S}')
        """
        self.client.query(delete_query).result()
        self._log(f"🗑️ Deletados dados de {table_id} a partir de {cutoff}", 2)

        df_update["timestamp_br"] = pd.to_datetime(df_update["timestamp_br"]).dt.tz_localize(None)

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            create_disposition="CREATE_IF_NEEDED"
        )
        self.client.load_table_from_dataframe(df_update, table_id, job_config=job_config).result()
        self._log(f"✅ {len(df_update)} linhas inseridas em {table_id}", 1)

    # --- NOVO: atualiza previsão 5min interpolando linearmente os dados ---
    def update_previsao_5min_interpolada(self, table_tratada, table_previsao, horas=12):
        now = pd.Timestamp.now(tz="America/Sao_Paulo")
        cutoff = (now - pd.Timedelta(hours=horas))

        # 1) Carrega previsões e timestamps da tratada nas últimas Xh
        df_prev = self.client.query(f"""
            SELECT timestamp_br, altura_prevista, altura_prev_getmare FROM `{table_previsao}`
            WHERE timestamp_br >= DATETIME('{cutoff:%Y-%m-%d %H:%M:%S}')
        """).to_dataframe()
        df_tratada = self.client.query(f"""
            SELECT timestamp_br FROM `{table_tratada}`
            WHERE timestamp_br >= DATETIME('{cutoff:%Y-%m-%d %H:%M:%S}')
        """).to_dataframe()

        if df_tratada.empty:
            print(f"Nenhuma linha para atualizar em {table_tratada}")
            return

        df_prev["timestamp_br"] = pd.to_datetime(df_prev["timestamp_br"])
        for col in ["altura_prevista", "altura_prev_getmare"]:
            df_prev[col] = pd.to_numeric(df_prev[col], errors="coerce")
        df_prev = df_prev.sort_values("timestamp_br")

        # Prepara todos os timestamps de 5min para update
        full_range = pd.DataFrame({
            "timestamp_br": df_tratada["timestamp_br"].sort_values().unique()
        })
        previsao_interp = pd.merge(full_range, df_prev, how="left", on="timestamp_br")

        for col in ["altura_prevista", "altura_prev_getmare"]:
            previsao_interp[col] = previsao_interp[col].interpolate("linear", limit_direction="both")
        previsao_interp["timestamp_prev"] = previsao_interp["timestamp_br"]

        # Merge com tratada para update só dos campos de previsão
        df_t = self.client.query(f"""
            SELECT * FROM `{table_tratada}` WHERE timestamp_br >= DATETIME('{cutoff:%Y-%m-%d %H:%M:%S}')
        """).to_dataframe()
        df_t["timestamp_br"] = pd.to_datetime(df_t["timestamp_br"])
        df_merge = pd.merge(df_t, previsao_interp, on="timestamp_br", how="left", suffixes=("", "_prev"))

        for col in ["altura_prevista", "altura_prev_getmare", "timestamp_prev"]:
            df_merge[col] = df_merge[f"{col}_prev"].combine_first(df_merge[col])
            df_merge.drop(columns=[f"{col}_prev"], inplace=True)

        # Salva sobrescrevendo só as linhas dessas X horas (WRITE_TRUNCATE)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED"
        )
        # Mantém apenas linhas desse período
        self.client.load_table_from_dataframe(df_merge, table_tratada, job_config=job_config).result()
        print(f"✅ Previsão (interpolada) atualizada na {table_tratada} para as últimas {horas} horas.")

    # ────────────── update previsao nas tratadas (1-hora direto) ────────── #
    def update_previsao_nas_tratadas(self, table_tratada, table_previsao, horas=12):
        now = pd.Timestamp.now(tz="America/Sao_Paulo")
        cutoff = (now - pd.Timedelta(hours=horas)).strftime('%Y-%m-%d %H:%M:%S')
        update_query = f"""
        UPDATE `{table_tratada}` AS t
        SET
        t.altura_prevista = CAST(p.altura_prevista AS NUMERIC),
        t.altura_prev_getmare = CAST(p.altura_prev_getmare AS NUMERIC),
        t.timestamp_prev = p.timestamp_br
        FROM `{table_previsao}` AS p
        WHERE
        t.timestamp_br = p.timestamp_br
        AND t.timestamp_br >= DATETIME('{cutoff}')
        """
        self.client.query(update_query).result()
        print(f"✅ Previsão atualizada na {table_tratada} para as últimas {horas} horas.")

    # ────────────── pipeline principal ────────────── #
    def run(self) -> None:
        df_raw = self.client.query(
            f"SELECT * FROM `{BQ_TABLE_SRC}` ORDER BY timestamp_br"
        ).to_dataframe()
        self._log(f"{len(df_raw):,} linhas lidas de {BQ_TABLE_SRC}", 1)

        df_raw = self._add_derived(df_raw)
        numeric_cols = self._find_numeric_cols(df_raw)

        df_5m = self._process(df_raw, "5T", self.max_gap_5m, numeric_cols)
        self._update_last_hours(df_5m, BQ_TABLE_DEST_5M)

        df_h = self._process(df_5m, "1H", self.max_gap_hour, numeric_cols)
        self._update_last_hours(df_h, BQ_TABLE_DEST_H)

        # Atualiza previsões: 5min com interpolação, 1h direto
        self.update_previsao_nas_tratadas(BQ_TABLE_DEST_H, BQ_TABLE_PREV, horas=24)
        self.update_previsao_5min_interpolada(BQ_TABLE_DEST_5M, BQ_TABLE_PREV, horas=24)

if __name__ == "__main__":
    try:
        BQTableInterpolator().run()
    except KeyboardInterrupt:
        sys.exit("\n⨇ Processo interrompido pelo usuário\n")
