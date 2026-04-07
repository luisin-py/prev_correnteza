#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Interpolador 5-min ↔︎ 1-hora + campos derivados

• direcao_<prof>_deg            (graus → N, NNE, …)
• enchente_vazante              (-1 vazante / +1 enchente)          – base 3 m
• enchente_vazante_<prof>       (-1 / +1 para cada profundidade)
• intensidade_<prof>_ajustada   = intensidade_<prof> * enchente_vazante_<prof>
• ventonum, fase_lua            → numérico
"""

from __future__ import annotations
import sys
from typing import List, Set

import numpy as np
import pandas as pd
from google.cloud import bigquery
from pandas.api.types import is_numeric_dtype

# ═══════════ BigQuery config ═══════════ #
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

        print("="*60)
        print("Gerador de tabelas tratadas/interpoladas no BigQuery")
        print("="*60)
        tipo = input("1 = 5-min | 2 = 1-hora | 3 = ambas  [3] > ").strip() or "3"
        self.tbl_tipo = int(tipo) if tipo in {"1", "2", "3"} else 3

        horas = input("Limite máx. de buraco em horas  [6] > ").strip() or "6"
        self.max_hours = int(horas) if horas.isdigit() else 6

        lvl = input("Log – 1=min, 2=resume, 3=verbose  [2] > ").strip() or "2"
        self.log_lvl = int(lvl) if lvl in {"1", "2", "3"} else 2
        print()

        self.max_gap_5m   = self.max_hours * 12
        self.max_gap_hour = self.max_hours

    def _log(self, msg: str, lvl: int = 2) -> None:
        if self.log_lvl >= lvl:
            print(msg)

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

            if dir_deg in df.columns:
                df[ev_col] = df[dir_deg].str.upper().map(
                    lambda x: -1 if x in QUADRANTES_VAZANTE else 1
                )
            else:
                df[ev_col] = np.nan

            if int_col in df.columns:
                df[adj_col] = (
                    pd.to_numeric(df[int_col], errors="coerce") * df[ev_col]
                )
            else:
                df[adj_col] = np.nan

        if "direcao_3m_deg" in df.columns:
            df["enchente_vazante"] = df["direcao_3m_deg"].str.upper().map(
                lambda x: -1 if x in QUADRANTES_VAZANTE else 1
            )
            if "intensidade_3m_kt" in df.columns:
                df["intensidade_3m_ajustada"] = (
                    pd.to_numeric(df["intensidade_3m_kt"], errors="coerce")
                    * df["enchente_vazante"]
                )
        else:
            df["enchente_vazante"] = np.nan
            df["intensidade_3m_ajustada"] = np.nan

        for col in NUMERIC_FORCE:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    def _find_numeric_cols(self, df: pd.DataFrame) -> List[str]:
        numeric_cols: List[str] = []
        sample_size = 2000
        for col in df.columns:
            if col in EXCLUDE_NUMERIC | {"timestamp_br"}:
                continue
            s = df[col].dropna()
            if len(s) < 2:
                continue
            if is_numeric_dtype(s):
                numeric_cols.append(col)
                continue
            coerced = pd.to_numeric(
                s.sample(min(len(s), sample_size)), errors="coerce"
            )
            if coerced.notna().mean() >= 0.60:
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
        return (
            df2.set_index("timestamp_br")
            .reindex(rng)
            .rename_axis("timestamp_br")
            .reset_index()
        )

    @staticmethod
    def _interp(s: pd.Series, limit: int) -> pd.Series:
        return s.interpolate("linear", limit=limit, limit_direction="both")

    def _ensure_previsao_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Garante as colunas de previsão, tipos e consistência no DataFrame."""
        cols_schema = [
            ("altura_prevista", float, np.nan),
            ("altura_prev_getmare", float, np.nan),
            ("timestamp_prev", "datetime64[ns]", pd.NaT)
        ]
        for col, tipo, default in cols_schema:
            if col not in df.columns:
                df[col] = default
            if tipo == float:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif tipo == "datetime64[ns]":
                df[col] = pd.to_datetime(df[col], errors="coerce")
        return df

    def _save(self, df: pd.DataFrame, table_id: str) -> None:
        # Garante as colunas de previsão antes de salvar
        df = self._ensure_previsao_schema(df)
        cfg = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED"
        )
        self.client.load_table_from_dataframe(df, table_id, job_config=cfg).result()
        self._log(f"✅ gravado em {table_id}", 1)

    def update_previsao_5min_interpolada(self, table_tratada, table_previsao):
        """
        Atualiza os campos de previsão na tabela tratada 5min interpolando linearmente
        os dados de previsão (tipicamente de 1h) para todos os timestamps da tratada.
        """
        # 1) Carrega previsões e timestamps da tratada
        df_prev = self.client.query(f"SELECT timestamp_br, altura_prevista, altura_prev_getmare FROM `{table_previsao}`").to_dataframe()
        df_tratada = self.client.query(f"SELECT timestamp_br FROM `{table_tratada}`").to_dataframe()

        # 2) Prepara DataFrame para interpolação (garante datetime e ordenação)
        df_prev["timestamp_br"] = pd.to_datetime(df_prev["timestamp_br"])
        for col in ["altura_prevista", "altura_prev_getmare"]:
            df_prev[col] = pd.to_numeric(df_prev[col], errors="coerce")
        df_prev = df_prev.sort_values("timestamp_br")

        # 3) Reindexa para todos os timestamps de 5min
        full_range = pd.DataFrame({
            "timestamp_br": df_tratada["timestamp_br"].sort_values().unique()
        })
        previsao_interp = pd.merge(full_range, df_prev, how="left", on="timestamp_br")

        # 4) Interpola os valores de previsão
        for col in ["altura_prevista", "altura_prev_getmare"]:
            previsao_interp[col] = previsao_interp[col].interpolate("linear", limit_direction="both")
        previsao_interp["timestamp_prev"] = previsao_interp["timestamp_br"]

        # 5) Atualiza só os campos de previsão (faz update usando merge com a tratada)
        # Carrega tratada para merge
        df_t = self.client.query(f"SELECT * FROM `{table_tratada}`").to_dataframe()
        df_t["timestamp_br"] = pd.to_datetime(df_t["timestamp_br"])
        df_merge = pd.merge(df_t, previsao_interp, on="timestamp_br", how="left", suffixes=("", "_prev"))

        # Atualiza os campos de previsão na tratada
        for col in ["altura_prevista", "altura_prev_getmare", "timestamp_prev"]:
            df_merge[col] = df_merge[f"{col}_prev"].combine_first(df_merge[col])
            df_merge.drop(columns=[f"{col}_prev"], inplace=True)

        # Garante schema antes de salvar
        df_merge = self._ensure_previsao_schema(df_merge)
        # Salva sobrescrevendo tudo na tratada (mantém outras colunas intactas)
        cfg = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED"
        )
        self.client.load_table_from_dataframe(df_merge, table_tratada, job_config=cfg).result()
        print(f"✅ Previsão interpolada (5min) adicionada em {table_tratada}")

    def update_previsao_completa(self, table_tratada, table_previsao):
        # Garante que as colunas existem na tabela BQ antes do update
        for col, bqtype in [
            ("altura_prevista", "NUMERIC"),
            ("altura_prev_getmare", "NUMERIC"),
            ("timestamp_prev", "DATETIME"),
        ]:
            self.client.query(
                f"ALTER TABLE `{table_tratada}` ADD COLUMN IF NOT EXISTS {col} {bqtype}"
            ).result()
        # UPDATE usando timestamp_br como chave (igual nas duas tabelas)
        update_query = f"""
        UPDATE `{table_tratada}` AS t
        SET
            t.altura_prevista = CAST(p.altura_prevista AS NUMERIC),
            t.altura_prev_getmare = CAST(p.altura_prev_getmare AS NUMERIC),
            t.timestamp_prev = p.timestamp_br
        FROM `{table_previsao}` AS p
        WHERE t.timestamp_br = p.timestamp_br
        """
        self.client.query(update_query).result()
        print(f"✅ Previsão completa atualizada em {table_tratada}")

    def run(self) -> None:
        df_raw = self.client.query(
            f"SELECT * FROM `{BQ_TABLE_SRC}` ORDER BY timestamp_br"
        ).to_dataframe()
        self._log(f"{len(df_raw):,} linhas lidas de {BQ_TABLE_SRC}", 1)

        # acrescenta os novos campos antes de qualquer re-amostragem
        df_raw = self._add_derived(df_raw)
        numeric_cols = self._find_numeric_cols(df_raw)

        # 5-min (com interpolação linear)
        if self.tbl_tipo in {1, 3}:
            df_5m = self._process(df_raw, "5T", self.max_gap_5m, numeric_cols)
            self._save(df_5m, BQ_TABLE_DEST_5M)

        # 1-hora
        if self.tbl_tipo in {2, 3}:
            base = df_5m if "df_5m" in locals() else df_raw
            df_h = self._process(base, "1H", self.max_gap_hour, numeric_cols)
            self._save(df_h, BQ_TABLE_DEST_H)

        # Atualiza previsões (5min: interpolado; 1h: update direto)
        self.update_previsao_5min_interpolada(BQ_TABLE_DEST_5M, BQ_TABLE_PREV)
        self.update_previsao_completa(BQ_TABLE_DEST_H, BQ_TABLE_PREV)

    def _process(self, df: pd.DataFrame, freq: str, limit: int,
                 numeric_cols: List[str]) -> pd.DataFrame:
        df_reg = self._reindex_regular(df, freq)
        for col in numeric_cols:
            df_reg[col] = pd.to_numeric(df_reg[col], errors="coerce")
        for col in numeric_cols:
            n_before = df_reg[col].isna().sum()
            if n_before == 0:
                self._log(f"[{col}] sem buracos", 3)
                continue
            df_reg[col] = self._interp(df_reg[col], limit)
            filled = n_before - df_reg[col].isna().sum()
            if filled:
                self._log(f"[{col}] +{filled} preenchidos", 2)
            if is_numeric_dtype(df_reg[col]):
                df_reg[col] = df_reg[col].round(4)
        return df_reg

# ═══════════════════════════════════════════════════════════════ #
if __name__ == "__main__":
    try:
        BQTableInterpolator().run()
    except KeyboardInterrupt:
        sys.exit("\n⨂ Processo interrompido pelo usuário\n")
