/* =========================================================
   TABELA MESTRE HORÁRIA
   Cria (ou recria) local-bliss-359814.wherehouse_tratado.mestre_hour
   Minha key mestre de data veem na update_... pq o script roda no functions e por isso nuncavai estar fora
   ========================================================= */
CREATE OR REPLACE TABLE
  `local-bliss-359814.wherehouse_tratado.mestre_hour`
AS
WITH
/* ──────────────────────────────────────────────
   1. UPDATE – 5 min  ➜ agrega para 1 h
   ────────────────────────────────────────────── */
upd_hour AS (
  SELECT
    TIMESTAMP_TRUNC(TIMESTAMP(timestamp), HOUR) AS ts_hour,
    ANY_VALUE(timestamp) AS timestamp,
    DATETIME_ADD(DATETIME_TRUNC(ANY_VALUE(timestamp), HOUR), INTERVAL 59 MINUTE) AS timestamp_end,
    ANY_VALUE(data_station_davis) AS data_station_davis,
    -- médias numéricas
    SAFE_CAST(AVG(umidade) AS NUMERIC) AS umidade,
    SAFE_CAST(AVG(pressao) AS NUMERIC) AS pressao,
    SAFE_CAST(AVG(ventointensidade) AS NUMERIC) AS ventointensidade,
    SAFE_CAST(AVG(ventonum) AS NUMERIC) AS ventonum,
    SAFE_CAST(AVG(temperatura) AS NUMERIC) AS temperatura,
    SAFE_CAST(AVG(sensacaotermica) AS NUMERIC) AS sensacaotermica,
    SAFE_CAST(AVG(altura_medida_getControls) AS NUMERIC) AS altura_medida_getcontrols,
    SAFE_CAST(AVG(intensidade_15m) AS NUMERIC) AS intensidade_15m,
    SAFE_CAST(AVG(direcao_15m) AS NUMERIC) AS direcao_15m,
    SAFE_CAST(AVG(intensidade_13_5m) AS NUMERIC) AS intensidade_13_5m,
    SAFE_CAST(AVG(direcao_13_5m) AS NUMERIC) AS direcao_13_5m,
    SAFE_CAST(AVG(intensidade_12m) AS NUMERIC) AS intensidade_12m,
    SAFE_CAST(AVG(direcao_12m) AS NUMERIC) AS direcao_12m,
    SAFE_CAST(AVG(intensidade_10_5m) AS NUMERIC) AS intensidade_10_5m,
    SAFE_CAST(AVG(direcao_10_5m) AS NUMERIC) AS direcao_10_5m,
    SAFE_CAST(AVG(intensidade_9m) AS NUMERIC) AS intensidade_9m,
    SAFE_CAST(AVG(direcao_9m) AS NUMERIC) AS direcao_9m,
    SAFE_CAST(AVG(intensidade_7_5m) AS NUMERIC) AS intensidade_7_5m,
    SAFE_CAST(AVG(direcao_7_5m) AS NUMERIC) AS direcao_7_5m,
    SAFE_CAST(AVG(intensidade_superficie) AS NUMERIC) AS intensidade_superficie,
    SAFE_CAST(AVG(direcao_superficie) AS NUMERIC) AS direcao_superficie,
    SAFE_CAST(AVG(intensidade_6m) AS NUMERIC) AS intensidade_6m,
    SAFE_CAST(AVG(direcao_6m) AS NUMERIC) AS direcao_6m,
    SAFE_CAST(AVG(intensidade_3m) AS NUMERIC) AS intensidade_3m,
    SAFE_CAST(AVG(direcao_3m) AS NUMERIC) AS direcao_3m,
    SAFE_CAST(AVG(intensidade_1_5m) AS NUMERIC) AS intensidade_1_5m,
    SAFE_CAST(AVG(direcao_1_5m) AS NUMERIC) AS direcao_1_5m,
    SAFE_CAST(AVG(altura_prev_getmare) AS NUMERIC) AS altura_prev_getmare,
    SAFE_CAST(AVG(altura_real_getmare) AS NUMERIC) AS altura_real_getmare,
    -- valores categóricos
    ANY_VALUE(ventodirecao) AS ventodirecao,
    ANY_VALUE(status) AS status,
    ANY_VALUE(data_inicio) AS data_inicio,
    ANY_VALUE(numero) AS numero,
    ANY_VALUE(descricao) AS descricao,
    ANY_VALUE(data) AS data,
    ANY_VALUE(tipo) AS tipo,
    ANY_VALUE(data_lua) AS data_lua,
    ANY_VALUE(nascer_do_sol) AS nascer_do_sol,
    ANY_VALUE(por_do_sol) AS por_do_sol,
    ANY_VALUE(matutino) AS matutino,
    ANY_VALUE(vespertino) AS vespertino,
    ANY_VALUE(tipo_mare_getControls) AS tipo_mare_getcontrols,
    ANY_VALUE(data_mare_getControls) AS data_mare_getcontrols,
    ANY_VALUE(data_hidromares) AS data_hidromares,
    ANY_VALUE(data_mare_getMare) AS data_mare_getmare,
    ANY_VALUE(data_mare_real_getMare) AS data_mare_real_getmare,
    ANY_VALUE(api_mare) AS api_mare,
    ANY_VALUE(api_hidromares) AS api_hidromares,
    ANY_VALUE(api_estatistica) AS api_estatistica,
    ANY_VALUE(motivo) AS motivo
  FROM `local-bliss-359814.wherehouse.update_rawdata_5min_backfill`
  GROUP BY ts_hour
),

/* ──────────────────────────────────────────────
   2. INMET – estação A802 (1 h)
   ────────────────────────────────────────────── */
inm_hour AS (
  SELECT
    TIMESTAMP_TRUNC(TIMESTAMP(timestamp_execucao), HOUR) AS ts_hour,
    ANY_VALUE(temperatura_inst) AS temperatura_inst_inmet,
    ANY_VALUE(temperatura_max) AS temperatura_max_inmet,
    ANY_VALUE(temperatura_min) AS temperatura_min_inmet,
    ANY_VALUE(umidade_inst) AS umidade_inst_inmet,
    ANY_VALUE(pressao_inst) AS pressao_inst_inmet,
    ANY_VALUE(vento_vel_m_s) AS vento_vel_m_s_inmet,
    ANY_VALUE(vento_dir_deg) AS vento_dir_deg_inmet,
    ANY_VALUE(chuva) AS chuva_inmet,
    ANY_VALUE(radiacao) AS radiacao_inmet
  FROM `local-bliss-359814.wherehouse.dados_inmet_estacoes_backfill`
  WHERE estacao = 'A802'
  GROUP BY ts_hour
),

/* ──────────────────────────────────────────────
   3. OpenWeather NOW – 5 min
   ────────────────────────────────────────────── */
ow_now AS (
  SELECT
    TIMESTAMP_TRUNC(TIMESTAMP(timestamp_utc), HOUR) AS ts_hour,
    ANY_VALUE(main_temp) AS ow_temp,
    ANY_VALUE(main_humidity) AS ow_humidity,
    ANY_VALUE(wind_speed) AS ow_wind_speed,
    ANY_VALUE(clouds_all) AS ow_clouds,
    ANY_VALUE(weather_main) AS ow_weather_main,
    ANY_VALUE(weather_description) AS ow_weather_desc
  FROM `local-bliss-359814.wherehouse.dados_openwhather`
  WHERE city = 'Rio Grande'
  GROUP BY ts_hour
),

/* ──────────────────────────────────────────────
   4. OpenWeather FORECAST – 3 h ⇒ replica 1 h
   ────────────────────────────────────────────── */
fc_rep AS (
  SELECT
    ts_hour,
    ANY_VALUE(temp) AS fc_temp,
    ANY_VALUE(pop) AS fc_pop,
    ANY_VALUE(feels_like) AS fc_feels_like,
    ANY_VALUE(weather_main) AS fc_weather_main,
    ANY_VALUE(weather_description) AS fc_weather_desc
  FROM (
    SELECT
      TIMESTAMP_TRUNC(TIMESTAMP(timestamp_utc), HOUR) AS base_ts,
      *
    FROM `local-bliss-359814.wherehouse.dados_openweather_forecast`
    WHERE city_name = 'Rio Grande'
  ),
  UNNEST(GENERATE_TIMESTAMP_ARRAY(base_ts, base_ts + INTERVAL 2 HOUR, INTERVAL 1 HOUR)) AS ts_hour
  GROUP BY ts_hour
),

/* ──────────────────────────────────────────────
   5. OpenWeather AIR POLLUTION – 3 h ⇒ replica 1 h
   ────────────────────────────────────────────── */
air_rep AS (
  SELECT
    ts_hour,
    ANY_VALUE(aqi) AS air_aqi,
    ANY_VALUE(pm2_5) AS air_pm2_5,
    ANY_VALUE(pm10) AS air_pm10,
    ANY_VALUE(co) AS air_co,
    ANY_VALUE(`no`) AS air_no,
    ANY_VALUE(no2) AS air_no2,
    ANY_VALUE(o3) AS air_o3,
    ANY_VALUE(so2) AS air_so2,
    ANY_VALUE(nh3) AS air_nh3
  FROM (
    SELECT
      TIMESTAMP_TRUNC(TIMESTAMP(timestamp_utc), HOUR) AS base_ts,
      *
    FROM `local-bliss-359814.wherehouse.dados_openweather_air_pollution`
    WHERE city_name = 'Rio Grande'
  ),
  UNNEST(GENERATE_TIMESTAMP_ARRAY(base_ts, base_ts + INTERVAL 2 HOUR, INTERVAL 1 HOUR)) AS ts_hour
  GROUP BY ts_hour
),

/* ──────────── 6. Conjunto completo de horas ──────────── */
horas AS (
  SELECT ts_hour FROM upd_hour
  UNION DISTINCT SELECT ts_hour FROM inm_hour
  UNION DISTINCT SELECT ts_hour FROM ow_now
  UNION DISTINCT SELECT ts_hour FROM fc_rep
  UNION DISTINCT SELECT ts_hour FROM air_rep
)

/* ──────────── 7. JOIN FINAL ──────────── */
SELECT
  h.ts_hour AS timestamp_br,
  u.* EXCEPT(ts_hour),
  i.* EXCEPT(ts_hour),
  o.* EXCEPT(ts_hour),
  f.* EXCEPT(ts_hour),
  a.* EXCEPT(ts_hour)
FROM horas h
LEFT JOIN upd_hour u ON h.ts_hour = u.ts_hour
LEFT JOIN inm_hour i ON h.ts_hour = i.ts_hour
LEFT JOIN ow_now o ON h.ts_hour = o.ts_hour
LEFT JOIN fc_rep f ON h.ts_hour = f.ts_hour
LEFT JOIN air_rep a ON h.ts_hour = a.ts_hour
ORDER BY timestamp_br;
