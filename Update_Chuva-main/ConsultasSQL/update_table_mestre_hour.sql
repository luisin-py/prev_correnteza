DECLARE limite_atualizacao TIMESTAMP;

-- 1. Definir o limite: 24 horas antes do último timestamp_br na tabela
SET limite_atualizacao = (
  SELECT TIMESTAMP_SUB(MAX(TIMESTAMP(timestamp_br)), INTERVAL 24 HOUR)
  FROM `local-bliss-359814.wherehouse_tratado.mestre_hour`
);

-- 2. Deletar linhas a partir do limite
DELETE FROM `local-bliss-359814.wherehouse_tratado.mestre_hour`
WHERE TIMESTAMP(timestamp_br) >= limite_atualizacao;

-- 3. Recalcular e reinserir as linhas a partir desse ponto
INSERT INTO `local-bliss-359814.wherehouse_tratado.mestre_hour`
WITH

upd_hour AS (
  SELECT
    SAFE_CAST(TIMESTAMP_TRUNC(TIMESTAMP(timestamp), HOUR) AS STRING) AS ts_hour,
    SAFE_CAST(ANY_VALUE(timestamp) AS STRING) AS timestamp,
    SAFE_CAST(DATETIME_ADD(DATETIME_TRUNC(SAFE_CAST(ANY_VALUE(timestamp) AS DATETIME), HOUR), INTERVAL 59 MINUTE) AS STRING) AS timestamp_end,
    SAFE_CAST(ANY_VALUE(data_station_davis) AS STRING) AS data_station_davis,
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
    SAFE_CAST(ANY_VALUE(ventodirecao) AS STRING) AS ventodirecao,
    SAFE_CAST(ANY_VALUE(status) AS STRING) AS status,
    SAFE_CAST(ANY_VALUE(data_inicio) AS STRING) AS data_inicio,
    SAFE_CAST(ANY_VALUE(numero) AS STRING) AS numero,
    SAFE_CAST(ANY_VALUE(descricao) AS STRING) AS descricao,
    SAFE_CAST(ANY_VALUE(data) AS STRING) AS data,
    SAFE_CAST(ANY_VALUE(tipo) AS STRING) AS tipo,
    SAFE_CAST(ANY_VALUE(data_lua) AS STRING) AS data_lua,
    SAFE_CAST(ANY_VALUE(nascer_do_sol) AS STRING) AS nascer_do_sol,
    SAFE_CAST(ANY_VALUE(por_do_sol) AS STRING) AS por_do_sol,
    SAFE_CAST(ANY_VALUE(matutino) AS STRING) AS matutino,
    SAFE_CAST(ANY_VALUE(vespertino) AS STRING) AS vespertino,
    SAFE_CAST(ANY_VALUE(tipo_mare_getControls) AS STRING) AS tipo_mare_getcontrols,
    SAFE_CAST(ANY_VALUE(data_mare_getControls) AS STRING) AS data_mare_getcontrols,
    SAFE_CAST(ANY_VALUE(data_hidromares) AS STRING) AS data_hidromares,
    SAFE_CAST(ANY_VALUE(data_mare_getMare) AS STRING) AS data_mare_getmare,
    SAFE_CAST(ANY_VALUE(data_mare_real_getMare) AS STRING) AS data_mare_real_getmare,
    SAFE_CAST(ANY_VALUE(api_mare) AS STRING) AS api_mare,
    SAFE_CAST(ANY_VALUE(api_hidromares) AS STRING) AS api_hidromares,
    SAFE_CAST(ANY_VALUE(api_estatistica) AS STRING) AS api_estatistica,
    SAFE_CAST(ANY_VALUE(motivo) AS STRING) AS motivo
  FROM `local-bliss-359814.wherehouse.update_rawdata_5min_backfill`
  WHERE TIMESTAMP(timestamp) >= limite_atualizacao
  GROUP BY ts_hour
),
/* ───────────────────────  INMET  ─────────────────────── */
inm_hour AS (
  SELECT
    -- UTC – 3 h ⇒ mesma hora-cheia do timestamp_br
    CAST(
      TIMESTAMP_TRUNC(
        TIMESTAMP_SUB(TIMESTAMP(dt_utc), INTERVAL 3 HOUR),  -- −3 h
        HOUR) AS STRING)                                    AS ts_hour,

    -- carimbo de leitura (UTC-3) apenas para referência
    CAST(
      TIMESTAMP_SUB(TIMESTAMP(dt_utc), INTERVAL 3 HOUR)
      AS STRING)                                           AS inmet_datetime,

    SAFE_CAST(ANY_VALUE(temperatura_inst)   AS NUMERIC) AS temperatura_inst_inmet,
    SAFE_CAST(ANY_VALUE(temperatura_max)    AS NUMERIC) AS temperatura_max_inmet,
    SAFE_CAST(ANY_VALUE(temperatura_min)    AS NUMERIC) AS temperatura_min_inmet,
    SAFE_CAST(ANY_VALUE(umidade_inst)       AS NUMERIC) AS umidade_inst_inmet,
    SAFE_CAST(ANY_VALUE(umidade_max)        AS NUMERIC) AS umidade_max_inmet,
    SAFE_CAST(ANY_VALUE(umidade_min)        AS NUMERIC) AS umidade_min_inmet,
    SAFE_CAST(ANY_VALUE(pto_orvalho_inst)   AS NUMERIC) AS pto_orvalho_inst_inmet,
    SAFE_CAST(ANY_VALUE(pto_orvalho_max)    AS NUMERIC) AS pto_orvalho_max_inmet,
    SAFE_CAST(ANY_VALUE(pto_orvalho_min)    AS NUMERIC) AS pto_orvalho_min_inmet,
    SAFE_CAST(ANY_VALUE(pressao_inst)       AS NUMERIC) AS pressao_inst_inmet,
    SAFE_CAST(ANY_VALUE(pressao_max)        AS NUMERIC) AS pressao_max_inmet,
    SAFE_CAST(ANY_VALUE(pressao_min)        AS NUMERIC) AS pressao_min_inmet,
    SAFE_CAST(ANY_VALUE(vento_vel_m_s)      AS NUMERIC) AS vento_vel_m_s_inmet,
    SAFE_CAST(ANY_VALUE(vento_dir_deg)      AS NUMERIC) AS vento_dir_deg_inmet,
    SAFE_CAST(ANY_VALUE(vento_raj_m_s)      AS NUMERIC) AS vento_raj_m_s_inmet,
    SAFE_CAST(ANY_VALUE(radiacao)           AS NUMERIC) AS radiacao_inmet,
    SAFE_CAST(ANY_VALUE(chuva)              AS NUMERIC) AS chuva_inmet,

    ANY_VALUE(dt_utc)             AS dt_utc_inmet,
    ANY_VALUE(data)               AS data_inmet,
    ANY_VALUE(hora_utc)           AS hora_utc_inmet,
    ANY_VALUE(timestamp_execucao) AS timestamp_execucao_inmet
  FROM `local-bliss-359814.wherehouse.dados_inmet_estacoes`
  WHERE estacao = 'A802'
  GROUP BY ts_hour, inmet_datetime
),

/* ─────────────────────  OpenWeather NOW  ───────────────────── */
ow_now AS (
  SELECT
    -- UTC–3 h ⇒ alinha com timestamp_br
    CAST(
      TIMESTAMP_TRUNC(
        TIMESTAMP_SUB(TIMESTAMP(timestamp_utc), INTERVAL 3 HOUR),
        HOUR
      ) AS STRING
    ) AS ts_hour,

    -- carimbo de coleta (UTC–3)
    CAST(
      TIMESTAMP_SUB(MAX(timestamp_utc), INTERVAL 3 HOUR)
      AS STRING
    ) AS openweather_timestamp,

    -- identificação de cidade
    CAST(ANY_VALUE(city)    AS STRING)  AS ow_city,
    SAFE_CAST(ANY_VALUE(city_id)   AS NUMERIC) AS ow_city_id,
    SAFE_CAST(ANY_VALUE(coord_lon) AS NUMERIC) AS ow_coord_lon,
    SAFE_CAST(ANY_VALUE(coord_lat) AS NUMERIC) AS ow_coord_lat,

    -- condições de tempo
    SAFE_CAST(ANY_VALUE(weather_id)     AS NUMERIC) AS ow_weather_id,
    CAST(ANY_VALUE(weather_main)        AS STRING)  AS ow_weather_main,
    CAST(ANY_VALUE(weather_description) AS STRING)  AS ow_weather_desc,
    CAST(ANY_VALUE(weather_icon)        AS STRING)  AS ow_weather_icon,
    CAST(ANY_VALUE(base)                AS STRING)  AS ow_base,

    -- principais parâmetros
    SAFE_CAST(ANY_VALUE(main_temp)       AS NUMERIC) AS ow_temp,
    SAFE_CAST(ANY_VALUE(main_feels_like) AS NUMERIC) AS ow_feels_like,
    SAFE_CAST(ANY_VALUE(main_temp_min)   AS NUMERIC) AS ow_temp_min,
    SAFE_CAST(ANY_VALUE(main_temp_max)   AS NUMERIC) AS ow_temp_max,
    SAFE_CAST(ANY_VALUE(main_pressure)   AS NUMERIC) AS ow_pressure,
    SAFE_CAST(ANY_VALUE(main_humidity)   AS NUMERIC) AS ow_humidity,
    SAFE_CAST(ANY_VALUE(main_sea_level)  AS NUMERIC) AS ow_sea_level,
    SAFE_CAST(ANY_VALUE(main_grnd_level) AS NUMERIC) AS ow_grnd_level,
    SAFE_CAST(ANY_VALUE(visibility)      AS NUMERIC) AS ow_visibility,

    -- vento e nuvens
    SAFE_CAST(ANY_VALUE(wind_speed) AS NUMERIC) AS ow_wind_speed,
    SAFE_CAST(ANY_VALUE(wind_deg)   AS NUMERIC) AS ow_wind_deg,
    SAFE_CAST(ANY_VALUE(wind_gust)  AS NUMERIC) AS ow_wind_gust,
    SAFE_CAST(ANY_VALUE(clouds_all) AS NUMERIC) AS ow_clouds,

    -- precipitação e neve
    SAFE_CAST(ANY_VALUE(rain_1h) AS NUMERIC) AS ow_rain_1h,
    SAFE_CAST(ANY_VALUE(rain_3h) AS NUMERIC) AS ow_rain_3h,
    SAFE_CAST(ANY_VALUE(snow_1h) AS NUMERIC) AS ow_snow_1h,
    SAFE_CAST(ANY_VALUE(snow_3h) AS NUMERIC) AS ow_snow_3h,

    -- timestamps originais
    ANY_VALUE(dt)        AS ow_dt,
    ANY_VALUE(timestamp) AS ow_timestamp,

    -- sistema
    SAFE_CAST(ANY_VALUE(sys_type)       AS NUMERIC) AS ow_sys_type,
    SAFE_CAST(ANY_VALUE(sys_id)         AS NUMERIC) AS ow_sys_id,
    CAST(ANY_VALUE(sys_country)         AS STRING)  AS ow_sys_country,
    ANY_VALUE(sys_sunrise)              AS ow_sys_sunrise,
    ANY_VALUE(sys_sunset)               AS ow_sys_sunset,
    SAFE_CAST(ANY_VALUE(timezone_offset) AS NUMERIC) AS ow_timezone_offset,
    SAFE_CAST(ANY_VALUE(cod)            AS NUMERIC) AS ow_cod,

    -- UTC original
    ANY_VALUE(timestamp_utc) AS ow_timestamp_utc

  FROM `local-bliss-359814.wherehouse.dados_openwhather`
  WHERE city = 'Rio Grande'
  GROUP BY ts_hour
),


limite_hora_real AS (
  SELECT MAX(TIMESTAMP_TRUNC(TIMESTAMP(timestamp), HOUR)) AS ts_limite
  FROM `local-bliss-359814.wherehouse.update_rawdata_5min_backfill`
),

fc_rep AS (
  SELECT
    CAST(TIMESTAMP_TRUNC(TIMESTAMP(timestamp_execucao), HOUR) AS STRING) AS ts_hour,
    CAST(MAX(timestamp_execucao) AS STRING) AS forecast_timestamp_execucao,
    SAFE_CAST(ANY_VALUE(timestamp_utc) AS STRING) AS fc_timestamp_utc,
    SAFE_CAST(ANY_VALUE(city_name) AS STRING) AS fc_city_name,
    SAFE_CAST(ANY_VALUE(lat) AS FLOAT64) AS fc_lat,
    SAFE_CAST(ANY_VALUE(lon) AS FLOAT64) AS fc_lon,
    SAFE_CAST(ANY_VALUE(temp) AS FLOAT64) AS fc_temp,
    SAFE_CAST(ANY_VALUE(feels_like) AS FLOAT64) AS fc_feels_like,
    SAFE_CAST(ANY_VALUE(temp_min) AS FLOAT64) AS fc_temp_min,
    SAFE_CAST(ANY_VALUE(temp_max) AS FLOAT64) AS fc_temp_max,
    SAFE_CAST(ANY_VALUE(pressure) AS FLOAT64) AS fc_pressure,
    SAFE_CAST(ANY_VALUE(sea_level) AS FLOAT64) AS fc_sea_level,
    SAFE_CAST(ANY_VALUE(grnd_level) AS FLOAT64) AS fc_grnd_level,
    SAFE_CAST(ANY_VALUE(humidity) AS FLOAT64) AS fc_humidity,
    SAFE_CAST(ANY_VALUE(weather_main) AS STRING) AS fc_weather_main,
    SAFE_CAST(ANY_VALUE(weather_description) AS STRING) AS fc_weather_desc,
    SAFE_CAST(ANY_VALUE(weather_icon) AS STRING) AS fc_weather_icon,
    SAFE_CAST(ANY_VALUE(clouds_all) AS FLOAT64) AS fc_clouds_all,
    SAFE_CAST(ANY_VALUE(wind_speed) AS FLOAT64) AS fc_wind_speed,
    SAFE_CAST(ANY_VALUE(wind_deg) AS FLOAT64) AS fc_wind_deg,
    SAFE_CAST(ANY_VALUE(wind_gust) AS FLOAT64) AS fc_wind_gust,
    SAFE_CAST(ANY_VALUE(visibility) AS FLOAT64) AS fc_visibility,
    SAFE_CAST(ANY_VALUE(pop) AS FLOAT64) AS fc_pop,
    SAFE_CAST(ANY_VALUE(rain_3h) AS FLOAT64) AS fc_rain_3h,
    SAFE_CAST(ANY_VALUE(snow_3h) AS FLOAT64) AS fc_snow_3h,
    SAFE_CAST(ANY_VALUE(sys_pod) AS STRING) AS fc_sys_pod,
    SAFE_CAST(ANY_VALUE(timezone) AS FLOAT64) AS fc_timezone,
    SAFE_CAST(ANY_VALUE(cod) AS FLOAT64) AS fc_cod,
    SAFE_CAST(ANY_VALUE(dt_txt) AS STRING) AS fc_dt_txt
  FROM `local-bliss-359814.wherehouse.dados_openweather_forecast`
  CROSS JOIN limite_hora_real
  WHERE city_name = 'Rio Grande'
    AND TIMESTAMP_TRUNC(TIMESTAMP(timestamp_execucao), HOUR) <= limite_hora_real.ts_limite
  GROUP BY ts_hour
),

air_rep AS (
  SELECT
    CAST(TIMESTAMP_TRUNC(TIMESTAMP(timestamp_execucao), HOUR) AS STRING) AS ts_hour,
    CAST(MAX(timestamp_execucao) AS STRING) AS airpollution_timestamp_execucao,
    SAFE_CAST(ANY_VALUE(aqi) AS NUMERIC) AS air_aqi,
    SAFE_CAST(ANY_VALUE(pm2_5) AS NUMERIC) AS air_pm2_5,
    SAFE_CAST(ANY_VALUE(pm10) AS NUMERIC) AS air_pm10,
    SAFE_CAST(ANY_VALUE(co) AS NUMERIC) AS air_co,
    SAFE_CAST(ANY_VALUE(`no`) AS NUMERIC) AS air_no,
    SAFE_CAST(ANY_VALUE(no2) AS NUMERIC) AS air_no2,
    SAFE_CAST(ANY_VALUE(o3) AS NUMERIC) AS air_o3,
    SAFE_CAST(ANY_VALUE(so2) AS NUMERIC) AS air_so2,
    SAFE_CAST(ANY_VALUE(nh3) AS NUMERIC) AS air_nh3
  FROM `local-bliss-359814.wherehouse.dados_openweather_air_pollution`
  CROSS JOIN limite_hora_real
  WHERE city_name = 'Rio Grande'
    AND TIMESTAMP_TRUNC(TIMESTAMP(timestamp_execucao), HOUR) <= limite_hora_real.ts_limite
  GROUP BY ts_hour
),

horas AS (
  SELECT ts_hour FROM upd_hour
)

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
