-- ───────────────────────── 0. Limite de recálculo (24 h) ─────────────────────────
DECLARE limite_atualizacao TIMESTAMP;

SET limite_atualizacao = (
  SELECT TIMESTAMP_SUB(MAX(TIMESTAMP(timestamp_br)), INTERVAL 24 HOUR)
  FROM `local-bliss-359814.wherehouse_tratado.mestre_5min`
);

-- ───────────────────────── 1. Remove as linhas a partir do limite ─────────────────
DELETE FROM `local-bliss-359814.wherehouse_tratado.mestre_5min`
WHERE TIMESTAMP(timestamp_br) >= limite_atualizacao;

-- ───────────────────────── 2. Reinsere linhas recalculadas ────────────────────────
INSERT INTO `local-bliss-359814.wherehouse_tratado.mestre_5min`
WITH
/* ───────────────────────── 2.1 UPDATE – 5 min ───────────────────────── */
upd_5min AS (
  SELECT
    SAFE_CAST(timestamp AS STRING)                                         AS timestamp_br,
    SAFE_CAST(TIMESTAMP_TRUNC(TIMESTAMP(timestamp), HOUR) AS STRING)       AS ts_hour,
    SAFE_CAST(umidade          AS NUMERIC) AS umidade,
    SAFE_CAST(pressao          AS NUMERIC) AS pressao,
    SAFE_CAST(ventointensidade AS NUMERIC) AS ventointensidade,
    SAFE_CAST(ventonum         AS NUMERIC) AS ventonum,
    SAFE_CAST(temperatura      AS NUMERIC) AS temperatura,
    SAFE_CAST(sensacaotermica  AS NUMERIC) AS sensacaotermica,
    SAFE_CAST(altura_medida_getControls AS NUMERIC) AS altura_medida_getcontrols,
    SAFE_CAST(intensidade_15m  AS NUMERIC) AS intensidade_15m,
    SAFE_CAST(direcao_15m      AS NUMERIC) AS direcao_15m,
    SAFE_CAST(intensidade_13_5m AS NUMERIC) AS intensidade_13_5m,
    SAFE_CAST(direcao_13_5m    AS NUMERIC) AS direcao_13_5m,
    SAFE_CAST(intensidade_12m  AS NUMERIC) AS intensidade_12m,
    SAFE_CAST(direcao_12m      AS NUMERIC) AS direcao_12m,
    SAFE_CAST(intensidade_10_5m AS NUMERIC) AS intensidade_10_5m,
    SAFE_CAST(direcao_10_5m    AS NUMERIC) AS direcao_10_5m,
    SAFE_CAST(intensidade_9m   AS NUMERIC) AS intensidade_9m,
    SAFE_CAST(direcao_9m       AS NUMERIC) AS direcao_9m,
    SAFE_CAST(intensidade_7_5m AS NUMERIC) AS intensidade_7_5m,
    SAFE_CAST(direcao_7_5m     AS NUMERIC) AS direcao_7_5m,
    SAFE_CAST(intensidade_superficie AS NUMERIC) AS intensidade_superficie,
    SAFE_CAST(direcao_superficie AS NUMERIC) AS direcao_superficie,
    SAFE_CAST(intensidade_6m    AS NUMERIC) AS intensidade_6m,
    SAFE_CAST(direcao_6m        AS NUMERIC) AS direcao_6m,
    SAFE_CAST(intensidade_3m    AS NUMERIC) AS intensidade_3m,
    SAFE_CAST(direcao_3m        AS NUMERIC) AS direcao_3m,
    SAFE_CAST(intensidade_1_5m  AS NUMERIC) AS intensidade_1_5m,
    SAFE_CAST(direcao_1_5m      AS NUMERIC) AS direcao_1_5m,
    SAFE_CAST(altura_prev_getmare AS NUMERIC) AS altura_prev_getmare,
    SAFE_CAST(altura_real_getmare AS NUMERIC) AS altura_real_getmare,
    SAFE_CAST(ventodirecao           AS STRING)  AS ventodirecao,
    SAFE_CAST(status                 AS STRING)  AS status,
    SAFE_CAST(data_station_davis     AS STRING)  AS data_station_davis,
    SAFE_CAST(data_inicio            AS STRING)  AS data_inicio,
    SAFE_CAST(numero                 AS STRING)  AS numero,
    SAFE_CAST(descricao              AS STRING)  AS descricao,
    SAFE_CAST(data                   AS STRING)  AS data,
    SAFE_CAST(tipo                   AS STRING)  AS tipo,
    SAFE_CAST(data_lua               AS STRING)  AS data_lua,
    SAFE_CAST(nascer_do_sol          AS STRING)  AS nascer_do_sol,
    SAFE_CAST(por_do_sol             AS STRING)  AS por_do_sol,
    SAFE_CAST(matutino               AS STRING)  AS matutino,
    SAFE_CAST(vespertino             AS STRING)  AS vespertino,
    SAFE_CAST(tipo_mare_getControls  AS STRING)  AS tipo_mare_getcontrols,
    SAFE_CAST(data_mare_getControls  AS STRING)  AS data_mare_getcontrols,
    SAFE_CAST(data_hidromares        AS STRING)  AS data_hidromares,
    SAFE_CAST(data_mare_getMare      AS STRING)  AS data_mare_getmare,
    SAFE_CAST(data_mare_real_getMare AS STRING)  AS data_mare_real_getmare,
    SAFE_CAST(api_mare               AS STRING)  AS api_mare,
    SAFE_CAST(api_hidromares         AS STRING)  AS api_hidromares,
    SAFE_CAST(api_estatistica        AS STRING)  AS api_estatistica,
    SAFE_CAST(motivo                 AS STRING)  AS motivo
  FROM `local-bliss-359814.wherehouse.update_rawdata_5min_backfill`
  WHERE TIMESTAMP(timestamp) >= limite_atualizacao
),

/* ─────────────── 2.2 INMET (último registro/h) ─────────────── */
inm_hour AS (
  SELECT *
  FROM (
    SELECT
      SAFE_CAST(
        TIMESTAMP_TRUNC(
          TIMESTAMP_SUB(TIMESTAMP(dt_utc), INTERVAL 3 HOUR), HOUR) AS STRING
      ) AS ts_hour,
      SAFE_CAST(TIMESTAMP_SUB(TIMESTAMP(dt_utc), INTERVAL 3 HOUR) AS STRING)
        AS inmet_datetime,
      SAFE_CAST(temperatura_inst AS NUMERIC) AS temperatura_inst_inmet,
      SAFE_CAST(temperatura_max  AS NUMERIC) AS temperatura_max_inmet,
      SAFE_CAST(temperatura_min  AS NUMERIC) AS temperatura_min_inmet,
      SAFE_CAST(umidade_inst     AS NUMERIC) AS umidade_inst_inmet,
      SAFE_CAST(umidade_max      AS NUMERIC) AS umidade_max_inmet,
      SAFE_CAST(umidade_min      AS NUMERIC) AS umidade_min_inmet,
      SAFE_CAST(pto_orvalho_inst AS NUMERIC) AS pto_orvalho_inst_inmet,
      SAFE_CAST(pto_orvalho_max  AS NUMERIC) AS pto_orvalho_max_inmet,
      SAFE_CAST(pto_orvalho_min  AS NUMERIC) AS pto_orvalho_min_inmet,
      SAFE_CAST(pressao_inst     AS NUMERIC) AS pressao_inst_inmet,
      SAFE_CAST(pressao_max      AS NUMERIC) AS pressao_max_inmet,
      SAFE_CAST(pressao_min      AS NUMERIC) AS pressao_min_inmet,
      SAFE_CAST(vento_vel_m_s    AS NUMERIC) AS vento_vel_m_s_inmet,
      SAFE_CAST(vento_dir_deg    AS NUMERIC) AS vento_dir_deg_inmet,
      SAFE_CAST(vento_raj_m_s    AS NUMERIC) AS vento_raj_m_s_inmet,
      SAFE_CAST(chuva            AS NUMERIC) AS chuva_inmet,
      SAFE_CAST(radiacao         AS NUMERIC) AS radiacao_inmet,
      dt_utc                     AS dt_utc_inmet,
      data                       AS data_inmet,
      hora_utc                   AS hora_utc_inmet,
      timestamp_execucao         AS timestamp_execucao_inmet,
      ROW_NUMBER() OVER (
        PARTITION BY TIMESTAMP_TRUNC(
          TIMESTAMP_SUB(TIMESTAMP(dt_utc), INTERVAL 3 HOUR), HOUR)
        ORDER BY dt_utc DESC
      ) AS rn
    FROM `local-bliss-359814.wherehouse.dados_inmet_estacoes`
    WHERE estacao = 'A802'
      AND TIMESTAMP_SUB(TIMESTAMP(dt_utc), INTERVAL 3 HOUR)
          >= limite_atualizacao
  )
  WHERE rn = 1
),

/* ─────────────── 2.3 OpenWeather NOW ─────────────── */
ow_now AS (
  SELECT
    SAFE_CAST(
      TIMESTAMP_TRUNC(
        TIMESTAMP_SUB(TIMESTAMP(timestamp_utc), INTERVAL 3 HOUR), HOUR
      ) AS STRING
    ) AS ts_hour,
    SAFE_CAST(TIMESTAMP_SUB(MAX(timestamp_utc), INTERVAL 3 HOUR) AS STRING)
      AS openweather_timestamp,
    SAFE_CAST(ANY_VALUE(city)    AS STRING)  AS ow_city,
    SAFE_CAST(ANY_VALUE(city_id)   AS NUMERIC) AS ow_city_id,
    SAFE_CAST(ANY_VALUE(coord_lon) AS NUMERIC) AS ow_coord_lon,
    SAFE_CAST(ANY_VALUE(coord_lat) AS NUMERIC) AS ow_coord_lat,
    SAFE_CAST(ANY_VALUE(weather_id) AS NUMERIC) AS ow_weather_id,
    CAST(ANY_VALUE(weather_main)        AS STRING)  AS ow_weather_main,
    CAST(ANY_VALUE(weather_description) AS STRING)  AS ow_weather_desc,
    CAST(ANY_VALUE(weather_icon)        AS STRING)  AS ow_weather_icon,
    CAST(ANY_VALUE(base)                AS STRING)  AS ow_base,
    SAFE_CAST(ANY_VALUE(main_temp)       AS NUMERIC) AS ow_temp,
    SAFE_CAST(ANY_VALUE(main_feels_like) AS NUMERIC) AS ow_feels_like,
    SAFE_CAST(ANY_VALUE(main_temp_min)   AS NUMERIC) AS ow_temp_min,
    SAFE_CAST(ANY_VALUE(main_temp_max)   AS NUMERIC) AS ow_temp_max,
    SAFE_CAST(ANY_VALUE(main_pressure)   AS NUMERIC) AS ow_pressure,
    SAFE_CAST(ANY_VALUE(main_humidity)   AS NUMERIC) AS ow_humidity,
    SAFE_CAST(ANY_VALUE(main_sea_level)  AS NUMERIC) AS ow_sea_level,
    SAFE_CAST(ANY_VALUE(main_grnd_level) AS NUMERIC) AS ow_grnd_level,
    SAFE_CAST(ANY_VALUE(visibility)      AS NUMERIC) AS ow_visibility,
    SAFE_CAST(ANY_VALUE(wind_speed) AS NUMERIC) AS ow_wind_speed,
    SAFE_CAST(ANY_VALUE(wind_deg)   AS NUMERIC) AS ow_wind_deg,
    SAFE_CAST(ANY_VALUE(wind_gust)  AS NUMERIC) AS ow_wind_gust,
    SAFE_CAST(ANY_VALUE(clouds_all) AS NUMERIC) AS ow_clouds,
    SAFE_CAST(ANY_VALUE(rain_1h) AS NUMERIC) AS ow_rain_1h,
    SAFE_CAST(ANY_VALUE(rain_3h) AS NUMERIC) AS ow_rain_3h,
    SAFE_CAST(ANY_VALUE(snow_1h) AS NUMERIC) AS ow_snow_1h,
    SAFE_CAST(ANY_VALUE(snow_3h) AS NUMERIC) AS ow_snow_3h,
    ANY_VALUE(dt)        AS ow_dt,
    ANY_VALUE(timestamp) AS ow_timestamp,
    SAFE_CAST(ANY_VALUE(sys_type)       AS NUMERIC) AS ow_sys_type,
    SAFE_CAST(ANY_VALUE(sys_id)         AS NUMERIC) AS ow_sys_id,
    CAST(ANY_VALUE(sys_country)         AS STRING)  AS ow_sys_country,
    ANY_VALUE(sys_sunrise)              AS ow_sys_sunrise,
    ANY_VALUE(sys_sunset)               AS ow_sys_sunset,
    SAFE_CAST(ANY_VALUE(timezone_offset) AS NUMERIC) AS ow_timezone_offset,
    SAFE_CAST(ANY_VALUE(cod)            AS NUMERIC) AS ow_cod,
    ANY_VALUE(timestamp_utc) AS ow_timestamp_utc
  FROM `local-bliss-359814.wherehouse.dados_openwhather`
  WHERE city = 'Rio Grande'
    AND TIMESTAMP_SUB(TIMESTAMP(timestamp_utc), INTERVAL 3 HOUR)
        >= limite_atualizacao
  GROUP BY ts_hour
),

/* ─────────── 2.4 Limite real ─────────── */
limite_hora_real AS (
  SELECT TIMESTAMP_TRUNC(limite_atualizacao, HOUR) AS ts_limite
),

/* ─────────── 2.5 Forecast ─────────── */
fc_rep AS (
  SELECT
    SAFE_CAST(
      TIMESTAMP_TRUNC(
        TIMESTAMP_SUB(TIMESTAMP(timestamp_execucao), INTERVAL 3 HOUR), HOUR) AS STRING
    ) AS ts_hour,
    SAFE_CAST(MAX(timestamp_execucao) AS STRING) AS forecast_timestamp_execucao,
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
    AND TIMESTAMP_TRUNC(TIMESTAMP(timestamp_execucao), HOUR)
        BETWEEN limite_hora_real.ts_limite
        AND TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR)
  GROUP BY ts_hour
),

/* ─────────── 2.6 Air-Pollution ─────────── */
air_rep AS (
  SELECT
    SAFE_CAST(
      TIMESTAMP_TRUNC(
        TIMESTAMP_SUB(TIMESTAMP(timestamp_execucao), INTERVAL 3 HOUR), HOUR) AS STRING
    ) AS ts_hour,
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
    AND TIMESTAMP_TRUNC(TIMESTAMP(timestamp_execucao), HOUR)
        BETWEEN limite_hora_real.ts_limite
        AND TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR)
  GROUP BY ts_hour
),

/* ─────────── 2.7 Consolidado de horas ─────────── */
horas AS (
  SELECT ts_hour FROM upd_5min
  UNION DISTINCT SELECT ts_hour FROM inm_hour
  UNION DISTINCT SELECT ts_hour FROM ow_now
  UNION DISTINCT SELECT ts_hour FROM fc_rep
  UNION DISTINCT SELECT ts_hour FROM air_rep
)

/* ─────────── 2.8 SELECT final ─────────── */
SELECT
  u.timestamp_br,
  u.ts_hour,

  /* ======= todos os campos Davis ======= */
  u.umidade, u.pressao, u.ventointensidade, u.ventonum, u.temperatura, u.sensacaotermica,
  u.altura_medida_getcontrols, u.intensidade_15m, u.direcao_15m, u.intensidade_13_5m, u.direcao_13_5m,
  u.intensidade_12m, u.direcao_12m, u.intensidade_10_5m, u.direcao_10_5m,
  u.intensidade_9m, u.direcao_9m, u.intensidade_7_5m, u.direcao_7_5m,
  u.intensidade_superficie, u.direcao_superficie, u.intensidade_6m, u.direcao_6m,
  u.intensidade_3m, u.direcao_3m, u.intensidade_1_5m, u.direcao_1_5m,
  u.altura_prev_getmare, u.altura_real_getmare, u.ventodirecao, u.status,
  u.data_station_davis, u.data_inicio, u.numero, u.descricao, u.data, u.tipo,
  u.data_lua, u.nascer_do_sol, u.por_do_sol, u.matutino, u.vespertino,
  u.tipo_mare_getcontrols, u.data_mare_getcontrols, u.data_hidromares,
  u.data_mare_getmare, u.data_mare_real_getmare, u.api_mare, u.api_hidromares,
  u.api_estatistica, u.motivo,

  /* ======= INMET (min=0) ======= */
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, i.inmet_datetime, NULL) AS inmet_datetime,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, i.temperatura_inst_inmet, NULL) AS temperatura_inst_inmet,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, i.temperatura_max_inmet, NULL) AS temperatura_max_inmet,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, i.temperatura_min_inmet, NULL) AS temperatura_min_inmet,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, i.umidade_inst_inmet, NULL) AS umidade_inst_inmet,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, i.umidade_max_inmet, NULL) AS umidade_max_inmet,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, i.umidade_min_inmet, NULL) AS umidade_min_inmet,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, i.pto_orvalho_inst_inmet, NULL) AS pto_orvalho_inst_inmet,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, i.pto_orvalho_max_inmet, NULL) AS pto_orvalho_max_inmet,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, i.pto_orvalho_min_inmet, NULL) AS pto_orvalho_min_inmet,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, i.pressao_inst_inmet, NULL) AS pressao_inst_inmet,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, i.pressao_max_inmet, NULL) AS pressao_max_inmet,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, i.pressao_min_inmet, NULL) AS pressao_min_inmet,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, i.vento_vel_m_s_inmet, NULL) AS vento_vel_m_s_inmet,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, i.vento_dir_deg_inmet, NULL) AS vento_dir_deg_inmet,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, i.vento_raj_m_s_inmet, NULL) AS vento_raj_m_s_inmet,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, i.chuva_inmet, NULL) AS chuva_inmet,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, i.radiacao_inmet, NULL) AS radiacao_inmet,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, i.dt_utc_inmet, NULL) AS dt_utc_inmet,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, i.data_inmet, NULL) AS data_inmet,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, i.hora_utc_inmet, NULL) AS hora_utc_inmet,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, i.timestamp_execucao_inmet, NULL) AS timestamp_execucao_inmet,

  /* ======= OpenWeather NOW ======= */
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.openweather_timestamp, NULL) AS openweather_timestamp,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_city, NULL) AS ow_city,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_city_id, NULL) AS ow_city_id,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_coord_lon, NULL) AS ow_coord_lon,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_coord_lat, NULL) AS ow_coord_lat,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_weather_id, NULL) AS ow_weather_id,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_weather_main, NULL) AS ow_weather_main,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_weather_desc, NULL) AS ow_weather_desc,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_weather_icon, NULL) AS ow_weather_icon,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_base, NULL) AS ow_base,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_temp, NULL) AS ow_temp,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_feels_like, NULL) AS ow_feels_like,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_temp_min, NULL) AS ow_temp_min,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_temp_max, NULL) AS ow_temp_max,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_pressure, NULL) AS ow_pressure,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_humidity, NULL) AS ow_humidity,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_sea_level, NULL) AS ow_sea_level,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_grnd_level, NULL) AS ow_grnd_level,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_visibility, NULL) AS ow_visibility,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_wind_speed, NULL) AS ow_wind_speed,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_wind_deg, NULL) AS ow_wind_deg,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_wind_gust, NULL) AS ow_wind_gust,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_clouds, NULL) AS ow_clouds,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_rain_1h, NULL) AS ow_rain_1h,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_rain_3h, NULL) AS ow_rain_3h,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_snow_1h, NULL) AS ow_snow_1h,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_snow_3h, NULL) AS ow_snow_3h,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_dt, NULL) AS ow_dt,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_timestamp, NULL) AS ow_timestamp,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_sys_type, NULL) AS ow_sys_type,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_sys_id, NULL) AS ow_sys_id,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_sys_country, NULL) AS ow_sys_country,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_sys_sunrise, NULL) AS ow_sys_sunrise,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_sys_sunset, NULL) AS ow_sys_sunset,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_timezone_offset, NULL) AS ow_timezone_offset,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_cod, NULL) AS ow_cod,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, o.ow_timestamp_utc, NULL) AS ow_timestamp_utc,

  /* ======= OpenWeather FORECAST ======= */
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.forecast_timestamp_execucao, NULL) AS forecast_timestamp_execucao,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_timestamp_utc, NULL) AS fc_timestamp_utc,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_city_name, NULL) AS fc_city_name,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_lat, NULL) AS fc_lat,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_lon, NULL) AS fc_lon,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_temp, NULL) AS fc_temp,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_feels_like, NULL) AS fc_feels_like,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_temp_min, NULL) AS fc_temp_min,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_temp_max, NULL) AS fc_temp_max,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_pressure, NULL) AS fc_pressure,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_sea_level, NULL) AS fc_sea_level,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_grnd_level, NULL) AS fc_grnd_level,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_humidity, NULL) AS fc_humidity,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_weather_main, NULL) AS fc_weather_main,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_weather_desc, NULL) AS fc_weather_desc,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_weather_icon, NULL) AS fc_weather_icon,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_clouds_all, NULL) AS fc_clouds_all,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_wind_speed, NULL) AS fc_wind_speed,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_wind_deg, NULL) AS fc_wind_deg,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_wind_gust, NULL) AS fc_wind_gust,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_visibility, NULL) AS fc_visibility,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_pop, NULL) AS fc_pop,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_rain_3h, NULL) AS fc_rain_3h,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_snow_3h, NULL) AS fc_snow_3h,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_sys_pod, NULL) AS fc_sys_pod,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_timezone, NULL) AS fc_timezone,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_cod, NULL) AS fc_cod,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, f.fc_dt_txt, NULL) AS fc_dt_txt,

  /* ======= Air-Pollution ======= */
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, a.air_aqi, NULL) AS air_aqi,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, a.air_pm2_5, NULL) AS air_pm2_5,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, a.air_pm10, NULL) AS air_pm10,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, a.air_co, NULL) AS air_co,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, a.air_no, NULL) AS air_no,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, a.air_no2, NULL) AS air_no2,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, a.air_o3, NULL) AS air_o3,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, a.air_so2, NULL) AS air_so2,
  IF(EXTRACT(MINUTE FROM TIMESTAMP(u.timestamp_br)) = 0, a.air_nh3, NULL) AS air_nh3

FROM horas h
LEFT JOIN upd_5min u USING (ts_hour)
LEFT JOIN inm_hour i USING (ts_hour)
LEFT JOIN ow_now   o USING (ts_hour)
LEFT JOIN fc_rep   f USING (ts_hour)
LEFT JOIN air_rep  a USING (ts_hour)
ORDER BY u.timestamp_br;
