-- BQ

CREATE TABLE `local-bliss-359814.wherehouse.dados_openweather_forecast` (
  timestamp_utc STRING,
  city_name STRING,
  lat NUMERIC,
  lon NUMERIC,
  temp NUMERIC,
  feels_like NUMERIC,
  temp_min NUMERIC,
  temp_max NUMERIC,
  pressure NUMERIC,
  sea_level NUMERIC,
  grnd_level NUMERIC,
  humidity NUMERIC,
  weather_main STRING,
  weather_description STRING,
  weather_icon STRING,
  clouds_all NUMERIC,
  wind_speed NUMERIC,
  wind_deg NUMERIC,
  wind_gust NUMERIC,
  visibility NUMERIC,
  pop NUMERIC,
  rain_3h NUMERIC,
  snow_3h NUMERIC,
  sys_pod STRING,
  timezone NUMERIC,
  cod NUMERIC,
  dt_txt STRING
);


CREATE TABLE `local-bliss-359814.wherehouse.dados_openweather_air_pollution` (
  timestamp_execucao STRING,
  timestamp_utc STRING,
  lat NUMERIC,
  lon NUMERIC,
  aqi NUMERIC,
  co NUMERIC,
  `no` NUMERIC,
  no2 NUMERIC,
  o3 NUMERIC,
  so2 NUMERIC,
  pm2_5 NUMERIC,
  pm10 NUMERIC,
  nh3 NUMERIC
);


-- Addd nova coluna timestamp:
ALTER TABLE `local-bliss-359814.wherehouse.dados_openwhather`
ADD COLUMN timestamp TIMESTAMP;

