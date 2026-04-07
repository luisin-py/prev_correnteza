CREATE TABLE `local-bliss-359814.wherehouse.dados_openwhather` (
  city                 STRING,
  city_id              INT64,

  coord_lon            NUMERIC,
  coord_lat            NUMERIC,

  weather_id           INT64,
  weather_main         STRING,
  weather_description  STRING,
  weather_icon         STRING,

  base                 STRING,

  main_temp            NUMERIC,
  main_feels_like      NUMERIC,
  main_temp_min        NUMERIC,
  main_temp_max        NUMERIC,
  main_pressure        INT64,
  main_humidity        INT64,
  main_sea_level       INT64,
  main_grnd_level      INT64,

  visibility           INT64,

  wind_speed           NUMERIC,
  wind_deg             INT64,
  wind_gust            NUMERIC,

  clouds_all           INT64,

  rain_1h              NUMERIC,
  rain_3h              NUMERIC,
  snow_1h              NUMERIC,
  snow_3h              NUMERIC,

  dt                   TIMESTAMP,   -- data/hora fornecida pela API
  sys_type             INT64,
  sys_id               INT64,
  sys_country          STRING,
  sys_sunrise          TIMESTAMP,
  sys_sunset           TIMESTAMP,

  timezone_offset      INT64,
  cod                  INT64,

  timestamp_utc        TIMESTAMP   -- instante da coleta
);


ALTER TABLE `local-bliss-359814.wherehouse.dados_openwhather`
ALTER COLUMN sys_type SET DATA TYPE FLOAT64
