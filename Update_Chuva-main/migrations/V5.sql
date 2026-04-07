
-- BQ
ALTER TABLE `local-bliss-359814.wherehouse.dados_inmet_estacoes_backfill`
ADD COLUMN timestamp_execucao TIMESTAMP;

ALTER TABLE `local-bliss-359814.wherehouse.dados_inmet_estacoes`
ADD COLUMN timestamp_execucao TIMESTAMP;


ALTER TABLE `local-bliss-359814.wherehouse.dados_openweather_air_pollution`
ADD COLUMN city_name STRING;


ALTER TABLE `local-bliss-359814.wherehouse.dados_openweather_forecast`
ADD COLUMN weather_id INT64;
