CREATE TABLE `local-bliss-359814.wherehouse_previsoes.previsao_mare` (
  timestamp_br   DATETIME NOT NULL,
  altura_prevista FLOAT64
)
PARTITION BY DATE(timestamp_br)
CLUSTER BY timestamp_br;



ALTER TABLE `local-bliss-359814.wherehouse_previsoes.previsao_mare`
ADD COLUMN IF NOT EXISTS altura_real_getmare NUMERIC;



--ASdicionar na tabela mestre tratada horaria => 
ALTER TABLE `local-bliss-359814.wherehouse_tratado.mestre_hour_tratada`
ADD COLUMN IF NOT EXISTS altura_prevista NUMERIC;

ALTER TABLE `local-bliss-359814.wherehouse_tratado.mestre_hour_tratada`
ADD COLUMN IF NOT EXISTS timestamp_prev DATETIME;
