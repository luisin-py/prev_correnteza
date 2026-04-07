
-- BQ
CREATE TABLE `local-bliss-359814.wherehouse.dados_inmet_estacoes` (
  data               DATE,
  hora_utc           STRING,
  temperatura_inst   FLOAT64,
  temperatura_max    FLOAT64,
  temperatura_min    FLOAT64,
  umidade_inst       FLOAT64,
  umidade_max        FLOAT64,
  umidade_min        FLOAT64,
  pto_orvalho_inst   FLOAT64,
  pto_orvalho_max    FLOAT64,
  pto_orvalho_min    FLOAT64,
  pressao_inst       FLOAT64,
  pressao_max        FLOAT64,
  pressao_min        FLOAT64,
  vento_vel_m_s      FLOAT64,
  vento_dir_deg      FLOAT64,
  vento_raj_m_s      FLOAT64,
  radiacao           FLOAT64,
  chuva              FLOAT64,
  estacao            STRING
);

ALTER TABLE `local-bliss-359814.wherehouse.dados_inmet_estacoes`
ADD COLUMN dt_utc DATETIME;



-- Banco de dados Mysql
USE patricagem;    -- substitua “seu_banco” pelo nome do schema que você está usando
CREATE TABLE IF NOT EXISTS dados_inmet_estacoes (
  estacao        VARCHAR(12)  NOT NULL,
  dt_utc         DATETIME    NOT NULL,
  data           DATE        NOT NULL,
  hora_utc       CHAR(4)     NOT NULL,
  temperatura_inst  DOUBLE,
  temperatura_max   DOUBLE,
  temperatura_min   DOUBLE,
  umidade_inst      DOUBLE,
  umidade_max       DOUBLE,
  umidade_min       DOUBLE,
  pto_orvalho_inst  DOUBLE,
  pto_orvalho_max   DOUBLE,
  pto_orvalho_min   DOUBLE,
  pressao_inst      DOUBLE,
  pressao_max       DOUBLE,
  pressao_min       DOUBLE,
  vento_vel_m_s     DOUBLE,
  vento_dir_deg     DOUBLE,
  vento_raj_m_s     DOUBLE,
  radiacao          DOUBLE,
  chuva             DOUBLE,
  PRIMARY KEY (estacao, dt_utc)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


ALTER TABLE dados_inmet_estacoes
  MODIFY estacao VARCHAR(220) NOT NULL;

USE patricagem;    -- substitua “seu_banco” pelo nome do schema que você está usando
CREATE TABLE IF NOT EXISTS dados_inmet_estacoes_backfill (
  estacao        VARCHAR(12)  NOT NULL,
  dt_utc         DATETIME    NOT NULL,
  data           DATE        NOT NULL,
  hora_utc       CHAR(4)     NOT NULL,
  temperatura_inst  DOUBLE,
  temperatura_max   DOUBLE,
  temperatura_min   DOUBLE,
  umidade_inst      DOUBLE,
  umidade_max       DOUBLE,
  umidade_min       DOUBLE,
  pto_orvalho_inst  DOUBLE,
  pto_orvalho_max   DOUBLE,
  pto_orvalho_min   DOUBLE,
  pressao_inst      DOUBLE,
  pressao_max       DOUBLE,
  pressao_min       DOUBLE,
  vento_vel_m_s     DOUBLE,
  vento_dir_deg     DOUBLE,
  vento_raj_m_s     DOUBLE,
  radiacao          DOUBLE,
  chuva             DOUBLE,
  PRIMARY KEY (estacao, dt_utc)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


ALTER TABLE dados_inmet_estacoes_backfill 
  MODIFY estacao VARCHAR(220) NOT NULL;
