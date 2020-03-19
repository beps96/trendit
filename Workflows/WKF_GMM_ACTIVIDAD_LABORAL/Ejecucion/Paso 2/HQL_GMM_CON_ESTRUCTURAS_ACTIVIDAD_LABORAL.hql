CREATE TABLE IF NOT EXISTS bddlalm.alm_actividad_laboral (
  num_poliza                    VARCHAR(14),
  vigencia_poliza               SMALLINT,
  num_version_poliza            SMALLINT,
  num_poliza_cobranza           VARCHAR(8),
  cve_sistema                   VARCHAR(4),
  cve_sistema_int               VARCHAR(4),
  cve_asegurado                 VARCHAR(10),
  id_secuencia_asegurado        SMALLINT,
  cve_actividad_laboral         VARCHAR(10),
  des_actividad_laboral         VARCHAR(100),
  ind_actividad_laboral_inc_exc VARCHAR(2),
  ts_alta_hdfs                  TIMESTAMP,
  idmd5                         VARCHAR(32),
  idmd5completo                 VARCHAR(32)
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS bddlalm.alm_actividad_laboralhis (
  num_poliza                    VARCHAR(14),
  vigencia_poliza               SMALLINT,
  num_version_poliza            SMALLINT,
  num_poliza_cobranza           VARCHAR(8),
  cve_sistema                   VARCHAR(4),
  cve_sistema_int               VARCHAR(4),
  cve_asegurado                 VARCHAR(10),
  id_secuencia_asegurado        SMALLINT,
  cve_actividad_laboral         VARCHAR(10),
  des_actividad_laboral         VARCHAR(100),
  ind_actividad_laboral_inc_exc VARCHAR(2),
  ts_alta_hdfs                  TIMESTAMP,
  idmd5                         VARCHAR(32),
  idmd5completo                 VARCHAR(32),
  fechhist                      TIMESTAMP
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS bddltrn.stg_gmm_con_actividad_laboral_delta_extraccion LIKE bddlalm.alm_actividad_laboral;

CREATE TABLE IF NOT EXISTS bddltrn.stg_gmm_con_actividad_laboral_delta_relleno LIKE bddlalm.alm_actividad_laboral;

CREATE TABLE IF NOT EXISTS bddltrn.stg_gmm_con_actividad_laboral_delta LIKE bddlalm.alm_actividad_laboral;

CREATE TABLE IF NOT EXISTS bddltrn.stg_gmm_con_actividad_laboral_delta_reglas_duplicados LIKE bddlalm.alm_actividad_laboral;