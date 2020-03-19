CREATE TABLE IF NOT EXISTS bddlalm.alm_padecimiento_declarado (
  num_poliza                VARCHAR(14),
  vigencia_poliza           SMALLINT,
  num_version_poliza        SMALLINT,
  num_poliza_cobranza       VARCHAR(8),
  cve_asegurado             VARCHAR(10),
  id_secuencia_asegurado    SMALLINT,
  cve_sistema               VARCHAR(4),
  cve_sistema_int           VARCHAR(4),
  cve_grupo_pad_icd9        VARCHAR(10),
  des_grupo_pad_icd9        VARCHAR(100),
  cve_pad_icd9              VARCHAR(10),
  des_pad_icd9              VARCHAR(100),
  ind_pad_inc_exc           VARCHAR(2),
  ban_preexistencia         VARCHAR(2),
  pct_extraprima_automatica DECIMAL(16,4),
  pct_extraprima_manual     DECIMAL(16,4),
  ts_alta_hdfs              TIMESTAMP,
  idmd5                     VARCHAR(32),
  idmd5completo             VARCHAR(32)
) STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS bddlalm.alm_padecimiento_declaradohis (
  num_poliza                VARCHAR(14),
  vigencia_poliza           SMALLINT,
  num_version_poliza        SMALLINT,
  num_poliza_cobranza       VARCHAR(8),
  cve_asegurado             VARCHAR(10),
  id_secuencia_asegurado    SMALLINT,
  cve_sistema               VARCHAR(4),
  cve_sistema_int           VARCHAR(4),
  cve_grupo_pad_icd9        VARCHAR(10),
  des_grupo_pad_icd9        VARCHAR(100),
  cve_pad_icd9              VARCHAR(10),
  des_pad_icd9              VARCHAR(100),
  ind_pad_inc_exc           VARCHAR(2),
  ban_preexistencia         VARCHAR(2),
  pct_extraprima_automatica DECIMAL(16,4),
  pct_extraprima_manual     DECIMAL(16,4),
  ts_alta_hdfs              TIMESTAMP,
  idmd5                     VARCHAR(32),
  idmd5completo             VARCHAR(32),
  fechhist TIMESTAMP
) STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS bddltrn.stg_gmm_con_padecimiento_declarado_delta_extraccion LIKE bddlalm.alm_padecimiento_declarado;

CREATE TABLE IF NOT EXISTS bddltrn.stg_gmm_con_padecimiento_declarado_delta_relleno LIKE bddlalm.alm_padecimiento_declarado;

CREATE TABLE IF NOT EXISTS bddltrn.stg_gmm_con_padecimiento_declarado_delta LIKE bddlalm.alm_padecimiento_declarado;

CREATE TABLE IF NOT EXISTS bddltrn.stg_gmm_con_padecimiento_declarado_delta_reglas_duplicidad LIKE bddlalm.alm_padecimiento_declarado;