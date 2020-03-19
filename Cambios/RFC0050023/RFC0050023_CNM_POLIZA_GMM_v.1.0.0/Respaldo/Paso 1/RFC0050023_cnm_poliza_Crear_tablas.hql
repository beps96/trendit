--- Borramos la estructura existente

DROP TABLE IF EXISTS BDDLALM.ALM_POLIZA;

DROP TABLE IF EXISTS BDDLALM.ALM_POLIZAHIS;

DROP TABLE IF EXISTS BDDLAPR.CNM_POLIZA;

DROP TABLE IF EXISTS BDDLAPR.CNM_POLIZAHIS;



--- Crear 4 tablas y 2 vistas


CREATE TABLE bddlalm.ALM_POLIZA(	
  num_poliza varchar(14), 	
  vigencia_poliza smallint, 	
  num_version_poliza smallint, 	
  num_poliza_cobranza varchar(8), 	
  cve_sistema varchar(4), 	
  cve_sistema_int varchar(4), 	
  cve_cobertura_contable varchar(3), 	
  des_cobertura_contable varchar(100), 	
  cve_estatus_poliza varchar(2), 	
  des_estatus_poliza varchar(100), 	
  cve_forma_pago varchar(1), 	
  des_forma_pago varchar(100), 	
  clave_intermediario_principal varchar(8), 	
  nombre_intermediario_principal varchar(100), 	
  num_contrato varchar(5), 	
  num_version_contrato smallint, 	
  cve_negocio varchar(10), 	
  des_negocio varchar(100), 	
  folio_intermediario_principal varchar(8), 	
  fch_ini_vigencia timestamp, 	
  fch_fin_vigencia timestamp, 	
  cve_contratante varchar(10), 	
  rfc_contratante varchar(13), 	
  nombre_contratante varchar(150), 	
  cve_canal_venta varchar(10), 	
  des_canal_venta varchar(100), 	
  cve_tipo_poliza varchar(10), 	
  des_tipo_poliza varchar(100), 	
  cve_prod_tecnico varchar(10), 	
  cve_prod_comercial varchar(10), 	
  des_prod_tecnico_comercial varchar(100), 	
  fch_efecto_movimiento timestamp, 	
  fch_fin_movimiento timestamp, 	
  fch_ini_poliza timestamp, 	
  fch_emision timestamp, 	
  am_emision int, 	
  cve_motivo_estatus_poliza varchar(10), 	
  des_motivo_estatus_poliza varchar(100), 	
  fch_estatus_poliza timestamp, 	
  ban_anulacion_falta_pago varchar(2), 	
  fch_anulacion_falta_pago timestamp, 	
  am_anulacion_falta_pago int, 	
  cve_tipo_movimiento varchar(2), 	
  des_tipo_movimiento varchar(100), 	
  cve_ultima_situacion varchar(2), 	
  des_ultima_situacion varchar(100), 	
  cve_indicador_poliza_padre_hija varchar(2), 	
  des_indicador_poliza_padre_hija varchar(50), 	
  num_poliza_padre varchar(14), 	
  num_version_poliza_padre smallint, 	
  cve_pagador varchar(10), 	
  nombre_pagador varchar(150), 	
  am_fin_vigencia int, 	
  cve_pool varchar(10), 	
  cve_subramo_admon varchar(10), 	
  des_subramo_admon varchar(100), 	
  cve_plan varchar(5), 	
  des_plan varchar(100), 	
  ind_conexion varchar(1), 	
  cve_usuario varchar(10), 	
  des_usuario varchar(100), 	
  cve_suscriptor varchar(10), 	
  des_suscriptor varchar(100), 	
  nivel_hospital varchar(100), 	
  gama_poliza varchar(50), 	
  plan_gama_nivel_poliza varchar(50), 	
  territorialidad_poliza varchar(50), 	
  cve_ramo varchar(2), 	
  des_ramo varchar(100), 	
  fch_cambio_estado timestamp, 	
  canal_venta_estadistico varchar(100), 	
  segmento_venta_estadistico varchar(100), 	
  cve_dir_plaza varchar(5), 	
  des_dir_plaza varchar(100), 	
  cve_edo_dir_plaza varchar(5), 	
  des_edo_dir_plaza varchar(100), 	
  cve_direccion_agencia varchar(5), 	
  des_direccion_agencia varchar(100), 	
  cve_oficina_intermediario smallint, 	
  red_territorial varchar(50), 	
  cve_segmento varchar(10), 	
  des_segmento varchar(100), 	
  cve_negocio_multinacional varchar(10), 	
  des_negocio_multinacional varchar(100), 	
  cve_tipo_bono varchar(5), 	
  des_tipo_bono varchar(100), 	
  afecto_plan_incentivo varchar(100), 	
  ban_poliza_contributoria varchar(2), 	
  pct_contribucion decimal(16,4), 	
  ban_poliza_prestacion varchar(2), 	
  cve_motivo_exclusion_plan_incentivo varchar(5), 	
  des_motivo_exclusion_plan_incentivo varchar(100), 	
  ts_alta_hdfs timestamp, 	
  idmd5 varchar(32), 	
  idmd5completo varchar(32)
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.RCFileInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.RCFileOutputFormat'
;


CREATE TABLE bddlalm.alm_polizahis (
  num_poliza VARCHAR(14),
  vigencia_poliza SMALLINT,
  num_version_poliza SMALLINT,
  num_poliza_cobranza VARCHAR(8),
  cve_sistema VARCHAR(4),
  cve_sistema_int VARCHAR(4),
  cve_cobertura_contable VARCHAR(3),
  des_cobertura_contable VARCHAR(100),
  cve_estatus_poliza VARCHAR(2),
  des_estatus_poliza VARCHAR(100),
  cve_forma_pago VARCHAR(1),
  des_forma_pago VARCHAR(100),
  clave_intermediario_principal VARCHAR(8),
  nombre_intermediario_principal VARCHAR(100),
  num_contrato VARCHAR(5),
  num_version_contrato SMALLINT,
  cve_negocio VARCHAR(10),
  des_negocio VARCHAR(100),
  folio_intermediario_principal VARCHAR(8),
  fch_ini_vigencia TIMESTAMP,
  fch_fin_vigencia TIMESTAMP,
  cve_contratante VARCHAR(10),
  rfc_contratante VARCHAR(13),
  nombre_contratante VARCHAR(150),
  cve_canal_venta VARCHAR(10),
  des_canal_venta VARCHAR(100),
  cve_tipo_poliza VARCHAR(10),
  des_tipo_poliza VARCHAR(100),
  cve_prod_tecnico VARCHAR(10),
  cve_prod_comercial VARCHAR(10),
  des_prod_tecnico_comercial VARCHAR(100),
  fch_efecto_movimiento TIMESTAMP,
  fch_fin_movimiento TIMESTAMP,
  fch_ini_poliza TIMESTAMP,
  fch_emision TIMESTAMP,
  am_emision INT,
  cve_motivo_estatus_poliza VARCHAR(10),
  des_motivo_estatus_poliza VARCHAR(100),
  fch_estatus_poliza TIMESTAMP,
  ban_anulacion_falta_pago VARCHAR(2),
  fch_anulacion_falta_pago TIMESTAMP,
  am_anulacion_falta_pago INT,
  cve_tipo_movimiento VARCHAR(2),
  des_tipo_movimiento VARCHAR(100),
  cve_ultima_situacion VARCHAR(2),
  des_ultima_situacion VARCHAR(100),
  cve_indicador_poliza_padre_hija VARCHAR(2),
  des_indicador_poliza_padre_hija VARCHAR(50),
  num_poliza_padre VARCHAR(14),
  num_version_poliza_padre SMALLINT,
  cve_pagador VARCHAR(10),
  nombre_pagador VARCHAR(150),
  am_fin_vigencia INT,
  cve_pool VARCHAR(10),
  cve_subramo_admon VARCHAR(10),
  des_subramo_admon VARCHAR(100),
  cve_plan VARCHAR(5),
  des_plan VARCHAR(100),
  ind_conexion VARCHAR(1),
  cve_usuario VARCHAR(10),
  des_usuario VARCHAR(100),
  cve_suscriptor VARCHAR(10),
  des_suscriptor VARCHAR(100),
  nivel_hospital VARCHAR(100),
  gama_poliza VARCHAR(50),
  plan_gama_nivel_poliza VARCHAR(50),
  territorialidad_poliza VARCHAR(50),
  cve_ramo VARCHAR(2),
  des_ramo VARCHAR(100),
  fch_cambio_estado TIMESTAMP,
  canal_venta_estadistico VARCHAR(100),
  segmento_venta_estadistico VARCHAR(100),
  cve_dir_plaza VARCHAR(5),
  des_dir_plaza VARCHAR(100),
  cve_edo_dir_plaza VARCHAR(5),
  des_edo_dir_plaza VARCHAR(100),
  cve_direccion_agencia VARCHAR(5),
  des_direccion_agencia VARCHAR(100),
  cve_oficina_intermediario smallint,
  red_territorial VARCHAR(50),
  cve_segmento VARCHAR(10),
  des_segmento VARCHAR(100),
  cve_negocio_multinacional VARCHAR(10),
  des_negocio_multinacional VARCHAR(100),
  cve_tipo_bono VARCHAR(5),
  des_tipo_bono VARCHAR(100),
  afecto_plan_incentivo VARCHAR(100),
  ban_poliza_contributoria VARCHAR(2),
  pct_contribucion DECIMAL(16,4),
  ban_poliza_prestacion VARCHAR(2),
  cve_motivo_exclusion_plan_incentivo VARCHAR(5),
  des_motivo_exclusion_plan_incentivo VARCHAR(100),
  ts_alta_hdfs TIMESTAMP,
  idmd5 VARCHAR(32),
  idmd5completo VARCHAR(32),
  fechhist TIMESTAMP
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.RCFileInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.RCFileOutputFormat'
;


CREATE TABLE bddltrn.stg_poliza_delta (
  num_poliza VARCHAR(14),
  vigencia_poliza SMALLINT,
  num_version_poliza SMALLINT,
  num_poliza_cobranza VARCHAR(8),
  cve_sistema VARCHAR(4),
  cve_sistema_int VARCHAR(4),
  cve_cobertura_contable VARCHAR(3),
  des_cobertura_contable VARCHAR(100),
  cve_estatus_poliza VARCHAR(2),
  des_estatus_poliza VARCHAR(100),
  cve_forma_pago VARCHAR(1),
  des_forma_pago VARCHAR(100),
  clave_intermediario_principal VARCHAR(8),
  nombre_intermediario_principal VARCHAR(100),
  num_contrato VARCHAR(5),
  num_version_contrato SMALLINT,
  cve_negocio VARCHAR(10),
  des_negocio VARCHAR(100),
  folio_intermediario_principal VARCHAR(8),
  fch_ini_vigencia TIMESTAMP,
  fch_fin_vigencia TIMESTAMP,
  cve_contratante VARCHAR(10),
  rfc_contratante VARCHAR(13),
  nombre_contratante VARCHAR(150),
  cve_canal_venta VARCHAR(10),
  des_canal_venta VARCHAR(100),
  cve_tipo_poliza VARCHAR(10),
  des_tipo_poliza VARCHAR(100),
  cve_prod_tecnico VARCHAR(10),
  cve_prod_comercial VARCHAR(10),
  des_prod_tecnico VARCHAR(100),
  fch_efecto_movimiento TIMESTAMP,
  fch_fin_movimiento TIMESTAMP,
  fch_ini_poliza TIMESTAMP,
  fch_emision TIMESTAMP,
  am_emision INT,
  cve_motivo_estatus_poliza VARCHAR(10),
  des_motivo_estatus_poliza VARCHAR(100),
  fch_estatus_poliza TIMESTAMP,
  ban_anulacion_falta_pago VARCHAR(2),
  fch_anulacion_falta_pago TIMESTAMP,
  am_anulacion_falta_pago INT,
  cve_tipo_movimiento VARCHAR(2),
  des_tipo_movimiento VARCHAR(100),
  cve_ultima_situacion VARCHAR(2),
  des_ultima_situacion VARCHAR(100),
  cve_indicador_poliza_padre_hija VARCHAR(2),
  des_indicador_poliza_padre_hija VARCHAR(50),
  num_poliza_padre VARCHAR(14),
  num_version_poliza_padre SMALLINT,
  cve_pagador VARCHAR(10),
  nombre_pagador VARCHAR(150),
  am_fin_vigencia INT,
  cve_pool VARCHAR(10),
  cve_subramo_admon VARCHAR(10),
  des_subramo_admon VARCHAR(100),
  cve_plan VARCHAR(5),
  des_plan VARCHAR(100),
  ind_conexion VARCHAR(1),
  cve_usuario VARCHAR(10),
  des_usuario VARCHAR(100),
  cve_suscriptor VARCHAR(10),
  des_suscriptor VARCHAR(100),
  nivel_hospital VARCHAR(100),
  gama_poliza VARCHAR(50),
  plan_gama_nivel_poliza VARCHAR(50),
  territorialidad_poliza VARCHAR(50),
  cve_ramo VARCHAR(2),
  des_ramo VARCHAR(100),
  fch_cambio_estado TIMESTAMP,
  canal_venta_estadistico VARCHAR(100),
  segmento_venta_estadistico VARCHAR(100),
  cve_dir_plaza VARCHAR(5),
  des_dir_plaza VARCHAR(100),
  cve_edo_dir_plaza VARCHAR(5),
  des_edo_dir_plaza VARCHAR(100),
  cve_direccion_agencia VARCHAR(5),
  des_direccion_agencia VARCHAR(100),
  cve_oficina_intermediario smallint,
  red_territorial VARCHAR(50),
  cve_segmento VARCHAR(10),
  des_segmento VARCHAR(100),
  cve_negocio_multinacional VARCHAR(10),
  des_negocio_multinacional VARCHAR(100),
  cve_tipo_bono VARCHAR(5),
  des_tipo_bono VARCHAR(100),
  afecto_plan_incentivo VARCHAR(100),
  ban_poliza_contributoria VARCHAR(2),
  pct_contribucion DECIMAL(16,4),
  ban_poliza_prestacion VARCHAR(2),
  cve_motivo_exclusion_plan_incentivo VARCHAR(5),
  des_motivo_exclusion_plan_incentivo VARCHAR(100)

)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.RCFileInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.RCFileOutputFormat'
;


CREATE TABLE bddltrn.stg_poliza_hash (
  num_poliza VARCHAR(14),
  vigencia_poliza SMALLINT,
  num_version_poliza SMALLINT,
  num_poliza_cobranza VARCHAR(8),
  cve_sistema VARCHAR(4),
  cve_sistema_int VARCHAR(4),
  cve_cobertura_contable VARCHAR(3),
  des_cobertura_contable VARCHAR(100),
  cve_estatus_poliza VARCHAR(2),
  des_estatus_poliza VARCHAR(100),
  cve_forma_pago VARCHAR(1),
  des_forma_pago VARCHAR(100),
  clave_intermediario_principal VARCHAR(8),
  nombre_intermediario_principal VARCHAR(100),
  num_contrato VARCHAR(5),
  num_version_contrato SMALLINT,
  cve_negocio VARCHAR(10),
  des_negocio VARCHAR(100),
  folio_intermediario_principal VARCHAR(8),
  fch_ini_vigencia TIMESTAMP,
  fch_fin_vigencia TIMESTAMP,
  cve_contratante VARCHAR(10),
  rfc_contratante VARCHAR(13),
  nombre_contratante VARCHAR(150),
  cve_canal_venta VARCHAR(10),
  des_canal_venta VARCHAR(100),
  cve_tipo_poliza VARCHAR(10),
  des_tipo_poliza VARCHAR(100),
  cve_prod_tecnico VARCHAR(10),
  cve_prod_comercial VARCHAR(10),
  des_prod_tecnico VARCHAR(100),
  fch_efecto_movimiento TIMESTAMP,
  fch_fin_movimiento TIMESTAMP,
  fch_ini_poliza TIMESTAMP,
  fch_emision TIMESTAMP,
  am_emision INT,
  cve_motivo_estatus_poliza VARCHAR(10),
  des_motivo_estatus_poliza VARCHAR(100),
  fch_estatus_poliza TIMESTAMP,
  ban_anulacion_falta_pago VARCHAR(2),
  fch_anulacion_falta_pago TIMESTAMP,
  am_anulacion_falta_pago INT,
  cve_tipo_movimiento VARCHAR(2),
  des_tipo_movimiento VARCHAR(100),
  cve_ultima_situacion VARCHAR(2),
  des_ultima_situacion VARCHAR(100),
  cve_indicador_poliza_padre_hija VARCHAR(2),
  des_indicador_poliza_padre_hija VARCHAR(50),
  num_poliza_padre VARCHAR(14),
  num_version_poliza_padre SMALLINT,
  cve_pagador VARCHAR(10),
  nombre_pagador VARCHAR(150),
  am_fin_vigencia INT,
  cve_pool VARCHAR(10),
  cve_subramo_admon VARCHAR(10),
  des_subramo_admon VARCHAR(100),
  cve_plan VARCHAR(5),
  des_plan VARCHAR(100),
  ind_conexion VARCHAR(1),
  cve_usuario VARCHAR(10),
  des_usuario VARCHAR(100),
  cve_suscriptor VARCHAR(10),
  des_suscriptor VARCHAR(100),
  nivel_hospital VARCHAR(100),
  gama_poliza VARCHAR(50),
  plan_gama_nivel_poliza VARCHAR(50),
  territorialidad_poliza VARCHAR(50),
  cve_ramo VARCHAR(2),
  des_ramo VARCHAR(100),
  fch_cambio_estado TIMESTAMP,
  canal_venta_estadistico VARCHAR(100),
  segmento_venta_estadistico VARCHAR(100),
  cve_dir_plaza VARCHAR(5),
  des_dir_plaza VARCHAR(100),
  cve_edo_dir_plaza VARCHAR(5),
  des_edo_dir_plaza VARCHAR(100),
  cve_direccion_agencia VARCHAR(5),
  des_direccion_agencia VARCHAR(100),
  cve_oficina_intermediario smallint,
  red_territorial VARCHAR(50),
  cve_segmento VARCHAR(10),
  des_segmento VARCHAR(100),
  cve_negocio_multinacional VARCHAR(10),
  des_negocio_multinacional VARCHAR(100),
  cve_tipo_bono VARCHAR(5),
  des_tipo_bono VARCHAR(100),
  afecto_plan_incentivo VARCHAR(100),
  ban_poliza_contributoria VARCHAR(2),
  pct_contribucion DECIMAL(16,4),
  ban_poliza_prestacion VARCHAR(2),
  cve_motivo_exclusion_plan_incentivo VARCHAR(5),
  des_motivo_exclusion_plan_incentivo VARCHAR(100),
  ts_alta_hdfs TIMESTAMP,
  idmd5 VARCHAR(32),
  idmd5completo VARCHAR(32)
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.RCFileInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.RCFileOutputFormat'
;


CREATE VIEW BDDLAPR.CNM_POLIZA  AS 
SELECT
num_poliza
,vigencia_poliza
,num_version_poliza
,num_poliza_cobranza
,cve_sistema
,cve_sistema_int
,cve_cobertura_contable
,des_cobertura_contable
,cve_estatus_poliza
,des_estatus_poliza
,cve_forma_pago
,des_forma_pago
,clave_intermediario_principal
,nombre_intermediario_principal
,num_contrato
,num_version_contrato
,cve_negocio
,des_negocio
,folio_intermediario_principal
,fch_ini_vigencia
,fch_fin_vigencia
,cve_contratante
,rfc_contratante
,nombre_contratante
,cve_canal_venta
,des_canal_venta
,cve_tipo_poliza
,des_tipo_poliza
,cve_prod_tecnico
,cve_prod_comercial
,des_prod_tecnico_comercial
,fch_efecto_movimiento
,fch_fin_movimiento
,fch_ini_poliza
,fch_emision
,am_emision
,cve_motivo_estatus_poliza
,des_motivo_estatus_poliza
,fch_estatus_poliza
,ban_anulacion_falta_pago
,fch_anulacion_falta_pago
,am_anulacion_falta_pago
,cve_tipo_movimiento
,des_tipo_movimiento
,cve_ultima_situacion
,des_ultima_situacion
,cve_indicador_poliza_padre_hija
,des_indicador_poliza_padre_hija
,num_poliza_padre
,num_version_poliza_padre
,cve_pagador
,nombre_pagador
,am_fin_vigencia
,cve_pool
,cve_subramo_admon
,des_subramo_admon
,cve_plan
,des_plan
,ind_conexion
,cve_usuario
,des_usuario
,cve_suscriptor
,des_suscriptor
,nivel_hospital
,gama_poliza
,plan_gama_nivel_poliza
,territorialidad_poliza
,cve_ramo
,des_ramo
,fch_cambio_estado
,canal_venta_estadistico
,segmento_venta_estadistico
,cve_dir_plaza
,des_dir_plaza
,cve_edo_dir_plaza
,des_edo_dir_plaza
,cve_direccion_agencia
,des_direccion_agencia
,cve_oficina_intermediario
,red_territorial
,cve_segmento
,des_segmento
,cve_negocio_multinacional
,des_negocio_multinacional
,cve_tipo_bono
,des_tipo_bono
,afecto_plan_incentivo
,ban_poliza_contributoria
,pct_contribucion
,ban_poliza_prestacion
,cve_motivo_exclusion_plan_incentivo
,des_motivo_exclusion_plan_incentivo
FROM BDDLALM.ALM_POLIZA;




CREATE VIEW BDDLAPR.CNM_POLIZAHIS  AS 
SELECT
num_poliza
,vigencia_poliza
,num_version_poliza
,num_poliza_cobranza
,cve_sistema
,cve_sistema_int
,cve_cobertura_contable
,des_cobertura_contable
,cve_estatus_poliza
,des_estatus_poliza
,cve_forma_pago
,des_forma_pago
,clave_intermediario_principal
,nombre_intermediario_principal
,num_contrato
,num_version_contrato
,cve_negocio
,des_negocio
,folio_intermediario_principal
,fch_ini_vigencia
,fch_fin_vigencia
,cve_contratante
,rfc_contratante
,nombre_contratante
,cve_canal_venta
,des_canal_venta
,cve_tipo_poliza
,des_tipo_poliza
,cve_prod_tecnico
,cve_prod_comercial
,des_prod_tecnico_comercial
,fch_efecto_movimiento
,fch_fin_movimiento
,fch_ini_poliza
,fch_emision
,am_emision
,cve_motivo_estatus_poliza
,des_motivo_estatus_poliza
,fch_estatus_poliza
,ban_anulacion_falta_pago
,fch_anulacion_falta_pago
,am_anulacion_falta_pago
,cve_tipo_movimiento
,des_tipo_movimiento
,cve_ultima_situacion
,des_ultima_situacion
,cve_indicador_poliza_padre_hija
,des_indicador_poliza_padre_hija
,num_poliza_padre
,num_version_poliza_padre
,cve_pagador
,nombre_pagador
,am_fin_vigencia
,cve_pool
,cve_subramo_admon
,des_subramo_admon
,cve_plan
,des_plan
,ind_conexion
,cve_usuario
,des_usuario
,cve_suscriptor
,des_suscriptor
,nivel_hospital
,gama_poliza
,plan_gama_nivel_poliza
,territorialidad_poliza
,cve_ramo
,des_ramo
,fch_cambio_estado
,canal_venta_estadistico
,segmento_venta_estadistico
,cve_dir_plaza
,des_dir_plaza
,cve_edo_dir_plaza
,des_edo_dir_plaza
,cve_direccion_agencia
,des_direccion_agencia
,cve_oficina_intermediario
,red_territorial
,cve_segmento
,des_segmento
,cve_negocio_multinacional
,des_negocio_multinacional
,cve_tipo_bono
,des_tipo_bono
,afecto_plan_incentivo
,ban_poliza_contributoria
,pct_contribucion
,ban_poliza_prestacion
,cve_motivo_exclusion_plan_incentivo
,des_motivo_exclusion_plan_incentivo
FROM BDDLALM.ALM_POLIZAHIS;