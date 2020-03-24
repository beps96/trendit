--- Respaldamos información existente
------------------------------------------------

Create table bddldes.alm_poliza_azul_bkp23MAR2020 as 
SELECT *
FROM BDDLALM.ALM_POLIZA_AZUL;

Create table bddldes.cnm_poliza_azul_bkp23MAR2020 as 
SELECT *
FROM BDDLAPR.CNM_POLIZA_AZUL;


--- Creamos tabla de paso para el proceso de venta_estadistico
------------------------------------------------


CREATE TABLE bddltrn.stg_gmm_pol_azul_formato_cnm_poliza_azul(	
  num_poliza varchar(14), 	
  num_poliza_cobranza varchar(8), 	
  cve_sistema varchar(4), 	
  cve_sistema_int varchar(4), 	
  cve_cobertura_contable varchar(3), 	
  des_cobertura_contable varchar(100), 	
  cve_estatus_poliza varchar(2), 	
  des_estatus_poliza varchar(100), 	
  cve_forma_pago varchar(1), 	
  des_forma_pago varchar(100), 	
  cve_intermediario_principal int, 	
  nombre_intermediario_principal varchar(150), 	
  fch_ini_vigencia timestamp, 	
  fch_fin_vigencia timestamp, 	
  rfc_contratante varchar(13), 	
  nombre_contratante varchar(150), 	
  cve_circulo_medico_contratado varchar(10), 	
  des_circulo_medico_contratado varchar(100), 	
  fch_ini_poliza timestamp, 	
  fch_emision timestamp, 	
  am_emision int, 	
  cve_experiencia varchar(5), 	
  des_experiencia varchar(50), 	
  cve_pool varchar(10), 	
  des_pool varchar(100), 	
  cve_subramo_admon varchar(10), 	
  des_subramo_admon varchar(100), 	
  cve_plan varchar(5), 	
  des_plan varchar(100), 	
  ban_coaseguro_cero varchar(2), 	
  cve_usuario varchar(10), 	
  des_usuario varchar(100), 	
  nivel_hospital varchar(100), 	
  cve_admon_poliza varchar(5), 	
  des_admon_poliza varchar(100), 	
  gama_poliza varchar(100), 	
  plan_gama_nivel_poliza varchar(100), 	
  territorialidad_poliza varchar(100), 	
  canal_venta_estadistico varchar(100), 	
  segmento_venta_estadistico varchar(100), 	
  cve_dir_plaza varchar(5), 	
  des_dir_plaza varchar(100), 	
  cve_edo_dir_plaza varchar(5), 	
  des_edo_dir_plaza varchar(100), 	
  cve_direccion_agencia varchar(5), 	
  des_direccion_agencia varchar(100), 	
  cve_oficina_intermediario smallint, 	
  cve_red_territorial varchar(5), 	
  des_red_territorial varchar(100), 	
  ban_preexistencia varchar(2), 	
  fch_registro_producto timestamp, 	
  num_registro_producto varchar(100), 	
  num_aseg_dependientes int, 	
  num_aseg_titulares int, 	
  registro_producto_recas varchar(50), 	
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
  des_motivo_exclusion_plan_incentivo varchar(100))	
ROW FORMAT SERDE 	
  'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe' 	
STORED AS INPUTFORMAT 	
  'org.apache.hadoop.hive.ql.io.RCFileInputFormat' 	
OUTPUTFORMAT 	
  'org.apache.hadoop.hive.ql.io.RCFileOutputFormat'	
;

TRUNCATE TABLE BDDLALM.ALM_POLIZA_AZUL;
TRUNCATE TABLE BDDLALM.ALM_POLIZA_AZULHIS;
