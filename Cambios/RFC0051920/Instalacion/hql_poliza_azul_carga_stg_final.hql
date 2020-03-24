--WF1: WKF_GMM_POLIZA_AZUL
drop table if exists bddltrn.stg_gmm_poliza_azul_row_number ;

 create table bddltrn.stg_gmm_poliza_azul_row_number as
  SELECT * 
  , row_number() over (partition by idmd5 order by idmd5 desc) registro_duplicado
  from bddltrn.stg_gmm_poliza_azul_idmd5;


drop table if exists  bddltrn.stg_gmm_poliza_azul_final;

create table bddltrn.stg_gmm_poliza_azul_final as
SELECT 
	num_poliza,num_poliza_cobranza,cve_sistema,cve_sistema_int,cve_cobertura_contable,des_cobertura_contable,cve_estatus_poliza,
	des_estatus_poliza,cve_forma_pago,des_forma_pago, cve_intermediario_principal,nombre_intermediario_principal,
	nvl(fch_ini_vigencia, cast(from_unixtime(unix_timestamp(TO_DATE('1400-01-01'), 'yyyy-MM-dd')) as TIMESTAMP)) as fch_ini_vigencia,
	nvl(fch_fin_vigencia,cast(from_unixtime(unix_timestamp(TO_DATE('1400-01-01'), 'yyyy-MM-dd')) as TIMESTAMP))  as fch_fin_vigencia,
	rfc_contratante,nombre_contratante	,cve_circulo_medico_contratado	,des_circulo_medico_contratado	,
	nvl(fch_ini_poliza,cast(from_unixtime(unix_timestamp(TO_DATE('1400-01-01'), 'yyyy-MM-dd')) as TIMESTAMP)) as fch_ini_poliza,
	nvl(fch_emision,cast(from_unixtime(unix_timestamp(TO_DATE('1400-01-01'), 'yyyy-MM-dd')) as TIMESTAMP))    as fch_emision,
	am_emision,cve_experiencia,des_experiencia,trim(cve_pool),des_pool,cve_subramo_admon,des_subramo_admon,cve_plan,des_plan,ban_coaseguro_cero,cve_usuario,
	des_usuario,nivel_hospital,cve_admon_poliza,des_admon_poliza,gama_poliza	,plan_gama_nivel_poliza	,
	territorialidad_poliza,canal_venta_estadistico,segmento_venta_estadistico,cve_dir_plaza	,des_dir_plaza,cve_edo_dir_plaza,
	des_edo_dir_plaza,cve_direccion_agencia,des_direccion_agencia,cve_oficina_intermediario,cve_red_territorial,des_red_territorial,ban_preexistencia,
	nvl(fch_registro_producto,cast(from_unixtime(unix_timestamp(TO_DATE('1400-01-01'), 'yyyy-MM-dd')) as TIMESTAMP)) as fch_registro_producto,
	num_registro_producto	,num_aseg_dependientes,num_aseg_titulares,registro_producto_recas,
	cve_segmento,des_segmento,cve_negocio_multinacional,des_negocio_multinacional,cve_tipo_bono,des_tipo_bono,afecto_plan_incentivo,
	ban_poliza_contributoria,pct_contribucion,ban_poliza_prestacion,cve_motivo_exclusion_plan_incentivo	,
	des_motivo_exclusion_plan_incentivo,ts_alta_hdfs,idmd5,idmd5completo
from bddltrn.stg_gmm_poliza_azul_row_number
  where registro_duplicado =1;