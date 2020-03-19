
DROP TABLE IF EXISTS BDDLTRN.STG_GMM_UNION_COBER_AD_ALTERNO PURGE;

CREATE TABLE IF NOT EXISTS BDDLTRN.STG_GMM_UNION_COBER_AD_ALTERNO AS
SELECT * FROM BDDLTRN.STG_CNM_INFO_COBER_DATOS_ADICIONALES_ALTERNO
UNION ALL
SELECT  * FROM BDDLTRN.STG_CNM_CBZA_COBER_DATOS_ADICIONALES_ALTERNO;

--mto_suma_asegurada
--Inserta registros
INSERT OVERWRITE TABLE BDDLDES.ALM_COBERTURA_DATOS_ADICIONALES_ALTERNO
SELECT DISTINCT
NUM_POLIZA,
VIGENCIA_POLIZA
,NUM_VERSION_POLIZA
,NUM_POLIZA_COBRANZA,
IF (cve_asegurado IS NULL,'',TRIM(cve_asegurado)) AS cve_asegurado,
id_secuencia_asegurado,
CVE_SISTEMA,
CVE_SISTEMA_INT,
IF (cve_cobertura_contratada IS NULL,'',cve_cobertura_contratada)                                           AS cve_cobertura_contratada,
--nuevo
des_cobertura_contratada,

IF (CVE_PLAN_COBERTURA IS NULL,'',CVE_PLAN_COBERTURA )                                AS CVE_PLAN_COBERTURA,
IF (nivel_hospital  IS NULL,'',nivel_hospital)                               AS  nivel_hospital,

IF (cve_suma_asegurada IS NULL,0.0,cve_suma_asegurada)                                AS cve_suma_asegurada,

--nuevo
nvl(des_suma_asegurada,'') as des_suma_asegurada,
--nuevo
nvl(mto_suma_asegurada,0) as mto_suma_asegurada,


IF (CVE_UNIDAD_SUMA_ASEGURADA IS NULL,'',CVE_UNIDAD_SUMA_ASEGURADA)                   AS CVE_UNIDAD_SUMA_ASEGURADA,
--nuevo
des_unidad_suma_asegurada,

IF (mto_deducible_nacional IS NULL,0.0,mto_deducible_nacional)                        AS mto_deducible_nacional,
IF (cve_unidad_deducible_nacional IS NULL,'',cve_unidad_deducible_nacional)           AS cve_unidad_deducible_nacional,
--nuevo
des_unidad_deducible_nacional,

CASE WHEN mto_deducible_internacional IS NULL THEN 0
     WHEN mto_deducible_internacional ='' THEN 0
	 ELSE mto_deducible_internacional END 
  AS mto_deducible_internacional,  
  
  
IF (cve_unidad_deducible_internacional IS NULL,'',cve_unidad_deducible_internacional) AS cve_unidad_deducible_internacional,
--nuevo
des_unidad_deducible_internacional,
--nuevo
CASE WHEN cve_coaseguro IS NULL THEN ''
     WHEN cve_coaseguro ='' THEN ''
	 ELSE cve_coaseguro END 
  AS   
cve_coaseguro,
--nuevo
des_coaseguro,


                  
CASE WHEN pct_coaseguro_nacional IS NULL THEN 0
     WHEN pct_coaseguro_nacional = '' THEN 0
	 ELSE pct_coaseguro_nacional END
	  AS PCT_COASEGURO_NACIONAL,

IF (mto_lim_coaseguro_nacional IS NULL,'',mto_lim_coaseguro_nacional)                         AS mto_lim_coaseguro_nacional,
IF (cve_unidad_lim_coaseguro_nacional IS NULL,'',cve_unidad_lim_coaseguro_nacional)           AS cve_unidad_lim_coaseguro_nacional,
--nuevo 
des_unidad_lim_coaseguro_nacional,


CASE WHEN PCT_COASEGURO_INTERNACIONAL = '' THEN 0
     WHEN PCT_COASEGURO_INTERNACIONAL IS NULL THEN 0
	 ELSE PCT_COASEGURO_INTERNACIONAL END 
	  AS PCT_COASEGURO_INTERNACIONAL,

IF (mto_lim_coaseguro_internacional IS NULL,'',mto_lim_coaseguro_internacional)               AS mto_lim_coaseguro_internacional,
IF (cve_unidad_lim_coaseguro_internal IS NULL,'',cve_unidad_lim_coaseguro_internal) AS cve_unidad_lim_coaseguro_internal,
des_unidad_lim_coaseguro_internal,

IF (CVE_ESTATUS_COBERTURA IS NULL,'',CVE_ESTATUS_COBERTURA)                           AS CVE_ESTATUS_COBERTURA,
--nuevo
des_estatus_cobertura,


IF( FCH_INI_COBERTURA IS NULL OR FCH_INI_COBERTURA =0 OR TRIM(FCH_INI_COBERTURA)='' ,
    cast('1400-01-01 00:00:00' as timestamp), 
	cast(from_unixtime(unix_timestamp(cast(FCH_INI_COBERTURA as varchar(8)), 'yyyyMMdd')) as timestamp))  AS FCH_INI_COBERTURA,
IF( FCH_FIN_COBERTURA IS NULL OR FCH_FIN_COBERTURA =0 OR TRIM(FCH_FIN_COBERTURA )='' ,
    cast('1400-01-01 00:00:00' as timestamp), 
	cast(from_unixtime(unix_timestamp(cast(FCH_FIN_COBERTURA as varchar(8)), 'yyyyMMdd')) as timestamp))  AS FCH_FIN_COBERTURA,
IF( FCH_EFECTO_MOVIMIENTO IS NULL OR FCH_EFECTO_MOVIMIENTO=0 OR TRIM(FCH_EFECTO_MOVIMIENTO)='' ,
    cast('1400-01-01 00:00:00' as timestamp), 
	cast(from_unixtime(unix_timestamp(cast(FCH_EFECTO_MOVIMIENTO as varchar(8)), 'yyyyMMdd')) as timestamp))  AS FCH_EFECTO_MOVIMIENTO,
IF( FCH_FIN_MOVIMIENTO IS NULL OR FCH_FIN_MOVIMIENTO=0 OR TRIM(FCH_FIN_MOVIMIENTO)='' ,
    cast('1400-01-01 00:00:00' as timestamp), 
	cast(from_unixtime(unix_timestamp(cast(FCH_FIN_MOVIMIENTO as varchar(8)), 'yyyyMMdd')) as timestamp))  AS FCH_FIN_MOVIMIENTO,
IF (cve_nivel_asignacion IS NULL,'',cve_nivel_asignacion)                             AS cve_nivel_asignacion,
--nuevo
des_nivel_asignacion,

IF (mto_extraprima_obesidad IS NULL,'',mto_extraprima_obesidad)                       AS mto_extraprima_obesidad,
IF (mto_extraprima_dictamen IS NULL,'',mto_extraprima_dictamen)                       AS mto_extraprima_dictamen,
IF (CVE_COBERTURA_CONTABLE IS NULL,'',CVE_COBERTURA_CONTABLE)                         AS CVE_COBERTURA_CONTABLE,
--nuevo 
des_cobertura_contable,
IF( FCH_SALIDA_VIAJE_EXTRANJERO IS NULL OR FCH_SALIDA_VIAJE_EXTRANJERO=0 OR TRIM(FCH_SALIDA_VIAJE_EXTRANJERO)='',
    cast('1400-01-01 00:00:00' as timestamp), 
	cast(from_unixtime(unix_timestamp(cast(FCH_SALIDA_VIAJE_EXTRANJERO as varchar(8)), 'yyyyMMdd')) as timestamp))  AS FCH_SALIDA_VIAJE_EXTRANJERO,
IF( FCH_REGRESO_VIAJE_EXTRANJERO IS NULL OR FCH_REGRESO_VIAJE_EXTRANJERO=0 OR TRIM(FCH_REGRESO_VIAJE_EXTRANJERO)='',
    cast('1400-01-01 00:00:00' as timestamp), 
	cast(from_unixtime(unix_timestamp(cast(FCH_REGRESO_VIAJE_EXTRANJERO as varchar(8)), 'yyyyMMdd')) as timestamp))  AS FCH_REGRESO_VIAJE_EXTRANJERO,

IF (CVE_PERIODO_VIAJE_EXTRANJERO IS NULL,'',CVE_PERIODO_VIAJE_EXTRANJERO)             AS CVE_PERIODO_VIAJE_EXTRANJERO,
--IF (CVE_PREEXISTENCIA_COBERTURA IS NULL,'',BAN_PREEXISTENCIA_COBERTURA)               AS BAN_PREEXISTENCIA_COBERTURA,
--nuevo
des_periodo_viaje_extranjero,	

IF (CVE_DESTINO_VIAJE_EXTRANJERO IS NULL,'',CVE_DESTINO_VIAJE_EXTRANJERO)             AS CVE_DESTINO_VIAJE_EXTRANJERO,
--nuevo
des_destino_viaje_extranjero,

IF (cve_tarificacion_viaje_extranjero IS NULL,'',cve_tarificacion_viaje_extranjero)         AS cve_tarificacion_viaje_extranjero,
--nuevo
des_tarificacion_viaje_extranjero,

IF (mto_suma_asegurada_nacional IS NULL,0,mto_suma_asegurada_nacional)               AS mto_suma_asegurada_nacional,
IF (mto_suma_asegurada_internacional IS NULL,0,mto_suma_asegurada_internacional)     AS mto_suma_asegurada_internacional,

IF (cve_unidad_suma_asegurada_nal_intl IS NULL,'',cve_unidad_suma_asegurada_nal_intl) AS cve_unidad_suma_asegurada_nal_intl,

--nuevo
des_unidad_suma_asegurada_nal_intl,

IF (mto_recargo IS NULL,'',mto_recargo)                                               AS mto_recargo,
IF( FCH_ANTIGUEDAD IS NULL OR FCH_ANTIGUEDAD=0  OR TRIM(FCH_ANTIGUEDAD)='',
    cast('1400-01-01 00:00:00' as timestamp), 
	cast(from_unixtime(unix_timestamp(cast(FCH_ANTIGUEDAD as varchar(8)), 'yyyyMMdd')) as timestamp))   AS FCH_ANTIGUEDAD,
IF (cve_suma_asegurada_diaria IS NULL,'',cve_suma_asegurada_diaria)                   AS cve_suma_asegurada_diaria,

--nuevo
des_suma_asegurada_diaria,

IF (BAN_HOSPITALIZACION IS NULL,'',BAN_HOSPITALIZACION)                               AS BAN_HOSPITALIZACION,

CASE WHEN pct_descuento_cobertura = '' THEN 0
     WHEN pct_descuento_cobertura IS NULL THEN 0
	 ELSE pct_descuento_cobertura  END
	  AS PCT_DESCUENTO_COBERTURA,

IF (CVE_CONCEPTO_DESCUENTO_COBERTURA IS NULL,'',cve_concepto_descuento_cobertura)     AS cve_concepto_descuento_cobertura,

--nuevo
des_concepto_descuento_cobertura,

CASE WHEN PCT_PAGO_HONORARIOS IS NULL THEN 0
     WHEN PCT_PAGO_HONORARIOS = '' THEN 0
     ELSE PCT_PAGO_HONORARIOS END 
	 AS PCT_PAGO_HONORARIOS,
--IF (PCT_DESCUENTO_RIESGO_SELECTO IS NULL,0,PCT_DESCUENTO_RIESGO_SELECTO)             AS PCT_DESCUENTO_RIESGO_SELECTO,
--nvl(PCT_DESCUENTO_RIESGO_SELECTO ,'0')             AS PCT_DESCUENTO_RIESGO_SELECTO,
case when PCT_DESCUENTO_RIESGO_SELECTO ='' then 0
     when PCT_DESCUENTO_RIESGO_SELECTO is null then 0
     else PCT_DESCUENTO_RIESGO_SELECTO end
     as PCT_DESCUENTO_RIESGO_SELECTO
,

CASE WHEN pct_descuento_comercial_cobertura IS NULL THEN 0
     WHEN pct_descuento_comercial_cobertura = '' THEN 0
     ELSE pct_descuento_comercial_cobertura END 
	 AS pct_descuento_comercial_cobertura,
IF (mto_sum_asegurada_dep_fallecimiento IS NULL,'',mto_sum_asegurada_dep_fallecimiento)  AS mto_sum_asegurada_dep_fallecimiento,
CVE_PREEXISTENCIA_COBERTURA,
DES_PREEXISTENCIA_COBERTURA,

--nuevo
cve_ultima_situacion_cobertura,
--nuevo
des_ultima_situacion_cobertura,
--nuevo
nvl(cve_rango_deducible,'') as cve_rango_deducible,
--nuevo
nvl(des_rango_deducible,'') as des_rango_deducible,
--nuevo
rango_coaseguro,
--nuevo
mto_extraprima_dep_lab,
--nuevo
mto_extraprima_edad,

CVE_PERIODO_ESPERA_A_PARTIR_DE,
DES_PERIODO_ESPERA_A_PARTIR_DE,

 '' ts_alta_hdfs ,   
 '' idmd5 ,   
 '' idmd5completo 



FROM BDDLTRN.STG_GMM_UNION_COBER_AD_ALTERNO;
