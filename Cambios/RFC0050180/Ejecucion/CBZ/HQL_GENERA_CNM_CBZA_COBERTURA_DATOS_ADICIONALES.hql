DROP TABLE IF EXISTS BDDLTRN.STG_CNM_CBZA_COBER_DATOS_ADICIONALES_ALTERNO PURGE;
CREATE TABLE IF NOT EXISTS BDDLTRN.STG_CNM_CBZA_COBER_DATOS_ADICIONALES_ALTERNO  AS
SELECT distinct
COBERTURA.NUM_POLIZA ,
-999                                           AS VIGENCIA_POLIZA,
-999                                           AS NUM_VERSION_POLIZA,
COBERTURA.NUM_POLIZA_COBRANZA,
cve_asegurado,
-999                                           AS id_secuencia_asegurado,
'AZUL'                                         AS CVE_SISTEMA,
'AZUL'                                         AS CVE_SISTEMA_INT,
CAT_CLAVE_COBERTURA_CONTRATADA.cve_homologada    AS cve_cobertura_contratada,

case when CAT_CLAVE_COBERTURA_CONTRATADA.des_homologada is null
     then concat('Descripción no encontrada para: ', trim(POL_NOMBRE_COB)    )
     else  CAT_CLAVE_COBERTURA_CONTRATADA.des_homologada
     end
     AS des_cobertura_contratada,


CAST(POL_PLAN    AS STRING)           AS CVE_PLAN_COBERTURA,
CAST(nvl(poliza.nivel_hospital,'')    AS STRING)               AS nivel_hospital,

'' as cve_suma_asegurada,
MTO_SUMA_ASEGURADA as des_suma_asegurada,


CAST(MTO_SUMA_ASEGURADA AS DECIMAL(16,4))      AS MTO_SUMA_ASEGURADA,
CAST(CVE_UNIDAD AS STRING)                             AS CVE_UNIDAD_SUMA_ASEGURADA,

'' as des_unidad_suma_asegurada,

CAST(MTO_DEDUCIBLE_NACIONAL AS DECIMAL(16,4))  AS MTO_DEDUCIBLE_NACIONAL,
CAST(CVE_MONEDA AS STRING)             AS cve_unidad_deducible_nacional,

case  when POL_NOMBRE_COB not in ('LAI','LAV') then pol_deducible
     else 0 end as des_unidad_deducible_nacional,

nvl(MTO_DEDUCIBLE_INTERNACIONAL,0)                             AS MTO_DEDUCIBLE_INTERNACIONAL,


case  when POL_NOMBRE_COB  in ('LAI','LAV') then pol_uni_dedu
     else 0 end as  cve_unidad_deducible_internacional,

	 
case  when POL_NOMBRE_COB  in ('LAI','LAV') then pol_deducible
     else 0 end as  des_unidad_deducible_internacional,
	 
	 
nvl(pol_unidad_coa,'') as cve_coaseguro,
POL_COASEGURO as des_coaseguro,


nvl(PCT_COASEGURO_NACIONAL ,'')                 AS PCT_COASEGURO_NACIONAL,
CAST(0 AS STRING)                             AS mto_lim_coaseguro_nacional,


case  when POL_NOMBRE_COB not in ('LAI','LAV') then nvl(pol_unidad_coa,'')
     else 0 end as  cve_unidad_lim_coaseguro_nacional,

case  when POL_NOMBRE_COB not in ('LAI','LAV') then pol_deducible
     else 0 end as  des_unidad_lim_coaseguro_nacional,

nvl(PCT_COASEGURO_INTERNACIONAL,'')                             AS PCT_COASEGURO_INTERNACIONAL,
CAST(0 AS STRING)                             AS mto_lim_coaseguro_internacional,


case  when POL_NOMBRE_COB  in ('LAI','LAV') then nvl(pol_unidad_coa,'')
     else 0 end as  cve_unidad_lim_coaseguro_internal,

case  when POL_NOMBRE_COB  in ('LAI','LAV') then pol_deducible
     else 0 end as  des_unidad_lim_coaseguro_internal,


CAST('' AS STRING)                             AS CVE_ESTATUS_COBERTURA,
'' as des_estatus_cobertura,

CAST('' AS STRING)                             AS FCH_INI_COBERTURA,
CAST('' AS STRING)                             AS FCH_FIN_COBERTURA,
CAST('' AS STRING)                             AS FCH_EFECTO_MOVIMIENTO,
CAST('' AS STRING)                             AS FCH_FIN_MOVIMIENTO,
CAST('' AS STRING)                             AS cve_nivel_asignacion,
'' as des_nivel_asignacion,

CAST(0 AS STRING)                             AS mto_extraprima_obesidad,
CAST(0 AS STRING)                             AS mto_extraprima_dictamen,


CVE_COBERTURA_CONTABLE                             AS CVE_COBERTURA_CONTABLE,


case when DES_COBERTURA_CONTABLE is null
     then concat('Descripción no encontrada para: ',  nvl(CAST(trim(CVE_COBERTURA_CONTABLE) AS STRING),''))    
     else  DES_COBERTURA_CONTABLE
     end
     AS DES_COBERTURA_CONTABLE,


--'' as des_cobertura_contable,
 
CAST('' AS STRING)                             AS FCH_SALIDA_VIAJE_EXTRANJERO,
CAST('' AS STRING)                             AS FCH_REGRESO_VIAJE_EXTRANJERO,
CAST('' AS STRING)                             AS CVE_PERIODO_VIAJE_EXTRANJERO,
'' as des_periodo_viaje_extranjero,


CAST('' AS STRING)                             AS CVE_DESTINO_VIAJE_EXTRANJERO,
'' as des_destino_viaje_extranjero,

CAST('' AS STRING)                             AS cve_tarificacion_viaje_extranjero,
'' as des_tarificacion_viaje_extranjero,

CAST(0 AS STRING)                             AS mto_suma_asegurada_nacional,
CAST(0 AS STRING)                             AS mto_suma_asegurada_internacional,

CAST('' AS STRING)                             AS cve_unidad_suma_asegurada_nal_intl,
'' as des_unidad_suma_asegurada_nal_intl,


CAST(0 AS STRING)                             AS mto_recargo,

CAST('' AS STRING)                             AS FCH_ANTIGUEDAD,
CAST('' AS STRING)                             AS cve_suma_asegurada_diaria,
'' as des_suma_asegurada_diaria,

CAST('' AS STRING)                             AS BAN_HOSPITALIZACION,
CAST('' AS STRING)                             AS PCT_DESCUENTO_COBERTURA,
CAST('' AS STRING)                             AS CVE_CONCEPTO_DESCUENTO_COBERTURA,
'' as des_concepto_descuento_cobertura,

CAST('' AS STRING)                             AS PCT_PAGO_HONORARIOS,
CAST(0 AS STRING)                             AS PCT_DESCUENTO_RIESGO_SELECTO,
CAST('' AS STRING)                             AS PCT_DESCUENTO_COMERCIAL_COBERTURA,
CAST(0 AS STRING)                             AS mto_sum_asegurada_dep_fallecimiento,
CAST('' AS STRING)                             AS CVE_PREEXISTENCIA_COBERTURA,
CAST('' AS STRING)                             AS DES_PREEXISTENCIA_COBERTURA,

'' as cve_ultima_situacion_cobertura,
'' as des_ultima_situacion_cobertura,
'' as cve_rango_deducible,
'' as des_rango_deducible,
'' as rango_coaseguro, 
0 as mto_extraprima_dep_lab,
0 as mto_extraprima_edad,



CAST('' AS STRING)                             AS CVE_PERIODO_ESPERA_A_PARTIR_DE,
CAST('' AS STRING)                             AS DES_PERIODO_ESPERA_A_PARTIR_DE
FROM BDDLTRN.STG_CNM_CBZA_COB_DATOS_ADICIONALES_ALTERNO COBERTURA

LEFT JOIN  ( select 
             cve_atributo_org,    --es la clave que compararé, obtenida del sistema origen para obtener
             cve_homologada,   --es la clave homologada que almacenaré en la tabla final
             des_homologada   --es la descripcion homologada que almacenaré en la tabla final 
             from bddlalm.cat_homologados
             where 
             nombre_catalogo='CAT_CLAVE_COBERTURA_CONTRATADA_AFECTADA'
             and
             cve_sistema='AZUL'
             and
             cve_ramo='SA'  
			 ) CAT_CLAVE_COBERTURA_CONTRATADA  
			 on CAT_CLAVE_COBERTURA_CONTRATADA.cve_atributo_org = trim(POL_NOMBRE_COB)
 
 LEFT JOIN
 (
 select distinct num_poliza, num_version_poliza, num_poliza_cobranza, vigencia_poliza, nivel_hospital from (
SELECT num_poliza, num_version_poliza, num_poliza_cobranza, vigencia_poliza, nivel_hospital FROM bddldes.cnm_poliza_testnov19 where cve_sistema='INFO'
union all
select num_poliza, -999 as num_version_poliza, num_poliza_cobranza, -999 vigencia_poliza, nivel_hospital from  bddldes.cnm_poliza_azul_TEST
)POL)Poliza

on 
POLIZA.num_poliza=COBERTURA.NUM_POLIZA
AND
POLIZA.num_poliza_cobranza=COBERTURA.NUM_POLIZA_COBRANZA
--POLIZA.vigencia_poliza=-999 AND
--POLIZA.num_version_poliza=-999
;
DROP TABLE IF EXISTS BDDLTRN.STG_CNM_CBZA_COBER_DATOS_ADICIONALES_ALTERNO PURGE;
CREATE TABLE IF NOT EXISTS BDDLTRN.STG_CNM_CBZA_COBER_DATOS_ADICIONALES_ALTERNO  AS
SELECT distinct
COBERTURA.NUM_POLIZA ,
-999                                           AS VIGENCIA_POLIZA,
-999                                           AS NUM_VERSION_POLIZA,
COBERTURA.NUM_POLIZA_COBRANZA,
cve_asegurado,
-999                                           AS id_secuencia_asegurado,
'AZUL'                                         AS CVE_SISTEMA,
'AZUL'                                         AS CVE_SISTEMA_INT,
CAT_CLAVE_COBERTURA_CONTRATADA.cve_homologada    AS cve_cobertura_contratada,

case when CAT_CLAVE_COBERTURA_CONTRATADA.des_homologada is null
     then concat('Descripción no encontrada para: ', trim(POL_NOMBRE_COB)    )
     else  CAT_CLAVE_COBERTURA_CONTRATADA.des_homologada
     end
     AS des_cobertura_contratada,


CAST(POL_PLAN    AS STRING)           AS CVE_PLAN_COBERTURA,
CAST(nvl(poliza.nivel_hospital,'')    AS STRING)               AS nivel_hospital,

'' as cve_suma_asegurada,
MTO_SUMA_ASEGURADA as des_suma_asegurada,


CAST(MTO_SUMA_ASEGURADA AS DECIMAL(16,4))      AS MTO_SUMA_ASEGURADA,
CAST(CVE_UNIDAD AS STRING)                             AS CVE_UNIDAD_SUMA_ASEGURADA,

'' as des_unidad_suma_asegurada,

CAST(MTO_DEDUCIBLE_NACIONAL AS DECIMAL(16,4))  AS MTO_DEDUCIBLE_NACIONAL,
CAST(CVE_MONEDA AS STRING)             AS cve_unidad_deducible_nacional,

case  when POL_NOMBRE_COB not in ('LAI','LAV') then pol_deducible
     else 0 end as des_unidad_deducible_nacional,

nvl(MTO_DEDUCIBLE_INTERNACIONAL,0)                             AS MTO_DEDUCIBLE_INTERNACIONAL,


case  when POL_NOMBRE_COB  in ('LAI','LAV') then pol_uni_dedu
     else 0 end as  cve_unidad_deducible_internacional,

	 
case  when POL_NOMBRE_COB  in ('LAI','LAV') then pol_deducible
     else 0 end as  des_unidad_deducible_internacional,
	 
	 
nvl(pol_unidad_coa,'') as cve_coaseguro,
POL_COASEGURO as des_coaseguro,


nvl(PCT_COASEGURO_NACIONAL ,'')                 AS PCT_COASEGURO_NACIONAL,
CAST(0 AS STRING)                             AS mto_lim_coaseguro_nacional,


case  when POL_NOMBRE_COB not in ('LAI','LAV') then nvl(pol_unidad_coa,'')
     else 0 end as  cve_unidad_lim_coaseguro_nacional,

case  when POL_NOMBRE_COB not in ('LAI','LAV') then pol_deducible
     else 0 end as  des_unidad_lim_coaseguro_nacional,

nvl(PCT_COASEGURO_INTERNACIONAL,'')                             AS PCT_COASEGURO_INTERNACIONAL,
CAST(0 AS STRING)                             AS mto_lim_coaseguro_internacional,


case  when POL_NOMBRE_COB  in ('LAI','LAV') then nvl(pol_unidad_coa,'')
     else 0 end as  cve_unidad_lim_coaseguro_internal,

case  when POL_NOMBRE_COB  in ('LAI','LAV') then pol_deducible
     else 0 end as  des_unidad_lim_coaseguro_internal,


CAST('' AS STRING)                             AS CVE_ESTATUS_COBERTURA,
'' as des_estatus_cobertura,

CAST('' AS STRING)                             AS FCH_INI_COBERTURA,
CAST('' AS STRING)                             AS FCH_FIN_COBERTURA,
CAST('' AS STRING)                             AS FCH_EFECTO_MOVIMIENTO,
CAST('' AS STRING)                             AS FCH_FIN_MOVIMIENTO,
CAST('' AS STRING)                             AS cve_nivel_asignacion,
'' as des_nivel_asignacion,

CAST(0 AS STRING)                             AS mto_extraprima_obesidad,
CAST(0 AS STRING)                             AS mto_extraprima_dictamen,


CVE_COBERTURA_CONTABLE                             AS CVE_COBERTURA_CONTABLE,


case when DES_COBERTURA_CONTABLE is null
     then concat('Descripción no encontrada para: ',  nvl(CAST(trim(CVE_COBERTURA_CONTABLE) AS STRING),''))    
     else  DES_COBERTURA_CONTABLE
     end
     AS DES_COBERTURA_CONTABLE,


--'' as des_cobertura_contable,
 
CAST('' AS STRING)                             AS FCH_SALIDA_VIAJE_EXTRANJERO,
CAST('' AS STRING)                             AS FCH_REGRESO_VIAJE_EXTRANJERO,
CAST('' AS STRING)                             AS CVE_PERIODO_VIAJE_EXTRANJERO,
'' as des_periodo_viaje_extranjero,


CAST('' AS STRING)                             AS CVE_DESTINO_VIAJE_EXTRANJERO,
'' as des_destino_viaje_extranjero,

CAST('' AS STRING)                             AS cve_tarificacion_viaje_extranjero,
'' as des_tarificacion_viaje_extranjero,

CAST(0 AS STRING)                             AS mto_suma_asegurada_nacional,
CAST(0 AS STRING)                             AS mto_suma_asegurada_internacional,

CAST('' AS STRING)                             AS cve_unidad_suma_asegurada_nal_intl,
'' as des_unidad_suma_asegurada_nal_intl,


CAST(0 AS STRING)                             AS mto_recargo,

CAST('' AS STRING)                             AS FCH_ANTIGUEDAD,
CAST('' AS STRING)                             AS cve_suma_asegurada_diaria,
'' as des_suma_asegurada_diaria,

CAST('' AS STRING)                             AS BAN_HOSPITALIZACION,
CAST('' AS STRING)                             AS PCT_DESCUENTO_COBERTURA,
CAST('' AS STRING)                             AS CVE_CONCEPTO_DESCUENTO_COBERTURA,
'' as des_concepto_descuento_cobertura,

CAST('' AS STRING)                             AS PCT_PAGO_HONORARIOS,
CAST(0 AS STRING)                             AS PCT_DESCUENTO_RIESGO_SELECTO,
CAST('' AS STRING)                             AS PCT_DESCUENTO_COMERCIAL_COBERTURA,
CAST(0 AS STRING)                             AS mto_sum_asegurada_dep_fallecimiento,
CAST('' AS STRING)                             AS CVE_PREEXISTENCIA_COBERTURA,
CAST('' AS STRING)                             AS DES_PREEXISTENCIA_COBERTURA,

'' as cve_ultima_situacion_cobertura,
'' as des_ultima_situacion_cobertura,
'' as cve_rango_deducible,
'' as des_rango_deducible,
'' as rango_coaseguro, 
0 as mto_extraprima_dep_lab,
0 as mto_extraprima_edad,



CAST('' AS STRING)                             AS CVE_PERIODO_ESPERA_A_PARTIR_DE,
CAST('' AS STRING)                             AS DES_PERIODO_ESPERA_A_PARTIR_DE
FROM BDDLTRN.STG_CNM_CBZA_COB_DATOS_ADICIONALES_ALTERNO COBERTURA

LEFT JOIN  ( select 
             cve_atributo_org,    --es la clave que compararé, obtenida del sistema origen para obtener
             cve_homologada,   --es la clave homologada que almacenaré en la tabla final
             des_homologada   --es la descripcion homologada que almacenaré en la tabla final 
             from bddlalm.cat_homologados
             where 
             nombre_catalogo='CAT_CLAVE_COBERTURA_CONTRATADA_AFECTADA'
             and
             cve_sistema='AZUL'
             and
             cve_ramo='SA'  
			 ) CAT_CLAVE_COBERTURA_CONTRATADA  
			 on CAT_CLAVE_COBERTURA_CONTRATADA.cve_atributo_org = trim(POL_NOMBRE_COB)
 
 INNER JOIN
 (

select num_poliza, -999 as num_version_poliza, num_poliza_cobranza, -999 vigencia_poliza, nivel_hospital from  bddldes.cnm_poliza_azul_TEST
)Poliza

on 
POLIZA.num_poliza=COBERTURA.NUM_POLIZA
AND
POLIZA.num_poliza_cobranza=COBERTURA.NUM_POLIZA_COBRANZA

;