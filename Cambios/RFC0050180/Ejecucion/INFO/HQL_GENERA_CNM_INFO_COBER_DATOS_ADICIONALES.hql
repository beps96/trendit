

DROP TABLE IF EXISTS BDDLTRN.STG_CNM_INFO_COBER_DATOS_ADICIONALES_ALTERNO   PURGE;
CREATE TABLE IF NOT EXISTS BDDLTRN.STG_CNM_INFO_COBER_DATOS_ADICIONALES_ALTERNO  AS --168853308  
SELECT NUM_POLIZA,
VIGENCIA_POLIZA, 
NUM_VERSION_POLIZA,
''                               AS NUM_POLIZA_COBRANZA,
cve_asegurado                         AS cve_asegurado,
CAST(SEC_OBJETO AS INT)          AS id_secuencia_asegurado,
'INFO'                           AS CVE_SISTEMA,
CAST('INFO'  AS STRING)          AS CVE_SISTEMA_INT,
nvl(CAT_CLAVE_COBERTURA_CONTRATADA.cve_homologada,'')            AS cve_cobertura_contratada,
case when CAT_CLAVE_COBERTURA_CONTRATADA.des_homologada is null
     then concat('Descripción no encontrada para: ',  trim(CVE_COBERTURA))    
     else  CAT_CLAVE_COBERTURA_CONTRATADA.des_homologada
     end
     AS des_cobertura_contratada,
''                               AS CVE_PLAN_COBERTURA,
nivel_hospital                               AS nivel_hospital,
nvl(cdsuaseg,'')   AS cve_suma_asegurada,


case when des_cdsuaseg is null
     then concat('Descripción no encontrada para: ',  nvl(CPASEGUR,''))    
     else  des_cdsuaseg
     end
     AS DES_SUMA_ASEGURADA,
	 
	 
nvl(cpasegur,0) AS MTO_SUMA_ASEGURADA,
nvl(CAST(TCCDMOSA AS STRING),'')        AS CVE_UNIDAD_SUMA_ASEGURADA,


case when des_tccdmosa is null
     then concat('Descripción no encontrada para: ',  nvl(CAST(TCCDMOSA AS STRING),''))    
     else  des_tccdmosa
     end
     AS DES_UNIDAD_SUMA_ASEGURADA,


CAST(VADEDUNA AS DECIMAL(16,4))  AS mto_deducible_nacional,
nvl(CAST(stg.TCCDMODE AS STRING),'')         AS cve_unidad_deducible_nacional,


case when des_tccdmode is null
     then concat('Descripción no encontrada para: ',  nvl(CAST(stg.TCCDMODE AS STRING),''))    
     else  des_tccdmode
     end
     AS DES_UNIDAD_DEDUCIBLE_NACIONAL,

--CAST(VADEDUIN AS STRING)         AS mto_deducible_internacional,
nvl(VADEDUIN,0)          AS mto_deducible_internacional,
nvl(CAST(TCCDMODI AS STRING),'')         AS cve_unidad_deducible_internacional,


case when des_tccdmodi is null
     then concat('Descripción no encontrada para: ',  nvl(CAST(TCCDMODI AS STRING),''))    
     else  des_tccdmodi
     end
     AS DES_UNIDAD_DEDUCIBLE_INTERNACIONAL,

	 stg.cdcoaint  as cve_coaseguro,
	 stg.des_cdcoaint as des_coaseguro,
	 




--CAST(stg.CDCOAINT AS STRING)         AS PCT_COASEGURO_NACIONAL,
stg.CDCOAINT         AS PCT_COASEGURO_NACIONAL,
CAST(VALIMCOA AS STRING)         AS mto_lim_coaseguro_nacional,
nvl(CAST(TCCDMOCO AS STRING),'')         AS cve_unidad_lim_coaseguro_nacional,


case when des_tccdmoco is null
     then concat('Descripción no encontrada para: ',  nvl(CAST(TCCDMOCO AS STRING),''))    
     else  des_tccdmoco
     end
     AS DES_UNIDAD_LIM_COASEGURO_NACIONAL,


--CAST(stg.CDCOAINT AS STRING)         AS PCT_COASEGURO_INTERNACIONAL,
nvl(stg.CDCOAINT,0)          AS PCT_COASEGURO_INTERNACIONAL,
CAST(VALIMCOI AS STRING)         AS mto_lim_coaseguro_internacional,
nvl(CAST(TCCDMOCI AS STRING),'')         AS cve_unidad_lim_coaseguro_internal,


case when des_tccdmoci is null
     then concat('Descripción no encontrada para: ',  nvl(CAST(TCCDMOCI AS STRING),''))    
     else  des_tccdmoci
     end
     AS DES_UNIDAD_LIM_COASEGURO_INTERNAL,


nvl(CAST(CAT_ESTATUS_COBERTURA.cve_homologada    AS STRING),'')         AS CVE_ESTATUS_COBERTURA,



case when CAT_ESTATUS_COBERTURA.des_homologada is null
     then concat('Descripción no encontrada para: ',  nvl(CAST(trim(ESMCT) AS STRING),''))    
     else  CAT_ESTATUS_COBERTURA.des_homologada
     end
     AS DES_ESTATUS_COBERTURA,


CAST(FEINIMCT AS STRING)         AS FCH_INI_COBERTURA,
CAST(FEFINMCT AS STRING)         AS FCH_FIN_COBERTURA,
CAST(FEEFTOMO AS STRING)         AS FCH_EFECTO_MOVIMIENTO,
CAST(FEFINEFE AS STRING)         AS FCH_FIN_MOVIMIENTO,
nvl(CAST(INNIVEAS AS STRING),'')         AS cve_nivel_asignacion,


case when des_inniveas is null
     then concat('Descripción no encontrada para: ',  nvl(CAST(trim(INNIVEAS) AS STRING),''))    
     else  des_inniveas
     end
     AS des_nivel_asignacion,

CAST(VAEXTOBS AS STRING)         AS mto_extraprima_obesidad,
CAST(VAEXTDIC AS STRING)         AS mto_extraprima_dictamen,
nvl(CAST(CVE_COBERTURA_CONTABLE AS STRING),'')    AS CVE_COBERTURA_CONTABLE,

case when DES_COBERTURA_CONTABLE is null
     then concat('Descripción no encontrada para: ',  nvl(CAST(trim(CVE_COBERTURA_CONTABLE) AS STRING),''))    
     else  DES_COBERTURA_CONTABLE
     end
     AS DES_COBERTURA_CONTABLE,


CAST(FESALIDA AS STRING)         AS FCH_SALIDA_VIAJE_EXTRANJERO,
CAST(FEREGRES AS STRING)         AS FCH_REGRESO_VIAJE_EXTRANJERO,
nvl(CAST(CDESTAN  AS STRING),'')         AS CVE_PERIODO_VIAJE_EXTRANJERO,

case when des_cdestan is null
     then concat('Descripción no encontrada para: ',  nvl(CAST(trim(CDESTAN) AS STRING),''))    
     else  des_cdestan
     end
     AS des_periodo_viaje_extranjero,
	 
nvl(CAST(CDDESTI  AS STRING),'')         AS CVE_DESTINO_VIAJE_EXTRANJERO,

case when des_cddesti is null
     then concat('Descripción no encontrada para: ',  nvl(CAST(trim(CDDESTI) AS STRING),''))    
     else  des_cddesti
     end
     AS des_destino_viaje_extranjero,

nvl(CAST(TCTIPCAG AS STRING),'')         AS cve_tarificacion_viaje_extranjero,

case when des_tctipcag is null
     then concat('Descripción no encontrada para: ',  nvl(CAST(trim(TCTIPCAG) AS STRING),''))    
     else  des_tctipcag
     end
     AS des_tarificacion_viaje_extranjero,

CAST(CPASENAC AS STRING)         AS mto_suma_asegurada_nacional,
CAST(CPASINTR AS STRING)         AS mto_suma_asegurada_internacional,
nvl(CAST(TCCDMOSA AS STRING),'')        AS cve_unidad_suma_asegurada_nal_intl,

case when des_tccdmosa is null
     then concat('Descripción no encontrada para: ',  nvl(CAST(trim(TCCDMOSA) AS STRING),''))    
     else  des_tccdmosa
     end
     AS des_unidad_suma_asegurada_nal_intl,

CAST(VARECCOB AS STRING)         AS mto_recargo,
CAST(FEANTCOB AS STRING)         AS FCH_ANTIGUEDAD,
nvl(CAST(CDSUASDI AS STRING),'')         AS cve_suma_asegurada_diaria,


case when des_cdsuasdi is null
     then concat('Descripción no encontrada para: ',  nvl(CAST(trim(CDSUASDI) AS STRING),''))    
     else  des_cdsuasdi
     end
     AS des_suma_asegurada_diaria,

CAST(INHOSPIT AS STRING)         AS BAN_HOSPITALIZACION,
--CAST(PODESAGE AS STRING)         AS PCT_DESCUENTO_COBERTURA,
nvl(PODESAGE,0)          AS PCT_DESCUENTO_COBERTURA,
nvl(CAST(CDDESCTO AS STRING),'')         AS cve_concepto_descuento_cobertura,

case when des_cddescto is null
     then concat('Descripción no encontrada para: ',  nvl(CAST(trim(CDDESCTO) AS STRING),''))    
     else  des_cddescto
     end
     AS des_concepto_descuento_cobertura,

--CAST(POPAGHON AS STRING)         AS PCT_PAGO_HONORARIOS,
nvl(POPAGHON,0)         AS PCT_PAGO_HONORARIOS,

--CAST(INDESRIE AS STRING)         AS PCT_DESCUENTO_RIESGO_SELECTO,
nvl(INDESRIE,0)          AS PCT_DESCUENTO_RIESGO_SELECTO,

--CAST(POREDE03 AS STRING)         AS PCT_DESCUENTO_COMERCIAL_COBERTURA,
nvl(POREDE03,0)          AS PCT_DESCUENTO_COMERCIAL_COBERTURA,

--CAST(CPASERFA AS STRING)         AS mto_sum_asegurada_dep_fallecimiento,  
CPASERFA          AS mto_sum_asegurada_dep_fallecimiento,  



--CAST(INPREEXT AS STRING)         AS CVE_PREEXISTENCIA_COBERTURA,  --BAN_PREEXISTENCIA_COBERTURA,
CASE WHEN INPREEXT IS NULL   THEN CAST('' AS STRING)   
     WHEN TRIM(INPREEXT) = ''   THEN CAST('' AS STRING)  
     ELSE    CAST(INPREEXT AS STRING)  
     END AS CVE_PREEXISTENCIA_COBERTURA,
	 
case when des_inpreext is null
     then concat('Descripción no encontrada para: ',  nvl(CAST(trim(INPREEXT) AS STRING),''))    
     else  des_inpreext
     end
     AS DES_PREEXISTENCIA_COBERTURA,
	 	 
nvl(ult_sit,'') AS	 cve_ultima_situacion_cobertura,

case when CAT_CLAVE_ULTIMA_SITUACION.dselemen is null
     then concat('Descripción no encontrada para: ',  nvl(CAST(trim(ult_sit) AS STRING),''))    
     else  CAT_CLAVE_ULTIMA_SITUACION.dselemen
     end
     AS des_ultima_situacion_cobertura,
	 
	 
	 
case when stg.cve_plan in ('17','31','33') 
     then stg.cve_rango_deducible_internacional
     else stg.cve_rango_deducible_nacional 
	 end 
	 as cve_rango_deducible,
	 
case when stg.cve_plan in ('17','31','33') 
     then stg.des_rango_deducible_internacional
     else stg.des_rango_deducible_nacional 
	 end 
	 as des_rango_deducible,

 
nvl(RGOCOA,'') AS rango_coaseguro,
VAEXTOCP AS mto_extraprima_dep_lab,  --mto_extraprima_dep_lab
VAEXTEDA AS mto_extraprima_edad,  --mto_extraprima_edad	 
	 
nvl(CDPERESP,'') as cve_periodo_espera_a_partir_de,

case when des_cdperesp is null
     then concat('Descripción no encontrada para: ',  nvl(CAST(trim(CDPERESP) AS STRING),''))    
     else  des_cdperesp
     end
     AS des_periodo_espera_a_partir_de 

	 
	 
FROM BDDLTRN.STG_GMM_COBERTURA_ADICIONALES_COMPLEMENTO_ALTERNO stg

			
LEFT JOIN  ( select 
             cve_atributo_org,    --es la clave que compararé, obtenida del sistema origen para obtener
             cve_homologada,   --es la clave homologada que almacenaré en la tabla final
             des_homologada   --es la descripcion homologada que almacenaré en la tabla final 
             from bddlalm.cat_homologados
             where 
             nombre_catalogo='CAT_ESTATUS_COBERTURA_CONTRATADA'
             and
             cve_sistema='INFO'
             and
             cve_ramo='SA'  
			 ) CAT_ESTATUS_COBERTURA 
			 on CAT_ESTATUS_COBERTURA.cve_atributo_org = trim(ESMCT)
			
LEFT JOIN  ( select 
             cve_atributo_org,    --es la clave que compararé, obtenida del sistema origen para obtener
             cve_homologada,   --es la clave homologada que almacenaré en la tabla final
             des_homologada   --es la descripcion homologada que almacenaré en la tabla final 
             from bddlalm.cat_homologados
             where 
             nombre_catalogo='CAT_CLAVE_COBERTURA_CONTRATADA_AFECTADA'
             and
             cve_sistema='INFO'
             and
             cve_ramo='SA'  
			 ) CAT_CLAVE_COBERTURA_CONTRATADA 
			 on CAT_CLAVE_COBERTURA_CONTRATADA.cve_atributo_org = trim(CVE_COBERTURA)
LEFT JOIN   ( 
			select trim(cdelemen) cdelemen, trim(dselemen) dselemen from bddlcru.ktctget where trim(cdtabla) ='KCIT46G' 
             )CAT_CLAVE_ULTIMA_SITUACION  
			 ON CAT_CLAVE_ULTIMA_SITUACION.cdelemen = ult_sit

LEFT JOIN   (
               select cdcoaint , trim(RGOCOA) RGOCOA from bddlcru.cat_cdcoaseg
			 ) CAT_RANGO_COASEGURO
			 ON CAT_RANGO_COASEGURO.cdcoaint=cast(stg.cdcoaint as int)
			 