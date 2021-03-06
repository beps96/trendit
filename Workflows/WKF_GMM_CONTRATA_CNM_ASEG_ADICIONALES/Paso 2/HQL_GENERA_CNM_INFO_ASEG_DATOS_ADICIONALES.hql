DROP TABLE IF EXISTS BDDLTRN.STG_CNM_INFO_ASEG_DATOS_ADICIONALES ;

CREATE TABLE  BDDLTRN.STG_CNM_INFO_ASEG_DATOS_ADICIONALES  
    STORED AS RCFile AS



 SELECT 
    IF(NUM_POLIZA IS NULL,'',NUM_POLIZA)                                               AS NUM_POLIZA,
    CAST(IF(VIGENCIA_POLIZA IS NULL,-999,VIGENCIA_POLIZA) AS SMALLINT)                   AS VIGENCIA_POLIZA,
    NUM_VERSION_POLIZA                                                                 AS NUM_VERSION_POLIZA,
    IF(NUM_POLIZA_COBRANZA IS NULL,'',NUM_POLIZA_COBRANZA)                             AS NUM_POLIZA_COBRANZA,
    IF(CVE_CLIENTE_ORIGEN IS NULL,'',TRIM(CVE_CLIENTE_ORIGEN))                         AS CVE_ASEGURADO,
    'INFO'                                                                             AS CVE_SISTEMA_INT,
    'INFO'                                                                             AS CVE_SISTEMA,
    IF(IND_CAMBIO_GPO_INDIVIDUAL IS NULL,'',IND_CAMBIO_GPO_INDIVIDUAL)                 AS CVE_CAMBIO_GPO_INDIVIDUAL,
    IF(CAMBIO_GPO_INDIVIDUAL IS NULL,'',CAMBIO_GPO_INDIVIDUAL)                         AS DES_CAMBIO_GPO_INDIVIDUAL,
    CAST(IF(IND_OBJETO IS NULL,0,CAST(IND_OBJETO AS INT)) AS INT)                                   AS ID_SECUENCIA_ASEGURADO,
    IF(CVE_ZONA_TARIFICACION IS NULL,'',CVE_ZONA_TARIFICACION)                         AS CVE_ZONA_TARIFICACION,
    IF(ZONA_TARIFICACION IS NULL,'',ZONA_TARIFICACION)                                 AS DES_ZONA_TARIFICACION,
    IF(CODIGO_POSTAL_TARIFICACION IS NULL,'',CODIGO_POSTAL_TARIFICACION)               AS CODIGO_POSTAL_TARIFICACION,
    IF(POBLACION IS NULL,'',POBLACION)                                                 AS NOMBRE_POBLACION,
    IF(ESTADO IS NULL,'',ESTADO)                                                       AS NOMBRE_ESTADO,
    IF(APELLIDO_PATERNO IS NULL,'',APELLIDO_PATERNO)                                   AS APELLIDO_PATERNO_ASEGURADO,
    IF(APELLIDO_MATERNO IS NULL,'',APELLIDO_MATERNO)                                   AS APELLIDO_MATERNO_ASEGURADO,
    IF(NOMBRE IS NULL,'',NOMBRE)                                                       AS NOMBRE_ASEGURADO,
    IF(BAN_RIESGO_SELECTO IS NULL,'',BAN_RIESGO_SELECTO)                               AS BAN_RIESGO_SELECTO,
    IF(CVE_PERIODO_RIESGO_SELECTO IS NULL,'',CVE_PERIODO_RIESGO_SELECTO)               AS CVE_PERIODO_RIESGO_SELECTO,
    IF(PERIODO_RIESGO_SELECTO IS NULL,'',PERIODO_RIESGO_SELECTO)                       AS DES_PERIODO_RIESGO_SELECTO,
    IF(FCH_ALTA IS NULL or FCH_ALTA=0, cast('1400-01-01 00:00:00' as timestamp),cast( concat(substr(cast(FCH_ALTA as string),1,4),"-",substr(cast(FCH_ALTA as string),5,2),"-",substr(cast(FCH_ALTA as string),7,2)) as timestamp)) AS FCH_ALTA, 
    IF(FCH_BAJA IS NULL or FCH_BAJA=0, cast('1400-01-01 00:00:00' as timestamp),cast( concat(substr(cast(FCH_BAJA as string),1,4),"-",substr(cast(FCH_BAJA as string),5,2),"-",substr(cast(FCH_BAJA as string),7,2)) as timestamp)) AS FCH_BAJA, 
    FCH_INI_MOVIMIENTO,
    FCH_FIN_MOVIMIENTO,
    IF(FCH_ANTIGUEDAD IS NULL or FCH_ANTIGUEDAD=0, cast('1400-01-01 00:00:00' as timestamp), cast( concat(substr(cast(FCH_ANTIGUEDAD as string),1,4),"-",substr(cast(FCH_ANTIGUEDAD as string),5,2),"-",substr(cast(FCH_ANTIGUEDAD as string),7,2) ) as timestamp) ) AS FCH_ANTIGUEDAD,  
    IF(FCH_ANTIGUEDAD_NAC IS NULL or FCH_ANTIGUEDAD_NAC=0 , cast('1400-01-01 00:00:00' as timestamp),cast( concat(substr(cast(FCH_ANTIGUEDAD_NAC as string),1,4),"-",substr(cast(FCH_ANTIGUEDAD_NAC as string),5,2),"-",substr(cast(FCH_ANTIGUEDAD_NAC as string),7,2) ) as timestamp) ) AS FCH_ANTIGUEDAD_NACIONAL,
    IF(FCH_ANTIGUEDAD_INT IS NULL or FCH_ANTIGUEDAD_INT=0, cast('1400-01-01 00:00:00' as timestamp),cast( concat(substr(cast(FCH_ANTIGUEDAD_INT as string),1,4),"-",substr(cast(FCH_ANTIGUEDAD_INT as string),5,2),"-",substr(cast(FCH_ANTIGUEDAD_INT as string),7,2) ) as timestamp)  ) AS FCH_ANTIGUEDAD_INTERNACIONAL, 
    IF(FCH_ANTIGUEDAD_NAC_OT_CIA IS NULL or FCH_ANTIGUEDAD_NAC_OT_CIA=0, cast('1400-01-01 00:00:00' as timestamp), cast( concat(substr(cast(FCH_ANTIGUEDAD_NAC_OT_CIA as string),1,4),"-",substr(cast(FCH_ANTIGUEDAD_NAC_OT_CIA as string),5,2),"-",substr(cast(FCH_ANTIGUEDAD_NAC_OT_CIA as string),7,2) ) as timestamp)   ) AS FCH_ANTIGUEDAD_NACIONAL_OTRA_CIA,
    IF(FCH_ANTIGUEDAD_INT_OT_CIA IS NULL or FCH_ANTIGUEDAD_INT_OT_CIA=0, cast('1400-01-01 00:00:00' as timestamp),cast( concat(substr(cast(FCH_ANTIGUEDAD_INT_OT_CIA as string),1,4),"-",substr(cast(FCH_ANTIGUEDAD_INT_OT_CIA as string),5,2),"-",substr(cast(FCH_ANTIGUEDAD_INT_OT_CIA as string),7,2) ) as timestamp)   ) AS FCH_ANTIGUEDAD_INTERNAL_OTRA_CIA,  
    IF(FCH_NACIMIENTO IS NULL or FCH_NACIMIENTO=0, cast('1400-01-01 00:00:00' as timestamp),cast( concat(substr(cast(FCH_NACIMIENTO as string),1,4),"-",substr(cast(FCH_NACIMIENTO as string),5,2),"-",substr(cast(FCH_NACIMIENTO as string),7,2) ) as timestamp)   )AS FCH_NACIMIENTO,
    CAST(IF(CVE_SEXO IS NULL,'',CVE_SEXO) AS VARCHAR(1))                               AS CVE_SEXO,
    IF(trim(DES_SEXO) is NULL ,'NO DEFINIDO',IF(trim(DES_SEXO) = '','NO DEFINIDO' ,DES_SEXO))                AS DES_SEXO,
    CAST(IF(CVE_ESTATUS_OBJETO IS NULL,'',CVE_ESTATUS_OBJETO) AS VARCHAR(1))        AS CVE_ESTATUS_ASEGURADO,
    IF(ESTATUS_OBJETO IS NULL,'', ESTATUS_OBJETO)                                            AS DES_ESTATUS_ASEGURADO,
    CAST(IF(IND_ULT_SITUACION IS NULL,'',IND_ULT_SITUACION) AS CHAR(1))                AS CVE_ULTIMA_SITUACION,
    NVL(DES_ULTIMA_SITUACION,'' )                                                      AS DES_ULTIMA_SITUACION,
    IF(IND_TITULAR_DEPENDIENTE IS NULL,'',IND_TITULAR_DEPENDIENTE)                     AS CVE_TITULAR_DEPENDIENTE,
    IF(TITULAR_DEPENDIENTE IS NULL,'',TITULAR_DEPENDIENTE)                             AS DES_TITULAR_DEPENDIENTE,
    IF(IND_RELACION IS NULL,'',IND_RELACION)                                           AS CVE_PARENTESCO,
    IF(RELACION IS NULL,'',RELACION)                                                   AS DES_PARENTESCO,
    IF(IND_GRATUIDAD IS NULL,'',IND_GRATUIDAD)                                         AS CVE_GRATUIDAD_ASEGURADO,
    IF(GRATUIDAD IS NULL,'',GRATUIDAD)                                                 AS DES_GRATUIDAD_ASEGURADO,
    IF(EDAD_EQUIVALENTE  IS NULL,-999,EDAD_EQUIVALENTE)                   AS EDAD_EQUIVALENTE,
    IF(CVE_ESTADO_CIVIL IS NULL,'',CVE_ESTADO_CIVIL)                                   AS CVE_ESTADO_CIVIL,
    IF(ESTADO_CIVIL IS NULL,'',ESTADO_CIVIL)                                           AS DES_ESTADO_CIVIL,
    IF( PCT_EXTRAPRIMA_AUT_DEPORTE  IS NULL,NULL,PCT_EXTRAPRIMA_AUT_DEPORTE)                                       AS PCT_EXTRAPRIMA_AUT_DEPORTE,
    IF( PCT_EXTRAPRIMA_MAN_DEPORTE  IS NULL,NULL,PCT_EXTRAPRIMA_MAN_DEPORTE)                                       AS PCT_EXTRAPRIMA_MAN_DEPORTE,
    IF( PCT_EXTRAPRIMA_MAN_LAB_DEP  IS NULL,NULL, PCT_EXTRAPRIMA_MAN_LAB_DEP)                                       AS PCT_EXTRAPRIMA_MAN_LAB_DEP,
    IF( PCT_EXTRAPRIMA_AUT_LABORAL  IS NULL,NULL,PCT_EXTRAPRIMA_AUT_LABORAL )                                       AS PCT_EXTRAPRIMA_AUT_LABORAL,
    IF( PCT_EXTRAPRIMA_MAN_LABORAL  IS NULL,NULL, PCT_EXTRAPRIMA_MAN_LABORAL)                                       AS PCT_EXTRAPRIMA_MAN_LABORAL,
    IF(PCT_EXTRAPRIMA_AUT_OBESIDAD  IS NULL, NULL,PCT_EXTRAPRIMA_AUT_OBESIDAD)  AS PCT_EXTRAPRIMA_AUT_OBESIDAD,
    IF(PCT_EXTRAPRIMA_AUT_EDAD IS NULL, NULL,PCT_EXTRAPRIMA_AUT_EDAD)  AS PCT_EXTRAPRIMA_AUT_EDAD,
    IF(CVE_TEMPORALIDAD_EXTRAPRIMA IS NULL,'',CVE_TEMPORALIDAD_EXTRAPRIMA)             AS CVE_TEMPORALIDAD_EXTRAPRIMA,
    IF(TEMPORALIDAD_EXTRAPRIMA IS NULL,'',TEMPORALIDAD_EXTRAPRIMA)                     AS DES_TEMPORALIDAD_EXTRAPRIMA,
     IF( fch_ini_poliza_otra_cia IS NULL or fch_ini_poliza_otra_cia=0 , cast('1400-01-01 00:00:00' as timestamp), cast(from_unixtime( unix_timestamp( cast(fch_ini_poliza_otra_cia as varchar(8)), 'yyyyMMdd')) as timestamp) ) AS fch_ini_poliza_otra_cia,
     IF( FCH_FIN_POLIZA_OTRA_CIA IS NULL or FCH_FIN_POLIZA_OTRA_CIA=0 , cast('1400-01-01 00:00:00' as timestamp), cast(from_unixtime( unix_timestamp( cast(FCH_FIN_POLIZA_OTRA_CIA as varchar(8)), 'yyyyMMdd')) as timestamp) ) AS FCH_FIN_POLIZA_OTRA_CIA,
    IF(NOMBRE_OTRA_CIA IS NULL, '', NOMBRE_OTRA_CIA)                                   AS NOMBRE_OTRA_CIA,
    IF( PESO IS NULL, -999,cast( PESO  as decimal(16,2)) )                                                             AS VAL_PESO,
    IF( TALLA  IS NULL,-999,cast(TALLA  as decimal(16,2)) )                                                            AS VAL_TALLA,
    IF(BAN_IMC IS NULL,'',BAN_IMC)                                                     AS BAN_INDICE_MASA_CORPORAL,
    CAST( PCT_EXTRAPRIMA_AUT AS STRING )                                               AS PCT_EXTRAPRIMA_AUTOMATICA,
    CAST( PCT_EXTRAPRIMA_MAN AS STRING )                                               AS PCT_EXTRAPRIMA_MANUAL,
    IF(CVE_TIPO_DICTAMEN IS NULL,'',CVE_TIPO_DICTAMEN)                                 AS CVE_TIPO_DICTAMEN,
    IF(TIPO_DICTAMEN IS NULL,'',TIPO_DICTAMEN)                                         AS DES_TIPO_DICTAMEN,
    IF(CVE_COBERTURA_CONTABLE IS NULL,'',CVE_COBERTURA_CONTABLE)                       AS CVE_COBERTURA_CONTABLE,
    IF(DES_COBERTURA_CONTABLE IS NULL,'',DES_COBERTURA_CONTABLE)                       AS DES_COBERTURA_CONTABLE,    
    CAST( IF(PCT_DESCUENTO IS NULL, 0, PCT_DESCUENTO)  AS STRING )                     AS PCT_DESCUENTO,
    IF(BAN_DERECHO_POL_MANUAL IS NULL,'',BAN_DERECHO_POL_MANUAL)                       AS BAN_DERECHO_POL_MANUAL,
    CAST( IMP_DERECHO_POL_MANUAL AS STRING )                                           AS IMP_DERECHO_POL_MANUAL,
    IF(RFC_ASEGURADO IS NULL,'',RFC_ASEGURADO)                                         AS RFC,
    IF(BAN_PENALIZACION_HOSP IS NULL,'',BAN_PENALIZACION_HOSP)                         AS BAN_PENALIZACION_HOSP,
    IF(BAN_DEDUCIBLE_ANUAL IS NULL,'',BAN_DEDUCIBLE_ANUAL)                             AS BAN_DEDUCIBLE_ANUAL,
    IF(DEDUCIBLE_ANUAL IS NULL,'',DEDUCIBLE_ANUAL)                                     AS CVE_DEDUCIBLE_ANUAL,    
    IF(VAL_DEDUCIBLE_ANUAL IS NULL,'',VAL_DEDUCIBLE_ANUAL)                             AS DES_DEDUCIBLE_ANUAL,
    NVL(ENTIDAD_FEDERATIVA,'')   AS ENTIDAD_FEDERATIVA  ,
     NVL(MUNICIPIO_DELEGACION,'')   AS MUNICIPIO_DELEGACION  ,
    IF(TOT_ACTIVIDADES_DEPORTIVAS IS NULL,-999,TOT_ACTIVIDADES_DEPORTIVAS)   AS TOT_ACTIVIDADES_DEPORTIVAS  ,
    IF(TOT_ACTIVIDADES_LABORALES IS NULL,-999,TOT_ACTIVIDADES_LABORALES)   AS TOT_ACTIVIDADES_LABORALES  ,
    IF(INDICE_MASA_CORPORAL  IS NULL,-999,INDICE_MASA_CORPORAL)   AS INDICE_MASA_CORPORAL  ,
    NVL(CUENTA_EMAIL,'')   AS CUENTA_EMAIL  ,
    NVL(CURP,'')   AS CURP  ,
    NVL(CVE_NACIONALIDAD,'')   AS CVE_NACIONALIDAD  ,
    NVL(DES_NACIONALIDAD,'')   AS DES_NACIONALIDAD  ,
    NVL(CVE_NACIONALIDAD_RESIDENCIA,'')   AS CVE_NACIONALIDAD_RESIDENCIA  ,
    NVL(DES_NACIONALIDAD_RESIDENCIA,'')   AS DES_NACIONALIDAD_RESIDENCIA  ,
    IF(NUM_EXTERIOR  IS NULL,'-999',NUM_EXTERIOR )   AS NUM_EXTERIOR  ,
    IF(NUM_INTERIOR  IS NULL,'-999',NUM_INTERIOR )   AS NUM_INTERIOR  ,
    NVL(NOMBRE_CALLE,'')   AS NOMBRE_CALLE  ,
    NVL(TIPO_ASENTAMIENTO,'')   AS TIPO_ASENTAMIENTO  ,
    NVL(NOMBRE_COLONIA,'')   AS NOMBRE_COLONIA  ,
    NVL(NUM_TELEFONO_1,'')   AS NUM_TELEFONO_1  ,
    NVL(NUM_TELEFONO_2,'')   AS NUM_TELEFONO_2  ,
    NVL(CODIGO_POSTAL,'')   AS CODIGO_POSTAL  ,
    NVL(CVE_PAIS,'')   AS CVE_PAIS  ,
    NVL(DES_PAIS,'')   AS DES_PAIS  

FROM BDDLTRN.STG_GMM_ASEGURADO_ADICIONALES_COMPLEMENTO;