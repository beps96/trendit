DROP TABLE IF EXISTS BDDLTRN.STG_CNM_CBZA_ASEG_DATOS_ADICIONALES PURGE;

CREATE TABLE IF NOT EXISTS BDDLTRN.STG_CNM_CBZA_ASEG_DATOS_ADICIONALES STORED AS RCFile
 AS
SELECT 
 COMPLEMENTO.NUM_POLIZA,
-- --POLIZA.NUM_POLIZA, --Se asigna valor de poliza original referente a la caratula
     CAST(-999 AS SMALLINT )  AS VIGENCIA_POLIZA,
     CAST(-999 AS SMALLINT )  AS NUM_VERSION_POLIZA,
     COMPLEMENTO.NUM_POLIZA_COBRANZA,
     TRIM(CVE_CLIENTE_ORIGEN) AS CVE_ASEGURADO,
    'AZUL'                    AS CVE_SISTEMA_INT,
    'AZUL'                    AS CVE_SISTEMA,
    ''                        AS CVE_CAMBIO_GPO_INDIVIDUAL,
    ''                        AS DES_CAMBIO_GPO_INDIVIDUAL,
    CAST(-999 AS SMALLINT )   AS ID_SECUENCIA_ASEGURADO,
    ''                        AS CVE_ZONA_TARIFICACION,
    ''                        AS DES_ZONA_TARIFICACION,
    0                         AS CODIGO_POSTAL_TARIFICACION,
    0                         AS NOMBRE_POBLACION,
    0                         AS NOMBRE_ESTADO,
    apellido_paterno          AS APELLIDO_PATERNO_ASEGURADO,
    apellido_materno          AS APELLIDO_MATERNO_ASEGURADO,
    nombre                    AS NOMBRE_ASEGURADO,
    ''                        AS BAN_RIESGO_SELECTO,
    ''                        AS CVE_PERIODO_RIESGO_SELECTO,
    ''                        AS DES_PERIODO_RIESGO_SELECTO,
    IF(FCH_ALTA IS NULL or FCH_ALTA =0, cast('1400-01-01 00:00:00' as timestamp), cast( concat(substr(cast(FCH_ALTA as string),1,4),"-",substr(cast(FCH_ALTA as string),5,2),"-",substr(cast(FCH_ALTA 
 as string),7,2) ) as timestamp) ) as FCH_ALTA,
    IF(FCH_BAJA IS NULL or FCH_BAJA =0, cast('1400-01-01 00:00:00' as timestamp), cast( concat(substr(cast(FCH_BAJA as string),1,4),"-",substr(cast(FCH_BAJA as string),5,2),"-",substr(cast(FCH_BAJA 
 as string),7,2) ) as timestamp) ) as FCH_BAJA,
    cast('1400-01-01 00:00:00' as TIMESTAMP)     AS FCH_INI_MOVIMIENTO,
    cast('1400-01-01 00:00:00' as TIMESTAMP)     AS FCH_FIN_MOVIMIENTO,
    CASE WHEN NOT POLIZA.CVE_PLAN IN ('17','31','33','CON31') 
              THEN --IF(FCH_ANTIGUEDAD_NAC = 0 OR FCH_ANTIGUEDAD_NAC IS NULL ,cast('1400-01-01 00:00:00' as TIMESTAMP),cast(FCH_ANTIGUEDAD_NAC as TIMESTAMP))
                     IF(FCH_ANTIGUEDAD_NAC IS NULL or FCH_ANTIGUEDAD_NAC =0, cast('1400-01-01 00:00:00' as timestamp), cast( concat(substr(cast(FCH_ANTIGUEDAD_NAC as string),1,4),"-",substr(cast(FCH_ANTIGUEDAD_NAC as string),5,2),"-",substr(cast(FCH_ANTIGUEDAD_NAC 
 as string),7,2) ) as timestamp) )
              ELSE --IF(FCH_ANTIGUEDAD_INT = 0 OR FCH_ANTIGUEDAD_INT IS NULL ,cast('1400-01-01 00:00:00' as TIMESTAMP),cast(FCH_ANTIGUEDAD_INT as TIMESTAMP)) 
                    IF(FCH_ANTIGUEDAD_INT IS NULL or FCH_ANTIGUEDAD_INT =0, cast('1400-01-01 00:00:00' as timestamp), cast( concat(substr(cast(FCH_ANTIGUEDAD_INT as string),1,4),"-",substr(cast(FCH_ANTIGUEDAD_INT as string),5,2),"-",substr(cast(FCH_ANTIGUEDAD_INT 
 as string),7,2) ) as timestamp) )
    END                       AS FCH_ANTIGUEDAD,
    IF(FCH_ANTIGUEDAD_NAC IS NULL or FCH_ANTIGUEDAD_NAC =0, cast('1400-01-01 00:00:00' as timestamp), cast( concat(substr(cast(FCH_ANTIGUEDAD_NAC as string),1,4),"-",substr(cast(FCH_ANTIGUEDAD_NAC as string),5,2),"-",substr(cast(FCH_ANTIGUEDAD_NAC as string),7,2) ) as timestamp) ) as FCH_ANTIGUEDAD_NACIONAL,
    IF(FCH_ANTIGUEDAD_INT IS NULL or FCH_ANTIGUEDAD_INT =0, cast('1400-01-01 00:00:00' as timestamp), cast( concat(substr(cast(FCH_ANTIGUEDAD_INT as string),1,4),"-",substr(cast(FCH_ANTIGUEDAD_INT as string),5,2),"-",substr(cast(FCH_ANTIGUEDAD_INT as string),7,2) ) as timestamp) ) as FCH_ANTIGUEDAD_INTERNACIONAL,
        IF(FCH_ANTIGUEDAD_NAC_OT_CIA IS NULL or FCH_ANTIGUEDAD_NAC_OT_CIA =0, cast('1400-01-01 00:00:00' as timestamp), cast( concat(substr(cast(FCH_ANTIGUEDAD_NAC_OT_CIA as string),1,4),"-",substr(cast(FCH_ANTIGUEDAD_NAC_OT_CIA as string),5,2),"-",substr(cast(FCH_ANTIGUEDAD_NAC_OT_CIA as string),7,2) ) as timestamp) )       AS fch_antiguedad_nacional_otra_cia,
    IF(FCH_ANTIGUEDAD_INT_OT_CIA IS NULL or FCH_ANTIGUEDAD_INT_OT_CIA =0, cast('1400-01-01 00:00:00' as timestamp), cast( concat(substr(cast(FCH_ANTIGUEDAD_INT_OT_CIA as string),1,4),"-",substr(cast(FCH_ANTIGUEDAD_INT_OT_CIA as string),5,2),"-",substr(cast(FCH_ANTIGUEDAD_INT_OT_CIA as string),7,2) ) as timestamp) )        AS fch_antiguedad_internal_otra_cia,
    IF(FCH_NACIMIENTO IS NULL or FCH_NACIMIENTO =0, cast('1400-01-01 00:00:00' as timestamp), cast( concat(substr(cast(FCH_NACIMIENTO as string),1,4),"-",substr(cast(FCH_NACIMIENTO as string),5,2),"-",substr(cast(FCH_NACIMIENTO as string),7,2) ) as timestamp) ) AS FCH_NACIMIENTO,
    --CVE_SEXO                    AS CVE_SEXO,
    if(CVE_SEXO is null or trim(CVE_SEXO) = ''  or CVE_SEXO= 'null','' ,CVE_SEXO) AS CVE_SEXO,
    --DES_SEXO                    AS DES_SEXO,
    if(DES_SEXO is null or trim(DES_SEXO) = ''  or DES_SEXO= 'null','sin descripcion' ,DES_SEXO) AS DES_SEXO,
    CVE_ESTATUS_ASEGURADO       AS CVE_ESTATUS_ASEGURADO,
    ''                          AS DES_ESTATUS_ASEGURADO,
    ''                          AS CVE_ULTIMA_SITUACION,
    ''                          AS DES_ULTIMA_SITUACION,
    IND_TITULAR_DEPENDIENTE     AS CVE_TITULAR_DEPENDIENTE,
    TITULAR_DEPENDIENTE         AS DES_TITULAR_DEPENDIENTE,
    IND_RELACION                AS CVE_PARENTESCO,
    RELACION                    AS DES_PARENTESCO,
    ''                          AS  CVE_GRATUIDAD_ASEGURADO,
    ''                          AS  DES_GRATUIDAD_ASEGURADO,
    EDAD_EQUIVALENTE            AS  EDAD_EQUIVALENTE,
    ''                          AS  CVE_ESTADO_CIVIL,
    ''                          AS  DES_ESTADO_CIVIL,
    NULL                    AS  PCT_EXTRAPRIMA_AUT_DEPORTE,
    NULL                        AS  PCT_EXTRAPRIMA_MAN_DEPORTE,
    NULL                        AS  PCT_EXTRAPRIMA_MAN_LAB_DEP,
    NULL                        AS  PCT_EXTRAPRIMA_AUT_LABORAL,
    NULL                  AS  PCT_EXTRAPRIMA_MAN_LABORAL,
    NULL                    AS  PCT_EXTRAPRIMA_AUT_OBESIDAD,
    NULL                        AS  PCT_EXTRAPRIMA_AUT_EDAD,
    ''                          AS  CVE_TEMPORALIDAD_EXTRAPRIMA,
    ''                        AS  DES_TEMPORALIDAD_EXTRAPRIMA,
    cast('1400-01-01 00:00:00' as TIMESTAMP)       AS FCH_INI_POLIZA_OTRA_CIA,
    cast('1400-01-01 00:00:00' as TIMESTAMP)       AS FCH_FIN_POLIZA_OTRA_CIA,
    ''                          AS  NOMBRE_OTRA_CIA,
    -999                        AS  VAL_PESO,
    -999                        AS  VAL_TALLA,
    ''                          AS  BAN_INDICE_MASA_CORPORAL,
    NULL                        AS  PCT_EXTRAPRIMA_AUTOMATICA,
    NULL                        AS  PCT_EXTRAPRIMA_MANUAL,
    ''                        AS  CVE_TIPO_DICTAMEN,
    ''                          AS  DES_TIPO_DICTAMEN,
    POLIZA.CVE_COBERTURA_CONTABLE     AS  CVE_COBERTURA_CONTABLE,
    DES_COBERTURA_CONTABLE      AS  DES_COBERTURA_CONTABLE, --PENDIENTE
    NULL                        AS  PCT_DESCUENTO,
    ''                      AS  BAN_DERECHO_POL_MANUAL,
    NULL                        AS  IMP_DERECHO_POL_MANUAL,
    NVL(RFC,'')                         AS  RFC,
    ''                          AS  BAN_PENALIZACION_HOSP,
    ''                    AS  BAN_DEDUCIBLE_ANUAL,
    ''                    AS  CVE_DEDUCIBLE_ANUAL,
    ''                    AS  DES_DEDUCIBLE_ANUAL,
    ''                          AS  ENTIDAD_FEDERATIVA,
    ''                          AS  MUNICIPIO_DELEGACION,
    -999                        AS  TOT_ACTIVIDADES_DEPORTIVAS,
    -999                        AS  TOT_ACTIVIDADES_LABORALES,
    -999                        AS  INDICE_MASA_CORPORAL,
    ''                          AS  CUENTA_EMAIL,
    ''                          AS  CURP,
    ''                          AS  CVE_NACIONALIDAD,
    ''                          AS  DES_NACIONALIDAD,
    ''                          AS  CVE_NACIONALIDAD_RESIDENCIA,
    ''                          AS  DES_NACIONALIDAD_RESIDENCIA,
    '-999'                        AS  NUM_EXTERIOR,
    '-999'                        AS  NUM_INTERIOR,
    ''                          AS  NOMBRE_CALLE,
    ''                          AS  TIPO_ASENTAMIENTO,
    ''                          AS  NOMBRE_COLONIA,
    ''                          AS  NUM_TELEFONO_1,
    ''                          AS  NUM_TELEFONO_2,
    CODIGO_POSTAL               AS  CODIGO_POSTAL,
    ''                          AS  CVE_PAIS,
    ''                          AS  DES_PAIS
    
 FROM  ( 
        SELECT *
         FROM BDDLAPR.CNM_POLIZA_AZUL
       WHERE CVE_SISTEMA_INT='AZUL'
         ) as POLIZA 
 INNER JOIN
     BDDLTRN.STG_GMM_ASEG_AD_CBZA_FILTER AS COMPLEMENTO 
     on      trim(POLIZA.NUM_POLIZA_COBRANZA) = trim(COMPLEMENTO.NUM_POLIZA_COBRANZA);