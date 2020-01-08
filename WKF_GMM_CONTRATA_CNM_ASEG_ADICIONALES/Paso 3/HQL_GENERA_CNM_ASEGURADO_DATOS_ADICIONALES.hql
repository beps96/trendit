--Creando Tabla con union de Datos
DROP TABLE IF EXISTS BDDLTRN.STG_GMM_UNION_ASEG_AD;

 CREATE TABLE IF NOT EXISTS BDDLTRN.STG_GMM_UNION_ASEG_AD STORED AS RCFile AS
 SELECT distinct *  FROM  BDDLTRN.STG_CNM_INFO_ASEG_DATOS_ADICIONALES  
 UNION ALL
 SELECT distinct  *  FROM BDDLTRN.STG_CNM_CBZA_ASEG_DATOS_ADICIONALES;


--Inserta registros
DROP TABLE IF EXISTS BDDLTRN.STG_ASEGURADO_DATOS_ADICIONALES PURGE;


CREATE TABLE IF NOT EXISTS BDDLTRN.STG_ASEGURADO_DATOS_ADICIONALES STORED AS RCFile AS
SELECT 
    NUM_POLIZA, 
    VIGENCIA_POLIZA, 
    NUM_VERSION_POLIZA, 
    NUM_POLIZA_COBRANZA, 
    CVE_ASEGURADO, 
    CVE_SISTEMA_INT, 
    CVE_SISTEMA, 
    CVE_CAMBIO_GPO_INDIVIDUAL, 
    DES_CAMBIO_GPO_INDIVIDUAL, 
    ID_SECUENCIA_ASEGURADO, 
    CVE_ZONA_TARIFICACION, 
    DES_ZONA_TARIFICACION, 
    CODIGO_POSTAL_TARIFICACION, 
    NOMBRE_POBLACION, 
    NOMBRE_ESTADO, 
    APELLIDO_PATERNO_ASEGURADO, 
    APELLIDO_MATERNO_ASEGURADO, 
    NOMBRE_ASEGURADO, 
    BAN_RIESGO_SELECTO, 
    CVE_PERIODO_RIESGO_SELECTO, 
    DES_PERIODO_RIESGO_SELECTO, 
    FCH_ALTA, 
    FCH_BAJA, 
    FCH_INI_MOVIMIENTO, 
    FCH_FIN_MOVIMIENTO, 
    FCH_ANTIGUEDAD, 
    FCH_ANTIGUEDAD_NACIONAL, 
    FCH_ANTIGUEDAD_INTERNACIONAL, 
    FCH_ANTIGUEDAD_NACIONAL_OTRA_CIA, 
    FCH_ANTIGUEDAD_INTERNAL_OTRA_CIA, 
    FCH_NACIMIENTO, 
    CVE_SEXO, 
    DES_SEXO, 
    CVE_ESTATUS_ASEGURADO, 
    DES_ESTATUS_ASEGURADO, 
    CVE_ULTIMA_SITUACION, 
    DES_ULTIMA_SITUACION, 
    CVE_TITULAR_DEPENDIENTE, 
    DES_TITULAR_DEPENDIENTE, 
    CVE_PARENTESCO, 
    DES_PARENTESCO, 
    CVE_GRATUIDAD_ASEGURADO, 
    DES_GRATUIDAD_ASEGURADO, 
    EDAD_EQUIVALENTE, 
    CVE_ESTADO_CIVIL, 
    DES_ESTADO_CIVIL, 
    PCT_EXTRAPRIMA_AUT_DEPORTE, 
    PCT_EXTRAPRIMA_MAN_DEPORTE, 
    PCT_EXTRAPRIMA_MAN_LAB_DEP, 
    PCT_EXTRAPRIMA_AUT_LABORAL, 
    PCT_EXTRAPRIMA_MAN_LABORAL, 
    PCT_EXTRAPRIMA_AUT_OBESIDAD, 
    PCT_EXTRAPRIMA_AUT_EDAD, 
    CVE_TEMPORALIDAD_EXTRAPRIMA, 
    DES_TEMPORALIDAD_EXTRAPRIMA, 
    FCH_INI_POLIZA_OTRA_CIA, 
    FCH_FIN_POLIZA_OTRA_CIA, 
    NOMBRE_OTRA_CIA, 
    VAL_PESO, 
    VAL_TALLA, 
    BAN_INDICE_MASA_CORPORAL, 
    PCT_EXTRAPRIMA_AUTOMATICA, 
    PCT_EXTRAPRIMA_MANUAL, 
    CVE_TIPO_DICTAMEN, 
    DES_TIPO_DICTAMEN, 
    CVE_COBERTURA_CONTABLE, 
    DES_COBERTURA_CONTABLE, 
    PCT_DESCUENTO, 
    BAN_DERECHO_POL_MANUAL, 
    IMP_DERECHO_POL_MANUAL, 
    RFC, 
    BAN_PENALIZACION_HOSP, 
    BAN_DEDUCIBLE_ANUAL, 
    CVE_DEDUCIBLE_ANUAL, 
    DES_DEDUCIBLE_ANUAL, 
    ENTIDAD_FEDERATIVA, 
    MUNICIPIO_DELEGACION, 
    TOT_ACTIVIDADES_DEPORTIVAS, 
    TOT_ACTIVIDADES_LABORALES, 
    INDICE_MASA_CORPORAL, 
    CUENTA_EMAIL, 
    CURP, 
    CVE_NACIONALIDAD, 
    DES_NACIONALIDAD, 
    CVE_NACIONALIDAD_RESIDENCIA, 
    DES_NACIONALIDAD_RESIDENCIA, 
    NUM_EXTERIOR, 
    NUM_INTERIOR, 
    NOMBRE_CALLE, 
    TIPO_ASENTAMIENTO, 
    NOMBRE_COLONIA, 
    NUM_TELEFONO_1, 
    NUM_TELEFONO_2, 
    CODIGO_POSTAL, 
    CVE_PAIS, 
    DES_PAIS, 
 	FROM_UNIXTIME(UNIX_TIMESTAMP())  AS TS_ALTA_HDFS
--  	CASE WHEN CVE_SISTEMA = 'INFO' THEN 
--  	                                     CAST(reflect('org.apache.commons.codec.digest.DigestUtils','md5Hex',CONCAT(NUM_POLIZA,"-", CAST(VIGENCIA_POLIZA AS STRING),"-",CAST(NUM_VERSION_POLIZA AS STRING),"-", NUM_POLIZA_COBRANZA,"-", CAST(ID_SECUENCIA_ASEGURADO AS STRING))) AS VARCHAR(32)) 
--  	                                ELSE CAST(reflect('org.apache.commons.codec.digest.DigestUtils','md5Hex',CONCAT(NUM_POLIZA,"-", CAST(VIGENCIA_POLIZA AS STRING),"-",CAST(NUM_VERSION_POLIZA AS STRING),"-", NUM_POLIZA_COBRANZA,"-", CAST(ID_SECUENCIA_ASEGURADO AS STRING),"-", ID_SECUENCIA_ASEGURADO )) AS VARCHAR(32)) 
--  	                                END AS IDMD5,
--  	CAST(reflect('org.apache.commons.codec.digest.DigestUtils','md5Hex',CONCAT(NUM_POLIZA,cast(VIGENCIA_POLIZA  as string),"-",cast(NUM_VERSION_POLIZA  as string),"-",cast(NUM_POLIZA_COBRANZA  as string),"-",cast(CVE_ASEGURADO  as string),"-",cast(CVE_SISTEMA_INT  as string),"-",cast(CVE_SISTEMA  as string),"-",cast(CVE_CAMBIO_GPO_INDIVIDUAL  as string),"-",cast(DES_CAMBIO_GPO_INDIVIDUAL  as string),"-",cast(ID_SECUENCIA_ASEGURADO  as string),"-",cast(CVE_ZONA_TARIFICACION  as    string)  ,  "-"  ,cast(DES_ZONA_TARIFICACION  as    string)  ,  "-"  ,cast(CODIGO_POSTAL_TARIFICACION  as    string)  ,  "-"  ,cast(NOMBRE_POBLACION  as    string)  ,  "-"  ,cast(NOMBRE_ESTADO  as    string)  ,  "-"  ,cast(APELLIDO_PATERNO_ASEGURADO  as    string)  ,  "-"  ,cast(APELLIDO_MATERNO_ASEGURADO  as    string)  ,  "-"  ,cast(NOMBRE_ASEGURADO  as    string)  ,  "-"  ,cast(BAN_RIESGO_SELECTO  as    string)  ,  "-"  ,cast(CVE_PERIODO_RIESGO_SELECTO  as    string)  ,  "-"  ,cast(DES_PERIODO_RIESGO_SELECTO  as    string)  ,  "-"  ,cast(CVE_SEXO  as    string)  ,  "-"  ,cast(DES_SEXO  as    string)  ,  "-"  ,cast(CVE_ESTATUS_ASEGURADO  as    string)  ,  "-"  ,cast(DES_ESTATUS_ASEGURADO  as    string)  ,  "-"  ,cast(CVE_ULTIMA_SITUACION  as    string)  ,  "-"  ,cast(DES_ULTIMA_SITUACION  as    string)  ,  "-"  ,cast(CVE_TITULAR_DEPENDIENTE  as    string)  ,  "-"  ,cast(DES_TITULAR_DEPENDIENTE  as    string)  ,  "-"  ,cast(CVE_PARENTESCO  as    string)  ,  "-"  ,cast(DES_PARENTESCO  as    string)  ,  "-"  ,cast(CVE_GRATUIDAD_ASEGURADO  as    string)  ,  "-"  ,cast(DES_GRATUIDAD_ASEGURADO  as    string)  ,  "-"  ,cast(EDAD_EQUIVALENTE  as    string)  ,  "-"  ,cast(CVE_ESTADO_CIVIL  as    string)  ,  "-"  ,cast(DES_ESTADO_CIVIL  as    string)  ,  "-"  ,cast(PCT_EXTRAPRIMA_AUT_DEPORTE  as    string)  ,  "-"  ,cast(PCT_EXTRAPRIMA_MAN_DEPORTE  as    string)  ,  "-"  ,cast(PCT_EXTRAPRIMA_MAN_LAB_DEP  as    string)  ,  "-"  ,cast(PCT_EXTRAPRIMA_AUT_LABORAL  as    string)  ,  "-"  ,cast(PCT_EXTRAPRIMA_MAN_LABORAL  as    string)  ,  "-"  ,cast(PCT_EXTRAPRIMA_AUT_OBESIDAD  as    string)  ,  "-"  ,cast(PCT_EXTRAPRIMA_AUT_EDAD  as    string)  ,  "-"  ,cast(CVE_TEMPORALIDAD_EXTRAPRIMA  as    string)  ,  "-"  ,cast(DES_TEMPORALIDAD_EXTRAPRIMA  as    string)  ,  "-"  ,cast(NOMBRE_OTRA_CIA  as    string)  ,  "-"  ,cast(VAL_PESO  as    string)  ,  "-"  ,cast(VAL_TALLA  as    string)  ,  "-"  ,cast(BAN_INDICE_MASA_CORPORAL  as    string)  ,  "-"  ,cast(PCT_EXTRAPRIMA_AUTOMATICA  as    string)  ,  "-"  ,cast(PCT_EXTRAPRIMA_MANUAL  as    string)  ,  "-"  ,cast(CVE_TIPO_DICTAMEN  as    string)  ,  "-"  ,cast(DES_TIPO_DICTAMEN  as    string)  ,  "-"  ,cast(CVE_COBERTURA_CONTABLE  as    string)  ,  "-"  ,cast(DES_COBERTURA_CONTABLE  as    string)  ,  "-"  ,cast(PCT_DESCUENTO  as    string)  ,  "-"  ,cast(BAN_DERECHO_POL_MANUAL  as    string)  ,  "-"  ,cast(IMP_DERECHO_POL_MANUAL  as    string)  ,  "-"  ,cast(RFC  as    string)  ,  "-"  ,cast(BAN_PENALIZACION_HOSP  as    string)  ,  "-"  ,cast(BAN_DEDUCIBLE_ANUAL  as    string)  ,  "-"  ,cast(CVE_DEDUCIBLE_ANUAL  as    string)  ,  "-"  ,cast(DES_DEDUCIBLE_ANUAL  as    string)  ,  "-"  ,cast(ENTIDAD_FEDERATIVA  as    string)  ,  "-"  ,cast(MUNICIPIO_DELEGACION  as    string)  ,  "-"  ,cast(TOT_ACTIVIDADES_DEPORTIVAS  as    string)  ,  "-"  ,cast(TOT_ACTIVIDADES_LABORALES  as    string)  ,  "-"  ,cast(INDICE_MASA_CORPORAL  as    string)  ,  "-"  ,cast(CUENTA_EMAIL  as    string)  ,  "-"  ,cast(CURP  as    string)  ,  "-"  ,cast(CVE_NACIONALIDAD  as    string)  ,  "-"  ,cast(DES_NACIONALIDAD  as    string)  ,  "-"  ,cast(CVE_NACIONALIDAD_RESIDENCIA  as    string)  ,  "-"  ,cast(DES_NACIONALIDAD_RESIDENCIA  as    string)  ,  "-"  ,cast(NUM_EXTERIOR  as    string)  ,  "-"  ,cast(NUM_INTERIOR  as    string)  ,  "-"  ,cast(NOMBRE_CALLE  as    string)  ,  "-"  ,cast(TIPO_ASENTAMIENTO  as    string)  ,  "-"  ,cast(NOMBRE_COLONIA  as    string)  ,  "-"  ,cast(NUM_TELEFONO_1  as    string)  ,  "-"  ,cast(NUM_TELEFONO_2  as    string)  ,  "-"  ,cast(CODIGO_POSTAL  as    string)  ,  "-"  ,cast(CVE_PAIS  as    string)  ,  "-"  ,cast(DES_PAIS  as string)   )
--  	)AS VARCHAR(32) ) AS IDMD5COMPLETO
FROM BDDLTRN.STG_GMM_UNION_ASEG_AD;