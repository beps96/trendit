--CATALOGO GENERO
DROP TABLE IF EXISTS BDDLTRN.STG_GMM_ASEG_CBZA_CATALOG PURGE;
CREATE TABLE IF NOT EXISTS BDDLTRN.STG_GMM_ASEG_CBZA_CATALOG AS
SELECT FILTER.NUM_POLIZA, FILTER.NUM_POLIZA_COBRANZA, FILTER.CVE_CLIENTE_ORIGEN, FILTER.APELLIDO_PATERNO, FILTER.APELLIDO_MATERNO, FILTER.NOMBRE,
FILTER.FCH_ALTA, FILTER.FCH_BAJA, FILTER.FCH_ANTIGUEDAD_NAC,FILTER.FCH_ANTIGUEDAD_INT, FILTER.FCH_NACIMIENTO, FILTER.CVE_SEXO, RFC ,DES_SEXO,CODIGO_POSTAL, EDAD_EQUIVALENTE,FCH_ANTIGUEDAD_INT_OT_CIA,FCH_ANTIGUEDAD_NAC_OT_CIA,
CASE 
   	WHEN FILTER.CVE_SEXO = ('F')
   	THEN 'FEMENINO'
   	WHEN FILTER.CVE_SEXO = 'M'
   	THEN 'MASCULINO'
   	ELSE ''	
END	AS SEXO,
FILTER.CVE_ESTATUS AS CVE_ESTATUS_ASEGURADO,
CASE 
   	WHEN FILTER.CVE_ESTATUS IN ('V', 'R', 'A', '')
   	THEN 'VIGENTE'
   	WHEN FILTER.CVE_ESTATUS= 'B'
   	THEN 'BAJA'
   	ELSE ''	
END	AS ESTATUS_ASEGURADO,
FILTER.end_fecha_ini,
FILTER.end_status, FILTER.CVE_COBERTURA_CONTABLE,DES_COBERTURA_CONTABLE, split(FILTER.ind_titular_dependiente,'-') as titular_dependienteMap, FILTER.ind_relacion, FILTER.relacion
FROM BDDLTRN.STG_GMM_ASEG_AD_CBZA_RELACION FILTER;

-- SE TIENE EL ULTIMO REGISTRO MODIFICADO APARTIR DEL CAMPO end_fecha_ini
DROP TABLE IF EXISTS BDDLTRN.STG_GMM_ASEG_AD_CBZA_FILTER PURGE;
CREATE TABLE IF NOT EXISTS BDDLTRN.STG_GMM_ASEG_AD_CBZA_FILTER AS
SELECT FILTER.NUM_POLIZA, FILTER.NUM_POLIZA_COBRANZA, FILTER.CVE_CLIENTE_ORIGEN, FILTER.APELLIDO_PATERNO, FILTER.APELLIDO_MATERNO, FILTER.NOMBRE,
FILTER.FCH_ALTA, FILTER.FCH_BAJA, FILTER.FCH_ANTIGUEDAD_NAC,FILTER.FCH_ANTIGUEDAD_INT, FILTER.FCH_NACIMIENTO, FILTER.CVE_SEXO,  RFC ,DES_SEXO,CODIGO_POSTAL, EDAD_EQUIVALENTE,FCH_ANTIGUEDAD_INT_OT_CIA,FCH_ANTIGUEDAD_NAC_OT_CIA,
FILTER.SEXO , CVE_ESTATUS_ASEGURADO, ESTATUS_ASEGURADO, FILTER.end_fecha_ini,
FILTER.end_status, FILTER.CVE_COBERTURA_CONTABLE, 
titular_dependienteMap[0] as ind_titular_dependiente,
titular_dependienteMap[1] as titular_dependiente,
 FILTER.ind_relacion, FILTER.relacion
FROM BDDLTRN.STG_GMM_ASEG_CBZA_CATALOG FILTER
    INNER JOIN (
           SELECT NUM_POLIZA,NUM_POLIZA_COBRANZA,CVE_CLIENTE_ORIGEN,MAX(END_FECHA_INI)AS END_FECHA_INI,MAX(FCH_NACIMIENTO) AS FCH_NACIMIENTO
           FROM BDDLTRN.STG_GMM_ASEG_CBZA_CATALOG 
           GROUP BY NUM_POLIZA,NUM_POLIZA_COBRANZA,CVE_CLIENTE_ORIGEN) CBZA_ULTIMO
    ON CBZA_ULTIMO.NUM_POLIZA=FILTER.NUM_POLIZA
    AND CBZA_ULTIMO.NUM_POLIZA_COBRANZA=FILTER.NUM_POLIZA_COBRANZA
    AND CBZA_ULTIMO.CVE_CLIENTE_ORIGEN=FILTER.CVE_CLIENTE_ORIGEN
    AND CBZA_ULTIMO.END_FECHA_INI =FILTER.END_FECHA_INI
	AND CBZA_ULTIMO.FCH_NACIMIENTO=FILTER.FCH_NACIMIENTO;


