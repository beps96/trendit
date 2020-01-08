CREATE TABLE IF NOT EXISTS BDDLTRN.STG_ALM_POLIZA_DATOS_ADICIONALES_DELTA (
	NUM_POLIZA VARCHAR(14),
	VIGENCIA_POLIZA SMALLINT,
	NUM_VERSION_POLIZA SMALLINT,
	NUM_POLIZA_COBRANZA VARCHAR(8),
	CVE_SISTEMA VARCHAR(4),
	CVE_SISTEMA_INT VARCHAR(4),
	CVE_CIRCULO_MEDICO_CONTRATADO VARCHAR(10),
	DES_CIRCULO_MEDICO_CONTRATADO VARCHAR(100),
	CVE_SEGMENTO_PLAN VARCHAR(5),
	DES_SEGMENTO_PLAN VARCHAR(100),
	CVE_INDICADOR_CAMBIO_GPO_IND VARCHAR(5),
	DES_INDICADOR_CAMBIO_GPO_IND VARCHAR(100),
	NUM_POLIZA_ANTERIOR VARCHAR(14),
	CVE_ESQUEMA VARCHAR(5),
	DES_ESQUEMA VARCHAR(50),
	CVE_EXPERIENCIA VARCHAR(5),
	DES_EXPERIENCIA VARCHAR(50),
	CVE_TIPO_DEDUCIBLE VARCHAR(5),
	DES_TIPO_DEDUCIBLE VARCHAR(100),
	CVE_DESCUENTO VARCHAR(5),
	DES_DESCUENTO VARCHAR(100),
	PCT_DESCUENTO DECIMAL(16,4),
	PCT_DESCUENTO_COMERCIAL DECIMAL(16,4),
	BAN_POLIZA_CASH_FLOW VARCHAR(2),
	NUM_POLIZA_CASH_FLOW VARCHAR(14),
	NUM_POLIZA_FONDO_ESPECIAL VARCHAR(14),
	NUM_POLIZA_STOP_LOSS VARCHAR(14),
	CVE_FONDO_ADMON_PERDIDA VARCHAR(5),
	DES_FONDO_ADMON_PERDIDA VARCHAR(100),
	CVE_TIPO_NEGOCIO VARCHAR(5),
	DES_TIPO_NEGOCIO VARCHAR(50),
	BAN_CEE VARCHAR(2) ,
	BAN_CECE VARCHAR(2),
	BAN_SALUD_FAMILIAR VARCHAR(2),
	ANIOS_SALUD_FAMILIAR_RESTANTES SMALLINT,
	NUM_POLIZA_PROVIENE VARCHAR(100),
	BAN_NUEVA_FORMA_PAGO VARCHAR(2),
	CVE_TIPO_SUMA_ASEGURADA VARCHAR(5),
	DES_TIPO_SUMA_ASEGURADA VARCHAR(100),
	BAN_CAMI VARCHAR(2),
	BAN_CDA VARCHAR(2),
	BAN_CAHL VARCHAR(2),
	BAN_CAHD VARCHAR(2),
	BAN_CECN VARCHAR(2),
	BAN_RN VARCHAR(2),
        BAN_COASEGURO_CERO VARCHAR(2),
	BAN_SOLIDEZ_FAMILIAR VARCHAR(2),
        NUM_ASEG_DEPENDIENTES SMALLINT,
        NUM_ASEG_TITULARES SMALLINT,
	CVE_ADMON_POLIZA VARCHAR(5),
	DES_ADMON_POLIZA VARCHAR(100),
	BAN_ELEGIBILIDAD VARCHAR(2),
	ANIOS_PAGO_COMISION SMALLINT,
	FCH_REGISTRO_PRODUCTO TIMESTAMP,
	NUM_REGISTRO_PRODUCTO VARCHAR(100),
	CVE_CANAL_COBRO_INICIAL VARCHAR(5),
	DES_CANAL_COBRO_INICIAL VARCHAR(100),
	CVE_CANAL_COBRO_SUCESIVO VARCHAR(5),
	DES_CANAL_COBRO_SUCESIVO VARCHAR(100),
	CVE_INDICADOR_DIVIDENDO VARCHAR(5),
	DES_INDICADOR_DIVIDENDO VARCHAR(50),
	CVE_FORMA_PAGO_DIVIDENDO VARCHAR(5),
	DES_FORMA_PAGO_DIVIDENDO VARCHAR(50),
	PCT_FACTOR_DIVIDENDO DECIMAL(16,4),
	PCT_FACTOR_PRIMA DECIMAL(16,4),
	PCT_FACTOR_COMISION DECIMAL(16,4),
	PCT_FACTOR_SINIESTROS DECIMAL(16,4),
	PCT_FACTOR_GIA DECIMAL(16,4),
	CVE_DURACION VARCHAR(5),
	DES_DURACION VARCHAR(50),
	REGISTRO_PRODUCTO_RECAS VARCHAR(100),
	BAN_RPF_MANUAL VARCHAR(2),
	PCT_RPF_MANUAL DECIMAL(16,4),
	BAN_VOLUNTARIOS VARCHAR(2),
	CVE_INDICADOR_NEGOCIO_LICITADO VARCHAR(5),
	DES_INDICADOR_NEGOCIO_LICITADO VARCHAR(100),
	BAN_AFECTA_PAI VARCHAR(2),
	BAN_NEGOCIO_MULTINACIONAL VARCHAR(2),
	NUM_ASEGURADOS_CONTRATO INT,
	BAN_POLIZA_PRESTACION_EMPLEADO VARCHAR(2),
	CVE_ELEGIBILIDAD VARCHAR(5),
	DES_ELEGIBILIDAD VARCHAR(100),
	BAN_IMPRIME_CONVERSION VARCHAR(2))
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS BDDLTRN.STG_POLIZA_DATOS_ADICIONALES_HASH (
	NUM_POLIZA VARCHAR(14),
	VIGENCIA_POLIZA SMALLINT,
	NUM_VERSION_POLIZA SMALLINT,
	NUM_POLIZA_COBRANZA VARCHAR(8),
	CVE_SISTEMA VARCHAR(4),
	CVE_SISTEMA_INT VARCHAR(4),
	CVE_CIRCULO_MEDICO_CONTRATADO VARCHAR(10),
	DES_CIRCULO_MEDICO_CONTRATADO VARCHAR(100),
	CVE_SEGMENTO_PLAN VARCHAR(5),
	DES_SEGMENTO_PLAN VARCHAR(100),
	CVE_INDICADOR_CAMBIO_GPO_IND VARCHAR(5),
	DES_INDICADOR_CAMBIO_GPO_IND VARCHAR(100),
	NUM_POLIZA_ANTERIOR VARCHAR(14),
	CVE_ESQUEMA VARCHAR(5),
	DES_ESQUEMA VARCHAR(50),
	CVE_EXPERIENCIA VARCHAR(5),
	DES_EXPERIENCIA VARCHAR(50),
	CVE_TIPO_DEDUCIBLE VARCHAR(5),
	DES_TIPO_DEDUCIBLE VARCHAR(100),
	CVE_DESCUENTO VARCHAR(5),
	DES_DESCUENTO VARCHAR(100),
	PCT_DESCUENTO DECIMAL(16,4),
	PCT_DESCUENTO_COMERCIAL DECIMAL(16,4),
	BAN_POLIZA_CASH_FLOW VARCHAR(2),
	NUM_POLIZA_CASH_FLOW VARCHAR(14),
	NUM_POLIZA_FONDO_ESPECIAL VARCHAR(14),
	NUM_POLIZA_STOP_LOSS VARCHAR(14),
	CVE_FONDO_ADMON_PERDIDA VARCHAR(5),
	DES_FONDO_ADMON_PERDIDA VARCHAR(100),
	CVE_TIPO_NEGOCIO VARCHAR(5),
	DES_TIPO_NEGOCIO VARCHAR(50),
	BAN_CEE VARCHAR(2) ,
	BAN_CECE VARCHAR(2),
	BAN_SALUD_FAMILIAR VARCHAR(2),
	ANIOS_SALUD_FAMILIAR_RESTANTES SMALLINT,
	NUM_POLIZA_PROVIENE VARCHAR(100),
	BAN_NUEVA_FORMA_PAGO VARCHAR(2),
	CVE_TIPO_SUMA_ASEGURADA VARCHAR(5),
	DES_TIPO_SUMA_ASEGURADA VARCHAR(100),
	BAN_CAMI VARCHAR(2),
	BAN_CDA VARCHAR(2),
	BAN_CAHL VARCHAR(2),
	BAN_CAHD VARCHAR(2),
	BAN_CECN VARCHAR(2),
	BAN_RN VARCHAR(2),
        BAN_COASEGURO_CERO VARCHAR(2),
	BAN_SOLIDEZ_FAMILIAR VARCHAR(2),
        NUM_ASEG_DEPENDIENTES SMALLINT,
        NUM_ASEG_TITULARES SMALLINT,
	CVE_ADMON_POLIZA VARCHAR(5),
	DES_ADMON_POLIZA VARCHAR(100),
	BAN_ELEGIBILIDAD VARCHAR(2),
	ANIOS_PAGO_COMISION SMALLINT,
	FCH_REGISTRO_PRODUCTO TIMESTAMP,
	NUM_REGISTRO_PRODUCTO VARCHAR(100),
	CVE_CANAL_COBRO_INICIAL VARCHAR(5),
	DES_CANAL_COBRO_INICIAL VARCHAR(100),
	CVE_CANAL_COBRO_SUCESIVO VARCHAR(5),
	DES_CANAL_COBRO_SUCESIVO VARCHAR(100),
	CVE_INDICADOR_DIVIDENDO VARCHAR(5),
	DES_INDICADOR_DIVIDENDO VARCHAR(50),
	CVE_FORMA_PAGO_DIVIDENDO VARCHAR(5),
	DES_FORMA_PAGO_DIVIDENDO VARCHAR(50),
	PCT_FACTOR_DIVIDENDO DECIMAL(16,4),
	PCT_FACTOR_PRIMA DECIMAL(16,4),
	PCT_FACTOR_COMISION DECIMAL(16,4),
	PCT_FACTOR_SINIESTROS DECIMAL(16,4),
	PCT_FACTOR_GIA DECIMAL(16,4),
	CVE_DURACION VARCHAR(5),
	DES_DURACION VARCHAR(50),
	REGISTRO_PRODUCTO_RECAS VARCHAR(100),
	BAN_RPF_MANUAL VARCHAR(2),
	PCT_RPF_MANUAL DECIMAL(16,4),
	BAN_VOLUNTARIOS VARCHAR(2),
	CVE_INDICADOR_NEGOCIO_LICITADO VARCHAR(5),
	DES_INDICADOR_NEGOCIO_LICITADO VARCHAR(100),
	BAN_AFECTA_PAI VARCHAR(2),
	BAN_NEGOCIO_MULTINACIONAL VARCHAR(2),
	NUM_ASEGURADOS_CONTRATO INT,
	BAN_POLIZA_PRESTACION_EMPLEADO VARCHAR(2),
	CVE_ELEGIBILIDAD VARCHAR(5),
	DES_ELEGIBILIDAD VARCHAR(100),
	BAN_IMPRIME_CONVERSION VARCHAR(2),
	TS_ALTA_HDFS TIMESTAMP,
	IDMD5 VARCHAR(32),
	IDMD5COMPLETO VARCHAR(32))
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS BDDLALM.ALM_POLIZA_DATOS_ADICIONALES (
	NUM_POLIZA VARCHAR(14),
	VIGENCIA_POLIZA SMALLINT,
	NUM_VERSION_POLIZA SMALLINT,
	NUM_POLIZA_COBRANZA VARCHAR(8),
	CVE_SISTEMA VARCHAR(4),
	CVE_SISTEMA_INT VARCHAR(4),
	CVE_CIRCULO_MEDICO_CONTRATADO VARCHAR(10),
	DES_CIRCULO_MEDICO_CONTRATADO VARCHAR(100),
	CVE_SEGMENTO_PLAN VARCHAR(5),
	DES_SEGMENTO_PLAN VARCHAR(100),
	CVE_INDICADOR_CAMBIO_GPO_IND VARCHAR(5),
	DES_INDICADOR_CAMBIO_GPO_IND VARCHAR(100),
	NUM_POLIZA_ANTERIOR VARCHAR(14),
	CVE_ESQUEMA VARCHAR(5),
	DES_ESQUEMA VARCHAR(50),
	CVE_EXPERIENCIA VARCHAR(5),
	DES_EXPERIENCIA VARCHAR(50),
	CVE_TIPO_DEDUCIBLE VARCHAR(5),
	DES_TIPO_DEDUCIBLE VARCHAR(100),
	CVE_DESCUENTO VARCHAR(5),
	DES_DESCUENTO VARCHAR(100),
	PCT_DESCUENTO DECIMAL(16,4),
	PCT_DESCUENTO_COMERCIAL DECIMAL(16,4),
	BAN_POLIZA_CASH_FLOW VARCHAR(2),
	NUM_POLIZA_CASH_FLOW VARCHAR(14),
	NUM_POLIZA_FONDO_ESPECIAL VARCHAR(14),
	NUM_POLIZA_STOP_LOSS VARCHAR(14),
	CVE_FONDO_ADMON_PERDIDA VARCHAR(5),
	DES_FONDO_ADMON_PERDIDA VARCHAR(100),
	CVE_TIPO_NEGOCIO VARCHAR(5),
	DES_TIPO_NEGOCIO VARCHAR(50),
	BAN_CEE VARCHAR(2) ,
	BAN_CECE VARCHAR(2),
	BAN_SALUD_FAMILIAR VARCHAR(2),
	ANIOS_SALUD_FAMILIAR_RESTANTES SMALLINT,
	NUM_POLIZA_PROVIENE VARCHAR(100),
	BAN_NUEVA_FORMA_PAGO VARCHAR(2),
	CVE_TIPO_SUMA_ASEGURADA VARCHAR(5),
	DES_TIPO_SUMA_ASEGURADA VARCHAR(100),
	BAN_CAMI VARCHAR(2),
	BAN_CDA VARCHAR(2),
	BAN_CAHL VARCHAR(2),
	BAN_CAHD VARCHAR(2),
	BAN_CECN VARCHAR(2),
	BAN_RN VARCHAR(2),
        BAN_COASEGURO_CERO VARCHAR(2),
	BAN_SOLIDEZ_FAMILIAR VARCHAR(2),
        NUM_ASEG_DEPENDIENTES SMALLINT,
        NUM_ASEG_TITULARES SMALLINT,
	CVE_ADMON_POLIZA VARCHAR(5),
	DES_ADMON_POLIZA VARCHAR(100),
	BAN_ELEGIBILIDAD VARCHAR(2),
	ANIOS_PAGO_COMISION SMALLINT,
	FCH_REGISTRO_PRODUCTO TIMESTAMP,
	NUM_REGISTRO_PRODUCTO VARCHAR(100),
	CVE_CANAL_COBRO_INICIAL VARCHAR(5),
	DES_CANAL_COBRO_INICIAL VARCHAR(100),
	CVE_CANAL_COBRO_SUCESIVO VARCHAR(5),
	DES_CANAL_COBRO_SUCESIVO VARCHAR(100),
	CVE_INDICADOR_DIVIDENDO VARCHAR(5),
	DES_INDICADOR_DIVIDENDO VARCHAR(50),
	CVE_FORMA_PAGO_DIVIDENDO VARCHAR(5),
	DES_FORMA_PAGO_DIVIDENDO VARCHAR(50),
	PCT_FACTOR_DIVIDENDO DECIMAL(16,4),
	PCT_FACTOR_PRIMA DECIMAL(16,4),
	PCT_FACTOR_COMISION DECIMAL(16,4),
	PCT_FACTOR_SINIESTROS DECIMAL(16,4),
	PCT_FACTOR_GIA DECIMAL(16,4),
	CVE_DURACION VARCHAR(5),
	DES_DURACION VARCHAR(50),
	REGISTRO_PRODUCTO_RECAS VARCHAR(100),
	BAN_RPF_MANUAL VARCHAR(2),
	PCT_RPF_MANUAL DECIMAL(16,4),
	BAN_VOLUNTARIOS VARCHAR(2),
	CVE_INDICADOR_NEGOCIO_LICITADO VARCHAR(5),
	DES_INDICADOR_NEGOCIO_LICITADO VARCHAR(100),
	BAN_AFECTA_PAI VARCHAR(2),
	BAN_NEGOCIO_MULTINACIONAL VARCHAR(2),
	NUM_ASEGURADOS_CONTRATO INT,
	BAN_POLIZA_PRESTACION_EMPLEADO VARCHAR(2),
	CVE_ELEGIBILIDAD VARCHAR(5),
	DES_ELEGIBILIDAD VARCHAR(100),
	BAN_IMPRIME_CONVERSION VARCHAR(2),
	TS_ALTA_HDFS TIMESTAMP,
	IDMD5 VARCHAR(32),
	IDMD5COMPLETO VARCHAR(32))
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS BDDLALM.ALM_POLIZA_DATOS_ADICIONALESHIS (
	NUM_POLIZA VARCHAR(14),
	VIGENCIA_POLIZA SMALLINT,
	NUM_VERSION_POLIZA SMALLINT,
	NUM_POLIZA_COBRANZA VARCHAR(8),
	CVE_SISTEMA VARCHAR(4),
	CVE_SISTEMA_INT VARCHAR(4),
	CVE_CIRCULO_MEDICO_CONTRATADO VARCHAR(10),
	DES_CIRCULO_MEDICO_CONTRATADO VARCHAR(100),
	CVE_SEGMENTO_PLAN VARCHAR(5),
	DES_SEGMENTO_PLAN VARCHAR(100),
	CVE_INDICADOR_CAMBIO_GPO_IND VARCHAR(5),
	DES_INDICADOR_CAMBIO_GPO_IND VARCHAR(100),
	NUM_POLIZA_ANTERIOR VARCHAR(14),
	CVE_ESQUEMA VARCHAR(5),
	DES_ESQUEMA VARCHAR(50),
	CVE_EXPERIENCIA VARCHAR(5),
	DES_EXPERIENCIA VARCHAR(50),
	CVE_TIPO_DEDUCIBLE VARCHAR(5),
	DES_TIPO_DEDUCIBLE VARCHAR(100),
	CVE_DESCUENTO VARCHAR(5),
	DES_DESCUENTO VARCHAR(100),
	PCT_DESCUENTO DECIMAL(16,4),
	PCT_DESCUENTO_COMERCIAL DECIMAL(16,4),
	BAN_POLIZA_CASH_FLOW VARCHAR(2),
	NUM_POLIZA_CASH_FLOW VARCHAR(14),
	NUM_POLIZA_FONDO_ESPECIAL VARCHAR(14),
	NUM_POLIZA_STOP_LOSS VARCHAR(14),
	CVE_FONDO_ADMON_PERDIDA VARCHAR(5),
	DES_FONDO_ADMON_PERDIDA VARCHAR(100),
	CVE_TIPO_NEGOCIO VARCHAR(5),
	DES_TIPO_NEGOCIO VARCHAR(50),
	BAN_CEE VARCHAR(2) ,
	BAN_CECE VARCHAR(2),
	BAN_SALUD_FAMILIAR VARCHAR(2),
	ANIOS_SALUD_FAMILIAR_RESTANTES SMALLINT,
	NUM_POLIZA_PROVIENE VARCHAR(100),
	BAN_NUEVA_FORMA_PAGO VARCHAR(2),
	CVE_TIPO_SUMA_ASEGURADA VARCHAR(5),
	DES_TIPO_SUMA_ASEGURADA VARCHAR(100),
	BAN_CAMI VARCHAR(2),
	BAN_CDA VARCHAR(2),
	BAN_CAHL VARCHAR(2),
	BAN_CAHD VARCHAR(2),
	BAN_CECN VARCHAR(2),
	BAN_RN VARCHAR(2),
        BAN_COASEGURO_CERO VARCHAR(2),
	BAN_SOLIDEZ_FAMILIAR VARCHAR(2),
        NUM_ASEG_DEPENDIENTES SMALLINT,
        NUM_ASEG_TITULARES SMALLINT,
	CVE_ADMON_POLIZA VARCHAR(5),
	DES_ADMON_POLIZA VARCHAR(100),
	BAN_ELEGIBILIDAD VARCHAR(2),
	ANIOS_PAGO_COMISION SMALLINT,
	FCH_REGISTRO_PRODUCTO TIMESTAMP,
	NUM_REGISTRO_PRODUCTO VARCHAR(100),
	CVE_CANAL_COBRO_INICIAL VARCHAR(5),
	DES_CANAL_COBRO_INICIAL VARCHAR(100),
	CVE_CANAL_COBRO_SUCESIVO VARCHAR(5),
	DES_CANAL_COBRO_SUCESIVO VARCHAR(100),
	CVE_INDICADOR_DIVIDENDO VARCHAR(5),
	DES_INDICADOR_DIVIDENDO VARCHAR(50),
	CVE_FORMA_PAGO_DIVIDENDO VARCHAR(5),
	DES_FORMA_PAGO_DIVIDENDO VARCHAR(50),
	PCT_FACTOR_DIVIDENDO DECIMAL(16,4),
	PCT_FACTOR_PRIMA DECIMAL(16,4),
	PCT_FACTOR_COMISION DECIMAL(16,4),
	PCT_FACTOR_SINIESTROS DECIMAL(16,4),
	PCT_FACTOR_GIA DECIMAL(16,4),
	CVE_DURACION VARCHAR(5),
	DES_DURACION VARCHAR(50),
	REGISTRO_PRODUCTO_RECAS VARCHAR(100),
	BAN_RPF_MANUAL VARCHAR(2),
	PCT_RPF_MANUAL DECIMAL(16,4),
	BAN_VOLUNTARIOS VARCHAR(2),
	CVE_INDICADOR_NEGOCIO_LICITADO VARCHAR(5),
	DES_INDICADOR_NEGOCIO_LICITADO VARCHAR(100),
	BAN_AFECTA_PAI VARCHAR(2),
	BAN_NEGOCIO_MULTINACIONAL VARCHAR(2),
	NUM_ASEGURADOS_CONTRATO INT,
	BAN_POLIZA_PRESTACION_EMPLEADO VARCHAR(2),
	CVE_ELEGIBILIDAD VARCHAR(5),
	DES_ELEGIBILIDAD VARCHAR(100),
	BAN_IMPRIME_CONVERSION VARCHAR(2),
	TS_ALTA_HDFS TIMESTAMP,
	IDMD5 VARCHAR(32),
	IDMD5COMPLETO VARCHAR(32),
	FECHHIST TIMESTAMP)
STORED AS PARQUET;