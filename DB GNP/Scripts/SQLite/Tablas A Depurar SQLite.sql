select
	COUNT(1)
from
	tablas_depurar_ORIGINAL tdo
where
	CLAVE_PARAMETRO <> 'last_modified_time';
-- 9803 Registros


 SELECT
	*
FROM
	(
	SELECT
		tdo.ESQUEMA,
		tdo.TABLA,
		tdo.OWNER,
		--tdo.FECHA_CREACION, 
 DATE(SUBSTR(tdo.FECHA_CREACION, 7, 4) || '-' || SUBSTR(tdo.FECHA_CREACION, 4, 2) || '-' || SUBSTR(tdo.FECHA_CREACION, 1, 2)) AS fechac,
		tdo.CLAVE_PARAMETRO,
		DATE(SUBSTR(tdo.VALOR_PARAMETRO, 7, 4) || '-' || SUBSTR(tdo.VALOR_PARAMETRO, 4, 2) || '-' || SUBSTR(tdo.VALOR_PARAMETRO, 1, 2)) AS fechap,
		tdo.ULTIMA_FECHA_ACCESO
	FROM
		tablas_depurar_ORIGINAL AS tdo ) AS tabla
WHERE
	fechac < '2019-11-01'
	and fechap < '2019-11-01'
	and CLAVE_PARAMETRO <> 'last_modified_time'
order by
	TABLA ;
-- 9603 Registros


 SELECT
	*
FROM
	(
	SELECT
		tdo.ESQUEMA,
		tdo.TABLA,
		tdo.OWNER,
		--tdo.FECHA_CREACION, 
 DATE(SUBSTR(tdo.FECHA_CREACION, 7, 4) || '-' || SUBSTR(tdo.FECHA_CREACION, 4, 2) || '-' || SUBSTR(tdo.FECHA_CREACION, 1, 2)) AS fechac,
		tdo.CLAVE_PARAMETRO,
		DATE(SUBSTR(tdo.VALOR_PARAMETRO, 7, 4) || '-' || SUBSTR(tdo.VALOR_PARAMETRO, 4, 2) || '-' || SUBSTR(tdo.VALOR_PARAMETRO, 1, 2)) AS fechap,
		tdo.ULTIMA_FECHA_ACCESO
	FROM
		tablas_depurar_ORIGINAL AS tdo ) AS tabla
WHERE
	(fechac >= '2019-11-01'
	or fechap >= '2019-11-01')
	and CLAVE_PARAMETRO <> 'last_modified_time'
order by
	TABLA ;
--200 Registros

