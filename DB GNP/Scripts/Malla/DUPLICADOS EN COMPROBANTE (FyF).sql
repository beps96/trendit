
-- *****PASO 1 IDENTIFICAR LA TABLA DE PASO (VERIFICAR SI HAY DUPLICADOS) *****

SELECT IDMD5, COUNT(*)
	FROM BDDLTRN.STG_GMM_CON_ALM_COMPROBANTE_DELTA
	GROUP BY IDMD5
	HAVING COUNT(*) > 1;


-- **** PASO 2 CREAR UNA TABLA TEMPORAL ****
CREATE TABLE 	BDDLTRN.STG_GMM_CON_ALM_COMPROBANTE_DELTA_DUPLICADOS_28_01_20 AS
	SELECT * FROM BDDLTRN.STG_GMM_CON_ALM_COMPROBANTE_DELTA;


-- **** PASO 3 VALIDAR QUE SE HAYA CREADO BIEN ****
select * from  BDDLTRN.STG_GMM_CON_ALM_COMPROBANTE_DELTA_DUPLICADOS_28_01_20;

 
-- **** PASO 4 TRUNCAR TABLA (ORIGINAL)***
TRUNCATE TABLE BDDLTRN.STG_GMM_CON_ALM_COMPROBANTE_DELTA;


-- **** PASO 5 DE LA TABLA COPIA PASAR LOS DATOS SIN DUPLICADOS ****
INSERT INTO TABLE BDDLTRN.STG_GMM_CON_ALM_COMPROBANTE_DELTA
SELECT * FROM BDDLTRN.STG_GMM_CON_ALM_COMPROBANTE_DELTA_DUPLICADOS_28_01_20
WHERE IDMD5 NOT IN ('a877de22e74a872de2eb8ab0d7b048ba');


-- **** PASO 6 VERIFICAR QUE LOS DATOS NO TENGAN DUPLICADOS (Te debe regresar el o los registros del paso 2, en este caso el que devolvi� folio_solicitud, no donde est� vacio folio_solicitud) **** 
SELECT * FROM BDDLTRN.STG_GMM_CON_ALM_COMPROBANTE_DELTA_DUPLICADOS_28_01_20
WHERE IDMD5 IN ('a877de22e74a872de2eb8ab0d7b048ba')
AND FOLIO_SOLICITUD <> '';


-- **** PASO 7 PASAR LA INFO YA LIMPIA (SIN DUPLICADOS) ****
INSERT INTO TABLE BDDLTRN.STG_GMM_CON_ALM_COMPROBANTE_DELTA
SELECT * FROM BDDLTRN.STG_GMM_CON_ALM_COMPROBANTE_DELTA_DUPLICADOS_28_01_20
WHERE IDMD5 IN ('a877de22e74a872de2eb8ab0d7b048ba')
AND FOLIO_SOLICITUD <> '';


-- **** PASO 8 CONFIRMAR QUE NO HAY DUPLICADOS ****
SELECT IDMD5, COUNT(*)
	FROM BDDLTRN.STG_GMM_CON_ALM_COMPROBANTE_DELTA
	GROUP BY IDMD5
	HAVING COUNT(*) > 1;



-- **** PASO 9 - RERUN al WKF_GMM_CDC
-- Hacemos clic en el subworkflow fallido, donde podremos ver que fall� por duplicados, Aplicamos RERUN
-- Debemos seleccionar "All or skip successful"


-- *** PASO 10 - Validaci�n de todo bien
-- El flujo debe continuar hac�a el otro nodo donde no hay duplicados

-- ***PASO 11 - Solicitar FORCE OK ****
-- Enviar evidencia de que termin� correctamente solicitando FORCE OK, al job en Control-M a las siguientes cuentes de coreo