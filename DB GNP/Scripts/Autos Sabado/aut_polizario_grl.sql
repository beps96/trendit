DROP TABLE IF EXISTS BDDLTRN.AUT_POLIZARIO;
CREATE TABLE BDDLTRN.AUT_POLIZARIO STORED AS PARQUET
AS
SELECT
ROW_NUMBER () OVER (PARTITION BY A.CDNUMPOL ORDER BY A.CDNUMPOL, A.CTVRSPOL) AS ID
,A.CDNUMPOL AS NUM_POLIZA_POLIZARIO
,A.CTVRSPOL AS NUM_VERSION_POLIZARIO
,A.cdprodte as cve_prod_tecnico
,A.cdprodco as  cve_prod_comercial
FROM
 (
 SELECT CDNUMPOL,CTVRSPOL ,cdprodte,cdprodco
    FROM BDDLCRU.KCIM06T
    WHERE UPPER(SUBSTR(cdprodte,1,1))='A' 
  GROUP BY CDNUMPOL,CTVRSPOL ,cdprodte,cdprodco 
 
UNION 

 SELECT CDNUMPOL,CTVRSPOL,cdprodte,cdprodco
    FROM BDDLCRU.KCIM06H
    WHERE UPPER(SUBSTR(cdprodte,1,1))='A'
  GROUP BY CDNUMPOL,CTVRSPOL ,cdprodte,cdprodco
)A
GROUP BY A.CDNUMPOL,A.CTVRSPOL,A.cdprodte,A.cdprodco;