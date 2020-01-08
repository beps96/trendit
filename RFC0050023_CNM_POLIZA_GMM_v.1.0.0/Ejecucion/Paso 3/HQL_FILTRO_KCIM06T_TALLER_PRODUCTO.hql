--**********************************************************************************************************************************************************************
-- Se obtienen solo las pólizas del periodo indicado (año-mes de la fecha de emisión) en la tabla bddlcru.kcim06t.


DROP TABLE IF EXISTS BDDLTRN.STG_GMM_KCIM04T_INTER_FILTER PURGE;
CREATE TABLE IF NOT EXISTS BDDLTRN.STG_GMM_KCIM04T_INTER_FILTER AS
select
    KCIM04T_CONTRATO.CDNUMPOL AS NUM_POL,
    KCIM04T_CONTRATO.CTVRSPOL AS VER_POL,
    KCIM04T_CONTRATO.TCCDFUNC AS COD_FUNCION,
    KCIM04T_CONTRATO.NUFLANUM AS NUM_FOL,
    KCIM04T_CONTRATO.tccsinte AS COD_CLASE_INTERM,
    KCIM04T_CONTRATO.CDINTERM AS COD_INTERM,
    KCIM04T_CONTRATO.ESINTERM AS EDO_INTERM,
    KCIM04T_CONTRATO.INPRIFUN AS IND_PRIMERO_FUNCION
FROM 
    BDDLCRU.KCIM04T AS KCIM04T_CONTRATO
INNER JOIN
    (SELECT 
        CDNUMPOL as NUM_POL, 
        CTVRSPOL as VER_POL,
        MAX(CTSECINT) AS SECUENCIA_INTERM
    FROM 
        BDDLCRU.KCIM04T as KCIM04T
    WHERE 
        (KCIM04T.tccsinte = 'A' or KCIM04T.tccsinte = 'R' or KCIM04T.tccsinte = 'O' ) AND
        KCIM04T.TCCDFUNC = 'P' AND
        --KCIM04T.ESINTERM = 'V' AND
        KCIM04T.INPRIFUN = 'S'
    GROUP BY CDNUMPOL, CTVRSPOL) 
    as MAXCTSECINT
ON
    MAXCTSECINT.NUM_POL = KCIM04T_CONTRATO.CDNUMPOL AND
    MAXCTSECINT.VER_POL = KCIM04T_CONTRATO.CTVRSPOL AND
    MAXCTSECINT.SECUENCIA_INTERM = KCIM04T_CONTRATO.CTSECINT
--------------------- DELTA --------------------------------------------------------------------------------------------------------------------------------------------
--INNER JOIN 
--    ( 
--       SELECT DISTINCT CDNUMPOL 
--       FROM BDDLCRU.KCIM06T 
--       WHERE CAST(SUBSTRING(NVL(CAST(FEMISION AS STRING),''),1,6) AS INT) in (201610,201812,201909)  
--       AND KCIM06T.CDCIAGRU = 'SA' 
--    ) DELTA
--ON KCIM04T_CONTRATO.CDNUMPOL = DELTA.CDNUMPOL
--------------------- DELTA --------------------------------------------------------------------------------------------------------------------------------------------
WHERE 
   (KCIM04T_CONTRATO.tccsinte = 'A' or KCIM04T_CONTRATO.tccsinte = 'R' or KCIM04T_CONTRATO.tccsinte = 'O' ) AND
   KCIM04T_CONTRATO.TCCDFUNC = 'P' AND
   --KCIM04T_CONTRATO.ESINTERM = 'V' AND
   KCIM04T_CONTRATO.INPRIFUN = 'S';


  
--**********************************************************************************************************************************************************************
------------------ Cuando el campo NUM_FOL sea nulo, se utilizará el valor de la versión anterior-----------------------------------------------------------------------

DROP TABLE IF EXISTS BDDLTRN.STG_GMM_KCIM04T_RELLENO PURGE;
CREATE TABLE IF NOT EXISTS BDDLTRN.STG_GMM_KCIM04T_RELLENO AS
   SELECT 
      *,
      FIRST_VALUE(NUM_FOL) over (partition by NUM_POLIZA06T, SUMA order by NUM_POLIZA06T, NUM_VERSION_POLIZA06T) as NUM_FOL2
   FROM (
           SELECT *,
              SUM(BAN_INTERM) over (partition by NUM_POLIZA06T order by NUM_POLIZA06T, NUM_VERSION_POLIZA06T range between unbounded preceding and current row) as SUMA
           FROM (
                   SELECT 
                      KCIM06T.CDNUMPOL AS NUM_POLIZA06T
                     ,KCIM06T.CTVRSPOL AS NUM_VERSION_POLIZA06T
                     ,KCIM04T.*
                     ,CASE WHEN KCIM04T.NUM_FOL IS NULL OR KCIM04T.NUM_FOL = '' THEN 0 ELSE 1 END AS BAN_INTERM
                   FROM BDDLCRU.KCIM06T KCIM06T
--------------------- DELTA ------------------------------------------------------------------------------------
--                INNER JOIN 
--                    ( 
--                       SELECT DISTINCT CDNUMPOL 
--                       FROM BDDLCRU.KCIM06T 
--                       WHERE CAST(SUBSTRING(NVL(CAST(FEMISION AS STRING),''),1,6) AS INT) in (201610,201812,201909)  
--                       AND KCIM06T.CDCIAGRU = 'SA' 
--                    ) DELTA
--                ON KCIM06T.CDNUMPOL = DELTA.CDNUMPOL
--------------------- DELTA ------------------------------------------------------------------------------------
                   LEFT JOIN BDDLTRN.STG_GMM_KCIM04T_INTER_FILTER KCIM04T
                   ON KCIM06T.CDNUMPOL = KCIM04T.NUM_POL  AND KCIM06T.CTVRSPOL = KCIM04T.VER_POL
        ) H    
) N
;


--*********************************************************************************************************************************
DROP TABLE IF EXISTS BDDLTRN.STG_GMM_CNM_POLIZA_INFO PURGE;
CREATE TABLE IF NOT EXISTS BDDLTRN.STG_GMM_CNM_POLIZA_INFO AS
SELECT DISTINCT
    KCIM06T_POL_CERTIF.CDNUMPOL as NUM_POLIZA,
    KCIM06T_POL_CERTIF.INANCACO as VIGENCIA_POLIZA,
    KCIM06T_POL_CERTIF.CTVRSPOL as NUM_VERSION_POLIZA,
    '' AS NUM_POLIZA_COBRANZA,
    CASE WHEN TRIM(KCIM06T_POL_CERTIF.CDUSUARI) = 'EVOLUCIO' 
        THEN 
            'EOT' 
        ELSE 
            'INFO' 
    END CVE_SISTEMA,
    'INFO' AS CVE_SISTEMA_INT,
    CAT.CVE_COBERTURA_CONTABLE,
    CASE WHEN CONCAT(TRIM(KCIM06T_POL_CERTIF.CDPRODTE),TRIM(KCIM06T_POL_CERTIF.CDPRODCO),TRIM(ALM_GM_PROD_POLIZA.CDTIPNEG)) IS NOT NULL AND CAT.CVE_COBERTURA_CONTABLE IS NULL 
         THEN 
              CONCAT('Descripción no encontrada para: ',CONCAT(TRIM(KCIM06T_POL_CERTIF.CDPRODTE),TRIM(KCIM06T_POL_CERTIF.CDPRODCO),TRIM(ALM_GM_PROD_POLIZA.CDTIPNEG))) 
         ELSE 
              NVL(CAT.DES_COBERTURA_CONTABLE,'')
    END AS DES_COBERTURA_CONTABLE,
    NVL(CAT_ESTATUS.CVE_ESTATUS,'') AS CVE_ESTATUS_POLIZA,
    CASE WHEN KCIM06T_POL_CERTIF.ESPOLIZA IS NOT NULL AND CAT_ESTATUS.CVE_ESTATUS IS NULL 
        THEN 
            CONCAT('Descripción no encontrada para: ',KCIM06T_POL_CERTIF.ESPOLIZA)  
        ELSE 
            NVL(CAT_ESTATUS.DES_ESTATUS_POLIZA,'') 
    END AS DES_ESTATUS_POLIZA,  
    NVL(CAT_FORMA_PAGO.CVE_FORMA_PAGO,'') AS CVE_FORMA_PAGO,  
    CASE WHEN KCIM06T_POL_CERTIF.TCFORPAG IS NOT NULL AND CAT_FORMA_PAGO.CVE_FORMA_PAGO IS NULL 
        THEN 
            CONCAT('Descripción no encontrada para: ',KCIM06T_POL_CERTIF.TCFORPAG) 
        ELSE 
            NVL(CAT_FORMA_PAGO.DES_FORMA_PAGO,'') 
    END AS DES_FORMA_PAGO,  
    CAST(REGEXP_REPLACE(NUM_FOL2,'P', '') AS INT) AS CVE_INTERMEDIARIO_PRINCIPAL,
    IF(CAT_CAGENTE2.DSNOMBRE IS NULL,
       --THEN
          IF(cast(SUBSTRING(cast(KCIM06T_POL_CERTIF.FEMISION as string),1,6) as int) < AM_PROC_min,
             --THEN
               NVL(DSNOMBRE_min,''),
             --ELSE
               CONCAT('Descripción no encontrada para: ', REGEXP_REPLACE(NUM_FOL2,'P', '') )
             ),
       --ELSE
            NVL(CAT_CAGENTE2.DSNOMBRE,'')
    ) AS NOMBRE_INTERMEDIARIO_PRINCIPAL,     
    NVL(TRIM(KCIM06T_POL_CERTIF.CDNUMCON),'') AS NUM_CONTRATO, 
    NVL(KCIM01T.CTVERCON,-999) AS NUM_VERSION_CONTRATO,
    NVL(KCIM01T.CDNEGOCI,'') AS CVE_NEGOCIO, 
    CASE WHEN KCIM01T.CDNEGOCI IS NOT NULL AND CAT_NEGOCIO.CVE_NEGOCIO IS NULL 
        THEN 
            CONCAT('Descripción no encontrada para: ',KCIM01T.CDNEGOCI) 
        ELSE 
            NVL(CAT_NEGOCIO.DES_NEGOCIO,'') 
    END AS DES_NEGOCIO,
    NVL(KCIM04T.NUM_FOL2,'') AS FOLIO_INTERMEDIARIO_PRINCIPAL,
    NVL(CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(KCIM06T_POL_CERTIF.FEEFTONA AS VARCHAR(8)),'yyyyMMdd')) AS TIMESTAMP),cast('1400-01-01 00:00:00' as timestamp)) AS FCH_INI_VIGENCIA,
    NVL(DATE_ADD(CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(KCIM06T_POL_CERTIF.FEVENCIM AS VARCHAR(8)),'yyyyMMdd')) AS TIMESTAMP),1),cast('1400-01-01 00:00:00' as timestamp)) AS FCH_FIN_VIGENCIA,
    NVL(KCIM06T_POL_CERTIF.CLFILTOM,'') AS CVE_CONTRATANTE,
    NVL(TRIM(KCIM06T_POL_CERTIF.CDNIFTOM),'') AS RFC_CONTRATANTE,
    CASE WHEN TRIM(KCIM06T_POL_CERTIF.CLFILTOM) IS NOT NULL AND CAT_FILTOM.COD_FILTOM_CAT IS NULL 
        THEN CONCAT('Descripción no encontrada para: ', TRIM(KCIM06T_POL_CERTIF.CLFILTOM)) 
        ELSE NVL(TRIM(CAT_FILTOM.DES_FILTOM),'') 
    END AS NOMBRE_CONTRATANTE, 
    NVL(TRIM(KCIM06T_POL_CERTIF.CDCANALD),'') AS CVE_CANAL_VENTA,
    CASE WHEN  TRIM(KCIM06T_POL_CERTIF.CDCANALD) IS NOT NULL AND TRIM(CAT_CANALD.COD_CANALD_CAT) IS NULL 
        THEN 
            CONCAT('Descripción no encontrada para: ',TRIM(KCIM06T_POL_CERTIF.CDCANALD)) 
        ELSE 
            NVL(CAT_CANALD.DES_CANAL_DIST,'') 
    END AS DES_CANAL_VENTA, 
    NVL(TRIM(ALM_GM_PROD_POLIZA.CDTIPPOL),'') AS CVE_TIPO_POLIZA,
    CASE WHEN ALM_GM_PROD_POLIZA.CDTIPPOL IS NOT NULL AND ALM_GM_PROD_POLIZA.DES_CDTIPPOL IS NULL
        THEN 
            CONCAT('Descripción no encontrada para: ',TRIM(ALM_GM_PROD_POLIZA.CDTIPPOL)) 
        ELSE 
            NVL(ALM_GM_PROD_POLIZA.DES_CDTIPPOL,'') 
    END AS DES_TIPO_POLIZA, 
    NVL(TRIM(KCIM06T_POL_CERTIF.CDPRODTE),'') AS CVE_PROD_TECNICO,
    NVL(TRIM(KCIM06T_POL_CERTIF.CDPRODCO),'') AS CVE_PROD_COMERCIAL,
    CASE WHEN concat(trim(KCIM06T_POL_CERTIF.CDPRODTE),trim(KCIM06T_POL_CERTIF.CDPRODCO)) IS NOT NULL AND CONEX.CDELEMEN IS NULL THEN 
         CONCAT('Descripción no encontrada para: ',concat(trim(KCIM06T_POL_CERTIF.CDPRODTE),trim(KCIM06T_POL_CERTIF.CDPRODCO))) 
    ELSE 
         NVL(TRIM(REGEXP_REPLACE(CONEX.DSELEMEN,'\\s+',' ')),'')
    END AS DES_PROD_TECNICO_COMERCIAL,
    NVL(CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(KCIM06T_POL_CERTIF.FEEFTOMO AS VARCHAR(8)),'yyyyMMdd')) AS TIMESTAMP),cast('1400-01-01 00:00:00' as timestamp)) AS FCH_EFECTO_MOVIMIENTO,
    NVL(DATE_ADD(CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(KCIM06T_POL_CERTIF.FEFINEFE AS VARCHAR(8)),'yyyyMMdd')) AS TIMESTAMP),1),cast('1400-01-01 00:00:00' as timestamp)) AS FCH_FIN_MOVIMIENTO,
    NVL(CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(KCIM06T_POL_CERTIF.FEINIPOL AS VARCHAR(8)),'yyyyMMdd')) AS TIMESTAMP),cast('1400-01-01 00:00:00' as timestamp))  AS FCH_INI_POLIZA,
    NVL(CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(KCIM06T_POL_CERTIF.FEMISION AS VARCHAR(8)),'yyyyMMdd')) AS TIMESTAMP),cast('1400-01-01 00:00:00' as timestamp)) AS FCH_EMISION,
    NVL(SUBSTRING(NVL(CAST(KCIM06T_POL_CERTIF.FEMISION AS STRING),''),1,6),'140001') AS AM_EMISION,
    NVL(KCIM06T_POL_CERTIF.TCMOMOVP,'') AS CVE_MOTIVO_ESTATUS_POLIZA,
    CASE WHEN CONCAT(TRIM(KCIM06T_POL_CERTIF.TCTIMOVP),TRIM(KCIM06T_POL_CERTIF.TCMOMOVP)) IS NOT NULL AND TRIM(CAT_MOTIV_MOV_POL.COD_MOTIV_MOV_POL_CAT) IS NULL THEN  
         CONCAT('Descripción no encontrada para: ',CONCAT(TRIM(KCIM06T_POL_CERTIF.TCTIMOVP),TRIM(KCIM06T_POL_CERTIF.TCMOMOVP))) 
    ELSE 
         NVL(CAT_MOTIV_MOV_POL.DES_MOTIV_MOV_POL,'')
    END AS DES_MOTIVO_ESTATUS_POLIZA, 
    NVL(CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(KCIM06T_POL_CERTIF.FEMISION AS VARCHAR(8)),'yyyyMMdd')) AS TIMESTAMP),cast('1400-01-01 00:00:00' as timestamp)) AS FCH_ESTATUS_POLIZA,
    CASE WHEN KCIM06T_POL_CERTIF.INANULFP = 'N' THEN 'NO' WHEN KCIM06T_POL_CERTIF.INANULFP = 'S' 
        THEN 
            'SI' 
        ELSE 
            NVL(TRIM(KCIM06T_POL_CERTIF.INANULFP),'') 
    END AS BAN_ANULACION_FALTA_PAGO, 
    NVL(CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(KCIM06T_POL_CERTIF.FEANULFP AS VARCHAR(8)),'yyyyMMdd')) AS TIMESTAMP),cast('1400-01-01 00:00:00' as timestamp))  AS FCH_ANULACION_FALTA_PAGO,
    CASE WHEN KCIM06T_POL_CERTIF.FEANULFP = 0 
        THEN 
            '140001'
        ELSE
            SUBSTRING(NVL(CAST(KCIM06T_POL_CERTIF.FEANULFP AS STRING),''),1,6) 
    END AS AM_ANULACION_FALTA_PAGO,
    NVL(KCIM06T_POL_CERTIF.TCTIMOVP,'') AS CVE_TIPO_MOVIMIENTO,
    CASE WHEN TRIM(KCIM06T_POL_CERTIF.TCTIMOVP) IS NOT NULL AND TRIM(CAT_MOTIV_MOV_POL.COD_MOTIV_MOV_POL_CAT) IS NULL THEN  
         CONCAT('Descripción no encontrada para: ',TRIM(KCIM06T_POL_CERTIF.TCTIMOVP)) 
    ELSE 
         NVL(CAT_TIP_MOV_POL.DES_TIP_MOV_POL,'')
    END AS DES_TIPO_MOVIMIENTO, 
    NVL(TRIM(KCIM06T_POL_CERTIF.INULTSIT),'') AS CVE_ULTIMA_SITUACION,
    CASE WHEN KCIM06T_POL_CERTIF.INULTSIT IS NOT NULL AND CAT_ULTSITU.COD_ULTSITU_CAT IS NULL 
        THEN 
            CONCAT('Descripción no encontrada para: ',TRIM(KCIM06T_POL_CERTIF.INULTSIT)) 
        ELSE 
            NVL(TRIM(CAT_ULTSITU.DES_ULTSITU),'') 
    END AS DES_ULTIMA_SITUACION, 
    NVL(TRIM(KCIM06T_POL_CERTIF.INPOLPAD),'') AS CVE_INDICADOR_POLIZA_PADRE_HIJA,
    CASE 
        WHEN KCIM06T_POL_CERTIF.INPOLPAD = 'H' 
            THEN 'POLIZA HIJA' 
        WHEN KCIM06T_POL_CERTIF.INPOLPAD = 'P' 
            THEN 'POLIZA PADRE'
        ELSE 
            '' 
    END AS DES_INDICADOR_POLIZA_PADRE_HIJA, 
    NVL(TRIM(KCIM06T_POL_CERTIF.CDPOLPAD),'') AS NUM_POLIZA_PADRE,
    NVL(KCIM06T_POL_CERTIF.CTVRSPAD,-999) AS NUM_VERSION_POLIZA_PADRE,
    NVL(TRIM(KCIM06T_POL_CERTIF.CLFILPAG),'') AS CVE_PAGADOR,
    CASE WHEN KCIM06T_POL_CERTIF.CLFILPAG IS NOT NULL AND CAT_FILPAG.COD_FILPAG_CAT IS NULL 
        THEN 
            CONCAT('Descripción no encontrada para: ',TRIM(KCIM06T_POL_CERTIF.CLFILPAG)) 
        ELSE 
            NVL(TRIM(DES_FILPAG),'') 
    END AS NOMBRE_PAGADOR, 
    CASE WHEN NVL(KCIM06T_POL_CERTIF.FEVENCIM,0) = 0 
        THEN 
            '140001'
        ELSE
            SUBSTRING(NVL(CAST(KCIM06T_POL_CERTIF.FEVENCIM AS STRING),''),1,6) 
    END AS AM_FIN_VIGENCIA,
    '' AS CVE_POOL,
    NVL(SUBSTRING(CAT_SUBRAMO_ADMON.DES_SUBRAMO_ADMON,3,2),'') AS CVE_SUBRAMO_ADMON,
    CASE WHEN CONCAT(RPAD(TRIM(KCIM06T_POL_CERTIF.CDPRODTE),10,' '),RPAD(TRIM(KCIM06T_POL_CERTIF.CDPRODCO),10,' '),RPAD(TRIM(ALM_GM_PROD_POLIZA.INTIPLAN),5,' '),TRIM(ALM_GM_PROD_POLIZA.CDTIPNEG)) IS NOT NULL AND TRIM(CVE_SUBRAMO_ADMON) IS NULL 
         THEN 
              CONCAT('Descripción no encontrada para: ',CONCAT(RPAD(TRIM(KCIM06T_POL_CERTIF.CDPRODTE),10,' '),RPAD(TRIM(KCIM06T_POL_CERTIF.CDPRODCO),10,' '),RPAD(TRIM(ALM_GM_PROD_POLIZA.INTIPLAN),5,' '),TRIM(ALM_GM_PROD_POLIZA.CDTIPNEG))) 
         ELSE 
              NVL(SUBSTR(CAT_SUBRAMO_ADMON.DES_SUBRAMO_ADMON,5,55),'')
    END AS DES_SUBRAMO_ADMON,
    NVL(TRIM(ALM_GM_PROD_POLIZA.INTIPLAN),'') AS CVE_PLAN,
    CASE WHEN ALM_GM_PROD_POLIZA.INTIPLAN IS NOT NULL AND ALM_GM_PROD_POLIZA.DES_INTIPLAN IS NULL 
        THEN 
            CONCAT('Descripción no encontrada para: ',TRIM(ALM_GM_PROD_POLIZA.INTIPLAN)) 
        ELSE 
            NVL(TRIM(ALM_GM_PROD_POLIZA.DES_INTIPLAN),'') 
    END AS DES_PLAN, 
    IF(CONEX.DSELEMEN IS NULL, 'N', (IF(instr(CONEX.DSELEMEN, 'CONEXION')>0,'S','N'))) AS IND_CONEXION,
    NVL(TRIM(KCIM06T_POL_CERTIF.CDUSUARI),'') AS CVE_USUARIO,
    CASE WHEN NVL(TRIM(KCIM06T_POL_CERTIF.CDUSUARI),'') <> ''  AND CAT_USUARIO.COD_USUARIO_CAT IS NULL 
        THEN 
            CONCAT('Descripción no encontrada para: ',TRIM(KCIM06T_POL_CERTIF.CDUSUARI)) 
        ELSE 
            NVL(TRIM(CAT_USUARIO.DES_USUARIO),'') 
    END AS DES_USUARIO,  
    NVL(TRIM(KCIM06T_POL_CERTIF.CDSUSCRI),'') AS CVE_SUSCRIPTOR, 
    CASE WHEN NVL(TRIM(KCIM06T_POL_CERTIF.CDSUSCRI),'') <> '' AND CAT_SUSCRIP.COD_SUSCRIP_CAT IS NULL 
        THEN 
            CONCAT('Descripción no encontrada para: ',TRIM(KCIM06T_POL_CERTIF.CDSUSCRI)) 
        ELSE 
            NVL(TRIM(CAT_SUSCRIP.DES_SUSCRIP),'') 
    END  AS DES_SUSCRIPTOR, 
    NVL(CAT_CPLANES.NIVHOS,'') AS NIVEL_HOSPITAL,
    NVL(CAT_CPLANES.PLNDSC,'') AS GAMA_POLIZA,
    NVL(CAT_CPLANES.PLNTPO,'') AS PLAN_GAMA_NIVEL_POLIZA,
    NVL(CAT_CPLANES.CLSPLN,'') AS TERRITORIALIDAD_POLIZA, 
    NVL(TRIM(KCIM06T_POL_CERTIF.CDCIAGRU),'') AS CVE_RAMO,
    CASE WHEN KCIM06T_POL_CERTIF.CDCIAGRU IS NOT NULL AND CAT_CIAGRU.COD_CIAGRU_CAT IS NULL 
        THEN 
            CONCAT('Descripción no encontrada para: ',TRIM(KCIM06T_POL_CERTIF.CDCIAGRU)) 
        ELSE 
            NVL(CAT_CIAGRU.DES_CIA_GRUPO,'') 
    END AS DES_RAMO, 
    NVL(CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(KCIM06T_POL_CERTIF.FECAMEST AS VARCHAR(8)),'yyyyMMdd')) AS TIMESTAMP),cast('1400-01-01 00:00:00' as timestamp)) AS FCH_CAMBIO_ESTADO,
    '' AS CANAL_VENTA_ESTADISTICO,
    '' AS SEGMENTO_VENTA_ESTADISTICO,
-->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>><
     
    NVL( CAST( IF(CAT_CAGENTE2.DIRPLZ IS NULL,
       --THEN
          IF(cast(SUBSTRING(cast(KCIM06T_POL_CERTIF.FEMISION as string),1,6) as int) < AM_PROC_min,
             --THEN
               DIRPLZ_min,
             --ELSE
               NULL
             ),
       --ELSE
            CAT_CAGENTE2.DIRPLZ
       ) AS VARCHAR),'') AS CVE_DIR_PLAZA, 
    IF(CAT_CDDIRPLZ.DSDIRPLZ IS NULL,
       --THEN
          IF(cast(SUBSTRING(cast(KCIM06T_POL_CERTIF.FEMISION as string),1,6) as int) < AM_PROC_min,
             --THEN
               DSDIRPLZ_min,
             --ELSE
               CASE WHEN CAT_CAGENTE2.DIRPLZ IS NOT NULL 
                   THEN 
                       CONCAT('Descripción no encontrada para: ',CAST(CAT_CAGENTE2.DIRPLZ AS STRING))
                   ELSE
                      ''
               END
             ),
       --ELSE
            CAT_CDDIRPLZ.DSDIRPLZ
       ) AS DES_DIR_PLAZA,

    NVL( CAST( IF(CAT_CAGENTE2.CDEDOPLZ IS NULL,
       --THEN
          IF(cast(SUBSTRING(cast(KCIM06T_POL_CERTIF.FEMISION as string),1,6) as int) < AM_PROC_min,
             --THEN
               CDEDOPLZ_min,
             --ELSE
               NULL
             ),
       --ELSE
            CAT_CAGENTE2.CDEDOPLZ
       ) AS VARCHAR),'') AS CVE_EDO_DIR_PLAZA,
       
    IF(CAT_CDEDOPLZ.DSEDOPLZ IS NULL,
       --THEN
          IF(cast(SUBSTRING(cast(KCIM06T_POL_CERTIF.FEMISION as string),1,6) as int) < AM_PROC_min,
             --THEN
               DSEDOPLZ_min,
             --ELSE
               CASE WHEN CAT_CAGENTE2.CDEDOPLZ IS NOT NULL 
                   THEN 
                       CONCAT('Descripción no encontrada para: ',CAST(CAT_CAGENTE2.CDEDOPLZ AS STRING))
                   ELSE
                       ''
               END
             ),
       --ELSE
            CAT_CDEDOPLZ.DSEDOPLZ
       ) AS DES_EDO_DIR_PLAZA,	   
	   
    NVL( CAST( IF(CAT_CAGENTE2.GRZCVE IS NULL,
       --THEN
          IF(cast(SUBSTRING(cast(KCIM06T_POL_CERTIF.FEMISION as string),1,6) as int) < AM_PROC_min,
             --THEN
               GRZCVE_min,
             --ELSE
               NULL
             ),
       --ELSE
            CAT_CAGENTE2.GRZCVE
       ) AS VARCHAR),'') AS CVE_DIRECCION_AGENCIA,
      
    IF(CAT_CCATDA.SAZONAP IS NULL,
       --THEN
          IF(cast(SUBSTRING(cast(KCIM06T_POL_CERTIF.FEMISION as string),1,6) as int) < AM_PROC_min,
             --THEN
               SAZONAP_min,
             --ELSE
               CASE WHEN CAT_CAGENTE2.GRZCVE IS NOT NULL 
                   THEN 
                       CONCAT('Descripción no encontrada para: ',CAST(CAT_CAGENTE2.GRZCVE AS STRING))
                   ELSE
                       ''
               END
             ),
       --ELSE
            CAT_CCATDA.SAZONAP   
       ) AS DES_DIRECCION_AGENCIA,
        
    NVL( CAST( IF(CAT_CAGENTE2.OFNCVE IS NULL,
       --THEN
          IF(cast(SUBSTRING(cast(KCIM06T_POL_CERTIF.FEMISION as string),1,6) as int) < AM_PROC_min,
             --THEN
               OFNCVE_min,
             --ELSE
               NULL
             ),
       --ELSE
            CAT_CAGENTE2.OFNCVE
    ) AS VARCHAR),'') AS CVE_OFICINA_INTERMEDIARIO, 
    IF(CAT_CAGENTE2.REDTERIN IS NULL,
       --THEN
          IF(cast(SUBSTRING(cast(KCIM06T_POL_CERTIF.FEMISION as string),1,6) as int) < AM_PROC_min,
             --THEN
               REDTERIN_min,
             --ELSE
               ''
             ),
       --ELSE
            CAT_CAGENTE2.REDTERIN
       ) AS RED_TERRITORIAL,
	    
    --KGETTCSEGPLA.CVE_SEGMENTO AS CVE_SEGMENTO,
    --KGETTCSEGPLA.DES_SEGMENTO AS DES_SEGMENTO,
    NVL(CAST(TRIM(ALM_GM_PROD_POLIZA.CDETISEG) AS STRING), '') AS CVE_SEGMENTO, 
    NVL(CAST(TRIM(KTCTGETSEG.dselemen) AS STRING), '') as DES_SEGMENTO,
    NVL(CAST(TRIM(ALM_GM_PROD_POLIZA.CDNEGMUL) AS STRING), '') AS CVE_NEGOCIO_MULTINACIONAL, 
    NVL(CAST(TRIM(KTCTGETMUL.dselemen) AS STRING), '') as DES_NEGOCIO_MULTINACIONAL,
    NVL(CAST(TRIM(ALM_GM_PROD_POLIZA.CDTIPBON) AS STRING), '') AS CVE_TIPO_BONO, 
    NVL(CAST(TRIM(KTCTGETBON.dselemen) AS STRING), '') as DES_TIPO_BONO,
    CASE WHEN (TRIM(KCIM06T_POL_CERTIF.INPOLPAI) = 'N') THEN 'True' ELSE 'False' END AS AFECTO_PLAN_INCENTIVO, 
    NVL(CAST(TRIM(KTCTGETPCO.dselemen) AS STRING), '') as BAN_POLIZA_CONTRIBUTORIA,
    CAST(NVL(ALM_GM_PROD_POLIZA.PORCONTR,0) AS INT) AS PCT_CONTRIBUCION, 
    NVL(CAST(TRIM(KTCTGETPPR.dselemen) AS STRING), '') as BAN_POLIZA_PRESTACION,
    NVL(CAST(TRIM(KCIM06T_POL_CERTIF.INPOLPAI) AS STRING), '')  AS CVE_MOTIVO_EXCLUSION_PLAN_INCENTIVO,
    NVL(CAST(TRIM(KTCTGETMOT.dselemen) AS STRING), '') as DES_MOTIVO_EXCLUSION_PLAN_INCENTIVO

FROM 
    BDDLCRU.KCIM06T  AS KCIM06T_POL_CERTIF

--------- DELTA ---------
--------- SE DEBE COMENTAR AL CARGAR LA TABLA COMPLETA  ---------
--INNER JOIN (SELECT MAX(tscdc) AS tscdc FROM BDDLCRU.KCIM06T) AS 06T2 ON TO_DATE(KCIM06T_POL_CERTIF.tscdc) = TO_DATE(06T2.tscdc) --> Delta
--------- DELTA ---------

--------------------------------------------------------------------

   INNER JOIN 
     (
        SELECT  INTIPLAN, INTABULA, NUM_POLIZA, NUM_VERSION, TCSEGPLA, CDTIPNEG, CDTIPPOL, DES_CDTIPPOL, DES_INTIPLAN, CDETISEG, CDNEGMUL, CDTIPBON, PORCONTR, INPOLCON, INPOLPRE 
        FROM BDDLALM.GMM_ELEMENTO_POLIZA 
            UNION ALL 
        SELECT  INTIPLAN, INTABULA, NUM_POLIZA, NUM_VERSION, TCSEGPLA, CDTIPNEG, CDTIPPOL, DES_CDTIPPOL, DES_INTIPLAN, CDETISEG, CDNEGMUL, CDTIPBON, PORCONTR, INPOLCON, INPOLPRE 
        FROM BDDLALM.GMM_ELEMENTO_POLIZA_RELLENO
      ) 
        AS ALM_GM_PROD_POLIZA
   ON 
      ALM_GM_PROD_POLIZA.NUM_POLIZA = KCIM06T_POL_CERTIF.CDNUMPOL
      AND ALM_GM_PROD_POLIZA.NUM_VERSION = KCIM06T_POL_CERTIF.CTVRSPOL	
	
--------------------------------------------------------------------

left join (select cdelemen, dselemen from bddlcru.KTCTGET where trim(cdtabla) =  'KTPTGXG') as KTCTGETSEG on  CAST(TRIM(ALM_GM_PROD_POLIZA.CDETISEG) AS STRING) = TRIM(KTCTGETSEG.cdelemen)
left join (select cdelemen, dselemen from bddlcru.KTCTGET where trim(cdtabla) =  'KCDTECG') as KTCTGETMOT on  CAST(TRIM(KCIM06T_POL_CERTIF.INPOLPAI) AS STRING)= TRIM(KTCTGETMOT.cdelemen)
left join (select cdelemen, dselemen from bddlcru.KTCTGET where trim(cdtabla) =  'KTPTGYG') as KTCTGETMUL on  CAST(TRIM(ALM_GM_PROD_POLIZA.CDNEGMUL) AS STRING) = TRIM(KTCTGETMUL.cdelemen)
left join (select cdelemen, dselemen from bddlcru.KTCTGET where trim(cdtabla) =  'KTPTGZG') as KTCTGETBON on  CAST(TRIM(ALM_GM_PROD_POLIZA.CDTIPBON) AS STRING)= TRIM(KTCTGETBON.cdelemen)
left join (select cdelemen, dselemen from bddlcru.KTCTGET where trim(cdtabla) =  'KTCTSNG') as KTCTGETPCO on  CAST(TRIM(ALM_GM_PROD_POLIZA.INPOLCON) AS STRING) = TRIM(KTCTGETPCO.cdelemen)
left join (select cdelemen, dselemen from bddlcru.KTCTGET where trim(cdtabla) =  'KTCTSNG') as KTCTGETPPR on  CAST(TRIM(ALM_GM_PROD_POLIZA.INPOLPRE) AS STRING) = TRIM(KTCTGETPPR.cdelemen)
	
--------------------------------------------------------------------

LEFT JOIN  (
      SELECT DISTINCT 
	         CDPRODCO,
			 CDPRODTE,
			 CDTABLA,
			 CDELEM
	  FROM
	         BDDLALM.ALM_GMM_TALLER_ELEMENTO_POLIZA) AS TALLER2
      ON 

      TRIM(TALLER2.CDPRODCO)= trim(KCIM06T_POL_CERTIF.CDPRODCO)
      AND TRIM(TALLER2.CDPRODTE)= trim(KCIM06T_POL_CERTIF.CDPRODTE)
      AND TRIM(TALLER2.CDELEM)='TCSEGPLA'
	  
--------------------------------------------------------------------  

LEFT JOIN( 
     SELECT DISTINCT
	        TRIM(DSELEMEN) AS DES_SEGMENTO,
			TRIM(CDELEMEN) AS CVE_SEGMENTO,
			TRIM(CDTABLA) AS CDTABLA 
	 FROM 
	        BDDLCRU.KTCTGET 
     WHERE
	        TRIM(CDIDIOMA) = 'ES') AS KGETTCSEGPLA
     ON
	 TRIM(KGETTCSEGPLA.CDTABLA) = TRIM(TALLER2.CDTABLA)
     AND TRIM(KGETTCSEGPLA.CVE_SEGMENTO) = TRIM(ALM_GM_PROD_POLIZA.TCSEGPLA)
	
--------------------------------------------------------------------    

LEFT OUTER JOIN (
    SELECT 
        SUBSTR(DSELEMEN, 2,3) AS CVE_COBERTURA_CONTABLE, SUBSUBCUENTA AS DES_COBERTURA_CONTABLE,
        REGEXP_REPLACE(CDELEMEN,'\\s+','') AS CDPRODCO
    FROM 
        BDDLCRU.KTCTGET, BDDLCRU.CON_CAT_SUBSUBCUENTA
    WHERE 
        TRIM(CDTABLA)='KCIT96S' AND
        TRIM(CDIDIOMA) = 'ES' AND
        SUBSTR(TRIM(CDELEMEN),1,1) ="G"
        AND SUBSTR(DSELEMEN,2,3) = CVE_SUBSUBCUENTA
) AS CAT 
ON 
    CAT.CDPRODCO = 
       CONCAT(
            TRIM(KCIM06T_POL_CERTIF.CDPRODTE),
            TRIM(KCIM06T_POL_CERTIF.CDPRODCO),
            TRIM(ALM_GM_PROD_POLIZA.CDTIPNEG)
            )

----------------------------------------------------------------------

LEFT OUTER JOIN (
    SELECT 
        TRIM(REGEXP_REPLACE(DSELEMEN,'\\s+',' '))  AS DES_TIP_MOV_POL,
        TRIM(REGEXP_REPLACE(CDELEMEN,'\\s+',' ')) AS COD_TIP_MOV_POL_CAT
    FROM 
        BDDLCRU.KTCTGET
    WHERE 
        TRIM(CDTABLA)='KCIT26S' AND CDIDIOMA = 'ES'
) AS CAT_TIP_MOV_POL
ON 
    TRIM(CAT_TIP_MOV_POL.COD_TIP_MOV_POL_CAT) = TRIM(KCIM06T_POL_CERTIF.TCTIMOVP)
              
--------------------------------------------------------------------

LEFT OUTER JOIN 
        BDDLTRN.STG_GMM_KCIM04T_RELLENO AS KCIM04T
ON 
    KCIM04T.NUM_POLIZA06T = KCIM06T_POL_CERTIF.CDNUMPOL 
    AND KCIM04T.NUM_VERSION_POLIZA06T = KCIM06T_POL_CERTIF.CTVRSPOL 

--------------------------------------------------------------------
LEFT OUTER JOIN
    BDDLCRU.KCIM01T
ON
    KCIM01T.CDNUMCON = KCIM06T_POL_CERTIF.CDNUMCON AND
    --KCIM01T.CDCIAGRU = 'SA' AND
    KCIM01T.INULTSIT = 'U'
--------------------------------------------------------------------

LEFT OUTER JOIN
    (
    SELECT
        TRIM(CDELEMEN) AS CDELEMEN,
        SUBSTR(TRIM(DSELEMEN),1,20) AS DSELEMEN 
    from BDDLCRU.KTCTGET
        WHERE TRIM(CDTABLA)='KTPT01S'
        AND TRIM(CDIDIOMA) = 'ES' 
        AND TRIM(CDEMPRES) = '0001' 
        ) AS CONEX
 ON
    CONEX.CDELEMEN = concat(
                        trim(KCIM06T_POL_CERTIF.CDPRODTE),
                        trim(KCIM06T_POL_CERTIF.CDPRODCO))
--------------------------------------------------------------------

LEFT OUTER JOIN (
    SELECT
         cve_homologada AS CVE_ESTATUS,
         des_homologada AS DES_ESTATUS_POLIZA,
         cve_atributo_org
    FROM 
	 bddlalm.cat_homologados
    where
	 nombre_catalogo = 'CAT_ESTATUS_POLIZA'
         and cve_ramo = 'SA'
         and cve_sistema = 'INFO'
) AS CAT_ESTATUS
ON
    CAT_ESTATUS.CVE_ATRIBUTO_ORG = KCIM06T_POL_CERTIF.ESPOLIZA

--------------------------------------------------------------------

LEFT OUTER JOIN (
    SELECT
         cve_homologada AS CVE_FORMA_PAGO,
         des_homologada AS DES_FORMA_PAGO,
         cve_atributo_org
    FROM bddlalm.cat_homologados
    where
         nombre_catalogo = 'CAT_FORMA_PAGO'
         and cve_ramo = 'SA'
         and cve_sistema = 'INFO'
    ) CAT_FORMA_PAGO
ON
    CAT_FORMA_PAGO.CVE_ATRIBUTO_ORG = KCIM06T_POL_CERTIF.TCFORPAG
                    
--------------------------------------------------------------------

LEFT OUTER JOIN (
    SELECT 
        TRIM(REGEXP_REPLACE(DSELEMEN,'\\s+',' '))  AS DES_MOTIV_MOV_POL,
        TRIM(REGEXP_REPLACE(CDELEMEN,'\\s+',' ')) AS COD_MOTIV_MOV_POL_CAT
    FROM 
        BDDLCRU.KTCTGET
    WHERE 
        TRIM(CDTABLA)='KCIT21S' AND CDIDIOMA = 'ES' AND TRIM(CDEMPRES) = '0001'
        
) AS CAT_MOTIV_MOV_POL
ON 
    TRIM(CAT_MOTIV_MOV_POL.COD_MOTIV_MOV_POL_CAT) = CONCAT(
                                                            TRIM(KCIM06T_POL_CERTIF.TCTIMOVP),
                                                            TRIM(KCIM06T_POL_CERTIF.TCMOMOVP))
--------------------------------------------------------------------

LEFT OUTER JOIN (

    SELECT DISTINCT
        UPPER(TRIM(REGEXP_REPLACE(concat(cdnombre,' ',cdapel1,' ',cdapel2),'\\s+',' '))) AS DES_USUARIO,
        TRIM(REGEXP_REPLACE(CDUSUAR,'\\s+',' ')) AS COD_USUARIO_CAT
    FROM 
        BDDLCRU.KAETS1T) CAT_USUARIO
ON 
    TRIM(CAT_USUARIO.COD_USUARIO_CAT) = TRIM(KCIM06T_POL_CERTIF.CDUSUARI)
--------------------------------------------------------------------

LEFT OUTER JOIN (

    SELECT DISTINCT
        UPPER(TRIM(REGEXP_REPLACE(concat(cdnombre,' ',cdapel1,' ',cdapel2),'\\s+',' '))) AS DES_SUSCRIP,
        TRIM(REGEXP_REPLACE(CDUSUAR,'\\s+',' ')) AS COD_SUSCRIP_CAT
    FROM 
        BDDLCRU.KAETS1T) CAT_SUSCRIP
ON 
    TRIM(CAT_SUSCRIP.COD_SUSCRIP_CAT) = TRIM(KCIM06T_POL_CERTIF.CDSUSCRI)
--------------------------------------------------------------------

LEFT OUTER JOIN (

    SELECT 
        TRIM(REGEXP_REPLACE(DSELEMEN,'\\s+',' ')) AS DES_ULTSITU,
        TRIM(REGEXP_REPLACE(CDELEMEN,'\\s+',' ')) AS COD_ULTSITU_CAT
    FROM 
        BDDLCRU.KTCTGET
    WHERE 
        TRIM(CDTABLA) IN ('KCIT46G') AND CDIDIOMA = 'ES' 
    ) CAT_ULTSITU
ON 
    TRIM(CAT_ULTSITU.COD_ULTSITU_CAT) = TRIM(KCIM06T_POL_CERTIF.INULTSIT)
--------------------------------------------------------------------

LEFT OUTER JOIN (

    SELECT 
        TRIM(REGEXP_REPLACE(SUBSTR(DSELEMEN,1,20),'\\s+',' ')) AS DES_CIA_GRUPO,
        TRIM(REGEXP_REPLACE(CDELEMEN,'\\s+',' ')) AS COD_CIAGRU_CAT
    FROM 
        BDDLCRU.KTCTGET
    WHERE 
        TRIM(CDTABLA) IN ('KTCTC1S') AND CDIDIOMA = 'ES' 
    ) CAT_CIAGRU
ON 
    TRIM(CAT_CIAGRU.COD_CIAGRU_CAT) = TRIM(KCIM06T_POL_CERTIF.CDCIAGRU)
--------------------------------------------------------------------

LEFT OUTER JOIN (

    SELECT DISTINCT
        UPPER(TRIM(REGEXP_REPLACE(concat(dnnomrzs,' ',dnap1rzs,' ',dnap2rzs),'\\s+',' '))) AS DES_FILTOM,
        TRIM(REGEXP_REPLACE(CDFILIAC,'\\s+',' ')) AS COD_FILTOM_CAT
    FROM 
        BDDLCRU.KPEM08T) CAT_FILTOM
ON 
    TRIM(CAT_FILTOM.COD_FILTOM_CAT) = TRIM(KCIM06T_POL_CERTIF.CLFILTOM)
    
------------------------------------------------------------------
 LEFT OUTER JOIN (

    SELECT DISTINCT
        UPPER(TRIM(REGEXP_REPLACE(concat(dnnomrzs,' ',dnap1rzs,' ',dnap2rzs),'\\s+',' '))) AS DES_FILPAG,
        TRIM(REGEXP_REPLACE(CDFILIAC,'\\s+',' ')) AS COD_FILPAG_CAT
    FROM 
        BDDLCRU.KPEM08T) CAT_FILPAG
ON 
    TRIM(CAT_FILPAG.COD_FILPAG_CAT) = TRIM(KCIM06T_POL_CERTIF.CLFILPAG)
    
------------------------------------------------------------------

LEFT OUTER JOIN (

    SELECT DISTINCT
        TRIM(REGEXP_REPLACE(DNNEGOCI,'\\s+',' ')) AS DES_NEGOCIO,
        TRIM(REGEXP_REPLACE(CDNEGOCI,'\\s+',' ')) AS CVE_NEGOCIO
    FROM 
        BDDLCRU.KCDM60T WHERE TRIM(INULTSIT)='U') CAT_NEGOCIO
ON 
    TRIM(CAT_NEGOCIO.CVE_NEGOCIO) = TRIM(KCIM01T.CDNEGOCI)
 
------------------------------------------------------------------

LEFT OUTER JOIN (

    SELECT 
        TRIM(REGEXP_REPLACE(DSCANALD,'\\s+',' ')) AS DES_CANAL_DIST,
        TRIM(REGEXP_REPLACE(CDCANALD,'\\s+',' ')) AS COD_CANALD_CAT
    FROM 
        BDDLCRU.KCDM20T) CAT_CANALD
 ON 
    TRIM(CAT_CANALD.COD_CANALD_CAT) = TRIM(KCIM06T_POL_CERTIF.CDCANALD)
 
-------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------- Esta sección es para obtener la información del intermediario cuando la fecha de emisión sea menor a la que se tiene registrado en los catálogos.

LEFT OUTER JOIN (

SELECT 
      CAT2.CLNUMFOL AS CLNUMFOL_min
    , CAT2.DSNOMBRE AS DSNOMBRE_min
    , CAT2.DIRPLZ AS DIRPLZ_min
    , CAT2.CDEDOPLZ AS CDEDOPLZ_min
    , CAT2.AM_PROC AS AM_PROC_min
    , PLZ.DSDIRPLZ AS DSDIRPLZ_min
    , EDOPLZ.DSEDOPLZ AS DSEDOPLZ_min
    , CAT2.REDTERIN AS REDTERIN_min
    , DA.SAZONAP AS SAZONAP_min
    , CAT2.GRZCVE AS GRZCVE_min
    , CAT2.OFNCVE AS OFNCVE_min
FROM 
   ( SELECT   
             CLNUMFOL
             ,DSNOMBRE
             ,DIRPLZ
             ,A.AM_PROC
             ,CDEDOPLZ
             ,REDTERIN
             ,GRZCVE
             ,OFNCVE
     FROM (
                SELECT 
                   CLNUMFOL, 
                   DSNOMBRE,
                   DIRPLZ,
                   CDEDOPLZ,
                   AM_PROC, 
                   REDTERIN,
                   GRZCVE,
                   OFNCVE,
                   RANK () OVER(PARTITION BY CLNUMFOL ORDER BY AM_PROC ASC)  AS POSICION 
                FROM
                   BDDLCRU.CAT_CAGENTE2
                WHERE
                   CLNUMFOL IS NOT NULL 
           ) A 
     WHERE 
           POSICION=1
     ) CAT2
    LEFT OUTER JOIN 
         BDDLCRU.CAT_CDDIRPLZ PLZ 
    ON 
         CAT2.DIRPLZ = PLZ.CDDIRPLZ
         
    LEFT OUTER JOIN
         BDDLCRU.CAT_CDEDOPLZ EDOPLZ
    ON
         CAT2.CDEDOPLZ = EDOPLZ.CDEDOPLZ
         
  LEFT OUTER JOIN
         BDDLCRU.CAT_CCATDA DA
    ON
         CAT2.GRZCVE = DA.ZONAP

)  OBTIENE_AGENTE_MIN

ON
    KCIM04T.NUM_FOL2 = 
    CONCAT(
            'P',
            LPAD(CAST(OBTIENE_AGENTE_MIN.CLNUMFOL_min AS STRING),7,'0')
        ) 
-------------------------------------------------------------------------------------

LEFT OUTER JOIN
    BDDLCRU.CAT_CAGENTE2
ON
    KCIM04T.NUM_FOL2 = 
    CONCAT(
            'P',
            LPAD(CAST(CAT_CAGENTE2.CLNUMFOL AS STRING),7,'0')
        ) 
    AND
    CAST(SUBSTRING(CAST(KCIM06T_POL_CERTIF.FEMISION AS STRING),1,6)AS INT) = CAT_CAGENTE2.AM_PROC
--------------------------------------------------------------------
     
LEFT OUTER JOIN
    BDDLCRU.CAT_CDDIRPLZ
ON
    CAT_CAGENTE2.DIRPLZ = CAT_CDDIRPLZ.CDDIRPLZ
--------------------------------------------------------------------
LEFT OUTER JOIN
    BDDLCRU.CAT_CDEDOPLZ
ON
    CAT_CAGENTE2.CDEDOPLZ = CAT_CDEDOPLZ.CDEDOPLZ
--------------------------------------------------------------------
LEFT OUTER JOIN
    BDDLCRU.CAT_CCATDA
ON
    CAT_CAGENTE2.GRZCVE = CAT_CCATDA.ZONAP
--------------------------------------------------------------------
LEFT OUTER JOIN
    BDDLCRU.CAT_CPLANES
ON
    TRIM(ALM_GM_PROD_POLIZA.INTIPLAN) = TRIM(CAT_CPLANES.CDTIPLAN)
--------------------------------------------------------------------   

LEFT OUTER JOIN (
    SELECT 
        TRIM(REGEXP_REPLACE(CDELEMEN,'\\s+',' ')) AS CVE_SUBRAMO_ADMON,
        TRIM(REGEXP_REPLACE(DSELEMEN,'\\s+',' '))  AS DES_SUBRAMO_ADMON,
        TRIM(CDELEMEN) AS CVE_JOIN
    FROM 
        BDDLCRU.KTCTGET
    WHERE 
        TRIM(CDTABLA)='KTOT89S' AND
        TRIM(CDIDIOMA) = 'ES'
) AS CAT_SUBRAMO_ADMON
ON 
    CAT_SUBRAMO_ADMON.CVE_JOIN= 
       CONCAT(
            RPAD(TRIM(KCIM06T_POL_CERTIF.CDPRODTE),10,' '),
            RPAD(TRIM(KCIM06T_POL_CERTIF.CDPRODCO),10,' '),
            RPAD(TRIM(ALM_GM_PROD_POLIZA.INTIPLAN),5,' '),
            TRIM(ALM_GM_PROD_POLIZA.CDTIPNEG)
            )
    
 WHERE KCIM06T_POL_CERTIF.CDCIAGRU = 'SA' 
--------------------- DELTA --------------------------------------------------------------------------------------------------------------------------------------------
--AND CAST(SUBSTRING(NVL(CAST(KCIM06T_POL_CERTIF.FEMISION AS STRING),''),1,6) AS INT) in (201610,201812,201909)  
--------------------- DELTA --------------------------------------------------------------------------------------------------------------------------------------------
;

