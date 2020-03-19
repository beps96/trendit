DROP TABLE IF EXISTS BDDLTRN.STG_GMM_CNM_POLIZA_CBZA PURGE;
CREATE TABLE IF NOT EXISTS BDDLTRN.STG_GMM_CNM_POLIZA_CBZA AS
SELECT DISTINCT 
    NVL( LPAD(SDPOLIZA.POLORI_POLIZA,8,'0') ,'') AS NUM_POLIZA,
    -999 AS VIGENCIA_POLIZA, 
    -999 AS NUM_VERSION_POLIZA,
    NVL( CONCAT(LPAD(CAST(COALESCE(SDPOLIZA.OFNA_POLIZA ,0) AS VARCHAR(2)),2,'0'), LPAD(CAST(COALESCE(SDPOLIZA.POLIZA_POLIZA,0) AS VARCHAR(6)),6,'0')) , '') AS NUM_POLIZA_COBRANZA,
    'CBZA' AS CVE_SISTEMA,
    'CBZA' AS CVE_SISTEMA_INT,
    CAT_SUBRAMO.SUBSUBCO AS CVE_COBERTURA_CONTABLE,  
    CASE WHEN NVL(CAT_SUBRAMO.SUBSUBCO,'') <> '' AND CAT.CVE_COBERTURA_CONTABLE IS NULL 
        THEN 
            CONCAT('Descripción no encontrada para: ',NVL(CAT_SUBRAMO.SUBSUBCO,''))  
        ELSE 
            NVL(CAT.DES_COBERTURA_CONTABLE,'') 
    END AS DES_COBERTURA_CONTABLE,  
    NVL(CAT_ESTATUS.CVE_ESTATUS,'') AS CVE_ESTATUS_POLIZA,
    CASE WHEN NVL(CAST(SDPOLIZA.STATUS_POLIZA AS STRING),'') <> '' AND CAT_ESTATUS.CVE_ESTATUS IS NULL 
        THEN 
            CONCAT('Descripción no encontrada para: ', CAST(SDPOLIZA.STATUS_POLIZA AS STRING)) 
        ELSE 
            NVL(CAT_ESTATUS.DES_ESTATUS_POLIZA,'') 
    END AS DES_ESTATUS_POLIZA,    
    NVL(CAT_FORMA_PAGO.CVE_FORMA_PAGO,'') AS CVE_FORMA_PAGO, 
    CASE WHEN NVL(CAST(SDPOLIZA.FORMAPAGO_POLIZA AS STRING),'') <> '' AND CAT_FORMA_PAGO.CVE_FORMA_PAGO IS NULL 
        THEN 
            CONCAT('Descripción no encontrada para: ',CAST(SDPOLIZA.FORMAPAGO_POLIZA AS STRING)) 
        ELSE 
            CAT_FORMA_PAGO.DES_FORMA_PAGO 
    END AS DES_FORMA_PAGO, 
    CASE WHEN SDPOLIZA_PORC_PART1 >= SDPOLIZA_PORC_PART2 
        THEN 
            CASE WHEN SDPOLIZA_PORC_PART1 >= SDPOLIZA_PORC_PART3 THEN SDPOLIZA_AGT1 ELSE SDPOLIZA_AGT3 END
        ELSE 
            CASE WHEN SDPOLIZA_PORC_PART2 >= SDPOLIZA_PORC_PART3 THEN SDPOLIZA_AGT2 ELSE SDPOLIZA_AGT3 END
    END  AS CVE_INTERMEDIARIO_PRINCIPAL,    
    IF(CAT_CAGENTE2.DSNOMBRE IS NULL,
       --THEN
          IF(cast(SUBSTRING(cast(SDPOLIZA.FECHEMIPOLIZA as string),1,6) as int) < AM_PROC_min,
             --THEN
               NVL(DSNOMBRE_min,''),
             --ELSE
               CONCAT('Descripción no encontrada para: ',     
                    CASE WHEN SDPOLIZA_PORC_PART1 >= SDPOLIZA_PORC_PART2 
                        THEN 
                            CASE WHEN SDPOLIZA_PORC_PART1 >= SDPOLIZA_PORC_PART3 THEN CAST(SDPOLIZA_AGT1 AS STRING) ELSE CAST(SDPOLIZA_AGT3 AS STRING) END
                        ELSE 
                            CASE WHEN SDPOLIZA_PORC_PART2 >= SDPOLIZA_PORC_PART3 THEN CAST(SDPOLIZA_AGT2 AS STRING) ELSE CAST(SDPOLIZA_AGT3 AS STRING) END
               END
               )
             ),
       --ELSE
            NVL(CAT_CAGENTE2.DSNOMBRE,'')
    ) AS NOMBRE_INTERMEDIARIO_PRINCIPAL, 
    '' AS NUM_CONTRATO,
    -999 AS NUM_VERSION_CONTRATO, 
    '' AS CVE_NEGOCIO, 
    '' AS DES_NEGOCIO,    
    CASE WHEN SDPOLIZA_PORC_PART1 >= SDPOLIZA_PORC_PART2 THEN 
         CASE WHEN SDPOLIZA_PORC_PART1 >= SDPOLIZA_PORC_PART3 THEN CONCAT('P',LPAD(CAST(SDPOLIZA_AGT1 AS STRING),7,'0')) ELSE CONCAT('P',LPAD(CAST(SDPOLIZA_AGT3 AS STRING),7,'0')) END
    ELSE 
         CASE WHEN SDPOLIZA_PORC_PART2 >= SDPOLIZA_PORC_PART3 THEN CONCAT('P',LPAD(CAST(SDPOLIZA_AGT2 AS STRING),7,'0')) ELSE CONCAT('P',LPAD(CAST(SDPOLIZA_AGT3 AS STRING),7,'0')) END
    END AS FOLIO_INTERMEDIARIO_PRINCIPAL,  
    IF( SDPOLIZA.FECHINIVIG_POLIZA IS NULL or SDPOLIZA.FECHINIVIG_POLIZA=0 ,
         cast('1400-01-01 00:00:00' as timestamp), 
         cast(from_unixtime(
              unix_timestamp(
                   cast(SDPOLIZA.FECHINIVIG_POLIZA as varchar(8)), 'yyyyMMdd')) as timestamp)
    ) AS FCH_INI_VIGENCIA,
    IF( SDPOLIZA.FECHTERVIG_POLIZA is null or SDPOLIZA.FECHTERVIG_POLIZA=0 ,
         cast('1400-01-01 00:00:00' as timestamp), 
         cast(from_unixtime(
              unix_timestamp(
                   cast(SDPOLIZA.FECHTERVIG_POLIZA as varchar(8)), 'yyyyMMdd')) as timestamp)
    ) as FCH_FIN_VIGENCIA,
    '' AS CVE_CONTRATANTE,
    NVL(TRIM(SDPOLIZA.RFC_POLIZA),'') AS RFC_CONTRATANTE,
    NVL(TRIM(SDPOLIZA.NOMBRE_POLIZA),'') AS NOMBRE_CONTRATANTE,
    '' AS CVE_CANAL_VENTA,
    '' AS DES_CANAL_VENTA,
    '' AS CVE_TIPO_POLIZA,
    '' AS DES_TIPO_POLIZA,	 
    '' AS CVE_PROD_TECNICO, 
    '' AS CVE_PROD_COMERCIAL,
    '' DES_PROD_TECNICO_COMERCIAL,
    CAST('1400-01-01 00:00:00' as TIMESTAMP) AS FCH_EFECTO_MOVIMIENTO, 
    CAST('1400-01-01 00:00:00' as TIMESTAMP) AS FCH_FIN_MOVIMIENTO,
    CAST('1400-01-01 00:00:00' as TIMESTAMP) AS FCH_INI_POLIZA, 
    IF( SDPOLIZA.FECHEMIPOLIZA is null or SDPOLIZA.FECHEMIPOLIZA=0 ,
        cast('1400-01-01 00:00:00' as timestamp), 
        cast(from_unixtime(
              unix_timestamp(
                   cast(SDPOLIZA.FECHEMIPOLIZA as varchar(8)), 'yyyyMMdd')) as timestamp)
    ) as FCH_EMISION,
    NVL( CAST(SUBSTRING(NVL(CAST(SDPOLIZA.FECHEMIPOLIZA AS STRING),''),1,6) AS INT) ,140000) AS AM_EMISION, 
    NVL( CAST(SDPOLIZA.MOTVOST_POLIZA AS STRING),'') AS CVE_MOTIVO_ESTATUS_POLIZA,  
    CASE WHEN NVL(CAST(SDPOLIZA.MOTVOST_POLIZA AS STRING),'') <> '' AND CAT_MOTIVO_ESTATUS_POLIZA.CVE_MOTIVO_ESTATUS_POLIZA IS NULL 
        THEN 
            CONCAT('Descripción no encontrada para: ',CAST(SDPOLIZA.MOTVOST_POLIZA AS STRING)) 
        ELSE 
            CAT_MOTIVO_ESTATUS_POLIZA.DES_MOTIVO_ESTATUS_POLIZA 
    END AS DES_MOTIVO_ESTATUS_POLIZA,    
    IF( SDPOLIZA.FECHST_POLIZA is null or SDPOLIZA.FECHST_POLIZA=0 ,
        cast('1400-01-01 00:00:00' as timestamp), 
        cast(from_unixtime(unix_timestamp(cast(SDPOLIZA.FECHST_POLIZA as varchar(8)), 'yyyyMMdd')) as timestamp)
    ) as FCH_ESTATUS_POLIZA, 
    CASE WHEN SDPOLIZA.STATUS_POLIZA = 2 AND SDPOLIZA.MOTVOST_POLIZA = 5 
        THEN 
            'SI' 
        ELSE 
            '' 
    END AS BAN_ANULACION_FALTA_PAGO, 
    CASE WHEN SDPOLIZA.STATUS_POLIZA = 2 AND SDPOLIZA.MOTVOST_POLIZA = 5 
        THEN 
            cast(from_unixtime(unix_timestamp(cast(SDPOLIZA.FECHST_POLIZA as varchar(8)), 'yyyyMMdd')) as timestamp) 
        ELSE 
            cast('1400-01-01 00:00:00' as timestamp)
    END AS FCH_ANULACION_FALTA_PAGO, 
    CASE WHEN SDPOLIZA.STATUS_POLIZA = 2 AND SDPOLIZA.MOTVOST_POLIZA = 5 
        THEN 
            SUBSTRING(CAST(SDPOLIZA.FECHST_POLIZA AS STRING),1,6) 
        ELSE 
            '140001' 
    END AS AM_ANULACION_FALTA_PAGO, 
    '' AS CVE_TIPO_MOVIMIENTO,
    '' AS DES_TIPO_MOVIMIENTO,
    '' AS CVE_ULTIMA_SITUACION,
    '' AS DES_ULTIMA_SITUACION,
    '' AS CVE_INDICADOR_POLIZA_PADRE_HIJA,
    '' AS DES_INDICADOR_POLIZA_PADRE_HIJA,
    '' AS NUM_POLIZA_PADRE,
    -999 AS NUM_VERSION_POLIZA_PADRE,
    '' AS CVE_PAGADOR, 
    '' AS NOMBRE_PAGADOR,
    NVL(SUBSTRING(CAST(SDPOLIZA.FECHTERVIG_POLIZA AS STRING),1,6),'140001') AS AM_FIN_VIGENCIA, 
    NVL( TRIM(SDPOLIZA.POOL_POLIZA),TRIM(POLIZA_AZUL.CVE_POOL) ) AS CVE_POOL,
    NVL( CAT_SUBRAMO_ADMINISTRATIVO.CVE_SUBRAMO_ADMON ,'') AS CVE_SUBRAMO_ADMON,  
    CASE WHEN CAST(CONCAT(CAST(SDPOLIZA.RAMO_POLIZA AS STRING),LPAD(CAST(SDPOLIZA.SUBRAMO_POLIZA AS STRING) ,2,'0')) AS DECIMAL(3,0)) IS NOT NULL AND CAT_SUBRAMO_ADMINISTRATIVO.CVE_SUBRAMO_ADMON IS NULL 
         THEN 
              CONCAT('Descripción no encontrada para: ',CONCAT(CAST(SDPOLIZA.RAMO_POLIZA AS STRING),LPAD(CAST(SDPOLIZA.SUBRAMO_POLIZA AS STRING) ,2,'0') ) )  
         ELSE 
              NVL(SUBSTR(CAT_SUBRAMO_ADMINISTRATIVO.DES_SUBRAMO_ADMON,5,55),'') 
    END AS DES_SUBRAMO_ADMON, 
    SDPOLIZA.PLAN_POLIZA AS CVE_PLAN,  
    CASE WHEN SDPOLIZA.PLAN_POLIZA IS NOT NULL AND GAZ0_TAB_PLANES.PLA_PLAN IS NULL 
        THEN 
            CONCAT('Descripción no encontrada para: ',CAST(SDPOLIZA.PLAN_POLIZA AS STRING)) 
        ELSE 
            NVL(GAZ0_TAB_PLANES.PLA_NOMBRE,'') 
    END AS DES_PLAN,
    '' AS IND_CONEXION,
    '' AS CVE_USUARIO, 
    '' AS DES_USUARIO, 
    '' AS CVE_SUSCRIPTOR,
    '' AS DES_SUSCRIPTOR,  
    NVL(CAT_CPLANES.NIVHOS,'') AS NIVEL_HOSPITAL,
    NVL(CAT_CPLANES.PLNDSC,'') AS GAMA_POLIZA, 
    NVL(CAT_CPLANES.PLNTPO,'') AS PLAN_GAMA_NIVEL_POLIZA,  
    NVL(CAT_CPLANES.CLSPLN,'') AS TERRITORIALIDAD_POLIZA, 
    SDPOLIZA.RAMO_POLIZA AS CVE_RAMO,
    'G.M.M.' AS DES_RAMO,
    CAST('1400-01-01 00:00:00' as TIMESTAMP) AS FCH_CAMBIO_ESTADO, 
    '' AS CANAL_VENTA_ESTADISTICO, 
    '' AS SEGMENTO_VENTA_ESTADISTICO,

--<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
 
    NVL( CAST( IF(CAT_CAGENTE2.DIRPLZ IS NULL,
       --THEN
          IF(cast(SUBSTRING(cast(SDPOLIZA.FECHEMIPOLIZA as string),1,6) as int) < AM_PROC_min,
             --THEN
               DIRPLZ_min,
             --ELSE
               NULL
             ),
       --ELSE
            CAT_CAGENTE2.DIRPLZ
    ) AS STRING) , '') AS CVE_DIR_PLAZA,
         
    IF(CAT_CDDIRPLZ.DSDIRPLZ IS NULL,
       --THEN
          IF(cast(SUBSTRING(cast(SDPOLIZA.FECHEMIPOLIZA as string),1,6) as int) < AM_PROC_min,
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
          IF(cast(SUBSTRING(cast(SDPOLIZA.FECHEMIPOLIZA as string),1,6) as int) < AM_PROC_min,
             --THEN
               CDEDOPLZ_min,
             --ELSE
               NULL
             ),
       --ELSE
            CAT_CAGENTE2.CDEDOPLZ
    ) AS STRING), '') AS CVE_EDO_DIR_PLAZA,
       
    IF(CAT_CDEDOPLZ.DSEDOPLZ IS NULL,
       --THEN
          IF(cast(SUBSTRING(cast(SDPOLIZA.FECHEMIPOLIZA as string),1,6) as int) < AM_PROC_min,
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
          IF(cast(SUBSTRING(cast(SDPOLIZA.FECHEMIPOLIZA as string),1,6) as int) < AM_PROC_min,
             --THEN
               GRZCVE_min,
             --ELSE
               NULL
             ),
       --ELSE
            CAT_CAGENTE2.GRZCVE
    ) AS STRING), '') AS CVE_DIRECCION_AGENCIA,
       
    IF(CAT_CCATDA.SAZONAP IS NULL,
       --THEN
          IF(cast(SUBSTRING(cast(SDPOLIZA.FECHEMIPOLIZA as string),1,6) as int) < AM_PROC_min,
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
          IF(cast(SUBSTRING(cast(SDPOLIZA.FECHEMIPOLIZA as string),1,6) as int) < AM_PROC_min,
             --THEN
               OFNCVE_min,
             --ELSE
               NULL
             ),
       --ELSE
            CAT_CAGENTE2.OFNCVE
    )  AS STRING) , '') AS CVE_OFICINA_INTERMEDIARIO,
    
    IF(CAT_CAGENTE2.REDTERIN IS NULL,
       --THEN
          IF(cast(SUBSTRING(cast(SDPOLIZA.FECHEMIPOLIZA as string),1,6) as int) < AM_PROC_min,
             --THEN
               REDTERIN_min,
             --ELSE
               ''
             ),
       --ELSE
            CAT_CAGENTE2.REDTERIN
    ) AS RED_TERRITORIAL,


-->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

    NVL(TRIM(EMAZ.segmento), '') AS CVE_SEGMENTO,
    NVL(TRIM(EMAZ.desc_segmento), '') AS DES_SEGMENTO,
    NVL(TRIM(EMAZ.negocio), '') AS CVE_NEGOCIO_MULTINACIONAL,
    NVL(TRIM(EMAZ.desc_negocio), '') AS DES_NEGOCIO_MULTINACIONAL,
    NVL(TRIM(EMAZ.bonos), '') AS CVE_TIPO_BONO,
    NVL(TRIM(EMAZ.desc_bonos), '') AS DES_TIPO_BONO,
    NVL(TRIM(EMAZ.plan_incentivos), '') AS AFECTO_PLAN_INCENTIVO,
    NVL(TRIM(EMAZ.desc_pol_contr), '') AS BAN_POLIZA_CONTRIBUTORIA,
    NVL(CAST(EMAZ.contrb as int),0) AS PCT_CONTRIBUCION,
    NVL(TRIM(EMAZ.desc_pol_prest), '') AS BAN_POLIZA_PRESTACION,
    NVL(TRIM(EMAZ.mot_exc), '') AS CVE_MOTIVO_EXCLUSION_PLAN_INCENTIVO,
    NVL(TRIM(EMAZ.desc_mot_exc), '') AS DES_MOTIVO_EXCLUSION_PLAN_INCENTIVO

FROM    BDDLCRU.SDPOLIZA


--------------------- DELTA ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------- SE DEBE COMENTAR AL CARGAR LA TABLA COMPLETA  ---------
--INNER JOIN (SELECT MAX(FECHAAUD) AS FECHAAUD FROM BDDLCRU.SDPOLIZA) AS SDPOLIZA2 ON TO_DATE(SDPOLIZA.FECHAAUD) = TO_DATE(SDPOLIZA2.FECHAAUD) --> Delta
--------------------- DELTA ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------


----------------------------------------------------------------------------------
    LEFT JOIN BDDLAPR.CNM_POLIZA_AZUL POLIZA_AZUL  -- se deberá cambiar al liberar a producción
--      LEFT JOIN bddldes.cnm_poliza_azul_TEST POLIZA_AZUL
        ON NVL(LPAD(sdpoliza.polori_poliza,8,'0'),'') = POLIZA_AZUL.NUM_POLIZA 
        AND NVL( CONCAT(LPAD(CAST(COALESCE(sdpoliza.OFNA_POLIZA ,0) AS VARCHAR(2)),2,'0'), LPAD(CAST(COALESCE(sdpoliza.POLIZA_POLIZA,0) AS VARCHAR(6)),6,'0')) , '') = POLIZA_AZUL.NUM_POLIZA_COBRANZA 
    
----------------------------------------------------------------------------------

    LEFT OUTER JOIN 
        BDDLCRU.CAT_SUBRAMO
    ON  CAT_SUBRAMO.PSRAMCOB = CAST(SDPOLIZA.SUBRAMO_POLIZA AS INT) 
    
----------------------------------------------------------------------------------

    LEFT OUTER JOIN (
    SELECT 
        TRIM(CVE_SUBSUBCUENTA) AS CVE_COBERTURA_CONTABLE, SUBSUBCUENTA AS DES_COBERTURA_CONTABLE
    FROM 
        BDDLCRU.CON_CAT_SUBSUBCUENTA
    WHERE 
        TRIM(CVE_UNIDAD_NEGOCIO)='G'
    ) AS CAT 
    ON 
    TRIM(CAT.CVE_COBERTURA_CONTABLE) = TRIM(CAT_SUBRAMO.SUBSUBCO)

----------------------------------------------------------------------------------

    LEFT OUTER JOIN (
    	SELECT
            cve_homologada AS CVE_FORMA_PAGO,
            des_homologada AS DES_FORMA_PAGO,
            cve_atributo_org
	FROM 
	    bddlalm.cat_homologados
	where
	    nombre_catalogo = 'CAT_FORMA_PAGO'
            and cve_ramo = 'SA'
            and cve_sistema = 'CBZA'
    ) CAT_FORMA_PAGO
    ON  CAST(CAT_FORMA_PAGO.CVE_ATRIBUTO_ORG AS DECIMAL(2,0)) = SDPOLIZA.FORMAPAGO_POLIZA
    
----------------------------------------------------------------------------------

    
    LEFT OUTER JOIN
        BDDLCRU.CAT_CPLANES
    ON CAST(SDPOLIZA.PLAN_POLIZA AS STRING) = CAT_CPLANES.CDTIPLAN 
    
----------------------------------------------------------------------------------

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
            and cve_sistema = 'CBZA'
	) AS CAT_ESTATUS
	ON
	cast(CAT_ESTATUS.CVE_ATRIBUTO_ORG as decimal(2,0)) = SDPOLIZA.STATUS_POLIZA
	
----------------------------------------------------------------------------------
	
    LEFT OUTER JOIN 
        BDDLCRU.CAT_CAGENTE2
    ON CASE WHEN SDPOLIZA_PORC_PART1 >= SDPOLIZA_PORC_PART2 THEN 
         CASE WHEN SDPOLIZA_PORC_PART1 >= SDPOLIZA_PORC_PART3 THEN SDPOLIZA_AGT1 ELSE SDPOLIZA_AGT3 END
    ELSE 
         CASE WHEN SDPOLIZA_PORC_PART2 >= SDPOLIZA_PORC_PART3 THEN SDPOLIZA_AGT2 ELSE SDPOLIZA_AGT3 END
    END = CAT_CAGENTE2.CLNUMFOL
    AND CAST(SUBSTRING(CAST(SDPOLIZA.FECHEMIPOLIZA AS STRING),1,6)AS INT) = CAT_CAGENTE2.AM_PROC 

----------------------------------------------------------------------------------
       
    LEFT OUTER JOIN
        BDDLCRU.CAT_CDDIRPLZ
    ON CAT_CAGENTE2.DIRPLZ = CAT_CDDIRPLZ.CDDIRPLZ 
    
----------------------------------------------------------------------------------

    LEFT OUTER JOIN
        BDDLCRU.CAT_CDEDOPLZ
    ON CAT_CAGENTE2.CDEDOPLZ = CAT_CDEDOPLZ.CDEDOPLZ 
     
----------------------------------------------------------------------------------

    LEFT OUTER JOIN
        BDDLCRU.CAT_CCATDA
    ON CAT_CCATDA.ZONAP = CAT_CAGENTE2.GRZCVE
    
----------------------------------------------------------------------------------
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
    --SDPOLIZA.SDPOLIZA_AGT1 = 
    CASE WHEN SDPOLIZA_PORC_PART1 >= SDPOLIZA_PORC_PART2 THEN 
         CASE WHEN SDPOLIZA_PORC_PART1 >= SDPOLIZA_PORC_PART3 THEN SDPOLIZA_AGT1 ELSE SDPOLIZA_AGT3 END
    ELSE 
         CASE WHEN SDPOLIZA_PORC_PART2 >= SDPOLIZA_PORC_PART3 THEN SDPOLIZA_AGT2 ELSE SDPOLIZA_AGT3 END
    END = 
    OBTIENE_AGENTE_MIN.CLNUMFOL_min

-------------------------------------------------------------------------------------
    LEFT OUTER JOIN
        BDDLCRU.GAZ0_TAB_PLANES
    ON SDPOLIZA.PLAN_POLIZA = GAZ0_TAB_PLANES.PLA_PLAN 

-------------------------------------------------------------------------------------	
LEFT OUTER JOIN
        (
        SELECT 
           CVE_ATRIBUTO AS CVE_MOTIVO_ESTATUS_POLIZA,
           DES_ATRIBUTO AS DES_MOTIVO_ESTATUS_POLIZA
        FROM BDDLCRU.CAT_SISTEMA_AZUL
        WHERE NOMBRE_CATALOGO = 'CAT_MOTIVO_ESTATUS_POLIZA'
          AND CVE_RAMO = 'SA'
          AND CVE_SISTEMA = 'CBZA'
        ) CAT_MOTIVO_ESTATUS_POLIZA
ON CAST(CAT_MOTIVO_ESTATUS_POLIZA.CVE_MOTIVO_ESTATUS_POLIZA AS DECIMAL(2,0)) = 
    CASE WHEN SDPOLIZA.MOTVOST_POLIZA = 9 AND TRIM(SDPOLIZA.INDAFECBONO_POLIZA) = 'S' 
        THEN 
            11
        ELSE 
            SDPOLIZA.MOTVOST_POLIZA
    END  

-------------------------------------------------------------------------------------	

    LEFT OUTER JOIN (
	SELECT
            cve_homologada AS CVE_SUBRAMO_ADMON,
            des_homologada AS DES_SUBRAMO_ADMON,
            cve_atributo_org
	FROM 
	    bddlalm.cat_homologados
	where
	    nombre_catalogo = 'CAT_CLAVE_SUBRAMO_ADMINISTRATIVO_POLIZA'
            and cve_ramo = 'SA'
            and cve_sistema = 'CBZA'
	) AS CAT_SUBRAMO_ADMINISTRATIVO
	ON
	cast(CAT_SUBRAMO_ADMINISTRATIVO.CVE_ATRIBUTO_ORG as decimal(3,0)) = CAST(CONCAT(CAST(SDPOLIZA.RAMO_POLIZA AS STRING),LPAD(CAST(SDPOLIZA.SUBRAMO_POLIZA AS STRING) ,2,'0')) AS DECIMAL(3,0))
	
-------------------------------------------------------------------------------------	
--etiquetado azul
      LEFT JOIN 
        (
            select POLIZA, cobranza, '' as  SEGMENTO, NVL(SEGMENTO, '') AS desc_segmento, '' as NEGOCIO, NVL(NEGOCIO, '') AS desc_negocio, '' AS BONOS, NVL(BONOS, '') as desc_bonos, NVL(PLAN_INCENTIVOS, '') AS PLAN_INCENTIVOS, '' as POL_CONTR, NVL(POL_CONTR, '') AS desc_POL_CONTR, NVL(CONTRB, '') AS CONTRB, '' as POL_PREST, NVL(POL_PREST, '') AS desc_pol_prest, '' as mot_exc, '' as desc_mot_exc from BDDLCRU.EMAZETIQ 
        ) EMAZ 
        on  NVL(LPAD(SDPOLIZA.POLORI_POLIZA ,8,'0'),'') = trim(EMAZ.POLIZA) and Cast(CONCAT(LPAD(CAST(COALESCE(SDPOLIZA.OFNA_POLIZA ,0) AS VARCHAR(2)),2,'0'), LPAD(CAST(COALESCE(SDPOLIZA.POLIZA_POLIZA,0) AS VARCHAR(6)),6,'0')) as varchar(8)) =  trim(EMAZ.cobranza)    

   
---------------------------------------------------------------------------------------------------------
-------  SE DEBE ELIMINAR AL MOMENTO DE LIBERAR 
--WHERE CAST(SUBSTRING(NVL(CAST(SDPOLIZA.FECHEMIPOLIZA AS STRING),''),1,6) AS INT) in (201610,201812,201909)  
---------------------------------------------------------------------------------------------------------
	;