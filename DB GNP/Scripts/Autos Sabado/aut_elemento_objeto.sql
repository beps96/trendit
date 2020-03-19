-----------------------------------------------------------------------------
----#####################---AUT_ELEMENTO_OBJETO_TP---###################-----
---JCCG 7/08/2018
-----------------------------------------------------------------------------
--####
--PASO 1---------------------------------------------------------------------
-- OBTIENE LOS DESCRIPTIVOS DE ALGUNOS ELEMENTOS DE LA TABLA KTCTGET
--TAKE 5MIN.
-- aut_elemento_objeto.sql
-----------------------------------------------------------------------------

DROP TABLE if exists BDDLALM.AUT_ELEMENTO_OBJETO_TMP;

INVALIDATE METADATA;

CREATE TABLE BDDLALM.AUT_ELEMENTO_OBJETO_TMP STORED AS PARQUET 
AS
 SELECT
 A.NUM_POLIZA               AS NUM_POLIZA
,A.NUM_VERSION              AS NUM_VERSION
,CAST(LEAD(NUM_VERSION-1) OVER(PARTITION BY NUM_POLIZA ORDER BY NUM_VERSION) AS SMALLINT) AS NUM_VERSION_NEXT
,A.CVE_PROD_VENTA           AS CVE_PROD_VENTA
,A.CVE_PROD_TECNICO         AS CVE_PROD_TECNICO
,A.CVE_PROD_COMERCIAL       AS CVE_PROD_COMERCIAL
,A.ID_OBJETO                AS ID_OBJETO                              /*ID OBJETO                     */
,A.NONOMBRE                 AS NOMBRE_CONDUCTOR                       /*NOMBRE DEL CONDUCTOR                              */
,A.CDGIROES                 AS CVE_GIRO_NEGOCIO_ASEGURADO             /*GIRO DEL NEGOCIO ASEGURADO                        */
,A.INALTRIE                 AS CVE_VEHICULO_ALTO_RIESGO               /*INDICADOR DE VEHICULO DE ALTO RIESGO              */
,A.CDFILIAC                 AS CVE_FILIACION                          /*CODIGO DE FILIACION                               */
,A.VAMODELO                 AS MODELO_VEHICULO                        /*MODELO VEHICULO                                   */
,A.VAVEHCON                 AS VALOR_CONVENIDO_VEHICULO               /*VALOR CONVENIDO DE VEHICULO                       */
,A.TCARMADO                 AS CVE_ARMADORA_VEHICULO                  /*ARMADORA DEL VEHICULO                             */
,A.VAVEHIC                  AS VALOR_VEHICULO                         /*VALOR VEHICULO                                    */
,A.DSDCAMPA                 AS DESC_CAMPANIA                          /*DESCRIPCION CAMPANIA                              */
,A.INEXALTR                 AS CVE_EXCLUSION_ALTO_RIESGO              /*INDICADOR DE EXCLUSION DE ALTO RIESGO             */
,A.TCEDOCIR                 AS CVE_ENTIDAD_CIRCULACION                /*ENTIDAD DONDE CIRCULA                             */
,A.CDZONA                   AS CVE_ZONA                             /*CODIGO ZONA                                         */  
,A.CPMONBON                 AS MTO_BONOS                              /*MONTO POR BONOS                                   */
--,A.CDPOS                    AS CVE_CODIGO_POSTAL                      /*CODIGO POS                                      */   -- 20190423 Este elemento se quita no trae la inf deseada - a Peticion de Anaximandro 
,A.NUMATVCO                 AS NUM_PLACAS_VEHICULO                    /*PLACAS DEL VEHICULO                               */
,A.TCCARROC                 AS CVE_CARROCERIA_VEHICULO                /*CARROCERIA DEL AUTOMOVIL                          */
,A.PODESCUP                 AS PTC_DESCUENTO_CUPONES                  /*PORCENTAJE DESCUENTO CUPONES                      */
,A.DSNUMSER                 AS NUM_SERIE                              /*NUMERO DE SERIE                                   */
,A.INSEXOCN                 AS CVE_GENERO_SEXUAL_CONDUCTOR            /*GENERO SEXUAL DEL CONDUCTOR                       */
,A.CDCANAL                  AS CVE_CANAL_VENTA                        /*CANAL DE VENTA                                    */
,A.INCLIVIP                 AS CVE_CLIENTE_VIP                        /*CLIENTE VIP                                       */
,A.DSNUMMOT                 AS NUM_MOTOR                              /*NUMERO DE MOTOR                                   */
,A.NUEDADCN                 AS EDAD_CONDUCTOR                         /*EDAD DEL CONDUCTOR                                */
,A.CDIDCAMP                 AS CVE_CAMPANIA_APLICADA                  /*CODIGO DE CAMPANIA APLICADA                       */
,A.PODESCAM                 AS PTC_DESCUENTO_CAMPANIA                 /*PORCENTAJE DE DESCUENTO DE CAMPA#A                */
,A.TCTIPVEH                 AS CVE_TIPO_VEHICULO                      /*TIPO VEHICULO                                     */
,A.PODESEXP                 AS PTC_DESCUENTO_EXP_SINIESTRO            /*PORCENTAJE DESCUENTO EXPERIENCIA SINIESTR         */
,A.TCUSOAUT                 AS CVE_USO_VEHICULO                       /*USO DEL VEHICULO                                  */
,B.DSUSOAUT                 AS DESC_USO_VEHICULO                      /*DESCRIPCION USO DEL VEHICULO                      */
,A.DSVEHICU                 AS DESC_VEHICULO                          /*DESCRIPCION DEL VEHICULO                          */  
,A.TCZONACP                 AS CVE_ZONA_CP                            /*ZONA CP                             */
,A.TCCPTOMC                 AS CVE_CODIGO_POSTAL_CONTRATANTE          /*CODIGO POSTAL CONTRATANTE                         */
,A.CDVERVEH                 AS CVE_VRS_VEHICULO                       /*VERSION DEL VEHICULO                              */
,A.PODESUBI                 AS PTC_DESCUENTO_UBICACION_GEO            /*PORCENTAJE DESCUENTO UBICACION GEOGRAFICA         */
,A.TCTIPCAR                 AS CVE_TIPO_CARGA                         /*TIPO DE CARGA                                     */
,A.PODESCOR                 AS PTC_DESCUENTO_SCORD                    /*PORCENTAJE DE DESCUENTO SCORD                     */
,A.INCTEGNP                 AS CVE_CLIENTE_GNP                        /*INDICADOR DE CLIENTE GNP                          */
,A.INTIVAVE                 AS CVE_TIPO_VALOR_VEHICULO                /*TIPO DE VALOR DE VEHICULO                         */
,A.FENACN                   AS FECHA_NAC_CONDUCTOR                    /*FECHA DE NACIMIENTO DEL CONDUCTOR                 */
,A.CDMARVEH                 AS  CVE_MARCA                              /*CLAVE DE MARCA                                    */
,A.CDCANAL                  AS  CANAL_DE_VENTA
,A.CPMONBON                 AS  MONTO_POR_BONOS 
,A.PODESCOR                 AS  PCT_DE_DESC_SCORD 
,A.DSDCAMPA                 AS  DESC_CAMPANA 
,A.CDIDFISC                 AS  RFC_ASEGURADO
,A.VAREFVEH                 as  valor_referencia
,A.IDMD5                    AS IDMD5
,A.TSULTMOD                 AS TSULTMOD
,A.SISTORIG                 AS SISTORIG
,A.FECHAAUD                 AS FECHAAUD
FROM BDDLALM.AUT_ELEMENTO_OBJETO A
  LEFT OUTER JOIN 
  (SELECT TRIM(CDELEMEN) AS TCUSOAUT, TRIM(DSELEMEN) AS DSUSOAUT FROM BDDLCRU.KTCTGET WHERE UPPER(TRIM(CDTABLA)) = 'KTPT2SG' AND TRIM(CDIDIOMA) ='ES') B
    ON TRIM(A.TCUSOAUT) = TRIM(B.TCUSOAUT)
----Se agregan descriptivos a los codigos 20190423 AMH
left join
        --descripcion genero
        (select cdelemen, trim(dselemen) as desc_genero_sexual_conductor
         from bddlcru.ktctget
         where trim(cdtabla) ='KTCTMNG'    ---- KTCTMNG   --- KPETSXG
         and cdidioma='es'
         )cc
         on trim(A.INSEXOCN) = trim(cc.cdelemen)
        left join
        --descripcion uso
        (select cdelemen as cve_uso_vehiculo , trim(dselemen)  as desc_uso_vehiculo
            from bddlcru.ktctget
            where trim(cdtabla) = 'KTPT2SG'
            group by cdelemen, dselemen
         )uso on trim(A.TCUSOAUT) = trim(uso.cve_uso_vehiculo)
        left join
        -- descripcion tipo vehiculo
        (select cdelemen , trim(dselemen)  as desc_tipo_vehiculo
            from bddlcru.ktctget
            where trim(cdtabla) = 'KTPT2OG'
            group by cdelemen, dselemen
         )dd on trim(A.TCTIPVEH) = trim(dd.cdelemen)
        left join
        --descripcion armadora
        (select cdelemen , trim(dselemen)  as desc_armadora_vehiculo
            from bddlcru.ktctget
            where trim(cdtabla) = 'KTPT3KG'
            group by cdelemen, dselemen
         )ee on trim(A.TCARMADO) = trim(ee.cdelemen)
        left join
        --descripcion tipo carga
        (select cdelemen , trim(dselemen)  as desc_tipo_carga
            from bddlcru.ktctget
            where trim(cdtabla) = 'KTPT3GG'
            group by cdelemen, dselemen
         )rr on trim(A.TCTIPCAR) = trim(rr.cdelemen)
        left join
         --descripcion tipo indemnizacion
        (select cdelemen , trim(dselemen)  as desc_tipo_indemnizacion
            from bddlcru.ktctget
            where trim(cdtabla) = 'KTPTGEG'
            group by cdelemen, dselemen
         )ss on trim(A.INTIVAVE) = trim(ss.cdelemen)
        left join
        --descripcion zona circulacion
        (select cdelemen , trim(dselemen)  as desc_zona_circulacion
            from bddlcru.ktctget
            where trim(cdtabla) = 'KCDT47G'
            group by cdelemen, dselemen
         )tt on trim(A.TCZONACP) = trim(tt.cdelemen)
        left join
         --descripcion entidad circulacion
        (select cdelemen , trim(dselemen)  as desc_entidad_circulacion
            from bddlcru.ktctget
            where trim(cdtabla) = 'KTPT2WG'
            group by cdelemen, dselemen
         )uu on trim(A.TCEDOCIR) = trim(uu.cdelemen)
        left join
        --ksincvt
         (select trim(insistem) as insistem,
                trim(cdarmad)   as cve_armadora_vehiculo,
                trim(cdcarro)   as cve_carroceria_vehiculo,
                trim(tctipveh)  as cve_tipo_vehiculo,
                trim(dscatveh)  as categoria_vehiculo
             from bddlcru.ksincvt
             )ksincvt
             on (trim(A.SISTORIG)                    = ksincvt.insistem 
                 and trim(A.TCTIPVEH)       = ksincvt.cve_tipo_vehiculo
                 and trim(A.TCARMADO)   = ksincvt.cve_armadora_vehiculo 
                 and trim(A.TCCARROC) = ksincvt.cve_carroceria_vehiculo)
            left outer join (
                  --- descripcion de la carroceria
                select 
                    trim(cdarmad)   as cve_armadora_vehiculo,
                    trim(cdcarro)   as cve_carroceria_vehiculo,
                    trim(tctipveh)  as cve_tipo_vehiculo,
                    dscarro as desc_cve_carroceria_vehiculo
                    FROM  bddlorg.KTPT2IT_sqp3
             ) cat_carr  ---Fix_2 01/04/2019 este fix corrige la forma de Pago que saldra naturalmente de aut_poliza 
             on concat(trim(A.TCTIPVEH), trim(A.TCARMADO), trim(A.TCCARROC))
              = concat(trim(cat_carr.cve_tipo_vehiculo), trim(cat_carr.cve_armadora_vehiculo), trim(cat_carr.cve_carroceria_vehiculo));

------------------------------------------------------------------------------------------
--#### PASO 2
--###CREA POLIZARIO_OBJETO (sabana con todos los valores incluyendo nullos)
--TAKE 5 MIN
--## Se genero un script para generar esta tabla
------------------------------------------------------------------------------------------

--CREA TABLA FISICA CON TODOS LAS VERSIONES


DROP TABLE IF EXISTS BDDLALM.AUT_ELEMENTO_OBJETO_TP;
INVALIDATE METADATA;

CREATE TABLE BDDLALM.AUT_ELEMENTO_OBJETO_TP STORED AS PARQUET
AS
SELECT POL.NUM_POLIZA_POLIZARIO NUM_POLIZA, POL.NUM_VERSION_POLIZARIO NUM_VERSION, COM.CVE_PROD_VENTA, 
COM.CVE_PROD_TECNICO, COM.CVE_PROD_COMERCIAL, COM.ID_OBJETO, COM.NOMBRE_CONDUCTOR, COM.CVE_GIRO_NEGOCIO_ASEGURADO, 
COM.CVE_VEHICULO_ALTO_RIESGO, COM.CVE_FILIACION, COM.MODELO_VEHICULO, COM.VALOR_CONVENIDO_VEHICULO, COM.CVE_ARMADORA_VEHICULO, 
COM.VALOR_VEHICULO, COM.DESC_CAMPANIA, COM.CVE_EXCLUSION_ALTO_RIESGO, COM.CVE_ENTIDAD_CIRCULACION, COM.CVE_ZONA, COM.MTO_BONOS,
COM.NUM_PLACAS_VEHICULO, COM.CVE_CARROCERIA_VEHICULO, COM.PTC_DESCUENTO_CUPONES, COM.NUM_SERIE, 
COM.CVE_GENERO_SEXUAL_CONDUCTOR, COM.CVE_CANAL_VENTA, COM.CVE_CLIENTE_VIP, COM.NUM_MOTOR, COM.EDAD_CONDUCTOR, 
COM.CVE_CAMPANIA_APLICADA, COM.PTC_DESCUENTO_CAMPANIA, COM.CVE_TIPO_VEHICULO, COM.PTC_DESCUENTO_EXP_SINIESTRO, 
COM.CVE_USO_VEHICULO, COM.DESC_USO_VEHICULO, COM.DESC_VEHICULO, COM.CVE_ZONA_CP, COM.CVE_CODIGO_POSTAL_CONTRATANTE, 
COM.CVE_VRS_VEHICULO, COM.PTC_DESCUENTO_UBICACION_GEO, COM.CVE_TIPO_CARGA, COM.PTC_DESCUENTO_SCORD, COM.CVE_CLIENTE_GNP, 
COM.CVE_TIPO_VALOR_VEHICULO, COM.FECHA_NAC_CONDUCTOR, COM.CVE_MARCA, 
COM.CANAL_DE_VENTA,COM.MONTO_POR_BONOS,COM.PCT_DE_DESC_SCORD,COM.DESC_CAMPANA ,COM.RFC_ASEGURADO,COM.VALOR_REFERENCIA,
COM.IDMD5, COM.TSULTMOD, COM.SISTORIG, COM.FECHAAUD 
FROM bddltrn.AUT_POLIZARIO pol 
INNER JOIN bddlalm.aut_elemento_objeto_tmp com 
ON POL.NUM_POLIZA_POLIZARIO = COM.NUM_POLIZA 
AND POL.NUM_VERSION_POLIZARIO BETWEEN COM.NUM_VERSION AND coalesce(COM.NUM_VERSION_NEXT, 100000);

COMPUTE STATS BDDLALM.AUT_ELEMENTO_OBJETO_TP;


