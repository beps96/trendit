---/*** EJECUTAR EN IMPALA ***/

INVALIDATE METADATA;

INSERT OVERWRITE TABLE BDDLALM.ALM_ENDOSO_EXC
SELECT DISTINCT
 CONCAT(LPAD(CAST(COALESCE(EXC.pol_oficina ,0) AS VARCHAR(2)),2,'0'), LPAD(CAST(COALESCE(EXC.pol_numero,0) AS VARCHAR(6)),6,'0')) AS num_poliza
,COALESCE(CONCAT(LPAD(CAST(POL.pol_ofi_renovac AS VARCHAR(2)),2,'0'), LPAD(CAST(POL.pol_num_renovac AS VARCHAR(6)),6,'0')),
          CONCAT(LPAD(CAST(POL2.pol_ofi_renovac AS VARCHAR(2)),2,'0'), LPAD(CAST(POL2.pol_num_renovac AS VARCHAR(6)),6,'0')),
          CONCAT(LPAD(CAST(COALESCE(EXC.pol_ofi_renovac ,0) AS VARCHAR(2)),2,'0'), LPAD(CAST(COALESCE(EXC.pol_num_renovac,0) AS VARCHAR(6)),6,'0'))
         ) AS num_poliza_cobranza
,COALESCE(EXC.exc_ofna, -99) AS cve_oficina_endoso
,'' AS descr_oficina_endoso --> Está pendiente el catálogo que bajará Vic
,LPAD(CAST(COALESCE(EXC.exc_numero,0) AS VARCHAR(6)),6,'0') AS num_endoso
,COALESCE(TRIM(EXC.exc_tipo),'') AS cve_tipo_endoso
,CASE TRIM(EXC.exc_tipo) ---> Vic comenta que no existe un catálogo
      WHEN 'P' THEN 'Póliza'
      WHEN 'A' THEN 'Asegurado'
 ELSE '' END AS descr_tipo_endoso
---,'' AS descr_nivel_aplicacion --> Para Endosos de Exclusión no aplica, se carga con vacío
,CASE TRIM(EXC.exc_tipo)
      WHEN 'P' THEN 'Póliza'
      WHEN 'A' THEN 'Asegurado'
 ELSE '' END AS descr_nivel_aplicacion
,COALESCE(TRIM(EXC.exc_certificado),'') AS certificado_endoso
---,COALESCE(CONCAT('T',CAST(EXC.exc_tipo AS STRING),CAST(EXC.exc_texto AS STRING),CAST(EXC.exc_numtexto AS STRING)),'') AS cve_endoso_exclusion
,COALESCE(CONCAT('T',CAST(EXC.exc_tipo AS STRING),LPAD(CAST(COALESCE(EXC.exc_texto,0) AS VARCHAR(2)),2,'0'),LPAD(CAST(COALESCE(EXC.exc_numtexto,0) AS VARCHAR(2)),2,'0')),'') AS cve_endoso_exclusion
---,TRIM(EXC.exc_texto_armado) AS texto_endoso
,TRIM(CONCAT(EXC_TMP.C1,"||",EXC_TMP.C2,"||",EXC_TMP.C3,"||",EXC_TMP.C4,"||",EXC_TMP.C5,"||",EXC_TMP.C6,"||",EXC_TMP.C7,"||",EXC_TMP.C8,"||",EXC_TMP.C9,"||",EXC_TMP.C10,"||",EXC_TMP.C11,"||",EXC_TMP.C12,"||",EXC_TMP.C13,"||",EXC_TMP.C14,"||",EXC_TMP.C15,"||",EXC_TMP.C16,"||",EXC_TMP.C17,"||",EXC_TMP.C18,"||",EXC_TMP.C19,"||",EXC_TMP.C20,"||",EXC_TMP.C21,"||",EXC_TMP.C22,"||",EXC_TMP.C23,"||",EXC_TMP.C24,"||",EXC_TMP.C25,"||",EXC_TMP.C26,"||",EXC_TMP.C27,"||",EXC_TMP.C28,"||",EXC_TMP.C29,"||",EXC_TMP.C30,"||",EXC_TMP.C31,"||",EXC_TMP.C32,"||",EXC_TMP.C33,"||",EXC_TMP.C34,"||",EXC_TMP.C35,"||",EXC_TMP.C36,"||",EXC_TMP.C37,"||",EXC_TMP.C38,"||",EXC_TMP.C39,"||",EXC_TMP.C40,"||",EXC_TMP.C41)) AS texto_endoso
---CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(FEOPERA AS CHAR(8)),'yyyyMMdd'),'yyyy-MM-dd') AS TIMESTAMP) AS fch_
,CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(
                                   (CASE WHEN COALESCE(LPAD(CAST(EXC.exc_d2_fec_movto AS STRING),6,'0'),'14000101') IN ('000000','14000101') THEN '14000101'
                                         WHEN COALESCE(SUBSTRING(LPAD(CAST(EXC.exc_d2_fec_movto AS STRING),6,'0'),1,2),'') < '50' THEN CONCAT('20',LPAD(CAST(EXC.exc_d2_fec_movto AS STRING),6,'0'))
                                    ELSE CONCAT('19',LPAD(CAST(EXC.exc_d2_fec_movto AS STRING),6,'0'))
                                    END)
                                  ,'yyyyMMdd'),'yyyy-MM-dd') AS TIMESTAMP) AS fch_ini_vig_endoso
,CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(EXC.pol_fec_fin_vig AS CHAR(8)),'yyyyMMdd'),'yyyy-MM-dd') AS TIMESTAMP) fch_fin_vig_endoso
,COALESCE(TRIM(EXC.exc_d2_user_alta),'') AS cve_usuario_endoso --> ¿?
,'' AS nombre_usuario_endoso --> Vic bajará un catálogo
---,COALESCE(TRIM(EXC.pol_status),'') AS cve_estatus_endoso --> Es el estatus de la póliza, para endosos de Exclusión no existe estatus de endoso.
,COALESCE(TRIM(POL.pol_status),TRIM(POL2.pol_status),TRIM(EXC.pol_status),'') AS cve_estatus_endoso --> Es el estatus de la póliza, para endosos de Exclusión no existe estatus de endoso.
---,CASE TRIM(EXC.pol_status) ---> ¿Existe algún catálogo?
---      WHEN 'SV' THEN 'Status Vigente'
---      WHEN 'SR' THEN ''
--- ELSE '' END AS descr_estatus_endoso
,'' AS descr_estatus_endoso --> Platicando con usuario queda pendiente para endosos de Exclusión
,CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(
                                   (CASE WHEN COALESCE(LPAD(CAST(EXC.exc_d2_fec_movto AS STRING),6,'0'),'14000101') IN ('000000','14000101') THEN '14000101'
                                         WHEN COALESCE(SUBSTRING(LPAD(CAST(EXC.exc_d2_fec_movto AS STRING),6,'0'),1,2),'') < '50' THEN CONCAT('20',LPAD(CAST(EXC.exc_d2_fec_movto AS STRING),6,'0'))
                                    ELSE CONCAT('19',LPAD(CAST(EXC.exc_d2_fec_movto AS STRING),6,'0'))
                                    END)
                                  ,'yyyyMMdd'),'yyyy-MM-dd') AS TIMESTAMP) AS fch_emision_endoso
/*** Campos nuevos de Proyecto Magno-Endosos ***/
,CAST(TRIM(NVL(EXC_TMP.exc_texto_descr_titulo, "")) as VARCHAR(60)) as exc_texto_descr_titulo
,CAST(TRIM(NVL(EXC_TMP.exc_texto_descr_var1, "")) as VARCHAR(60)) as exc_texto_descr_var1
,CAST(TRIM(NVL(EXC_TMP.exc_texto_descr_var2, "")) as VARCHAR(60)) as exc_texto_descr_var2
,CAST(TRIM(NVL(EXC_TMP.exc_texto_descr_var3, "")) as VARCHAR(60)) as exc_texto_descr_var3
,CAST(TRIM(NVL(EXC_TMP.exc_texto_descr_var4, "")) as VARCHAR(60)) as exc_texto_descr_var4
,CAST(TRIM(NVL(EXC_TMP.exc_texto_descr_var5, "")) as VARCHAR(60)) as exc_texto_descr_var5
,CAST(TRIM(NVL(EXC_TMP.exc_texto_descr_var6, "")) as VARCHAR(60)) as exc_texto_descr_var6
,CAST(TRIM(NVL(EXC_TMP.exc_texto_descr_var7, "")) as VARCHAR(60)) as exc_texto_descr_var7
,CAST(TRIM(NVL(EXC_TMP.exc_texto_descr_var8, "")) as VARCHAR(60)) as exc_texto_descr_var8
,CAST(TRIM(NVL(EXC_TMP.exc_estatus_endoso, "")) as VARCHAR(1)) as exc_estatus_endoso

/*** campos de control ***/
,'' AS idmd5
,'' AS idmd5completo
,CURRENT_TIMESTAMP() AS fechaaud
,'E' AS cve_subtipo_endoso --> N=Normal (A,B,D), E=Exclusión, R=Redacción Libre
---,EXC.exc_consec AS cons_texto_endoso
,'AZUL' AS cve_sistema
FROM BDDLCRU.GAZ0_EXCLUSION EXC
INNER JOIN
(
select
a.POL_NUMERO,
a.POL_OFICINA,
a.POL_OFI_RENOVAC,
a.POL_NUM_RENOVAC,
a.EXC_NUMERO,
a.POL_STATUS,
a.exc_d2_fec_movto,
a.c1,
a.c2,
a.c3,
a.c4,
a.c5,
a.c6,
a.c7,
a.c8,
a.c9,
a.c10,
a.c11,
a.c12,
a.c13,
a.c14,
a.c15,
a.c16,
a.c17,
a.c18,
a.c19,
a.c20,
a.c21,
a.c22,
a.c23,
a.c24,
a.c25,
a.c26,
a.c27,
a.c28,
a.c29,
a.c30,
a.c31,
a.c32,
a.c33,
a.c34,
a.c35,
a.c36,
a.c37,
a.c38,
a.c39,
a.c40,
a.c41,
CAST(TRIM(NVL(b.exc_texto_descr_titulo, "")) as VARCHAR(60)) as exc_texto_descr_titulo,
CAST(TRIM(NVL(b.exc_texto_descr_var1, "")) as VARCHAR(60)) as exc_texto_descr_var1,
CAST(TRIM(NVL(b.exc_texto_descr_var2, "")) as VARCHAR(60)) as exc_texto_descr_var2,
CAST(TRIM(NVL(b.exc_texto_descr_var3, "")) as VARCHAR(60)) as exc_texto_descr_var3,
CAST(TRIM(NVL(b.exc_texto_descr_var4, "")) as VARCHAR(60)) as exc_texto_descr_var4,
CAST(TRIM(NVL(b.exc_texto_descr_var5, "")) as VARCHAR(60)) as exc_texto_descr_var5,
CAST(TRIM(NVL(b.exc_texto_descr_var6, "")) as VARCHAR(60)) as exc_texto_descr_var6,
CAST(TRIM(NVL(b.exc_texto_descr_var7, "")) as VARCHAR(60)) as exc_texto_descr_var7,
CAST(TRIM(NVL(b.exc_texto_descr_var8, "")) as VARCHAR(60)) as exc_texto_descr_var8,
CAST(TRIM(NVL(b.exc_estatus_endoso, "")) as VARCHAR(1)) as exc_estatus_endoso
from 
(SELECT
 POL_NUMERO,POL_OFICINA,POL_OFI_RENOVAC,POL_NUM_RENOVAC,EXC_NUMERO,POL_STATUS,exc_d2_fec_movto
,MAX(CASE WHEN EXC_CONSEC=1 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=1 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C1
,MAX(CASE WHEN EXC_CONSEC=2 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=2 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C2
,MAX(CASE WHEN EXC_CONSEC=3 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=3 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C3
,MAX(CASE WHEN EXC_CONSEC=4 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=4 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C4
,MAX(CASE WHEN EXC_CONSEC=5 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=5 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C5
,MAX(CASE WHEN EXC_CONSEC=6 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=6 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C6
,MAX(CASE WHEN EXC_CONSEC=7 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=7 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C7
,MAX(CASE WHEN EXC_CONSEC=8 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=8 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C8
,MAX(CASE WHEN EXC_CONSEC=9 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=9 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C9
,MAX(CASE WHEN EXC_CONSEC=10 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=10 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C10
,MAX(CASE WHEN EXC_CONSEC=11 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=11 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C11
,MAX(CASE WHEN EXC_CONSEC=12 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=12 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C12
,MAX(CASE WHEN EXC_CONSEC=13 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=13 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C13
,MAX(CASE WHEN EXC_CONSEC=14 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=14 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C14
,MAX(CASE WHEN EXC_CONSEC=15 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=15 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C15
,MAX(CASE WHEN EXC_CONSEC=16 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=16 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C16
,MAX(CASE WHEN EXC_CONSEC=17 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=17 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C17
,MAX(CASE WHEN EXC_CONSEC=18 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=18 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C18
,MAX(CASE WHEN EXC_CONSEC=19 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=19 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C19
,MAX(CASE WHEN EXC_CONSEC=20 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=20 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C20
,MAX(CASE WHEN EXC_CONSEC=21 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=21 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C21
,MAX(CASE WHEN EXC_CONSEC=22 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=22 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C22
,MAX(CASE WHEN EXC_CONSEC=23 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=23 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C23
,MAX(CASE WHEN EXC_CONSEC=24 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=24 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C24
,MAX(CASE WHEN EXC_CONSEC=25 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=25 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C25
,MAX(CASE WHEN EXC_CONSEC=26 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=26 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C26
,MAX(CASE WHEN EXC_CONSEC=27 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=27 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C27
,MAX(CASE WHEN EXC_CONSEC=28 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=28 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C28
,MAX(CASE WHEN EXC_CONSEC=29 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=29 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C29
,MAX(CASE WHEN EXC_CONSEC=30 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=30 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C30
,MAX(CASE WHEN EXC_CONSEC=31 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=31 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C31
,MAX(CASE WHEN EXC_CONSEC=32 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=32 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C32
,MAX(CASE WHEN EXC_CONSEC=33 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=33 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C33
,MAX(CASE WHEN EXC_CONSEC=34 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=34 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C34
,MAX(CASE WHEN EXC_CONSEC=35 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=35 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C35
,MAX(CASE WHEN EXC_CONSEC=36 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=36 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C36
,MAX(CASE WHEN EXC_CONSEC=37 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=37 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C37
,MAX(CASE WHEN EXC_CONSEC=38 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=38 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C38
,MAX(CASE WHEN EXC_CONSEC=39 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=39 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C39
,MAX(CASE WHEN EXC_CONSEC=40 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=40 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C40
,MAX(CASE WHEN EXC_CONSEC=41 AND REGEXP_LIKE(EXC_TEXTO_ARMADO,'-')=TRUE THEN TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')) WHEN EXC_CONSEC=41 AND TRIM(EXC_TEXTO_ARMADO)<>'' THEN CONCAT(TRIM(REGEXP_REPLACE(EXC_TEXTO_ARMADO, '[^a-zA-Z0-9*!@+-/#$%()_=/<>?\|&ÑÁÉÍÓÚ\\s]+', '')),' ') ELSE '' END) AS C41
,case when trim(exc_estatus_endoso) = 'B' then 'B' when trim(exc_estatus_endoso) = 'V' then 'V' else '' end as exc_estatus_endoso
/*** Campos nuevos de Proyecto Magno-Endosos ***/
FROM BDDLCRU.GAZ0_EXCLUSION
GROUP BY POL_NUMERO
,POL_OFICINA
,POL_OFI_RENOVAC
,POL_NUM_RENOVAC
,EXC_NUMERO
,POL_STATUS
,exc_d2_fec_movto
,exc_estatus_endoso
) a 
left join 
(SELECT
 POL_NUMERO,POL_OFICINA,POL_OFI_RENOVAC,POL_NUM_RENOVAC,EXC_NUMERO,POL_STATUS,exc_d2_fec_movto,
CAST(TRIM(NVL(exc_texto_descr_titulo, "")) as VARCHAR(60)) as exc_texto_descr_titulo,
CAST(TRIM(NVL(exc_texto_descr_var1, "")) as VARCHAR(60)) as exc_texto_descr_var1,
CAST(TRIM(NVL(exc_texto_descr_var2, "")) as VARCHAR(60)) as exc_texto_descr_var2,
CAST(TRIM(NVL(exc_texto_descr_var3, "")) as VARCHAR(60)) as exc_texto_descr_var3,
CAST(TRIM(NVL(exc_texto_descr_var4, "")) as VARCHAR(60)) as exc_texto_descr_var4,
CAST(TRIM(NVL(exc_texto_descr_var5, "")) as VARCHAR(60)) as exc_texto_descr_var5,
CAST(TRIM(NVL(exc_texto_descr_var6, "")) as VARCHAR(60)) as exc_texto_descr_var6,
CAST(TRIM(NVL(exc_texto_descr_var7, "")) as VARCHAR(60)) as exc_texto_descr_var7,
CAST(TRIM(NVL(exc_texto_descr_var8, "")) as VARCHAR(60)) as exc_texto_descr_var8,
case when trim(exc_estatus_endoso) = 'B' then 'B' when trim(exc_estatus_endoso) = 'V' then 'V' else '' end as exc_estatus_endoso
FROM BDDLCRU.GAZ0_EXCLUSION
where TRIM(exc_texto_descr_titulo) != ''
GROUP BY POL_NUMERO
,POL_OFICINA
,POL_OFI_RENOVAC
,POL_NUM_RENOVAC
,EXC_NUMERO
,POL_STATUS
,exc_d2_fec_movto
/*** Campos nuevos de Proyecto Magno-Endosos ***/
,exc_texto_descr_titulo
,exc_texto_descr_var1
,exc_texto_descr_var2
,exc_texto_descr_var3
,exc_texto_descr_var4
,exc_texto_descr_var5
,exc_texto_descr_var6
,exc_texto_descr_var7
,exc_texto_descr_var8
,exc_estatus_endoso ) b
on a.POL_NUMERO = b.POL_NUMERO and a.POL_OFICINA = b.POL_OFICINA and a.POL_OFI_RENOVAC = b.POL_OFI_RENOVAC and a.POL_NUM_RENOVAC = b.POL_NUM_RENOVAC and a.EXC_NUMERO = b.EXC_NUMERO and a.exc_estatus_endoso = b.exc_estatus_endoso and a.exc_d2_fec_movto = b.exc_d2_fec_movto

) EXC_TMP
ON EXC.POL_NUMERO = EXC_TMP.POL_NUMERO
AND EXC.POL_OFICINA = EXC_TMP.POL_OFICINA
AND EXC.POL_OFI_RENOVAC = EXC_TMP.POL_OFI_RENOVAC
AND EXC.POL_NUM_RENOVAC = EXC_TMP.POL_NUM_RENOVAC
AND EXC.EXC_NUMERO = EXC_TMP.EXC_NUMERO
AND EXC.POL_STATUS = EXC_TMP.POL_STATUS
and EXC.exc_d2_fec_movto = EXC_TMP.exc_d2_fec_movto
LEFT JOIN
   (SELECT POL_OFICINA,POL_NUMERO,POL_OFI_RENOVAC,POL_NUM_RENOVAC,POL_FEC_INI_VIG,POL_FEC_FIN_VIG   ,CONCAT(pol_status_calc,pol_status_emis) AS pol_status FROM BDDLCRU.GAZ0_POLIZA WHERE TRIM(POL_STATUS_CALC) <> ''
    UNION DISTINCT
    SELECT HISP_OFICINA AS POL_OFICINA,HISP_NUMERO AS POL_NUMERO,HISP_OFI_RENOVAC AS POL_OFI_RENOVAC,HISP_NUM_RENOVAC AS POL_NUM_RENOVAC,HISP_FEC_INI_VIG AS POL_FEC_INI_VIG,HISP_FEC_FIN_VIG AS POL_FEC_FIN_VIG   ,CONCAT(hisp_status_calc,hisp_status_emis) AS pol_status FROM BDDLCRU.GAZ0_HISPOLIZA WHERE TRIM(HISP_STATUS_CALC) <> ''
   ) POL
ON EXC_TMP.pol_oficina = POL.POL_OFICINA
AND EXC_TMP.pol_numero = POL.POL_NUMERO
AND CAST((CASE WHEN COALESCE(LPAD(CAST(EXC_TMP.exc_d2_fec_movto AS STRING),6,'0'),'14000101') IN ('000000','14000101') THEN '14000101'
               WHEN COALESCE(SUBSTRING(LPAD(CAST(EXC_TMP.exc_d2_fec_movto AS STRING),6,'0'),1,2),'') < '50' THEN CONCAT('20',LPAD(CAST(EXC_TMP.exc_d2_fec_movto AS STRING),6,'0'))
          ELSE CONCAT('19',LPAD(CAST(EXC_TMP.exc_d2_fec_movto AS STRING),6,'0'))
          END)
         AS INT)
     BETWEEN POL.POL_FEC_INI_VIG AND POL.POL_FEC_FIN_VIG

LEFT JOIN
   (SELECT POL_OFICINA,POL_NUMERO,POL_OFI_RENOVAC,POL_NUM_RENOVAC,POL_FEC_INI_VIG,POL_FEC_FIN_VIG   ,CONCAT(pol_status_calc,pol_status_emis) AS pol_status FROM BDDLCRU.GAZ0_POLIZA WHERE TRIM(POL_STATUS_CALC) <> ''
    UNION DISTINCT
    SELECT HISP_OFICINA AS POL_OFICINA,HISP_NUMERO AS POL_NUMERO,HISP_OFI_RENOVAC AS POL_OFI_RENOVAC,HISP_NUM_RENOVAC AS POL_NUM_RENOVAC,HISP_FEC_INI_VIG AS POL_FEC_INI_VIG,HISP_FEC_FIN_VIG AS POL_FEC_FIN_VIG   ,CONCAT(hisp_status_calc,hisp_status_emis) AS pol_status FROM BDDLCRU.GAZ0_HISPOLIZA WHERE TRIM(HISP_STATUS_CALC) <> ''
   ) POL2
ON EXC_TMP.pol_oficina = POL2.POL_OFICINA
AND EXC_TMP.pol_numero = POL2.POL_NUMERO
AND DAYS_ADD(
             CAST(FROM_UNIXTIME(UNIX_TIMESTAMP((CASE WHEN COALESCE(LPAD(CAST(EXC_TMP.exc_d2_fec_movto AS STRING),6,'0'),'14000101') IN ('000000','14000101') THEN '14000101'
                                                     WHEN COALESCE(SUBSTRING(LPAD(CAST(EXC_TMP.exc_d2_fec_movto AS STRING),6,'0'),1,2),'') < '50' THEN CONCAT('20',LPAD(CAST(EXC_TMP.exc_d2_fec_movto AS STRING),6,'0'))
                                                ELSE CONCAT('19',LPAD(CAST(EXC_TMP.exc_d2_fec_movto AS STRING),6,'0'))
                                                END)
                                               ,'yyyyMMdd'),'yyyy-MM-dd') AS TIMESTAMP)
             ,35)
    BETWEEN CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(POL2.POL_FEC_INI_VIG AS STRING),'yyyyMMdd'),'yyyy-MM-dd') AS TIMESTAMP) AND CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(POL2.POL_FEC_FIN_VIG AS STRING),'yyyyMMdd'),'yyyy-MM-dd') AS TIMESTAMP)

ORDER BY 1,2,3,5,10
;

INVALIDATE METADATA BDDLALM.ALM_ENDOSO_EXC;
REFRESH BDDLALM.ALM_ENDOSO_EXC;