---/***********************************************************************************/
---/****************   I   M   P   A   L   A   --- Histórico --- **********************/
---/***********************************************************************************/
INSERT OVERWRITE TABLE BDDLAPR.CNM_ENDOSOhis
SELECT
 num_poliza
,num_poliza_cobranza
,cve_oficina_endoso
,descr_oficina_endoso
,num_endoso

,cve_tipo_endoso
,descr_tipo_endoso
,descr_nivel_aplicacion
,certificado_endoso
,cve_endoso_exclusion
,texto_endoso
,fch_ini_vig_endoso
,fch_fin_vig_endoso
,cve_usuario_endoso
,nombre_usuario_endoso
,cve_estatus_endoso
,descr_estatus_endoso
,fch_emision_endoso
,descr_texto_endoso_titulo
,descr_texto_endoso_var1
,descr_texto_endoso_var2
,descr_texto_endoso_var3
,descr_texto_endoso_var4
,descr_texto_endoso_var5
,descr_texto_endoso_var6
,descr_texto_endoso_var7
,descr_texto_endoso_var8
,exc_estatus_endoso
,idmd5
,idmd5completo
,fechaaud
,cve_subtipo_endoso
,cve_sistema
,MIN(fechaaud_his) AS fechaaud_his
FROM
(
   SELECT * FROM BDDLAPR.CNM_ENDOSOhis
   
   UNION DISTINCT
   
   SELECT
    num_poliza
   ,num_poliza_cobranza
   ,cve_oficina_endoso
   ,descr_oficina_endoso
   ,num_endoso
   ,cve_tipo_endoso
   ,descr_tipo_endoso
   ,descr_nivel_aplicacion
   ,certificado_endoso
   ,cve_endoso_exclusion
   ,texto_endoso
   ,fch_ini_vig_endoso
   ,fch_fin_vig_endoso
   ,cve_usuario_endoso
   ,nombre_usuario_endoso
   ,cve_estatus_endoso
   ,descr_estatus_endoso
   ,fch_emision_endoso
   ,des_titulo_endoso
   ,des_var1_endoso
   ,des_var2_endoso
   ,des_var3_endoso
   ,des_var4_endoso
   ,des_var5_endoso
   ,des_var6_endoso
   ,des_var7_endoso
   ,des_var8_endoso
   ,ind_endoso_cancelado
   ,idmd5
   ,idmd5completo
   ,fechaaud
   ,cve_subtipo_endoso
   ,cve_sistema
   ,CURRENT_TIMESTAMP() AS fechaaud_his
   FROM
     (
      SELECT EH.*
      FROM (SELECT * FROM BDDLAPR.CNM_ENDOSO WHERE CVE_TIPO_ENDOSO IN ('A','B','P')) EH
      FULL JOIN (SELECT * FROM BDDLALM.ALM_ENDOSO_EXC WHERE CVE_TIPO_ENDOSO IN ('A','B','P')) E
      ON EH.num_poliza = E.num_poliza
      AND EH.num_poliza_cobranza = E.num_poliza_cobranza
      AND EH.num_endoso = E.num_endoso
      WHERE E.cve_tipo_endoso IS NULL
      
      UNION DISTINCT
      
      SELECT EH.*
      FROM (SELECT * FROM BDDLAPR.CNM_ENDOSO WHERE CVE_TIPO_ENDOSO IN ('A','B','P')) EH
      FULL JOIN (SELECT * FROM BDDLALM.ALM_ENDOSO_EXC WHERE CVE_TIPO_ENDOSO IN ('A','B','P')) E
      ON EH.num_poliza = E.num_poliza
      AND EH.num_poliza_cobranza = E.num_poliza_cobranza
      AND EH.cve_endoso_exclusion = E.cve_endoso_exclusion
      WHERE E.cve_tipo_endoso IS NULL
     ) H
)T
GROUP BY
 num_poliza
,num_poliza_cobranza
,cve_oficina_endoso
,descr_oficina_endoso
,num_endoso
,cve_tipo_endoso
,descr_tipo_endoso
,descr_nivel_aplicacion
,certificado_endoso
,cve_endoso_exclusion
,texto_endoso
,fch_ini_vig_endoso
,fch_fin_vig_endoso
,cve_usuario_endoso
,nombre_usuario_endoso
,cve_estatus_endoso
,descr_estatus_endoso
,fch_emision_endoso
,descr_texto_endoso_titulo
,descr_texto_endoso_var1
,descr_texto_endoso_var2
,descr_texto_endoso_var3
,descr_texto_endoso_var4
,descr_texto_endoso_var5
,descr_texto_endoso_var6
,descr_texto_endoso_var7
,descr_texto_endoso_var8
,exc_estatus_endoso
,idmd5
,idmd5completo
,fechaaud
,cve_subtipo_endoso
,cve_sistema
;