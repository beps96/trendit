INSERT OVERWRITE TABLE BDDLAPR.CNM_ENDOSO
SELECT DISTINCT
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
,NVL(TRIM(descr_texto_endoso_titulo), "") as des_titulo_endoso
,NVL(TRIM(descr_texto_endoso_var1), "") as des_var1_endoso
,NVL(TRIM(descr_texto_endoso_var2), "") as des_var2_endoso 
,NVL(TRIM(descr_texto_endoso_var3), "") as des_var3_endoso
,NVL(TRIM(descr_texto_endoso_var4), "") as des_var4_endoso
,NVL(TRIM(descr_texto_endoso_var5), "") as des_var5_endoso
,NVL(TRIM(descr_texto_endoso_var6), "") as des_var6_endoso
,NVL(TRIM(descr_texto_endoso_var7), "") as des_var7_endoso
,NVL(TRIM(descr_texto_endoso_var8), "") as des_var8_endoso
,NVL(TRIM(exc_estatus_endoso), "") as ind_endoso_cancelado
,reflect(
'org.apache.commons.codec.digest.DigestUtils','md5Hex',concat(
NVL(num_poliza,''),
NVL(num_poliza_cobranza,''),
NVL(CAST(num_endoso AS STRING), ''),
NVL(cve_tipo_endoso,''),
NVL(cve_endoso_exclusion,''))) AS idmd5
,reflect('org.apache.commons.codec.digest.DigestUtils','md5Hex',concat(
NVL(num_poliza,''),
NVL(num_poliza_cobranza,''),
NVL(CAST(cve_oficina_endoso AS STRING), ''),
NVL(descr_oficina_endoso,''),
NVL(CAST(num_endoso AS STRING), ''),
NVL(cve_tipo_endoso,''),
NVL(descr_tipo_endoso,''),
NVL(descr_nivel_aplicacion,''),
NVL(certificado_endoso,''),
NVL(cve_endoso_exclusion,''),
NVL(texto_endoso,''),
NVL(CAST(fch_ini_vig_endoso AS STRING), ''),
NVL(CAST(fch_fin_vig_endoso AS STRING), ''),
NVL(cve_usuario_endoso,''),
NVL(nombre_usuario_endoso,''),
NVL(cve_estatus_endoso,''),
NVL(descr_estatus_endoso,''),
NVL(CAST(fch_emision_endoso AS STRING), ''),
NVL(descr_texto_endoso_titulo, ""),
NVL(descr_texto_endoso_var1, ""),
NVL(descr_texto_endoso_var2, ""),
NVL(descr_texto_endoso_var3, ""),
NVL(descr_texto_endoso_var4, ""),
NVL(descr_texto_endoso_var5, ""),
NVL(descr_texto_endoso_var6, ""),
NVL(descr_texto_endoso_var7, ""),
NVL(descr_texto_endoso_var8, ""),
NVL(exc_estatus_endoso, ""),
NVL(CAST(fechaaud AS STRING), ''),
NVL(cve_subtipo_endoso,''),
NVL(cve_sistema,''))) AS idmd5completo
,fechaaud
,cve_subtipo_endoso
,cve_sistema
FROM BDDLALM.ALM_ENDOSO_EXC
WHERE CVE_TIPO_ENDOSO IN ('A','B','P')

UNION ALL

SELECT EH.*
FROM (SELECT * FROM BDDLAPR.CNM_ENDOSO WHERE CVE_TIPO_ENDOSO IN ('A','B','P')) EH
FULL JOIN (SELECT * FROM BDDLALM.ALM_ENDOSO_EXC WHERE CVE_TIPO_ENDOSO IN ('A','B','P')) E
ON EH.num_poliza = E.num_poliza
AND EH.num_poliza_cobranza = E.num_poliza_cobranza
WHERE E.cve_tipo_endoso IS NULL
;