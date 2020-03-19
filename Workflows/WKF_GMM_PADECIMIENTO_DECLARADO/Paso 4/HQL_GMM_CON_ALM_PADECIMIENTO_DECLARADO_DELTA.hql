INSERT OVERWRITE TABLE bddltrn.stg_gmm_con_padecimiento_declarado_delta
SELECT DISTINCT
  num_poliza,
  vigencia_poliza,
  num_version_poliza,
  num_poliza_cobranza,
  cve_asegurado,
  id_secuencia_asegurado,
  cve_sistema,
  cve_sistema_int,
  cve_grupo_pad_icd9,
  des_grupo_pad_icd9,
  cve_pad_icd9,
  des_pad_icd9,
  ind_pad_inc_exc,
  ban_preexistencia,
  pct_extraprima_automatica,
  pct_extraprima_manual,
  FROM_UNIXTIME(UNIX_TIMESTAMP()) AS ts_alta_hdfs,
  REFLECT('org.apache.commons.codec.digest.DigestUtils', 'md5Hex', CONCAT(num_poliza, ('_'), vigencia_poliza, ('_')
      , num_version_poliza, ('_'), num_poliza_cobranza, ('_'), id_secuencia_asegurado, ('_'), cve_grupo_pad_icd9
      , ('_'), cve_pad_icd9)) AS idmd5,
  REFLECT('org.apache.commons.codec.digest.DigestUtils', 'md5Hex', CONCAT(num_poliza, ('_'), vigencia_poliza, ('_')
      , num_version_poliza, ('_'), num_poliza_cobranza, ('_'), NVL(cve_asegurado, ''), ('_'), id_secuencia_asegurado
      , ('_'), cve_grupo_pad_icd9, ('_'), NVL(des_grupo_pad_icd9, ''), ('_'), cve_pad_icd9, ('_'), NVL(des_pad_icd9,'')
      , ('_'), NVL(ind_pad_inc_exc, ''), ('_'), NVL(ban_preexistencia, ''), ('_'), NVL(pct_extraprima_automatica, '')
      , ('_'), NVL(pct_extraprima_manual, ''))) AS idmd5completo
FROM bddltrn.stg_gmm_con_padecimiento_declarado_delta_relleno;