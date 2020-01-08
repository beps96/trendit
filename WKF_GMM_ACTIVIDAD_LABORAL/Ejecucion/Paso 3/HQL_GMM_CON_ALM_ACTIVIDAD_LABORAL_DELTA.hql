INSERT OVERWRITE TABLE bddltrn.stg_gmm_con_actividad_laboral_delta
SELECT
  num_poliza,
  vigencia_poliza,
  num_version_poliza,
  num_poliza_cobranza,
  cve_sistema,
  cve_sistema_int,
  cve_asegurado,
  id_secuencia_asegurado,
  cve_actividad_laboral,
  des_actividad_laboral,
  ind_actividad_laboral_inc_exc,
  ts_alta_hdfs,
  REFLECT('org.apache.commons.codec.digest.DigestUtils', 'md5Hex', CONCAT(num_poliza, ('_')
    , num_version_poliza, ('_'), num_poliza_cobranza, ('_'), id_secuencia_asegurado, ('_')
    , cve_actividad_laboral)) as idmd5,
  REFLECT('org.apache.commons.codec.digest.DigestUtils', 'md5Hex', CONCAT(num_poliza, ('_')
    , NVL(vigencia_poliza,''), num_version_poliza, ('_'), num_poliza_cobranza, ('_')
    , NVL(cve_asegurado,''), ('_'), id_secuencia_asegurado, ('_'), cve_actividad_laboral, ('_')
    , NVL(des_actividad_laboral,''), ('_'), NVL(ind_actividad_laboral_inc_exc,''))) as idmd5completo
FROM bddltrn.stg_gmm_con_actividad_laboral_delta_relleno;