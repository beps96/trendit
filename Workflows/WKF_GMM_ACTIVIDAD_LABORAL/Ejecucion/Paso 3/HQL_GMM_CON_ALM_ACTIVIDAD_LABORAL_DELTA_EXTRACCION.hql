INSERT OVERWRITE TABLE bddltrn.stg_gmm_con_actividad_laboral_delta_extraccion
SELECT DISTINCT
  ase.num_poliza,
  ase.vigencia_poliza,
  ase.num_version_poliza,
  CAST('' AS VARCHAR(8))                          AS num_poliza_cobranza,
  'INFO'                                          AS cve_sistema,
  'INFO'                                          AS cve_sistema_int,
  NVL(ase.cve_asegurado, ''),
  ase.id_secuencia_asegurado,
  CAST(NVL(kacm08t.cdactlab, '') AS VARCHAR(10))  AS cve_actividad_laboral,
  CAST(NVL(kacm08t.dsactlab, '') AS VARCHAR(100)) AS des_actividad_laboral,
  CAST(NVL(kacm08t.inincexc, '') AS VARCHAR(2))   AS ind_actividad_laboral_inc_exc,
  from_unixtime(unix_timestamp())                 AS ts_alta_hdfs,
  CAST('' AS VARCHAR(32))                         AS idmd5,
  CAST('' AS VARCHAR(32))                         AS idmd5completo
FROM (SELECT num_poliza, vigencia_poliza, num_version_poliza, cve_asegurado, id_secuencia_asegurado 
      FROM bddlalm.alm_asegurado_datos_adicionales WHERE cve_sistema = 'INFO') AS ase  -- Se cambia tabla pivote y se elimina Delta por definicion de Backlog Unico
JOIN bddlcru.kacm08t kacm08t ON
      ase.num_poliza = kacm08t.cdnumpol
  AND ase.num_version_poliza = kacm08t.ctvrspol
  AND ase.id_secuencia_asegurado = kacm08t.ctsecobj;