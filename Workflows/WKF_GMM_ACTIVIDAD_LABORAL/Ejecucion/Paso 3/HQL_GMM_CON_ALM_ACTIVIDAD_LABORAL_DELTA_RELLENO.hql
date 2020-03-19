-- Se realiza relleno por poliza asegurado

INSERT OVERWRITE TABLE bddltrn.stg_gmm_con_actividad_laboral_delta_relleno
SELECT DISTINCT
  rl2.num_poliza,
  alab.vigencia_poliza,
  rl2.num_version_poliza,
  alab.num_poliza_cobranza,
  alab.cve_sistema,
  alab.cve_sistema_int,
  alab.cve_asegurado,
  alab.id_secuencia_asegurado,
  alab.cve_actividad_laboral,
  alab.des_actividad_laboral,
  alab.ind_actividad_laboral_inc_exc,  
  alab.ts_alta_hdfs,
  alab.idmd5,
  alab.idmd5completo
FROM (
  SELECT rl1.num_poliza, rl1.num_version_poliza, rl1.id_secuencia_asegurado, MAX(alab.num_version_poliza) num_version_poliza_max
  FROM (
    SELECT ase.num_poliza, ase.num_version_poliza, ase.id_secuencia_asegurado
    FROM (SELECT DISTINCT num_poliza FROM bddltrn.stg_gmm_con_actividad_laboral_delta_extraccion) alab  
    JOIN bddlalm.alm_asegurado_datos_adicionales ase ON
      alab.num_poliza = ase.num_poliza
        ) rl1,
    bddltrn.stg_gmm_con_actividad_laboral_delta_extraccion alab
    WHERE rl1.num_poliza = alab.num_poliza
      AND rl1.id_secuencia_asegurado = alab.id_secuencia_asegurado
      AND alab.num_version_poliza <= rl1.num_version_poliza
    GROUP BY rl1.num_poliza, rl1.num_version_poliza, rl1.id_secuencia_asegurado
    ) rl2 
LEFT OUTER JOIN bddltrn.stg_gmm_con_actividad_laboral_delta_extraccion alab ON 
      rl2.num_poliza = alab.num_poliza
  AND rl2.num_version_poliza_max = alab.num_version_poliza
  AND rl2.id_secuencia_asegurado = alab.id_secuencia_asegurado;