INSERT OVERWRITE TABLE bddltrn.stg_gmm_con_padecimiento_declarado_delta_relleno
SELECT
  rl2.num_poliza,
  pad.vigencia_poliza,
  rl2.num_version_poliza,
  pad.num_poliza_cobranza,
  pad.cve_asegurado,
  pad.id_secuencia_asegurado,
  pad.cve_sistema,
  pad.cve_sistema_int,
  pad.cve_grupo_pad_icd9,
  pad.des_grupo_pad_icd9,
  pad.cve_pad_icd9,
  pad.des_pad_icd9,
  pad.ind_pad_inc_exc,
  pad.ban_preexistencia,
  pad.pct_extraprima_automatica,
  pad.pct_extraprima_manual,
  pad.ts_alta_hdfs,
  pad.idmd5,
  pad.idmd5completo
FROM (
  SELECT rl1.num_poliza, rl1.num_version_poliza, rl1.id_secuencia_asegurado, MAX(pad.num_version_poliza) num_version_poliza_max
  FROM (
    SELECT ase.num_poliza, ase.num_version_poliza, ase.id_secuencia_asegurado
    FROM (SELECT DISTINCT num_poliza FROM bddltrn.stg_gmm_con_padecimiento_declarado_delta_extraccion) pad  
    JOIN bddlalm.alm_asegurado_datos_adicionales ase ON
      pad.num_poliza = ase.num_poliza
    ) rl1,
    bddltrn.stg_gmm_con_padecimiento_declarado_delta_extraccion pad
  WHERE   rl1.num_poliza = pad.num_poliza
    AND rl1.id_secuencia_asegurado = pad.id_secuencia_asegurado
    AND pad.num_version_poliza <= rl1.num_version_poliza        
  GROUP BY rl1.num_poliza, rl1.num_version_poliza, rl1.id_secuencia_asegurado
  ) rl2 
LEFT OUTER JOIN bddltrn.stg_gmm_con_padecimiento_declarado_delta_extraccion pad ON 
      rl2.num_poliza = pad.num_poliza
  AND rl2.num_version_poliza_max = pad.num_version_poliza
  AND rl2.id_secuencia_asegurado = pad.id_secuencia_asegurado;