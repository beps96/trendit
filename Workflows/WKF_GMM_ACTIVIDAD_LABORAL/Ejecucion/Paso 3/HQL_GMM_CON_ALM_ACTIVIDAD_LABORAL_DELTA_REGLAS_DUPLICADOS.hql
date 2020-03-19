INSERT INTO bddlalm.alm_actividad_laboralhis
SELECT delta.*, from_unixtime(unix_timestamp()) AS fechhist
FROM bddltrn.stg_gmm_con_actividad_laboral_delta delta
WHERE delta.idmd5 IN (SELECT idmd5 FROM bddltrn.stg_gmm_con_actividad_laboral_delta GROUP BY idmd5 HAVING COUNT(*) > 1)
  AND delta.ind_actividad_laboral_inc_exc = '';

-----------------
INSERT OVERWRITE TABLE bddltrn.stg_gmm_con_actividad_laboral_delta_reglas_duplicados
SELECT delta.*
FROM bddltrn.stg_gmm_con_actividad_laboral_delta delta
WHERE delta.idmd5 IN (SELECT idmd5 FROM bddltrn.stg_gmm_con_actividad_laboral_delta GROUP BY idmd5 HAVING COUNT(*) > 1)
  AND delta.ind_actividad_laboral_inc_exc != ''
UNION ALL
SELECT delta.*
FROM bddltrn.stg_gmm_con_actividad_laboral_delta delta
WHERE delta.idmd5 IN (SELECT idmd5 FROM bddltrn.stg_gmm_con_actividad_laboral_delta GROUP BY idmd5 HAVING COUNT(*) = 1);