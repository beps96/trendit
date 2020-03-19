CREATE OR REPLACE VIEW bddlapr.cnm_actividad_laboral AS
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
  ind_actividad_laboral_inc_exc
FROM bddlalm.alm_actividad_laboral;

-- Vista datos históricos
CREATE OR REPLACE VIEW bddlapr.cnm_actividad_laboralhis AS
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
  fechhist
FROM bddlalm.alm_actividad_laboralhis;