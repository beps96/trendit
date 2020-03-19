--Vista de Tabla principal
CREATE OR REPLACE VIEW bddlapr.cnm_padecimiento_declarado AS
SELECT
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
  pct_extraprima_manual
FROM bddlalm.alm_padecimiento_declarado;

-- Vista datos históricos
CREATE OR REPLACE VIEW bddlapr.cnm_padecimiento_declaradohis AS
SELECT
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
  fechhist
FROM bddlalm.alm_padecimiento_declaradohis;