-- CIFRAS CONTROL PREVIOS

SELECT 'bddlapr.aut_insumos_sb1_emision_poliza' tabla, COUNT (1) registros
  FROM bddlapr.aut_insumos_sb1_emision_poliza
UNION ALL
SELECT 'bddlapr.aut_insumos_sb1_emision_cobertura' tabla, COUNT (1) registros
  FROM bddlapr.aut_insumos_sb1_emision_cobertura
UNION ALL
SELECT 'bddlapr.aut_siniestro_poliza' tabla, COUNT (1) registros
  FROM bddlapr.aut_siniestro_poliza
UNION ALL
SELECT 'bddlapr.aut_siniestro_cob_afecta' tabla, COUNT (1) registros
  FROM bddlapr.aut_siniestro_cob_afecta;


-- PASO 1 Actualizar Elementos a nivel póliza, objeto y cobertura

SELECT 'bddlalm.aut_elemento_poliza_tp' tabla, COUNT (1) registros
  FROM bddlalm.aut_elemento_poliza_tp
UNION ALL
SELECT 'bddlalm.aut_elemento_objeto_tp' tabla, COUNT (1) registros
  FROM bddlalm.aut_elemento_objeto_tp
UNION ALL
SELECT 'bddlalm.aut_elemento_cobertura_tp' tabla, COUNT (1) registros
  FROM bddlalm.aut_elemento_cobertura_tp;



SELECT 'bddlalm.aut_elemento_poliza_tp' tabla, MAX (tsultmod) fechamax
  FROM bddlalm.aut_elemento_poliza_tp
UNION ALL
SELECT 'bddlalm.aut_elemento_objeto_tp' tabla, MAX (tsultmod) fechamax
  FROM bddlalm.aut_elemento_objeto_tp
UNION ALL
SELECT 'bddlalm.aut_elemento_cobertura_tp' tabla, MAX (tsultmod) fechamax
  FROM bddlalm.aut_elemento_cobertura_tp;

-- PASO 2 Actualizar la tabla Aut Poliza

SELECT 'bddlalm.aut_poliza' tabla, COUNT (1) registros
  FROM bddlalm.aut_poliza;


SELECT 'bddlalm.aut_poliza' tabla, MAX (ts_ultima_modificacion) fechamax
  FROM bddlalm.aut_poliza;