-- CIFRAS CONTROL PREVIOS

select count(1) from bddlapr.aut_insumos_sb1_emision_poliza;
select count(1) from bddlapr.aut_insumos_sb1_emision_cobertura;
select count(1) from bddlapr.aut_siniestro_poliza;
select count(1) from bddlapr.aut_siniestro_cob_afecta;


-- PASO 1 Actualizar Elementos a nivel póliza, objeto y cobertura

select count(1) from bddlalm.aut_elemento_poliza_tp;
select count(1) from bddlalm.aut_elemento_objeto_tp;
select count(1) from bddlalm.aut_elemento_cobertura_tp;

select max(tsultmod) from bddlalm.aut_elemento_poliza_tp;


-- PASO 2 Actualizar la tabla Aut Poliza

select count(1) from bddlalm.aut_poliza;



