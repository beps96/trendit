DESCRIBE FORMATTED bddlcru.crm_respuesta_cuestionario;
DESCRIBE FORMATTED bddlalm.opr_precalificados_emision;

show table extended like 'CRM_RESPUESTA_CUESTIONARIO';
show table extended like 'OPR_PRECALIFICADOS_EMISION';

select count(1) from CRM_RESPUESTA_CUESTIONARIO;
select count(1) from OPR_PRECALIFICADOS_EMISION;


-- lastAccessTime:1577381800937
-- lastUpdateTime:1547146722676
 
SHOW TBLPROPERTIES bddlalm.opr_precalificados_emision("transient_lastDdlTime");

SHOW TBLPROPERTIES CRM_RESPUESTA_CUESTIONARIO("transient_lastDdlTime");
--1571861403 

DESCRIBE FORMATTED bddldes.aut_siniestro_cob_afecta_insumo;


select * from bddldes.aut_siniestro_cob_afecta_insumo;

select cast(from_unixtime(1577462345) AS timestamp);

ALTER TABLE bddlint.cobertura_contratada_RFC0050297 RENAME TO bddlint.cobertura_contratada;

ALTER TABLE bddltrn.kcim06_gmm_tmp_RFC0050297 RENAME TO bddltrn.kcim06_gmm_tmp;


ALTER TABLE bddlint.cat_unidad_medida_dl_norm_RFC0050297 RENAME TO bddlint.cat_unidad_medida_dl_norm;

ALTER TABLE bddlint.cat_estatus_comprobante_gasto_dl_norm_RFC0050297 RENAME TO bddlint.cat_estatus_comprobante_gasto_dl_norm;



ALTER TABLE bddlint.cat_tipo_gasto_codigo_beneficio_dl_norm_RFC0050297 RENAME TO bddlint.cat_tipo_gasto_codigo_beneficio_dl_norm;

ALTER TABLE bddldes.cnm_comisiones_asegurado_cobertura_RFC0050297 RENAME TO bddldes.cnm_comisiones_asegurado_cobertura;

select * from bddldes.cnm_comisiones_asegurado_cobertura;

ALTER TABLE bddltrn.cnm_comisiones_asegurado_cobertura_base3_RFC0050297 RENAME TO bddltrn.cnm_comisiones_asegurado_cobertura_base3;

ALTER TABLE bddlint.cat_resultado_dictamen_dl_norm_RFC0050297 RENAME TO bddlint.cat_resultado_dictamen_dl_norm;

ALTER TABLE bddltrn.cnm_comisiones_asegurado_cobertura_RFC0050297 RENAME TO bddltrn.cnm_comisiones_asegurado_cobertura;



describe formatted bddlcru.ktpm04t_dmnd0030628
