DROP TABLE IF EXISTS BDDLTRN.STG_CNM_CBZA_CAT_TIPO_UNIDAD_ALTERNO PURGE;
CREATE TABLE IF NOT EXISTS BDDLTRN.STG_CNM_CBZA_CAT_TIPO_UNIDAD_ALTERNO AS
SELECT
gaz0_poliza.*
,nvl(trim(tipo_unidad_sum.dca_desc), '') as cve_unidad 
,nvl(trim(tipo_unidad_dedu.dca_desc), '')  AS cve_moneda 
FROM BDDLTRN.STG_CNM_TRANSPOSE_COBERTURA_ALTERNO as gaz0_poliza
LEFT JOIN bddlcru.gaz0_cat_tipo_unidad as tipo_unidad_sum
 on trim(gaz0_poliza.pol_uni_sum) = trim(tipo_unidad_sum.dca_clave)
LEFT JOIN bddlcru.gaz0_cat_tipo_unidad as tipo_unidad_dedu
 on trim(gaz0_poliza.pol_uni_dedu) = trim(tipo_unidad_dedu.dca_clave);