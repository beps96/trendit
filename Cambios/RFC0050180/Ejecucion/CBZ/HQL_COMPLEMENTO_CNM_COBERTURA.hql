DROP TABLE IF EXISTS BDDLTRN.STG_CNM_CBZA_COB_DATOS_ADICIONALES_PREVIO_ALTERNO PURGE;
CREATE TABLE IF NOT EXISTS BDDLTRN.STG_CNM_CBZA_COB_DATOS_ADICIONALES_PREVIO_ALTERNO AS
SELECT DISTINCT 
--LPAD(CAST(ASE_POL_NUM AS STRING) ,8,'0')                 AS NUM_POLIZA,
CONCAT(LPAD(CAST(COALESCE(gaz0_poliza.pol_oficina ,0) AS VARCHAR(2)),2,'0'), LPAD(CAST(COALESCE(gaz0_poliza.pol_numero,0) AS VARCHAR(6)),6,'0')) AS NUM_POLIZA,
CONCAT(LPAD(CAST(COALESCE(gaz0_poliza.pol_ofi_renovac ,0) AS VARCHAR(2)),2,'0'), LPAD(CAST(COALESCE(gaz0_poliza.pol_num_renovac,0) AS VARCHAR(6)),6,'0')) AS NUM_POLIZA_COBRANZA,
CONCAT(NVL(gaz0_asegurado.ase_num_certif, ''), NVL(gaz0_asegurado.ase_cve_parien, '')) AS cve_asegurado,
gaz0_poliza.pol_nombre_cob,
gaz0_poliza.pol_uni_sum,
gaz0_poliza.pol_deducible,
gaz0_poliza.pol_uni_dedu,
nvl(gaz0_poliza.pol_unidad_coa,'') as pol_unidad_coa,
CAST(pol_suma_aseg AS DECIMAL(18,2)) AS MTO_SUMA_ASEGURADA,

case when trim(pol_plan) <> 'E'
     then CAST(pol_deducible AS DECIMAL (18,2))/100
	 else '' end 
	 AS MTO_DEDUCIBLE_NACIONAL,

case when trim(pol_plan) = 'E'
     then CAST(pol_deducible AS DECIMAL (18,2))/100
	 else 0 end 
	 AS MTO_DEDUCIBLE_INTERNACIONAL,
	 
case when trim(pol_plan) <> 'E'
     then POL_COASEGURO 
	 else '' end 
	 AS  PCT_COASEGURO_NACIONAL,

case when trim(pol_plan) = 'E'
     then POL_COASEGURO 
	 else '' end 
	 AS  PCT_COASEGURO_INTERNACIONAL,



	 
	 
gaz0_poliza.pol_coaseguro,
CAST(gaz0_poliza.pol_plan as VARCHAR(1)) AS pol_plan  ,
CAST(gaz0_poliza.pol_stdr as VARCHAR(1)) AS pol_stdr
,CAST(gaz0_poliza.cve_unidad AS VARCHAR(15)) as cve_unidad
,CAST(gaz0_poliza.cve_moneda AS VARCHAR(15)) AS cve_moneda
FROM BDDLTRN.STG_CNM_CBZA_CAT_TIPO_UNIDAD_ALTERNO as gaz0_poliza
LEFT JOIN  BDDLTRN.STG_CNM_CBZA_UNION_GAZ0_ASEGURADO_MAP_ALTERNO as gaz0_asegurado
     ON gaz0_poliza.pol_oficina = gaz0_asegurado.ase_pol_ofna 
	AND gaz0_poliza.pol_numero = gaz0_asegurado.ase_pol_num 	
AND gaz0_poliza.pol_ofi_renovac = gaz0_asegurado.pol_ofi_renovac 
AND gaz0_poliza.pol_num_renovac = gaz0_asegurado.pol_num_renovac 
AND gaz0_poliza.pol_nombre_cob = gaz0_asegurado.ase_ramo
AND gaz0_poliza.pol_plan = gaz0_asegurado.ase_plan 
AND gaz0_poliza.end_fecha_ini = gaz0_asegurado.end_fecha_ini 
	 AND gaz0_poliza.end_subramo = gaz0_asegurado.end_subramo 


;


DROP TABLE IF EXISTS BDDLTRN.STG_CNM_CBZA_COB_DATOS_ADICIONALES_ALTERNO PURGE;
CREATE TABLE IF NOT EXISTS BDDLTRN.STG_CNM_CBZA_COB_DATOS_ADICIONALES_ALTERNO AS
select distinct todo.* from (  SELECT  gaz0.*, ASEGURADO.cve_cobertura_contable,ASEGURADO.des_cobertura_contable FROM  BDDLTRN.STG_CNM_CBZA_EXTRACCION_ALTERNO ASEGURADO 
inner  join
 BDDLTRN.STG_CNM_CBZA_COB_DATOS_ADICIONALES_PREVIO_ALTERNO GAZ0 --133,887,958

on 
asegurado.NUM_POLIZA                  =     GAZ0.NUM_POLIZA
and
asegurado.NUM_POLIZA_COBRANZA         =     GAZ0.NUM_POLIZA_COBRANZA
and
asegurado.CVE_ASEGURADO               =     GAZ0.cve_asegurado
) Todo 
order by 
Todo.NUM_POLIZA ,
Todo.NUM_POLIZA_COBRANZA,
Todo.CVE_ASEGURADO     
asc;