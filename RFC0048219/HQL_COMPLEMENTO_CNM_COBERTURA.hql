DROP TABLE IF EXISTS BDDLTRN.STG_CNM_CBZA_COB_DATOS_ADICIONALES PURGE;
CREATE TABLE IF NOT EXISTS BDDLTRN.STG_CNM_CBZA_COB_DATOS_ADICIONALES AS
SELECT DISTINCT
--LPAD(CAST(ASE_POL_NUM AS STRING) ,8,'0')                 AS NUM_POLIZA,
CONCAT(LPAD(CAST(COALESCE(gaz0_poliza.pol_oficina ,0) AS VARCHAR(2)),2,'0'), LPAD(CAST(COALESCE(gaz0_poliza.pol_numero,0) AS VARCHAR(6)),6,'0')) AS NUM_POLIZA,
CONCAT(LPAD(CAST(COALESCE(gaz0_poliza.pol_ofi_renovac ,0) AS VARCHAR(2)),2,'0'), LPAD(CAST(COALESCE(gaz0_poliza.pol_num_renovac,0) AS VARCHAR(6)),6,'0')) AS NUM_POLIZA_COBRANZA,
CONCAT(NVL(gaz0_asegurado.ase_num_certif, ''), NVL(gaz0_asegurado.ase_cve_parien, '')) AS CVE_CLIENTE_ORIGEN,
gaz0_poliza.pol_nombre_cob,
CAST(pol_suma_aseg AS DECIMAL(18,2)) AS val_suma_asegurada,
CAST(pol_deducible AS DECIMAL (18,2))/100 AS val_deducible_nacional,
gaz0_poliza.pol_coaseguro,
CAST(gaz0_poliza.pol_plan as VARCHAR(1)) AS pol_plan  ,
CAST(gaz0_poliza.pol_stdr as VARCHAR(1)) AS pol_stdr
,CAST(gaz0_poliza.cve_unidad AS VARCHAR(15)) as cve_unidad
,CAST(gaz0_poliza.cve_moneda AS VARCHAR(15)) AS cve_moneda
FROM BDDLTRN.STG_CNM_CBZA_CAT_TIPO_UNIDAD as gaz0_poliza
LEFT JOIN  BDDLTRN.STG_CNM_CBZA_UNION_GAZ0_ASEGURADO as gaz0_asegurado
     ON gaz0_poliza.pol_oficina = gaz0_asegurado.ase_pol_ofna 
	 AND gaz0_poliza.pol_numero = gaz0_asegurado.ase_pol_num 
	 AND gaz0_poliza.pol_ofi_renovac = gaz0_asegurado.pol_ofi_renovac 
	 AND gaz0_poliza.pol_num_renovac = gaz0_asegurado.pol_num_renovac 
	 AND gaz0_poliza.end_fecha_ini = gaz0_asegurado.end_fecha_ini 
	 AND gaz0_poliza.end_num_oficina = gaz0_asegurado.end_num_oficina 
	 AND gaz0_poliza.end_num_tipo = gaz0_asegurado.end_num_tipo 
	 AND gaz0_poliza.end_num_consec = gaz0_asegurado.end_num_consec 
	 AND gaz0_poliza.end_num_control = gaz0_asegurado.end_num_control 
	 AND gaz0_poliza.end_liga_pol_ase = gaz0_asegurado.end_liga_pol_ase 
	 AND gaz0_poliza.end_incluye_aseg = gaz0_asegurado.end_incluye_aseg 
	 AND gaz0_poliza.end_subramo = gaz0_asegurado.end_subramo 
	 AND gaz0_poliza.end_fecha_fin = gaz0_asegurado.end_fecha_fin;

