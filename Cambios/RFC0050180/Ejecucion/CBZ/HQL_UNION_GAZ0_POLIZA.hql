
DROP TABLE IF EXISTS BDDLTRN.STG_CNM_CBZA_EXTRACCION_ALTERNO PURGE;
CREATE TABLE IF NOT EXISTS BDDLTRN.STG_CNM_CBZA_EXTRACCION_ALTERNO AS


select
num_poliza,
vigencia_poliza,
num_version_poliza,
num_poliza_cobranza,
cve_asegurado,
cve_cobertura_contable,
des_cobertura_contable,
cve_sistema_int,
ts_alta_hdfs
from
bddlalm.ALM_ASEGURADO_DATOS_ADICIONALES_RFC0049313
where cve_sistema ='AZUL';


DROP TABLE IF EXISTS BDDLTRN.STG_CNM_CBZA_UNION_GAZ0_POLIZA_ALTERNO PURGE;
CREATE TABLE IF NOT EXISTS BDDLTRN.STG_CNM_CBZA_UNION_GAZ0_POLIZA_ALTERNO AS
select distinct 
A.pol_oficina, A.pol_numero,A.pol_ofi_renovac,A.pol_num_renovac,
    A.end_fecha_ini,A.end_num_oficina,A.end_num_tipo,A.end_num_consec,
    A.end_num_control,A.end_liga_pol_ase,A.end_incluye_aseg,A.end_subramo, A.end_fecha_fin,    
    A.pol_nombre_cob1,A.pol_coaseguro1,A.pol_suma_aseg1,A.pol_deducible1, A.pol_plan1, A.pol_cob_stdr1,
    A.pol_nombre_cob2,A.pol_coaseguro2,A.pol_suma_aseg2,A.pol_deducible2, A.pol_plan2, A.pol_cob_stdr2,
    A.pol_nombre_cob3,A.pol_coaseguro3,A.pol_suma_aseg3,A.pol_deducible3, A.pol_plan3, A.pol_cob_stdr3,
    A.pol_nombre_cob4,A.pol_coaseguro4,A.pol_suma_aseg4,A.pol_deducible4, A.pol_plan4, A.pol_cob_stdr4,
    A.pol_nombre_cob5,A.pol_coaseguro5,A.pol_suma_aseg5,A.pol_deducible5, A.pol_plan5, A.pol_cob_stdr5,
    A.pol_nombre_cob6,A.pol_coaseguro6,A.pol_suma_aseg6,A.pol_deducible6, A.pol_plan6, A.pol_cob_stdr6,
    A.pol_nombre_cob7,A.pol_coaseguro7,A.pol_suma_aseg7,A.pol_deducible7, A.pol_plan7, A.pol_cob_stdr7,
    A.pol_nombre_cob8,A.pol_coaseguro8,A.pol_suma_aseg8,A.pol_deducible8, A.pol_plan8, A.pol_cob_stdr8,
    A.pol_nombre_cob9,A.pol_coaseguro9,A.pol_suma_aseg9,A.pol_deducible9, A.pol_plan9, A.pol_cob_stdr9,
    A.pol_nombre_cob10,A.pol_coaseguro10,A.pol_suma_aseg10,A.pol_deducible10, A.pol_plan10, A.pol_cob_stdr10,
    A.pol_nombre_cob11,A.pol_coaseguro11,A.pol_suma_aseg11,A.pol_deducible11, A.pol_plan11, A.pol_cob_stdr11,
    A.pol_nombre_cob12,A.pol_coaseguro12,A.pol_suma_aseg12,A.pol_deducible12, A.pol_plan12, A.pol_cob_stdr12,
    A.pol_nombre_cob13,A.pol_coaseguro13,A.pol_suma_aseg13,A.pol_deducible13, A.pol_plan13, A.pol_cob_stdr13,
    A.pol_nombre_cob14,A.pol_coaseguro14,A.pol_suma_aseg14,A.pol_deducible14, A.pol_plan14, A.pol_cob_stdr14,
    A.pol_nombre_cob15,A.pol_coaseguro15,A.pol_suma_aseg15,A.pol_deducible15, A.pol_plan15, A.pol_cob_stdr15
    ,A.pol_unidad_suma_aseg1, A.pol_unidad_deducible1, A.pol_unidad_suma_aseg2, A.pol_unidad_deducible2, 
    A.pol_unidad_suma_aseg3, A.pol_unidad_deducible3, A.pol_unidad_suma_aseg4, A.pol_unidad_deducible4, 
    A.pol_unidad_suma_aseg5, A.pol_unidad_deducible5, A.pol_unidad_suma_aseg6, A.pol_unidad_deducible6, 
    A.pol_unidad_suma_aseg7, A.pol_unidad_deducible7, A.pol_unidad_suma_aseg8, A.pol_unidad_deducible8, 
    A.pol_unidad_suma_aseg9, A.pol_unidad_deducible9, A.pol_unidad_suma_aseg10, A.pol_unidad_deducible10, 
    A.pol_unidad_suma_aseg11, A.pol_unidad_deducible11, A.pol_unidad_suma_aseg12, A.pol_unidad_deducible12, 
    A.pol_unidad_suma_aseg13, A.pol_unidad_deducible13, A.pol_unidad_suma_aseg14, A.pol_unidad_deducible14, 
    A.pol_unidad_suma_aseg15, A.pol_unidad_deducible15, 
	A.pol_unidad_coaseguro1,A.pol_unidad_coaseguro2,A.pol_unidad_coaseguro3,A.pol_unidad_coaseguro4,A.pol_unidad_coaseguro5,
	A.pol_unidad_coaseguro6,A.pol_unidad_coaseguro7,A.pol_unidad_coaseguro8,A.pol_unidad_coaseguro9,A.pol_unidad_coaseguro10,
	A.pol_unidad_coaseguro11,A.pol_unidad_coaseguro12,A.pol_unidad_coaseguro13,A.pol_unidad_coaseguro14,A.pol_unidad_coaseguro15,
	
	
	POL_SRAMO_CONT
	
	from
	(

 SELECT pol_oficina, pol_numero,pol_ofi_renovac,pol_num_renovac,
    end_fecha_ini,end_num_oficina,end_num_tipo,end_num_consec,
    end_num_control,end_liga_pol_ase,end_incluye_aseg,end_subramo, end_fecha_fin,    
    pol_nombre_cob1,pol_coaseguro1,pol_suma_aseg1,pol_deducible1, pol_plan1, pol_cob_stdr1,
    pol_nombre_cob2,pol_coaseguro2,pol_suma_aseg2,pol_deducible2, pol_plan2, pol_cob_stdr2,
    pol_nombre_cob3,pol_coaseguro3,pol_suma_aseg3,pol_deducible3, pol_plan3, pol_cob_stdr3,
    pol_nombre_cob4,pol_coaseguro4,pol_suma_aseg4,pol_deducible4, pol_plan4, pol_cob_stdr4,
    pol_nombre_cob5,pol_coaseguro5,pol_suma_aseg5,pol_deducible5, pol_plan5, pol_cob_stdr5,
    pol_nombre_cob6,pol_coaseguro6,pol_suma_aseg6,pol_deducible6, pol_plan6, pol_cob_stdr6,
    pol_nombre_cob7,pol_coaseguro7,pol_suma_aseg7,pol_deducible7, pol_plan7, pol_cob_stdr7,
    pol_nombre_cob8,pol_coaseguro8,pol_suma_aseg8,pol_deducible8, pol_plan8, pol_cob_stdr8,
    pol_nombre_cob9,pol_coaseguro9,pol_suma_aseg9,pol_deducible9, pol_plan9, pol_cob_stdr9,
    pol_nombre_cob10,pol_coaseguro10,pol_suma_aseg10,pol_deducible10, pol_plan10, pol_cob_stdr10,
    pol_nombre_cob11,pol_coaseguro11,pol_suma_aseg11,pol_deducible11, pol_plan11, pol_cob_stdr11,
    pol_nombre_cob12,pol_coaseguro12,pol_suma_aseg12,pol_deducible12, pol_plan12, pol_cob_stdr12,
    pol_nombre_cob13,pol_coaseguro13,pol_suma_aseg13,pol_deducible13, pol_plan13, pol_cob_stdr13,
    pol_nombre_cob14,pol_coaseguro14,pol_suma_aseg14,pol_deducible14, pol_plan14, pol_cob_stdr14,
    pol_nombre_cob15,pol_coaseguro15,pol_suma_aseg15,pol_deducible15, pol_plan15, pol_cob_stdr15
    ,pol_unidad_suma_aseg1, pol_unidad_deducible1, pol_unidad_suma_aseg2, pol_unidad_deducible2, 
    pol_unidad_suma_aseg3, pol_unidad_deducible3, pol_unidad_suma_aseg4, pol_unidad_deducible4, 
    pol_unidad_suma_aseg5, pol_unidad_deducible5, pol_unidad_suma_aseg6, pol_unidad_deducible6, 
    pol_unidad_suma_aseg7, pol_unidad_deducible7, pol_unidad_suma_aseg8, pol_unidad_deducible8, 
    pol_unidad_suma_aseg9, pol_unidad_deducible9, pol_unidad_suma_aseg10, pol_unidad_deducible10, 
    pol_unidad_suma_aseg11, pol_unidad_deducible11, pol_unidad_suma_aseg12, pol_unidad_deducible12, 
    pol_unidad_suma_aseg13, pol_unidad_deducible13, pol_unidad_suma_aseg14, pol_unidad_deducible14, 
    pol_unidad_suma_aseg15, pol_unidad_deducible15, POL_SRAMO_CONT,
	pol_unidad_coaseguro1,pol_unidad_coaseguro2,pol_unidad_coaseguro3,pol_unidad_coaseguro4,pol_unidad_coaseguro5,
	pol_unidad_coaseguro6,pol_unidad_coaseguro7,pol_unidad_coaseguro8,pol_unidad_coaseguro9,pol_unidad_coaseguro10,
	pol_unidad_coaseguro11,pol_unidad_coaseguro12,pol_unidad_coaseguro13,pol_unidad_coaseguro14,pol_unidad_coaseguro15,
	fechaaud
FROM bddlcru.gaz0_poliza 
where end_num_tipo = 0 and end_num_consec = 0
  UNION ALL  --tabla historica
SELECT hisp_oficina as pol_oficina, hisp_numero as pol_numero,hisp_ofi_renovac as pol_ofi_renovac,hisp_num_renovac as pol_num_renovac ,
    end_fecha_ini,end_num_oficina,end_num_tipo,end_num_consec,
    end_num_control,end_liga_pol_ase,end_incluye_aseg,end_subramo ,end_fecha_fin,    
    hisp_nombre_cob1 as pol_nombre_cob1,  hisp_coaseguro1 as pol_coaseguro1,  hisp_suma_aseg1 as pol_suma_aseg1,  hisp_deducible1 as pol_deducible1, hisp_plan1 as pol_plan1, hisp_cob_stdr1 as pol_cob_stdr1,
    hisp_nombre_cob2 as pol_nombre_cob2,  hisp_coaseguro2 as pol_coaseguro2,  hisp_suma_aseg2 as pol_suma_aseg2,  hisp_deducible2 as pol_deducible2, hisp_plan2 as pol_plan2, hisp_cob_stdr2 as pol_cob_stdr2,
    hisp_nombre_cob3 as pol_nombre_cob3,  hisp_coaseguro3 as pol_coaseguro3,  hisp_suma_aseg3 as pol_suma_aseg3,  hisp_deducible3 as pol_deducible3, hisp_plan3 as pol_plan3, hisp_cob_stdr3 as pol_cob_stdr3,
    hisp_nombre_cob4 as pol_nombre_cob4,  hisp_coaseguro4 as pol_coaseguro4,  hisp_suma_aseg4 as pol_suma_aseg4,  hisp_deducible4 as pol_deducible4, hisp_plan4 as pol_plan4, hisp_cob_stdr4 as pol_cob_stdr4,
    hisp_nombre_cob5 as pol_nombre_cob5,  hisp_coaseguro5 as pol_coaseguro5,  hisp_suma_aseg5 as pol_suma_aseg5,  hisp_deducible5 as pol_deducible5, hisp_plan5 as pol_plan5, hisp_cob_stdr5 as pol_cob_stdr5,
    hisp_nombre_cob6 as pol_nombre_cob6,  hisp_coaseguro6 as pol_coaseguro6,  hisp_suma_aseg6 as pol_suma_aseg6,  hisp_deducible6 as pol_deducible6, hisp_plan6 as pol_plan6, hisp_cob_stdr6 as pol_cob_stdr6,
    hisp_nombre_cob7 as pol_nombre_cob7,  hisp_coaseguro7 as pol_coaseguro7,  hisp_suma_aseg7 as pol_suma_aseg7,  hisp_deducible7 as pol_deducible7, hisp_plan7 as pol_plan7, hisp_cob_stdr7 as pol_cob_stdr7,
    hisp_nombre_cob8 as pol_nombre_cob8,  hisp_coaseguro8 as pol_coaseguro8,  hisp_suma_aseg8 as pol_suma_aseg8,  hisp_deducible8 as pol_deducible8, hisp_plan8 as pol_plan8, hisp_cob_stdr8 as pol_cob_stdr8,
    hisp_nombre_cob9 as pol_nombre_cob9,  hisp_coaseguro9 as pol_coaseguro9,  hisp_suma_aseg9 as pol_suma_aseg9,  hisp_deducible9 as pol_deducible9, hisp_plan9 as pol_plan9, hisp_cob_stdr9 as pol_cob_stdr9,
    hisp_nombre_cob10 as pol_nombre_cob10,hisp_coaseguro10 as pol_coaseguro10,hisp_suma_aseg10 as pol_suma_aseg10,hisp_deducible10 as pol_deducible10, hisp_plan10 as pol_plan10, hisp_cob_stdr10 as pol_cob_stdr10,
    hisp_nombre_cob11 as pol_nombre_cob11,hisp_coaseguro11 as pol_coaseguro11,hisp_suma_aseg11 as pol_suma_aseg11,hisp_deducible11 as pol_deducible11, hisp_plan11 as pol_plan11, hisp_cob_stdr11 as pol_cob_stdr11,
    hisp_nombre_cob12 as pol_nombre_cob12,hisp_coaseguro12 as pol_coaseguro12,hisp_suma_aseg12 as pol_suma_aseg12,hisp_deducible12 as pol_deducible12, hisp_plan12 as pol_plan12, hisp_cob_stdr12 as pol_cob_stdr12,
    hisp_nombre_cob13 as pol_nombre_cob13,hisp_coaseguro13 as pol_coaseguro13,hisp_suma_aseg13 as pol_suma_aseg13,hisp_deducible13 as pol_deducible13, hisp_plan13 as pol_plan13, hisp_cob_stdr13 as pol_cob_stdr13,
    hisp_nombre_cob14 as pol_nombre_cob14,hisp_coaseguro14 as pol_coaseguro14,hisp_suma_aseg14 as pol_suma_aseg14,hisp_deducible14 as pol_deducible14, hisp_plan14 as pol_plan14, hisp_cob_stdr14 as pol_cob_stdr14,
    hisp_nombre_cob15 as pol_nombre_cob15,hisp_coaseguro15 as pol_coaseguro15,hisp_suma_aseg15 as pol_suma_aseg15,hisp_deducible15 as pol_deducible15, hisp_plan15 as pol_plan15, hisp_cob_stdr15 as pol_cob_stdr15
    ,hisp_unidad_suma_aseg1 as pol_unidad_suma_aseg1, hisp_unidad_deducible1 as pol_unidad_deducible1, hisp_unidad_suma_aseg2 as pol_unidad_suma_aseg2, hisp_unidad_deducible2 as pol_unidad_deducible2,
    hisp_unidad_suma_aseg3 as pol_unidad_suma_aseg3, hisp_unidad_deducible3 as pol_unidad_deducible3, hisp_unidad_suma_aseg4 as pol_unidad_suma_aseg4, hisp_unidad_deducible4 as pol_unidad_deducible4,
    hisp_unidad_suma_aseg5 as pol_unidad_suma_aseg5, hisp_unidad_deducible5 as pol_unidad_deducible5, hisp_unidad_suma_aseg6 as pol_unidad_suma_aseg6, hisp_unidad_deducible6 as pol_unidad_deducible6,
    hisp_unidad_suma_aseg7 as pol_unidad_suma_aseg7, hisp_unidad_deducible7 as pol_unidad_deducible7, hisp_unidad_suma_aseg8 as pol_unidad_suma_aseg8, hisp_unidad_deducible8 as pol_unidad_deducible8,
    hisp_unidad_suma_aseg9 as pol_unidad_suma_aseg9, hisp_unidad_deducible9 as pol_unidad_deducible9, hisp_unidad_suma_aseg10 as pol_unidad_suma_aseg10, hisp_unidad_deducible10 as pol_unidad_deducible10,
    hisp_unidad_suma_aseg11 as pol_unidad_suma_aseg11, hisp_unidad_deducible11 as pol_unidad_deducible11, hisp_unidad_suma_aseg12 as pol_unidad_suma_aseg12, hisp_unidad_deducible12 as pol_unidad_deducible12,
    hisp_unidad_suma_aseg13 as pol_unidad_suma_aseg13, hisp_unidad_deducible13 as pol_unidad_deducible13, hisp_unidad_suma_aseg14 as pol_unidad_suma_aseg14, hisp_unidad_deducible14 as pol_unidad_deducible14,
    hisp_unidad_suma_aseg15 as pol_unidad_suma_aseg15, hisp_unidad_deducible15 as pol_unidad_deducible15 , hisp_sramo_cont as  POL_SRAMO_CONT
	,hisp_unidad_coaseguro1 pol_unidad_coaseguro1,hisp_unidad_coaseguro2 pol_unidad_coaseguro2,hisp_unidad_coaseguro3 pol_unidad_coaseguro3,hisp_unidad_coaseguro4 pol_unidad_coaseguro4,hisp_unidad_coaseguro5 pol_unidad_coaseguro5,
	hisp_unidad_coaseguro6 pol_unidad_coaseguro6,hisp_unidad_coaseguro7 pol_unidad_coaseguro7,hisp_unidad_coaseguro8 pol_unidad_coaseguro8,hisp_unidad_coaseguro9 pol_unidad_coaseguro9,hisp_unidad_coaseguro10 pol_unidad_coaseguro10,
	hisp_unidad_coaseguro11 pol_unidad_coaseguro11,hisp_unidad_coaseguro12 pol_unidad_coaseguro12,hisp_unidad_coaseguro13 pol_unidad_coaseguro13,hisp_unidad_coaseguro14 pol_unidad_coaseguro14,hisp_unidad_coaseguro15 pol_unidad_coaseguro15,
	fechaaud
    FROM bddlcru.gaz0_hispoliza
	where end_num_tipo = 0 and end_num_consec = 0
	
	)A
	inner join 
	( SELECT pol_oficina, pol_numero,pol_ofi_renovac,pol_num_renovac, max(fechaaud) AS fechaaud
FROM  bddlcru.gaz0_poliza 
group by pol_oficina, pol_numero,pol_ofi_renovac,pol_num_renovac
union all 
select hisp_oficina as pol_oficina, hisp_numero as pol_numero,hisp_ofi_renovac as pol_ofi_renovac,hisp_num_renovac as pol_num_renovac, max(fechaaud) AS fechaaud
from  bddlcru.gaz0_hispoliza
group by hisp_oficina , hisp_numero ,hisp_ofi_renovac ,hisp_num_renovac )Maximos

on 
maximos.pol_oficina = A.pol_oficina AND
maximos.pol_numero = A.pol_numero AND
maximos.pol_ofi_renovac = A.pol_ofi_renovac AND
maximos.pol_num_renovac = A.pol_num_renovac AND
maximos.fechaaud = A.fechaaud 
;