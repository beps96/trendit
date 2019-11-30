--transpose
DROP TABLE IF EXISTS BDDLTRN.STG_TMP_TRANSPOSE_COBERTURA PURGE;
CREATE TABLE IF NOT EXISTS BDDLTRN.STG_TMP_TRANSPOSE_COBERTURA AS
 SELECT pol_oficina, pol_numero,pol_ofi_renovac,pol_num_renovac,
               end_fecha_ini,end_num_oficina,end_num_tipo,end_num_consec,
               end_num_control,end_liga_pol_ase,end_incluye_aseg,end_subramo ,end_fecha_fin
    , MAP(
    'cob01', concat(nvl(pol_nombre_cob1,''),'-',nvl(pol_coaseguro1,''),'-',nvl(pol_suma_aseg1,''),'-',nvl(pol_deducible1,''),'-',nvl(pol_plan1,''),'-',nvl(pol_cob_stdr1,''),'-',nvl(pol_unidad_suma_aseg1,''),'-',nvl(pol_unidad_deducible1,'')),
    'cob02', concat(nvl(pol_nombre_cob2,''),'-',nvl(pol_coaseguro2,''),'-',nvl(pol_suma_aseg2,''),'-',nvl(pol_deducible2,''),'-',nvl(pol_plan2,''),'-',nvl(pol_cob_stdr2,''),'-',nvl(pol_unidad_suma_aseg2,''),'-',nvl(pol_unidad_deducible2,'')),
    'cob03', concat(nvl(pol_nombre_cob3,''),'-',nvl(pol_coaseguro3,''),'-',nvl(pol_suma_aseg3,''),'-',nvl(pol_deducible3,''),'-',nvl(pol_plan3,''),'-',nvl(pol_cob_stdr3,''),'-',nvl(pol_unidad_suma_aseg3,''),'-',nvl(pol_unidad_deducible3,'')),
    'cob04', concat(nvl(pol_nombre_cob4,''),'-',nvl(pol_coaseguro4,''),'-',nvl(pol_suma_aseg4,''),'-',nvl(pol_deducible4,''),'-',nvl(pol_plan4,''),'-',nvl(pol_cob_stdr4,''),'-',nvl(pol_unidad_suma_aseg4,''),'-',nvl(pol_unidad_deducible4,'')),
    'cob05', concat(nvl(pol_nombre_cob5,''),'-',nvl(pol_coaseguro5,''),'-',nvl(pol_suma_aseg5,''),'-',nvl(pol_deducible5,''),'-',nvl(pol_plan5,''),'-',nvl(pol_cob_stdr5,''),'-',nvl(pol_unidad_suma_aseg5,''),'-',nvl(pol_unidad_deducible5,'')),
    'cob06', concat(nvl(pol_nombre_cob6,''),'-',nvl(pol_coaseguro6,''),'-',nvl(pol_suma_aseg6,''),'-',nvl(pol_deducible6,''),'-',nvl(pol_plan6,''),'-',nvl(pol_cob_stdr6,''),'-',nvl(pol_unidad_suma_aseg6,''),'-',nvl(pol_unidad_deducible6,'')),
    'cob07', concat(nvl(pol_nombre_cob7,''),'-',nvl(pol_coaseguro7,''),'-',nvl(pol_suma_aseg7,''),'-',nvl(pol_deducible7,''),'-',nvl(pol_plan7,''),'-',nvl(pol_cob_stdr7,''),'-',nvl(pol_unidad_suma_aseg7,''),'-',nvl(pol_unidad_deducible7,'')),
    'cob08', concat(nvl(pol_nombre_cob8,''),'-',nvl(pol_coaseguro8,''),'-',nvl(pol_suma_aseg8,''),'-',nvl(pol_deducible8,''),'-',nvl(pol_plan8,''),'-',nvl(pol_cob_stdr8,''),'-',nvl(pol_unidad_suma_aseg8,''),'-',nvl(pol_unidad_deducible8,'')),
    'cob09', concat(nvl(pol_nombre_cob9,''),'-',nvl(pol_coaseguro9,''),'-',nvl(pol_suma_aseg9,''),'-',nvl(pol_deducible9,''),'-',nvl(pol_plan9,''),'-',nvl(pol_cob_stdr9,''),'-',nvl(pol_unidad_suma_aseg9,''),'-',nvl(pol_unidad_deducible9,'')),
    'cob10', concat(nvl(pol_nombre_cob10,''),'-',nvl(pol_coaseguro10,''),'-',nvl(pol_suma_aseg10,''),'-',nvl(pol_deducible10,''),'-',nvl(pol_plan10,''),'-',nvl(pol_cob_stdr10,''),'-',nvl(pol_unidad_suma_aseg10,''),'-',nvl(pol_unidad_deducible10,'')), 
    'cob11', concat(nvl(pol_nombre_cob11,''),'-',nvl(pol_coaseguro11,''),'-',nvl(pol_suma_aseg11,''),'-',nvl(pol_deducible11,''),'-',nvl(pol_plan11,''),'-',nvl(pol_cob_stdr11,''),'-',nvl(pol_unidad_suma_aseg11,''),'-',nvl(pol_unidad_deducible11,'')), 
    'cob12', concat(nvl(pol_nombre_cob12,''),'-',nvl(pol_coaseguro12,''),'-',nvl(pol_suma_aseg12,''),'-',nvl(pol_deducible12,''),'-',nvl(pol_plan12,''),'-',nvl(pol_cob_stdr12,''),'-',nvl(pol_unidad_suma_aseg12,''),'-',nvl(pol_unidad_deducible12,'')), 
    'cob13', concat(nvl(pol_nombre_cob13,''),'-',nvl(pol_coaseguro13,''),'-',nvl(pol_suma_aseg13,''),'-',nvl(pol_deducible13,''),'-',nvl(pol_plan13,''),'-',nvl(pol_cob_stdr13,''),'-',nvl(pol_unidad_suma_aseg13,''),'-',nvl(pol_unidad_deducible13,'')), 
    'cob14', concat(nvl(pol_nombre_cob14,''),'-',nvl(pol_coaseguro14,''),'-',nvl(pol_suma_aseg14,''),'-',nvl(pol_deducible14,''),'-',nvl(pol_plan14,''),'-',nvl(pol_cob_stdr14,''),'-',nvl(pol_unidad_suma_aseg14,''),'-',nvl(pol_unidad_deducible14,'')), 
    'cob15', concat(nvl(pol_nombre_cob15,''),'-',nvl(pol_coaseguro15,''),'-',nvl(pol_suma_aseg15,''),'-',nvl(pol_deducible15,''),'-',nvl(pol_plan15,''),'-',nvl(pol_cob_stdr15,''),'-',nvl(pol_unidad_suma_aseg15,''),'-',nvl(pol_unidad_deducible15,''))) AS coberturaAZUL
  FROM BDDLTRN.STG_CNM_CBZA_UNION_GAZ0_POLIZA;

--Split Map
DROP TABLE IF EXISTS BDDLTRN.STG_TMP_SPLIT_MAP_COBERTURA PURGE;
CREATE TABLE IF NOT EXISTS BDDLTRN.STG_TMP_SPLIT_MAP_COBERTURA AS
SELECT pol_oficina, pol_numero,pol_ofi_renovac,pol_num_renovac,
                end_fecha_ini,end_num_oficina,end_num_tipo,end_num_consec,
                end_num_control,end_liga_pol_ase,end_incluye_aseg,end_subramo ,end_fecha_fin,
                num_cobertura, split(cobertura,'-') as cobeturaMap
FROM BDDLTRN.STG_TMP_TRANSPOSE_COBERTURA
LATERAL VIEW EXPLODE(coberturaAZUL) explode_table AS num_cobertura, cobertura;
--Extract Map
DROP TABLE IF EXISTS BDDLTRN.STG_CNM_TRANSPOSE_COBERTURA  PURGE;
CREATE TABLE IF NOT EXISTS BDDLTRN.STG_CNM_TRANSPOSE_COBERTURA AS
select * FROM(
select pol_oficina, pol_numero,pol_ofi_renovac,pol_num_renovac,
                end_fecha_ini,end_num_oficina,end_num_tipo,end_num_consec,
                end_num_control,end_liga_pol_ase,end_incluye_aseg,end_subramo ,end_fecha_fin,
                cobeturaMap[0] as pol_nombre_cob,
                cobeturaMap[1] as pol_coaseguro,
                cobeturaMap[2] as pol_suma_aseg,
                cobeturaMap[3] as pol_deducible,
                cobeturaMap[4] as pol_plan,
                cobeturaMap[5] as pol_stdr
                ,cobeturaMap[6] as pol_uni_sum,
                cobeturaMap[7] as pol_uni_dedu
FROM  BDDLTRN.STG_TMP_SPLIT_MAP_COBERTURA ) as TMP 
where pol_nombre_cob != '';
