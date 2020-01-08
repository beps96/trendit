
DROP TABLE IF EXISTS BDDLTRN.STG_CNM_CBZA_UNION_GAZ0_ASEGURADO_ALTERNO PURGE;
CREATE TABLE IF NOT EXISTS BDDLTRN.STG_CNM_CBZA_UNION_GAZ0_ASEGURADO_ALTERNO AS
SELECT DISTINCT ase_num_certif,ase_cve_parien,ase_pol_ofna,ase_pol_num,pol_ofi_renovac,pol_num_renovac,end_fecha_ini,end_subramo,ase_ramo1,ase_plan1,ase_ramo2,ase_plan2,
ase_ramo3,ase_plan3,ase_ramo4,ase_plan4,ase_ramo5,ase_plan5,ase_ramo6,ase_plan6,ase_ramo7,ase_plan7,ase_ramo8,ase_plan8,ase_ramo9,ase_plan9,ase_ramo10,ase_plan10 
FROM 
(
 SELECT ase_num_certif,ase_cve_parien,ase_pol_ofna,ase_pol_num,pol_ofi_renovac,pol_num_renovac,end_fecha_ini,end_subramo,ase_ramo1,ase_plan1,ase_ramo2,ase_plan2,ase_ramo3,
ase_plan3,ase_ramo4,ase_plan4,ase_ramo5,ase_plan5,ase_ramo6,ase_plan6,ase_ramo7,ase_plan7,ase_ramo8,ase_plan8,ase_ramo9,ase_plan9,ase_ramo10,ase_plan10
FROM BDDLCRU.gaz0_asegurado
UNION ALL
---historico
SELECT      ase_num_certif,ase_cve_parien,ase_pol_ofna,ase_pol_num,pol_ofi_renovac,pol_num_renovac,end_fecha_ini,end_subramo,ase_ramo1,ase_plan1,ase_ramo2,ase_plan2,ase_ramo3,
ase_plan3,ase_ramo4,ase_plan4,ase_ramo5,ase_plan5,ase_ramo6,ase_plan6,ase_ramo7,ase_plan7,ase_ramo8,ase_plan8,ase_ramo9,ase_plan9,ase_ramo10,ase_plan10
FROM BDDLCRU.gaz0_his_asegurado 
)B;

DROP TABLE IF EXISTS BDDLTRN.STG_CNM_CBZA_UNION_GAZ0_ASEGURADO_BR_ALTERNO PURGE;
CREATE TABLE IF NOT EXISTS BDDLTRN.STG_CNM_CBZA_UNION_GAZ0_ASEGURADO_BR_ALTERNO AS
SELECT ase_num_certif,ase_cve_parien,ase_pol_ofna,ase_pol_num,pol_ofi_renovac,pol_num_renovac,end_fecha_ini,end_subramo,
MAP(
'aseg01', concat(nvl(ase_ramo1,''),'-',nvl(ase_plan1,'')),
'aseg02', concat(nvl(ase_ramo2,''),'-',nvl(ase_plan2,'')),
'aseg03', concat(nvl(ase_ramo3,''),'-',nvl(ase_plan3,'')),
'aseg04', concat(nvl(ase_ramo4,''),'-',nvl(ase_plan4,'')),
'aseg05', concat(nvl(ase_ramo5,''),'-',nvl(ase_plan5,'')),
'aseg06', concat(nvl(ase_ramo6,''),'-',nvl(ase_plan6,'')),
'aseg07', concat(nvl(ase_ramo7,''),'-',nvl(ase_plan7,'')),
'aseg08', concat(nvl(ase_ramo8,''),'-',nvl(ase_plan8,'')),
'aseg09', concat(nvl(ase_ramo9,''),'-',nvl(ase_plan9,'')),
'aseg010', concat(nvl(ase_ramo10,''),'-',nvl(ase_plan10,''))) AS AsegAZUL

from 

BDDLTRN.STG_CNM_CBZA_UNION_GAZ0_ASEGURADO_ALTERNO;

--SLIT MAP
DROP TABLE IF EXISTS BDDLTRN.STG_CNM_CBZA_UNION_GAZ0_ASEGURADO_SPT_BR_ALTERNO PURGE;
CREATE TABLE IF NOT EXISTS BDDLTRN.STG_CNM_CBZA_UNION_GAZ0_ASEGURADO_SPT_BR_ALTERNO  AS
SELECT 
ase_num_certif,ase_cve_parien,ase_pol_ofna,ase_pol_num,pol_ofi_renovac,pol_num_renovac,end_fecha_ini,end_subramo,
NUM_ASEGURADO, SPLIT(asegura,'-') as aseguraMap
from  BDDLTRN.STG_CNM_CBZA_UNION_GAZ0_ASEGURADO_BR_ALTERNO
lateral view explode (AsegAzul) explode_table as num_asegurado, asegura;

--extract map
DROP TABLE IF EXISTS BDDLTRN.STG_CNM_CBZA_UNION_GAZ0_ASEGURADO_MAP_ALTERNO PURGE;
CREATE TABLE IF NOT EXISTS BDDLTRN.STG_CNM_CBZA_UNION_GAZ0_ASEGURADO_MAP_ALTERNO AS
SELECT * FROM (
SELECT 
ase_num_certif,ase_cve_parien,ase_pol_ofna,ase_pol_num,pol_ofi_renovac,pol_num_renovac,end_fecha_ini,end_subramo,
  aseguraMap[0] as ase_ramo,
  aseguraMap[1] as ase_plan
  from BDDLTRN.STG_CNM_CBZA_UNION_GAZ0_ASEGURADO_SPT_BR_ALTERNO
) as TMP WHERE ase_ramo !='';
