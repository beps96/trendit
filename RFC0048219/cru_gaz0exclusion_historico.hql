--ok-- bddlcru.gaz0_exclusion
drop table if EXISTS test_migracion.exclsion_magno;  
create table test_migracion.exclsion_magno as 
select C.* from test_migracion.gaz0_exclusion C where C.idmd5 not in (
select a.idmd5 from test_migracion.gaz0_exclusion A
inner JOIN bddlcru.gaz0_exclusion B
on trim(A.idmd5) = trim(B.idmd5));

drop table if EXISTS test_migracion.exclsion_magno2;  
create table test_migracion.exclsion_magno2 as 
SELECT * from test_migracion.exclsion_magno WHERE pol_numero is not null; 

insert into  bddlcru.gaz0_exclusion 
select pol_oficina	,pol_numero	,pol_ofi_renovac	,pol_num_renovac	,pol_fec_ini_vig	,pol_fec_fin_vig	,pol_status	,exc_d2_user_alta	,exc_d2_fec_movto	,exc_ofna	,exc_poliza	,exc_certificado	,exc_numero	,exc_tipo	,exc_texto	,exc_numtexto	,exc_consec	,exc_texto_armado ,'' as exc_texto_descr_titulo ,'' as exc_texto_descr_var1 ,'' as exc_texto_descr_var2	,'' as exc_texto_descr_var3	,'' as exc_texto_descr_var4	,'' as exc_texto_descr_var5	,'' as exc_texto_descr_var6	,'' as exc_texto_descr_var7	,'' as exc_texto_descr_var8	,'' as exc_estatus_endoso, exc_observa	,id_hist	,idmd5	,idmd5completo	,fechaaud	,sistorig
from test_migracion.exclsion_magno2;

--ok-- bddlint.gaz0_exclusionsc
drop table if EXISTS test_migracion.exclsionsc_magno;  
create table test_migracion.exclsionsc_magno as 
select C.* from test_migracion.gaz0_exclusionsc C where C.idmd5 not in (
select a.idmd5 from test_migracion.gaz0_exclusionsc A
inner JOIN bddlint.gaz0_exclusionsc B
on trim(A.idmd5) = trim(B.idmd5));

drop table if EXISTS test_migracion.exclsionsc_magno2;  
create table test_migracion.exclsionsc_magno2 as 
SELECT * from test_migracion.exclsionsc_magno WHERE pol_numero is not null; 

insert into  bddlint.gaz0_exclusionsc 
select pol_oficina, pol_numero, pol_ofi_renovac, pol_num_renovac, pol_fec_ini_vig, pol_fec_fin_vig, pol_status, exc_d2_user_alta, exc_d2_fec_movto, exc_ofna, exc_poliza, exc_certificado, exc_numero, exc_tipo, exc_texto, exc_numtexto, exc_consec, exc_texto_armado, '' as exc_texto_descr_titulo ,'' as exc_texto_descr_var1 ,'' as exc_texto_descr_var2, '' as exc_texto_descr_var3	,'' as exc_texto_descr_var4	,'' as exc_texto_descr_var5	,'' as exc_texto_descr_var6	,'' as exc_texto_descr_var7	,'' as exc_texto_descr_var8	,'' as exc_estatus_endoso, exc_observa, id_hist, idmd5, idmd5completo, fechaaud, sistorig
from test_migracion.exclsionsc_magno2;

--ok-- bddlcru.gaz0_exclusionhis
drop table if EXISTS test_migracion.exclsionhis_magno;  
create table test_migracion.exclsionhis_magno as 
select C.* from test_migracion.gaz0_exclusionhis C where C.idmd5 not in (
select a.idmd5 from test_migracion.gaz0_exclusionhis A
inner JOIN bddlcru.gaz0_exclusionhis B
on trim(A.idmd5) = trim(B.idmd5));

drop table if EXISTS test_migracion.exclsionhis_magno2;  
create table test_migracion.exclsionhis_magno2 as 
SELECT * from test_migracion.exclsionhis_magno WHERE pol_numero is not null; 

insert into  bddlcru.gaz0_exclusionhis 
select pol_oficina, pol_numero, pol_ofi_renovac, pol_num_renovac, pol_fec_ini_vig, pol_fec_fin_vig, pol_status, exc_d2_user_alta, exc_d2_fec_movto, exc_ofna, exc_poliza, exc_certificado, exc_numero, exc_tipo, exc_texto, exc_numtexto, exc_consec, exc_texto_armado, '' as exc_texto_descr_titulo ,'' as exc_texto_descr_var1 ,'' as exc_texto_descr_var2, '' as exc_texto_descr_var3	,'' as exc_texto_descr_var4	,'' as exc_texto_descr_var5	,'' as exc_texto_descr_var6	,'' as exc_texto_descr_var7	,'' as exc_texto_descr_var8	,'' as exc_estatus_endoso,  exc_observa, id_hist, idmd5, idmd5completo, fechaaud, sistorig, fechhist
from test_migracion.exclsionhis_magno2;

