--ok-- BDDLAPR.CNM_ENDOSOhis

drop table if EXISTS test_migracion.endoso_magnohis;  
create table test_migracion.endoso_magnohis as 
select C.* from test_migracion.cnm_endosohis C where C.idmd5 not in (
select a.idmd5 from test_migracion.cnm_endosohis A
inner JOIN BDDLAPR.CNM_ENDOSOhis B
on trim(A.idmd5) = trim(B.idmd5));

drop table if EXISTS test_migracion.endoso_magnohis2;  
create table test_migracion.endoso_magnohis2 as 
SELECT * from test_migracion.endoso_magnohis WHERE num_poliza is not null; 

insert into BDDLAPR.CNM_ENDOSOhis
select num_poliza, num_poliza_cobranza, cve_oficina_endoso, descr_oficina_endoso, num_endoso, cve_tipo_endoso, descr_tipo_endoso, descr_nivel_aplicacion, certificado_endoso, cve_endoso_exclusion, texto_endoso, fch_ini_vig_endoso, fch_fin_vig_endoso, cve_usuario_endoso, nombre_usuario_endoso, cve_estatus_endoso, descr_estatus_endoso, fch_emision_endoso, cast('' as varchar (60)) as descr_texto_endoso_titulo, cast('' as varchar (60)) as descr_texto_endoso_var1, cast('' as varchar (60)) as descr_texto_endoso_var2, cast('' as varchar (60)) as descr_texto_endoso_var3, cast('' as varchar (60)) as descr_texto_endoso_var4, cast('' as varchar (60)) as descr_texto_endoso_var5, cast('' as varchar (60)) as descr_texto_endoso_var6, cast('' as varchar (60)) as descr_texto_endoso_var7, cast('' as varchar (60)) as descr_texto_endoso_var8, cast('' as varchar (1)) as exc_estatus_endoso, idmd5, idmd5completo, fechaaud, cve_subtipo_endoso, cve_sistema, fechaaud_his 
from test_migracion.endoso_magnohis2

--ok-- BDDLAPR.CNM_ENDOSO
drop table if EXISTS test_migracion.endoso_magno;  
create table test_migracion.endoso_magno as 
select C.* from test_migracion.cnm_endoso C where C.idmd5 not in (
select a.idmd5 from test_migracion.cnm_endoso A
inner JOIN bddlapr.cnm_endoso B
on trim(A.idmd5) = trim(B.idmd5));

drop table if EXISTS test_migracion.endoso_magno2;  
create table test_migracion.endoso_magno2 as 
SELECT * from test_migracion.endoso_magno WHERE num_poliza is not null; 

insert into BDDLAPR.CNM_ENDOSO
select num_poliza, num_poliza_cobranza, cve_oficina_endoso, descr_oficina_endoso, num_endoso, cve_tipo_endoso, descr_tipo_endoso, descr_nivel_aplicacion, certificado_endoso, cve_endoso_exclusion, texto_endoso, fch_ini_vig_endoso, fch_fin_vig_endoso, cve_usuario_endoso, nombre_usuario_endoso, cve_estatus_endoso, descr_estatus_endoso, fch_emision_endoso, cast('' as varchar (60)) as des_titulo_endoso, cast('' as varchar (60)) as des_var1_endoso, cast('' as varchar (60)) as des_var2_endoso, cast('' as varchar (60)) as des_var3_endoso, cast('' as varchar (60)) as des_var4_endoso, cast('' as varchar (60)) as des_var5_endoso, cast('' as varchar (60)) as des_var6_endoso, cast('' as varchar (60)) as des_var7_endoso, cast('' as varchar (60)) as des_var8_endoso, cast('' as varchar (1)) as ind_endoso_cancelado, idmd5, idmd5completo, fechaaud, cve_subtipo_endoso, cve_sistema 
from test_migracion.endoso_magno2
