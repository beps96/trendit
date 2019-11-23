--ok-- bddlcru.gaz0_exclusion
insert into  bddlcru.gaz0_exclusion 
select pol_oficina	,pol_numero	,pol_ofi_renovac	,pol_num_renovac	,pol_fec_ini_vig	,pol_fec_fin_vig	,pol_status	,exc_d2_user_alta	,exc_d2_fec_movto	,exc_ofna	,exc_poliza	,exc_certificado	,exc_numero	,exc_tipo	,exc_texto	,exc_numtexto	,exc_consec	,exc_texto_armado ,'' as exc_texto_descr_titulo ,'' as exc_texto_descr_var1 ,'' as exc_texto_descr_var2	,'' as exc_texto_descr_var3	,'' as exc_texto_descr_var4	,'' as exc_texto_descr_var5	,'' as exc_texto_descr_var6	,'' as exc_texto_descr_var7	,'' as exc_texto_descr_var8	,'' as exc_estatus_endoso, exc_observa	,id_hist	,idmd5	,idmd5completo	,fechaaud	,sistorig
from test_migracion.gaz0_exclusion;

--ok-- bddlint.gaz0_exclusionsc
insert into  bddlint.gaz0_exclusionsc 
select pol_oficina, pol_numero, pol_ofi_renovac, pol_num_renovac, pol_fec_ini_vig, pol_fec_fin_vig, pol_status, exc_d2_user_alta, exc_d2_fec_movto, exc_ofna, exc_poliza, exc_certificado, exc_numero, exc_tipo, exc_texto, exc_numtexto, exc_consec, exc_texto_armado, '' as exc_texto_descr_titulo ,'' as exc_texto_descr_var1 ,'' as exc_texto_descr_var2, '' as exc_texto_descr_var3	,'' as exc_texto_descr_var4	,'' as exc_texto_descr_var5	,'' as exc_texto_descr_var6	,'' as exc_texto_descr_var7	,'' as exc_texto_descr_var8	,'' as exc_estatus_endoso, exc_observa, id_hist, idmd5, idmd5completo, fechaaud, sistorig
from test_migracion.gaz0_exclusionsc;

--ok-- bddlcru.gaz0_exclusionhis
insert into  bddlcru.gaz0_exclusionhis 
select pol_oficina, pol_numero, pol_ofi_renovac, pol_num_renovac, pol_fec_ini_vig, pol_fec_fin_vig, pol_status, exc_d2_user_alta, exc_d2_fec_movto, exc_ofna, exc_poliza, exc_certificado, exc_numero, exc_tipo, exc_texto, exc_numtexto, exc_consec, exc_texto_armado, '' as exc_texto_descr_titulo ,'' as exc_texto_descr_var1 ,'' as exc_texto_descr_var2, '' as exc_texto_descr_var3	,'' as exc_texto_descr_var4	,'' as exc_texto_descr_var5	,'' as exc_texto_descr_var6	,'' as exc_texto_descr_var7	,'' as exc_texto_descr_var8	,'' as exc_estatus_endoso,  exc_observa, id_hist, idmd5, idmd5completo, fechaaud, sistorig, fechhist
from test_migracion.gaz0_exclusionhis;

