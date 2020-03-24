drop table if exists  bddltrn.agrupado_gaz0_polizahis_duplicados;
create table bddltrn.agrupado_gaz0_polizahis_duplicados as 

SELECT *,
       row_number() over (partition by pol_oficina, pol_numero, pol_ofi_renovac, pol_num_renovac order by end_num_consec desc, end_num_control desc, fechaaud desc) ultimo_reg,
       CAST( SUBSTR(CAST(pol_fec_emision AS STRING),1,6) AS INT) AS pol_fec_emision_join
  FROM (
        SELECT 	gaz.pol_oficina as pol_oficina,          	gaz.pol_numero as pol_numero,          			gaz.pol_ofi_renovac as pol_ofi_renovac,          
		gaz.pol_num_renovac as pol_num_renovac,     	gaz.end_fecha_ini as end_fecha_ini,          	gaz.end_num_oficina as end_num_oficina,
		gaz.end_num_tipo as end_num_tipo,           	gaz.end_num_consec as end_num_consec,          	gaz.end_num_control as end_num_control,          
		gaz.end_liga_pol_ase as end_liga_pol_ase,   	gaz.end_incluye_aseg as end_incluye_aseg,       gaz.end_subramo as end_subramo,
		gaz.end_fecha_fin as end_fecha_fin,     		gaz.end_status as end_status,          			gaz.end_cve_pago as end_cve_pago,          
		gaz.end_control as end_control,         		gaz.end_prima_neta as end_prima_neta,          	gaz.end_der_pol as end_der_pol,          
		gaz.end_costo as end_costo,          			gaz.pol_pool as pol_pool,          				gaz.pol_razon_soc as pol_razon_soc,          
		gaz.pol_calle as pol_calle,          			gaz.pol_colonia as pol_colonia,         		gaz.pol_cod_postal as pol_cod_postal,          
		gaz.pol_poblacion as pol_poblacion,     		gaz.pol_estado as pol_estado,          			gaz.pol_zona as pol_zona,          
		gaz.pol_cve_agente as pol_cve_agente,   		gaz.pol_por_comis as pol_por_comis,          	gaz.pol_cve_pago as pol_cve_pago,          
		gaz.pol_num_tipo as pol_num_tipo,       		gaz.pol_cve_experiencia as pol_cve_experiencia, gaz.pol_format_fnac as pol_format_fnac,          
		gaz.pol_num_asg_pri as pol_num_asg_pri, 		gaz.pol_num_asg_dep as pol_num_asg_dep,         gaz.pol_status_calc as pol_status_calc,          
		gaz.pol_status_emis as pol_status_emis, 		gaz.pol_fec_ini_vig as pol_fec_ini_vig,         gaz.pol_fec_fin_vig as pol_fec_fin_vig,          
		gaz.pol_fec_origen as pol_fec_origen,   		gaz.pol_dispo1 as pol_dispo1,          			gaz.pol_fec_emis_endoso as pol_fec_emis_endoso,          
		gaz.pol_fec_extran as pol_fec_extran,   		gaz.pol_endoso_forzado as pol_endoso_forzado,   gaz.pol_pointer_lim as pol_pointer_lim,          
		gaz.pol_prima_neta as pol_prima_neta,   		gaz.pol_gmm_lai as pol_gmm_lai,          		gaz.pol_nombre_cob1 as pol_nombre_cob1,          
		gaz.pol_plan1 as pol_plan1,          			gaz.pol_cob_stdr1 as pol_cob_stdr1,          	gaz.pol_suma_aseg1 as pol_suma_aseg1,          
		gaz.pol_prima_cober1 as pol_prima_cober1,   	gaz.pol_coaseguro1 as pol_coaseguro1,          	gaz.pol_deducible1 as pol_deducible1,          
		gaz.pol_iq1 as pol_iq1,          				gaz.pol_fact_codesa1 as pol_fact_codesa1,       gaz.pol_experiencia1 as pol_experiencia1,          
		gaz.pol_preexistencia1 as pol_preexistencia1,   gaz.pol_tarifa1 as pol_tarifa1,          		gaz.pol_nombre_cob2 as pol_nombre_cob2,          
		gaz.pol_plan2 as pol_plan2,          			gaz.pol_cob_stdr2 as pol_cob_stdr2,          	gaz.pol_suma_aseg2 as pol_suma_aseg2,          
		gaz.pol_prima_cober2 as pol_prima_cober2,       gaz.pol_coaseguro2 as pol_coaseguro2,          	gaz.pol_deducible2 as pol_deducible2,          
		gaz.pol_iq2 as pol_iq2,          				gaz.pol_fact_codesa2 as pol_fact_codesa2,       gaz.pol_experiencia2 as pol_experiencia2,          
		gaz.pol_preexistencia2 as pol_preexistencia2,   gaz.pol_tarifa2 as pol_tarifa2,          		gaz.pol_nombre_cob3 as pol_nombre_cob3,          
		gaz.pol_plan3 as pol_plan3,          			gaz.pol_cob_stdr3 as pol_cob_stdr3,          	gaz.pol_suma_aseg3 as pol_suma_aseg3,          
		gaz.pol_prima_cober3 as pol_prima_cober3,       gaz.pol_coaseguro3 as pol_coaseguro3,          	gaz.pol_deducible3 as pol_deducible3,          
		gaz.pol_iq3 as pol_iq3,          				gaz.pol_fact_codesa3 as pol_fact_codesa3,       gaz.pol_experiencia3 as pol_experiencia3,          
		gaz.pol_preexistencia3 as pol_preexistencia3,   gaz.pol_tarifa3 as pol_tarifa3,          		gaz.pol_nombre_cob4 as pol_nombre_cob4,          
		gaz.pol_plan4 as pol_plan4,          			gaz.pol_cob_stdr4 as pol_cob_stdr4,          	gaz.pol_suma_aseg4 as pol_suma_aseg4,          
		gaz.pol_prima_cober4 as pol_prima_cober4,       gaz.pol_coaseguro4 as pol_coaseguro4,          	gaz.pol_deducible4 as pol_deducible4,          
		gaz.pol_iq4 as pol_iq4,          				gaz.pol_fact_codesa4 as pol_fact_codesa4,       gaz.pol_experiencia4 as pol_experiencia4,          
		gaz.pol_preexistencia4 as pol_preexistencia4,   gaz.pol_tarifa4 as pol_tarifa4,          		gaz.pol_nombre_cob5 as pol_nombre_cob5,          
		gaz.pol_plan5 as pol_plan5,          			gaz.pol_cob_stdr5 as pol_cob_stdr5,          	gaz.pol_suma_aseg5 as pol_suma_aseg5,          
		gaz.pol_prima_cober5 as pol_prima_cober5,       gaz.pol_coaseguro5 as pol_coaseguro5,         	gaz.pol_deducible5 as pol_deducible5,          
		gaz.pol_iq5 as pol_iq5,          				gaz.pol_fact_codesa5 as pol_fact_codesa5,       gaz.pol_experiencia5 as pol_experiencia5,          
		gaz.pol_preexistencia5 as pol_preexistencia5,   gaz.pol_tarifa5 as pol_tarifa5,          		gaz.pol_nombre_cob6 as pol_nombre_cob6,          
		gaz.pol_plan6 as pol_plan6,          			gaz.pol_cob_stdr6 as pol_cob_stdr6,          	gaz.pol_suma_aseg6 as pol_suma_aseg6,          
		gaz.pol_prima_cober6 as pol_prima_cober6,       gaz.pol_coaseguro6 as pol_coaseguro6,          	gaz.pol_deducible6 as pol_deducible6,          
		gaz.pol_iq6 as pol_iq6,          				gaz.pol_fact_codesa6 as pol_fact_codesa6,       gaz.pol_experiencia6 as pol_experiencia6,          
		gaz.pol_preexistencia6 as pol_preexistencia6,   gaz.pol_tarifa6 as pol_tarifa6,          		gaz.pol_nombre_cob7 as pol_nombre_cob7,          
		gaz.pol_plan7 as pol_plan7,          			gaz.pol_cob_stdr7 as pol_cob_stdr7,          	gaz.pol_suma_aseg7 as pol_suma_aseg7,          
		gaz.pol_prima_cober7 as pol_prima_cober7,       gaz.pol_coaseguro7 as pol_coaseguro7,          	gaz.pol_deducible7 as pol_deducible7,          
		gaz.pol_iq7 as pol_iq7,          				gaz.pol_fact_codesa7 as pol_fact_codesa7,       gaz.pol_experiencia7 as pol_experiencia7,          
		gaz.pol_preexistencia7 as pol_preexistencia7,   gaz.pol_tarifa7 as pol_tarifa7,          		gaz.pol_nombre_cob8 as pol_nombre_cob8,          
		gaz.pol_plan8 as pol_plan8,          			gaz.pol_cob_stdr8 as pol_cob_stdr8,          	gaz.pol_suma_aseg8 as pol_suma_aseg8,          
		gaz.pol_prima_cober8 as pol_prima_cober8,       gaz.pol_coaseguro8 as pol_coaseguro8,          	gaz.pol_deducible8 as pol_deducible8,          
		gaz.pol_iq8 as pol_iq8,          				gaz.pol_fact_codesa8 as pol_fact_codesa8,       gaz.pol_experiencia8 as pol_experiencia8,          
		gaz.pol_preexistencia8 as pol_preexistencia8,   gaz.pol_tarifa8 as pol_tarifa8,          		gaz.pol_nombre_cob9 as pol_nombre_cob9,          
		gaz.pol_plan9 as pol_plan9,          			gaz.pol_cob_stdr9 as pol_cob_stdr9,          	gaz.pol_suma_aseg9 as pol_suma_aseg9,          
		gaz.pol_prima_cober9 as pol_prima_cober9,       gaz.pol_coaseguro9 as pol_coaseguro9,          	gaz.pol_deducible9 as pol_deducible9,
		gaz.pol_iq9 as pol_iq9,          				gaz.pol_fact_codesa9 as pol_fact_codesa9,       gaz.pol_experiencia9 as pol_experiencia9,          
		gaz.pol_preexistencia9 as pol_preexistencia9,   gaz.pol_tarifa9 as pol_tarifa9,          		gaz.pol_nombre_cob10 as pol_nombre_cob10,          
		gaz.pol_plan10 as pol_plan10,          			gaz.pol_cob_stdr10 as pol_cob_stdr10,           gaz.pol_suma_aseg10 as pol_suma_aseg10,          
		gaz.pol_prima_cober10 as pol_prima_cober10,     gaz.pol_coaseguro10 as pol_coaseguro10,         gaz.pol_deducible10 as pol_deducible10,          
		gaz.pol_iq10 as pol_iq10,          				gaz.pol_fact_codesa10 as pol_fact_codesa10,     gaz.pol_experiencia10 as pol_experiencia10,          
		gaz.pol_preexistencia10 as pol_preexistencia10, gaz.pol_tarifa10 as pol_tarifa10,          		gaz.pol_nombre_cob11 as pol_nombre_cob11,          
		gaz.pol_plan11 as pol_plan11,          			gaz.pol_cob_stdr11 as pol_cob_stdr11,          	gaz.pol_suma_aseg11 as pol_suma_aseg11,          
		gaz.pol_prima_cober11 as pol_prima_cober11,     gaz.pol_coaseguro11 as pol_coaseguro11,         gaz.pol_deducible11 as pol_deducible11,          
		gaz.pol_iq11 as pol_iq11,          				gaz.pol_fact_codesa11 as pol_fact_codesa11,     gaz.pol_experiencia11 as pol_experiencia11,          
		gaz.pol_preexistencia11 as pol_preexistencia11, gaz.pol_tarifa11 as pol_tarifa11,          		gaz.pol_nombre_cob12 as pol_nombre_cob12,          
		gaz.pol_plan12 as pol_plan12,          			gaz.pol_cob_stdr12 as pol_cob_stdr12,          	gaz.pol_suma_aseg12 as pol_suma_aseg12,          
		gaz.pol_prima_cober12 as pol_prima_cober12,     gaz.pol_coaseguro12 as pol_coaseguro12,         gaz.pol_deducible12 as pol_deducible12,          
		gaz.pol_iq12 as pol_iq12,          				gaz.pol_fact_codesa12 as pol_fact_codesa12,     gaz.pol_experiencia12 as pol_experiencia12,          
		gaz.pol_preexistencia12 as pol_preexistencia12, gaz.pol_tarifa12 as pol_tarifa12,          		gaz.pol_nombre_cob13 as pol_nombre_cob13,          
		gaz.pol_plan13 as pol_plan13,          			gaz.pol_cob_stdr13 as pol_cob_stdr13,          	gaz.pol_suma_aseg13 as pol_suma_aseg13,          
		gaz.pol_prima_cober13 as pol_prima_cober13,     gaz.pol_coaseguro13 as pol_coaseguro13,         gaz.pol_deducible13 as pol_deducible13,          
		gaz.pol_iq13 as pol_iq13,          				gaz.pol_fact_codesa13 as pol_fact_codesa13,     gaz.pol_experiencia13 as pol_experiencia13,          
		gaz.pol_preexistencia13 as pol_preexistencia13, gaz.pol_tarifa13 as pol_tarifa13,          		gaz.pol_nombre_cob14 as pol_nombre_cob14,          
		gaz.pol_plan14 as pol_plan14,          			gaz.pol_cob_stdr14 as pol_cob_stdr14,          	gaz.pol_suma_aseg14 as pol_suma_aseg14,
		gaz.pol_prima_cober14 as pol_prima_cober14,     gaz.pol_coaseguro14 as pol_coaseguro14,         gaz.pol_deducible14 as pol_deducible14,          
		gaz.pol_iq14 as pol_iq14,          				gaz.pol_fact_codesa14 as pol_fact_codesa14,     gaz.pol_experiencia14 as pol_experiencia14,          
		gaz.pol_preexistencia14 as pol_preexistencia14, gaz.pol_tarifa14 as pol_tarifa14,          		gaz.pol_nombre_cob15 as pol_nombre_cob15,          
		gaz.pol_plan15 as pol_plan15,          			gaz.pol_cob_stdr15 as pol_cob_stdr15,          	gaz.pol_suma_aseg15 as pol_suma_aseg15,          
		gaz.pol_prima_cober15 as pol_prima_cober15,     gaz.pol_coaseguro15 as pol_coaseguro15,         gaz.pol_deducible15 as pol_deducible15,          
		gaz.pol_iq15 as pol_iq15,          				gaz.pol_fact_codesa15 as pol_fact_codesa15,     gaz.pol_experiencia15 as pol_experiencia15,          
		gaz.pol_preexistencia15 as pol_preexistencia15, gaz.pol_tarifa15 as pol_tarifa15,          		gaz.pol_dispo3 as pol_dispo3,          
		gaz.pol_recargos as pol_recargos,          		gaz.pol_derpol as pol_derpol,          			gaz.pol_swt_sida as pol_swt_sida,          
		gaz.pol_existe_ajuste as pol_existe_ajuste,     gaz.pol_tipo_admon as pol_tipo_admon,          	gaz.pol_sramo_cont as pol_sramo_cont,          
		gaz.pol_ofi_adminst as pol_ofi_adminst,         gaz.pol_fact_derpol as pol_fact_derpol,         gaz.pol_dispo4 as pol_dispo4,          
		gaz.pol_dincod as pol_dincod,          			gaz.pol_rfc as pol_rfc,          				gaz.pol_telefono as pol_telefono,          gaz.pol_cond_envio as pol_cond_envio,          
		gaz.pol_fec_emision as pol_fec_emision,         gaz.pol_fec_captura as pol_fec_captura,         gaz.pol_fec_calculo as pol_fec_calculo,          
		gaz.pol_idnom as pol_idnom,          			gaz.pol_sist_ant as pol_sist_ant,          		gaz.pol_cob_comis as pol_cob_comis,          
		gaz.pol_fact_exper as pol_fact_exper,           gaz.pol_fact_cp as pol_fact_cp,          		gaz.pol_cod_postal_alt as pol_cod_postal_alt,          
		gaz.pol_dispo5 as pol_dispo5,          			gaz.pol_cve_iva as pol_cve_iva,          		gaz.pol_ley_tarjeta as pol_ley_tarjeta,          
		gaz.pol_tipo_tarjeta_sin_mm as pol_tipo_tarjeta_sin_mm, gaz.pol_tipo_tarjeta_con_mm as pol_tipo_tarjeta_con_mm,     gaz.pol_sramo_plan as pol_sramo_plan,          
		gaz.pol_tar_aplica as pol_tar_aplica,          			gaz.end_fecha_expide as end_fecha_expide,          			gaz.end_disponible as end_disponible,          
		gaz.end_num_texto as end_num_texto,          			gaz.end_tot_ocurren as end_tot_ocurren,          			gaz.end_tabla_datos as end_tabla_datos,          gaz.id_hist as id_hist,          
		gaz.idmd5 as idmd5,          							gaz.idmd5completo as idmd5completo,          				gaz.fechaaud as fechaaud,          gaz.sistorig as sistorig 
                  FROM bddlcru.gaz0_poliza gaz 
		  WHERE  gaz.end_num_consec = 0
			and gaz.end_num_control = 0
			--AND to_date(fechaaud) >= to_date(current_timestamp())
     union all
        SELECT hisp_oficina as pol_oficina,                 hisp_numero as pol_numero,                   hisp_ofi_renovac as pol_ofi_renovac, 
               hisp_num_renovac as pol_num_renovac,         end_fecha_ini as end_fecha_ini,              end_num_oficina as end_num_oficina, 
               end_num_tipo as end_num_tipo,               end_num_consec as end_num_consec,            end_num_control as end_num_control, 
               end_liga_pol_ase as end_liga_pol_ase,        end_incluye_aseg as end_incluye_aseg,        end_subramo as end_subramo, 
               end_fecha_fin as end_fecha_fin,              end_status as end_status,                    end_cve_pago as end_cve_pago, 
               end_control as end_control,                  end_prima_neta as end_prima_neta,            end_der_pol as end_der_pol, 
               end_costo as end_costo,                      hisp_pool as pol_pool,                       hisp_razon_soc as pol_razon_soc, 
               hisp_calle as pol_calle,                     hisp_colonia as pol_colonia,                 hisp_cod_postal as pol_cod_postal, 	
               hisp_poblacion as pol_poblacion,             hisp_estado as pol_estado,                   hisp_zona as pol_zona, 	
               hisp_cve_agente as pol_cve_agente,           hisp_por_comis as pol_por_comis,             hisp_cve_pago as pol_cve_pago, 	
               hisp_num_tipo as pol_num_tipo,               hisp_cve_experiencia as pol_cve_experiencia, hisp_format_fnac as pol_format_fnac, 	
               hisp_num_asg_pri as pol_num_asg_pri,         hisp_num_asg_dep as pol_num_asg_dep,         hisp_status_calc as pol_status_calc, 	
               hisp_status_emis as pol_status_emis,         hisp_fec_ini_vig as pol_fec_ini_vig,         hisp_fec_fin_vig as pol_fec_fin_vig, 	
               hisp_fec_origen as pol_fec_origen,           hisp_dispo1 as pol_dispo1,                   hisp_fec_emis_endoso as pol_fec_emis_endoso, 	
               hisp_fec_extran as pol_fec_extran,           hisp_endoso_forzado as pol_endoso_forzado,   hisp_pointer_lim as pol_pointer_lim, 	
               hisp_prima_neta as pol_prima_neta,           hisp_gmm_lai as pol_gmm_lai,                 hisp_nombre_cob1 as pol_nombre_cob1, 	
               hisp_plan1 as pol_plan1,                     hisp_cob_stdr1 as pol_cob_stdr1,             hisp_suma_aseg1 as pol_suma_aseg1, 	
               hisp_prima_cober1 as pol_prima_cober1,       hisp_coaseguro1 as pol_coaseguro1,           hisp_deducible1 as pol_deducible1, 	
               hisp_iq1 as pol_iq1,                         hisp_fact_codesa1 as pol_fact_codesa1,       hisp_experiencia1 as pol_experiencia1, 	
               hisp_preexistencia1 as pol_preexistencia1,   hisp_tarifa1 as pol_tarifa1,                 hisp_nombre_cob2 as pol_nombre_cob2, 	
               hisp_plan2 as pol_plan2,                     hisp_cob_stdr2 as pol_cob_stdr2,             hisp_suma_aseg2 as pol_suma_aseg2, 	
               hisp_prima_cober2 as pol_prima_cober2,       hisp_coaseguro2 as pol_coaseguro2,           hisp_deducible2 as pol_deducible2, 	
               hisp_iq2 as pol_iq2,                         hisp_fact_codesa2 as pol_fact_codesa2,       hisp_experiencia2 as pol_experiencia2, 	
               hisp_preexistencia2 as pol_preexistencia2,   hisp_tarifa2 as pol_tarifa2,                 hisp_nombre_cob3 as pol_nombre_cob3, 	
               hisp_plan3 as pol_plan3,                     hisp_cob_stdr3 as pol_cob_stdr3,             hisp_suma_aseg3 as pol_suma_aseg3, 	
               hisp_prima_cober3 as pol_prima_cober3,       hisp_coaseguro3 as pol_coaseguro3,           hisp_deducible3 as pol_deducible3, 	
               hisp_iq3 as pol_iq3,                         hisp_fact_codesa3 as pol_fact_codesa3,       hisp_experiencia3 as pol_experiencia3, 	
               hisp_preexistencia3 as pol_preexistencia3,   hisp_tarifa3 as pol_tarifa3,                 hisp_nombre_cob4 as pol_nombre_cob4, 	
               hisp_plan4 as pol_plan4,                     hisp_cob_stdr4 as pol_cob_stdr4,             hisp_suma_aseg4 as pol_suma_aseg4, 	
               hisp_prima_cober4 as pol_prima_cober4,       hisp_coaseguro4 as pol_coaseguro4,           hisp_deducible4 as pol_deducible4, 	
               hisp_iq4 as pol_iq4,                         hisp_fact_codesa4 as pol_fact_codesa4,       hisp_experiencia4 as pol_experiencia4, 	
               hisp_preexistencia4 as pol_preexistencia4,   hisp_tarifa4 as pol_tarifa4,                 hisp_nombre_cob5 as pol_nombre_cob5, 	
               hisp_plan5 as pol_plan5,                     hisp_cob_stdr5 as pol_cob_stdr5,             hisp_suma_aseg5 as pol_suma_aseg5, 	
               hisp_prima_cober5 as pol_prima_cober5,       hisp_coaseguro5 as pol_coaseguro5,           hisp_deducible5 as pol_deducible5, 	
               hisp_iq5 as pol_iq5,                         hisp_fact_codesa5 as pol_fact_codesa5,       hisp_experiencia5 as pol_experiencia5, 	
               hisp_preexistencia5 as pol_preexistencia5,   hisp_tarifa5 as pol_tarifa5,                 hisp_nombre_cob6 as pol_nombre_cob6, 	
               hisp_plan6 as pol_plan6,                     hisp_cob_stdr6 as pol_cob_stdr6,             hisp_suma_aseg6 as pol_suma_aseg6, 	
               hisp_prima_cober6 as pol_prima_cober6,       hisp_coaseguro6 as pol_coaseguro6,           hisp_deducible6 as pol_deducible6, 	
               hisp_iq6 as pol_iq6,                         hisp_fact_codesa6 as pol_fact_codesa6,       hisp_experiencia6 as pol_experiencia6, 	
               hisp_preexistencia6 as pol_preexistencia6,   hisp_tarifa6 as pol_tarifa6,                 hisp_nombre_cob7 as pol_nombre_cob7, 	
               hisp_plan7 as pol_plan7,                     hisp_cob_stdr7 as pol_cob_stdr7,             hisp_suma_aseg7 as pol_suma_aseg7, 	
               hisp_prima_cober7 as pol_prima_cober7,       hisp_coaseguro7 as pol_coaseguro7,           hisp_deducible7 as pol_deducible7, 	
               hisp_iq7 as pol_iq7,                         hisp_fact_codesa7 as pol_fact_codesa7,       hisp_experiencia7 as pol_experiencia7, 	
               hisp_preexistencia7 as pol_preexistencia7,   hisp_tarifa7 as pol_tarifa7,                 hisp_nombre_cob8 as pol_nombre_cob8, 
               hisp_plan8 as pol_plan8,                     hisp_cob_stdr8 as pol_cob_stdr8,             hisp_suma_aseg8 as pol_suma_aseg8, 	
               hisp_prima_cober8 as pol_prima_cober8,       hisp_coaseguro8 as pol_coaseguro8,           hisp_deducible8 as pol_deducible8, 	
               hisp_iq8 as pol_iq8,                         hisp_fact_codesa8 as pol_fact_codesa8,       hisp_experiencia8 as pol_experiencia8, 	
               hisp_preexistencia8 as pol_preexistencia8,   hisp_tarifa8 as pol_tarifa8,                 hisp_nombre_cob9 as pol_nombre_cob9, 	
               hisp_plan9 as pol_plan9,                     hisp_cob_stdr9 as pol_cob_stdr9,             hisp_suma_aseg9 as pol_suma_aseg9, 	
               hisp_prima_cober9 as pol_prima_cober9,       hisp_coaseguro9 as pol_coaseguro9,           hisp_deducible9 as pol_deducible9, 	
               hisp_iq9 as pol_iq9,                         hisp_fact_codesa9 as pol_fact_codesa9,       hisp_experiencia9 as pol_experiencia9, 	
               hisp_preexistencia9 as pol_preexistencia9,   hisp_tarifa9 as pol_tarifa9,                 hisp_nombre_cob10 as pol_nombre_cob10, 	
               hisp_plan10 as pol_plan10,                   hisp_cob_stdr10 as pol_cob_stdr10,           hisp_suma_aseg10 as pol_suma_aseg10, 	
               hisp_prima_cober10 as pol_prima_cober10,     hisp_coaseguro10 as pol_coaseguro10,         hisp_deducible10 as pol_deducible10, 	
               hisp_iq10 as pol_iq10,                       hisp_fact_codesa10 as pol_fact_codesa10,     hisp_experiencia10 as pol_experiencia10, 	
               hisp_preexistencia10 as pol_preexistencia10, hisp_tarifa10 as pol_tarifa10,               hisp_nombre_cob11 as pol_nombre_cob11, 	
               hisp_plan11 as pol_plan11,                   hisp_cob_stdr11 as pol_cob_stdr11,           hisp_suma_aseg11 as pol_suma_aseg11, 	
               hisp_prima_cober11 as pol_prima_cober11,     hisp_coaseguro11 as pol_coaseguro11,         hisp_deducible11 as pol_deducible11, 	
               hisp_iq11 as pol_iq11,                       hisp_fact_codesa11 as pol_fact_codesa11,     hisp_experiencia11 as pol_experiencia11, 	
               hisp_preexistencia11 as pol_preexistencia11, hisp_tarifa11 as pol_tarifa11,               hisp_nombre_cob12 as pol_nombre_cob12, 	
               hisp_plan12 as pol_plan12,                   hisp_cob_stdr12 as pol_cob_stdr12,           hisp_suma_aseg12 as pol_suma_aseg12, 
               hisp_prima_cober12 as pol_prima_cober12,     hisp_coaseguro12 as pol_coaseguro12,         hisp_deducible12 as pol_deducible12, 
               hisp_iq12 as pol_iq12,                       hisp_fact_codesa12 as pol_fact_codesa12,     hisp_experiencia12 as pol_experiencia12, 
               hisp_preexistencia12 as pol_preexistencia12, hisp_tarifa12 as pol_tarifa12,               hisp_nombre_cob13 as pol_nombre_cob13, 
               hisp_plan13 as pol_plan13,                   hisp_cob_stdr13 as pol_cob_stdr13,           hisp_suma_aseg13 as pol_suma_aseg13, 
               hisp_prima_cober13 as pol_prima_cober13,     hisp_coaseguro13 as pol_coaseguro13,         hisp_deducible13 as pol_deducible13, 
               hisp_iq13 as pol_iq13,                       hisp_fact_codesa13 as pol_fact_codesa13,     hisp_experiencia13 as pol_experiencia13, 
               hisp_preexistencia13 as pol_preexistencia13, hisp_tarifa13 as pol_tarifa13,               hisp_nombre_cob14 as pol_nombre_cob14, 
               hisp_plan14 as pol_plan14,                   hisp_cob_stdr14 as pol_cob_stdr14,           hisp_suma_aseg14 as pol_suma_aseg14, 
               hisp_prima_cober14 as pol_prima_cober14,     hisp_coaseguro14 as pol_coaseguro14,         hisp_deducible14 as pol_deducible14, 
               hisp_iq14 as pol_iq14,                       hisp_fact_codesa14 as pol_fact_codesa14,     hisp_experiencia14 as pol_experiencia14, 
               hisp_preexistencia14 as pol_preexistencia14, hisp_tarifa14 as pol_tarifa14,               hisp_nombre_cob15 as pol_nombre_cob15, 
               hisp_plan15 as pol_plan15,                   hisp_cob_stdr15 as pol_cob_stdr15,           hisp_suma_aseg15 as pol_suma_aseg15, 
               hisp_prima_cober15 as pol_prima_cober15,     hisp_coaseguro15 as pol_coaseguro15,         hisp_deducible15 as pol_deducible15, 
               hisp_iq15 as pol_iq15,                       hisp_fact_codesa15 as pol_fact_codesa15,     hisp_experiencia15 as pol_experiencia15, 
               hisp_preexistencia15 as pol_preexistencia15, hisp_tarifa15 as pol_tarifa15,               hisp_dispo3 as pol_dispo3, 
               hisp_recargos as pol_recargos,               hisp_derpol as pol_derpol,                   hisp_swt_sida as pol_swt_sida, 
               hisp_existe_ajuste as pol_existe_ajuste,     hisp_tipo_admon as pol_tipo_admon,           hisp_sramo_cont as pol_sramo_cont, 
               hisp_ofi_adminst as pol_ofi_adminst,         hisp_fact_derpol as pol_fact_derpol,         hisp_dispo4 as pol_dispo4, 
               hisp_dincod as pol_dincod,                   hisp_rfc as pol_rfc,                         hisp_telefono as pol_telefono, 
               hisp_cond_envio as pol_cond_envio,           hisp_fec_emision as pol_fec_emision,         hisp_fec_captura as pol_fec_captura, 
               hisp_fec_calculo as pol_fec_calculo,         hisp_idnom as pol_idnom,                     hisp_sist_ant as pol_sist_ant, 
               hisp_cob_comis as pol_cob_comis,             hisp_fact_exper as pol_fact_exper,           hisp_fact_cp as pol_fact_cp, 
               hisp_cod_postal_alt as pol_cod_postal_alt,   hisp_dispo5 as pol_dispo5,                   hisp_cve_iva as pol_cve_iva, 
               hisp_ley_tarjeta as pol_ley_tarjeta,         hisp_tipo_tarjeta_sin_mm as pol_tipo_tarjeta_sin_mm, hisp_tipo_tarjeta_con_mm as pol_tipo_tarjeta_con_mm, 
               hisp_sramo_plan as pol_sramo_plan,           hisp_tar_aplica as pol_tar_aplica,           end_fecha_expide as end_fecha_expide, 
               end_disponible as end_disponible,            end_num_texto as end_num_texto,              end_tot_ocurren as end_tot_ocurren, 
               end_tabla_datos as end_tabla_datos,          id_hist as id_hist,                          idmd5 as idmd5, 
               idmd5completo as idmd5completo,              fechaaud as fechaaud,                        sistorig as sistorig 
          FROM BDDLCRU.GAZ0_HISPOLIZA gaz
	  WHERE  gaz.end_num_consec = 0
			and gaz.end_num_control = 0
			--AND to_date(fechaaud) >= to_date(current_timestamp())
       )as gas;