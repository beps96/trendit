create table bddlapr.aut_insumos_sb1_emision_cobertura as
select
	cast('Autos' as char(10) ) as linea_de_negocio ,
	cast(a.id_poliza as char(10) ) as id_poliza ,
	cast('INFO' as char(4) ) as sistema_fuente ,
	cast(z.num_poliza as char(14) ) as num_poliza ,
	cast(z.num_version as int ) as num_version ,
	cast(a.num_renovacion as int ) as num_renovacion ,
	cast (
	case
		when b.aniomes is null then 0
		else b.aniomes
end as int ) as aniomes ,
	cast(b.fch_contable as timestamp ) as fch_contable ,
	cast(a.cve_cobertura_contable as char(4) ) as cve_cobertura_contable ,
	cast(a.desc_cobertura_contable as char(250) ) as desc_cobertura_contable ,
	cast(a.num_contrato as int) as num_contrato ,
	cast(a.num_version_contrato as int ) as num_version_contrato ,
	cast(a.num_poliza_padre as char(14) ) as num_poliza_padre ,
	cast(a.num_version_poliza_padre as int ) as num_version_poliza_padre ,
	cast(a.ban_poliza_padre as char(1) ) as ban_poliza_padre ,
	cast(a.cve_prod_tecnico as char(10) ) as cve_prod_tecnico ,
	cast(a.cve_prod_comercial as char(10) ) as cve_prod_comercial ,
	cast(a.nombre_prod_venta as varchar(250) ) as nombre_prod_venta ,
	cast(a.desc_prod_venta as varchar(250) ) as desc_prod_venta ,
	cast(a.cve_subramo_automovil as char(2) ) as cve_subramo_automovil ,
	cast(a.desc_subramo_automovil as varchar(250) ) as desc_subramo_automovil ,
	cast(a.cve_uso_vehiculo as char(2) ) as cve_uso_vehiculo ,
	cast(a.desc_uso_vehiculo as varchar(250) ) as desc_uso_vehiculo ,
	cast(a.registro_de_nota_tecnica_cnsf as char(20) ) as registro_de_nota_tecnica_cnsf ,
	cast(a.referencia_1 as varchar(250) ) as referencia_1 ,
	cast(a.referencia_2 as varchar(250) ) as referencia_2 ,
	cast(a.estatus_poliza_sistema as char(1) ) as estatus_poliza_sistema ,
	cast(a.desc_estatus_poliza_sistema as varchar(250) ) as desc_estatus_poliza_sistema ,
	cast(a.estatus_endoso as int ) as estatus_endoso ,
	cast(a.desc_estatus_endoso as varchar(250) ) as desc_estatus_endoso ,
	cast(a.estatus_poliza_transformada_vigor as int ) as estatus_poliza_transformada_vigor ,
	cast(a.desc_estatus_poliza_transformada_vigor as varchar(250) ) as desc_estatus_poliza_transformada_vigor ,
	cast(a.fch_emision as timestamp ) as fch_emision ,
	cast(a.fch_inicio_vigencia as timestamp ) as fch_inicio_vigencia ,
	cast(a.fch_fin_vigencia as timestamp ) as fch_fin_vigencia ,
	cast(a.fch_inicio_vig_renovacion as timestamp ) as fch_inicio_vig_renovacion ,
	cast(a.fch_fin_vig_renovacion as timestamp ) as fch_fin_vig_renovacion ,
	cast(a.fch_efecto_movimiento as timestamp ) as fch_efecto_movimiento ,
	cast(a.fch_fin_efecto_movimiento as timestamp ) as fch_fin_efecto_movimiento ,
	cast(a.fecha_inicio_vigencia_contrato as timestamp ) as fecha_inicio_vigencia_contrato ,
	cast(a.fecha_fin_vigencia_contrato as timestamp ) as fecha_fin_vigencia_contrato ,
	cast(a.cve_tipo_movimiento as char(1) ) as cve_tipo_movimiento ,
	cast(a.desc_tipo_movimiento as varchar(250) ) as desc_tipo_movimiento ,
	cast(a.cve_motivo_movimiento as char(5) ) as cve_motivo_movimiento ,
	cast(a.desc_motivo_movimiento as varchar(250) ) as desc_motivo_movimiento ,
	cast(a.cve_forma_pago as char(1) ) as cve_forma_pago ,
	cast(a.desc_forma_pago as varchar(250) ) as desc_forma_pago ,
	cast(a.cve_conducto_cobro_inicial as char(2) ) as cve_conducto_cobro_inicial ,
	cast(a.desc_canal_cobro_inicial as varchar(250) ) as desc_canal_cobro_inicial ,
	cast(a.cve_canal_cobro_sucesivos as char(2) ) as cve_canal_cobro_sucesivos ,
	cast(a.desc_canal_cobro_sucesivos as varchar(250) ) as desc_canal_cobro_sucesivos ,
	coalesce(cast(b.ban_negocio_tomado_emi_max as char(1) ),
	'') as ban_negocio_tomado ,
	case
		when b.ban_negocio_tomado_emi_max = 'S' then 5
		else 0
end as porc_negocio_tomado
	------Aplicar CASE  when b.ban_negocio_tomado_emi_max=S then porc_negocio_tomado
,
	cast (
	case
		when a.ban_migrado <> 'S' then 'N'
		else a.ban_migrado
end as char(1) ) as ban_migrado ,
	cast(a.ban_ultima_situacion as char(1) ) as ban_ultima_situacion ,
	cast(a.ban_anulacion_falta_de_pago as char(1) ) as ban_anulacion_falta_de_pago ,
	cast(a.fecha_anulacion_falta_de_pago as timestamp ) as fecha_anulacion_falta_de_pago ,
	cast(a.cve_contratante as varchar(20) ) as cve_contratante ,
	cast(a.nombre_completo_contratante as varchar(250) ) as nombre_completo_contratante ,
	cast(a.tipo_persona_fiscal_contratante as char(1) ) as tipo_persona_fiscal_contratante ,
	cast(a.desc_tipo_persona_fiscal_contratante as varchar(6) ) as desc_tipo_persona_fiscal_contratante ,
	cast(a.cve_asegurado as char(10) ) as cve_asegurado ,
	cast(a.rfc_asegurado as char(14) ) as rfc_asegurado ,
	cast(a.nombre_completo_asegurado as varchar(250) ) as nombre_completo_asegurado ,
	cast(a.tipo_persona_fiscal_asegurado as char(1) ) as tipo_persona_fiscal_asegurado ,
	cast(a.desc_tipo_persona_fiscal_asegurado as varchar(6) ) as desc_tipo_persona_fiscal_asegurado ,
	cast(a.indicador_tipo_coaseguro as char(1) ) as indicador_tipo_coaseguro ,
	cast(a.desc_tipo_coaseguro as varchar(250) ) as desc_tipo_coaseguro ,
	cast(aaa.cve_contrato_coaseguro as char(8)) as cve_contrato_coaseguro ,
	cast(aaaa.cve_compania_coaseguradora as CHAR(10)) as cve_compania_coaseguradora ,
	cast(aaaa.desc_compania_coaseguradora as CHAR(30)) as nombre_compania_coaseguradora ,
	cast(a.porc_coaseguro as float ) as porc_coaseguro ,
	cast(a.cve_genero_sexual_conductor as char(1) ) as cve_genero_sexual_conductor ,
	cast(a.desc_genero_sexual_conductor as varchar(250) ) as desc_genero_sexual_conductor ,
	cast(a.fecha_nac_conductor as timestamp ) as fecha_nac_conductor ,
	cast(a.cve_tipo_vehiculo as char(3) ) as cve_tipo_vehiculo ,
	cast(a.desc_tipo_vehiculo as varchar(250) ) as desc_tipo_vehiculo ,
	cast(a.cve_armadora_vehiculo as char(2) ) as cve_armadora_vehiculo ,
	cast(a.desc_armadora_vehiculo as varchar(250) ) as desc_armadora_vehiculo ,
	cast(a.cve_carroceria_vehiculo as char(2) ) as cve_carroceria_vehiculo ,
	cast(a.desc_carroceria_vehiculo as varchar(250) ) as desc_carroceria_vehiculo ,
	cast(a.cve_version_vehiculo as char(2) ) as cve_version_vehiculo ,
	cast(a.cve_marca as char(14) ) as cve_marca ,
	cast(a.desc_clave_marca as varchar(250) ) as desc_clave_marca ,
	cast(a.modelo_vehiculo as int ) as modelo_vehiculo ,
	cast(a.desc_vehiculo as varchar(250) ) as desc_vehiculo ,
	cast(a.categoria_vehiculo as varchar(250) ) as categoria_vehiculo ,
	cast(a.desc_categoria_vehiculo as varchar(250) ) as desc_categoria_vehiculo ,
	cast(a.num_serie as varchar(30) ) as num_serie ,
	cast(a.num_placas_vehiculo as varchar(12) ) as num_placas_vehiculo ,
	cast(a.motor as varchar(20) ) as motor ,
	cast(a.tipo_indemnizacion as char(3) ) as tipo_indemnizacion ,
	cast(a.desc_tipo_indemnizacion as varchar(250) ) as desc_tipo_indemnizacion ,
	cast(a.codigo_postal_contratante as char(5) ) as codigo_postal_contratante ,
	cast(a.cve_entidad_circulacion as char(2) ) as cve_entidad_circulacion ,
	cast(a.desc_entidad_circulacion as varchar(250) ) as desc_entidad_circulacion ,
	cast(z.cve_cobertura_tarificable as char(10) ) as cve_cobertura_tarificable ,
	cast(regexp_replace(z.desc_cobertura_tarificable,
	'DA¬O',
	'DAÑO') as varchar(250) ) as desc_cobertura_tarificable ,
	cast(aa.status_cobertura_tarificable as char(1) ) as status_cobertura_tarificable ,
	cast(aa.desc_status_cobertura_tarificable as varchar(250) ) as desc_status_cobertura_tarificable ,
	cast(aa.cobertura_agrupada as char(2) ) as cobertura_agrupada ,
	cast (
	case
		when trim(b.ban_servicios_conexos_fx2)= 'S' then 'S'
		else 'N'
end as char(1) ) as ban_servicios_conexos ,
	cast(aa.suma_asegurada_origen as float ) as suma_asegurada_origen ,
	cast(aa.unidad_suma_asegurada_origen as char(20) ) as unidad_suma_asegurada_origen ,
	cast(cast(aa.deducible_origen as double)/ 100 as float ) as deducible_origen ,
	cast(aa.unidad_deducible_origen as char(20) ) as unidad_deducible_origen
	---Lib2 de momento no se les hace casteo
,
	cast(aa.segmento as char(8) ) as segmento ,
	cast(aa.subsegmento as char(2) ) as subsegmento ,
	cast(aa.au_line_neg as char(1) ) as au_line_neg ,
	cast(aa.segmento_autos_negocio as char(5) ) as segmento_autos_negocio ,
	cast(aa.segmento_autos_presupuesto as char(5) ) as segmento_autos_presupuesto ,
	cast(aa.ban_gobierno as char(1) ) as ban_gobierno ,
	cast( '' as char(2) ) as ban_plan_piso
	---Este campo quda fuera de alacance
,
	cast(aa.ban_chofer_priv as char(1) ) as ban_chofer_priv ,
	cast(a.paquete as int ) as paquete ,
	cast(a.desc_paquete as char(43)) as desc_paquete ,
	cast(aa.paquete_agrupado as char(3) ) as paquete_agrupado ,
	cast(aa.cve_da_agente_principal as char(4) ) as cve_da_agente_principal ,
	cast(aa.desc_da_agente_principal as char(26)) as desc_da_agente_principal ,
	cast(aa.oficina_folio as char(4) ) as oficina_folio ,
	cast(aa.descripcion_oficina as char(27)) as desc_oficina
	-----Faltaban 
,
	cast(aa.cve_intermediario_principal as char(10) ) as cve_intermediario_principal ,
	cast(aa.cve_unica_agente_principal as char(5) ) as cve_unica_agente_principal ,
	cast(aa.cve_folio_agente_principal as char(8) ) as cve_folio_agente_principal ,
	cast(aa.nombre_agente_principal as char(50) ) as nombre_agente_principal ,
	cast(aa.estatus_intermediario_agente_principal as char(1) ) as estatus_intermediario_agente_principal ,
	cast(aa.desc_estatus_intermediario_agente_principal as char(7) ) as desc_estatus_intermediario_agente_principal ,
	cast(aa.cve_tipo_base_comision_agente_principal as char(2) ) as cve_tipo_base_comision_agente_principal ,
	cast(aa.desc_tipo_base_comision_agente_principal as char(46) ) as desc_tipo_base_comision_agente_principal
	--------
,
	cast(coalesce(aa.porc_comisios_agente_principal,
	0) as float ) as porc_particip_en_comision_interm_principal ,
	cast(coalesce(aa.porc_comisios_udi_primaria,
	0) as float ) as porc_particip_en_comision_interm_udi_primaria ,
	cast(coalesce(aa.porc_comisios_udi_secundaria,
	0) as float ) as porc_particip_en_comision_interm_udi_secundaria ,
	cast(coalesce(aa.porc_comisios_supervisor,
	0) as float ) as porc_particip_en_comision_interm_supervisor ,
	cast(aa.cve_version_tarifa as decimal(15,
	3) ) as cve_version_tarifa ,
	cast(aa.descuento_por_volumen as decimal(15,
	3) ) as descuento_por_volumen ,
	cast(aa.cve_tipo_carga as char(1)) as cve_tipo_carga ,
	cast(aa.desc_tipo_carga as char(19)) as desc_tipo_carga ,
	cast(aa.cve_zona_tarificacion as char(3)) as cve_zona_tarificacion ,
	cast(aa.ban_alto_riesgo as char(1)) as ban_alto_riesgo ,
	cast(aa.valor_vehiculo as decimal(15,
	3)) as valor_vehiculo ,
	cast(aa.edad_conductor as int ) as edad_conductor ,
	cast('' as char(1) ) as ban_nuevo_renovada_tecnico ,
	case
		when coalesce(trim(aaaa.cve_compania_coaseguradora),
		'') = '0002103504' then aa.ptc_descuento_scord
		else 0
end as ptc_descuento_scord_banamex ,
	case
		when coalesce(trim(aaaa.cve_compania_coaseguradora),
		'') = '0002103504' then aa.ptc_descuento_campania
		else 0
end as ptc_descuento_campania_banamex ,
	cast (
	case
		when b.cve_moneda_moveco is null then a.cve_moneda_info
		else b.cve_moneda_moveco
end as char(3) ) as cve_moneda_poliza ,
	cast (
	case
		when b.desc_moneda_moveco is null then a.desc_moneda_info
		else b.desc_moneda_moveco
end as varchar(20) ) as desc_moneda_poliza ,
	cast (
	case
		when b.tipo_cambio is null then 1
		else b.tipo_cambio
end as float ) as tipo_cambio_emision ,
	cast(coalesce(b.mto_prima_total_emision_ori,
	0) as float ) as mto_prima_total_emision_ori ,
	cast(coalesce(b.mto_prima_neta_emision_ori,
	0) as float ) as mto_prima_neta_emision_ori ,
	cast(coalesce(b.mto_gasto_emision_ori,
	0) as float ) as mto_gasto_emision_ori ,
	cast(coalesce(b.mto_rpf_emision_ori,
	0) as float ) as mto_rpf_emision_ori ,
	cast(coalesce(b.mto_iva_emision_ori,
	0) as float ) as mto_iva_emision_ori ,
	cast(coalesce(b.mto_cesion_comision_emision_ori,
	0) as float ) as mto_cesion_comision_emision_ori ,
	cast(coalesce(b.mto_comision_sobre_prima_neta_emision_ori,
	0) as float ) as mto_comision_sobre_prima_neta_emision_ori ,
	cast(coalesce(b.mto_udi_sobre_prima_neta_emision_ori,
	0) as float ) as mto_udi_sobre_prima_neta_emision_ori ,
	cast(coalesce(b.mto_prima_cedida_en_coaseguro_emision_ori ,
	0) as float ) as mto_prima_neta_cedida_en_coaseguro_emision_ori ,
	cast(coalesce(b.mto_prima_neta_tomada_emision_ori,
	0) as double ) as mto_prima_neta_tomada_emision_ori ,
	cast(coalesce(b.mto_rpf_cedido_en_coaseguro_emision_ori,
	0) as float ) as mto_rpf_cedido_en_coaseguro_emision_ori ,
	cast(coalesce(b.mto_iva_cedido_en_coaseguro_emision_ori,
	0) as float ) as mto_iva_cedido_en_coaseguro_emision_ori ,
	cast(coalesce(b.mto_prima_neta_sin_coaseguro_cedido_emision_ori,
	0) as float ) as mto_prima_neta_sin_coaseguro_cedido_emision_ori ,
	cast(coalesce(b.mto_rpf_sin_coaseguro_emision_ori,
	0) as float ) as mto_rpf_sin_coaseguro_emision_ori ,
	cast(coalesce(b.mto_iva_sin_coaseguro_cedido_emision_ori,
	0) as float ) as mto_iva_sin_coaseguro_cedido_emision_ori ,
	cast(coalesce(b.mto_prima_total_emision_int,
	0) as float ) as mto_prima_total_emision_int ,
	cast(coalesce(b.mto_prima_neta_emision_int,
	0) as float ) as mto_prima_neta_emision_int ,
	cast(coalesce(b.mto_gasto_emision_int,
	0) as float ) as mto_gasto_emision_int ,
	cast(coalesce(b.mto_rpf_emision_int,
	0) as float ) as mto_rpf_emision_int ,
	cast(coalesce(b.mto_iva_emision_int,
	0) as float ) as mto_iva_emision_int ,
	cast(coalesce(b.mto_cesion_comision_emision_int,
	0) as float ) as mto_cesion_comision_emision_int ,
	cast(coalesce(b.mto_comision_sobre_prima_neta_emision_int,
	0) as float ) as mto_comision_sobre_prima_neta_emision_int ,
	cast(coalesce(b.mto_udi_sobre_prima_neta_emision_int,
	0) as float ) as mto_udi_sobre_prima_neta_emision_int ,
	cast(coalesce(b.mto_prima_cedida_en_coaseguro_emision_int ,
	0) as float ) as mto_prima_neta_cedida_en_coaseguro_emision_int ,
	cast(coalesce(b.mto_prima_neta_tomada_emision_int ,
	0) as double ) as mto_prima_neta_tomada_emision_int ,
	cast(coalesce(b.mto_rpf_cedido_en_coaseguro_emision_int ,
	0) as float ) as mto_rpf_cedido_en_coaseguro_emision_int ,
	cast(coalesce(b.mto_iva_cedido_en_coaseguro_emision_int ,
	0) as float ) as mto_iva_cedido_en_coaseguro_emision_int ,
	cast(coalesce(b.mto_prima_neta_sin_coaseguro_cedido_emision_int ,
	0) as float ) as mto_prima_neta_sin_coaseguro_cedido_emision_int ,
	cast(coalesce(b.mto_rpf_sin_coaseguro_emision_int ,
	0) as float ) as mto_rpf_sin_coaseguro_emision_int ,
	cast(coalesce(b.mto_iva_sin_coaseguro_cedido_emision_int ,
	0) as float ) as mto_iva_sin_coaseguro_cedido_emision_int ,
	cast(coalesce(b.mto_comision_cedida_coaseguro_ori,
	0) as float) as mto_comision_cedida_coaseguro_ori ,
	cast(coalesce(b.mto_comision_cedida_coaseguro_int,
	0) as float) as mto_comision_cedida_coaseguro_int ,
	cast(coalesce(b.mto_comision_sin_coaseguro_cedido_emision_ori,
	0) as float) as mto_comision_sin_coaseguro_cedido_emision_ori ,
	cast(coalesce(b.mto_comision_sin_coaseguro_cedido_emision_int,
	0) as float) as mto_comision_sin_coaseguro_cedido_emision_int
	--Faltaban 
,
	cast(coalesce(b.mto_comision_sobre_prima_neta_emision_ori,
	0) as float) as mto_comision_seguro_directo_emision_ori ,
	cast(coalesce(b.mto_comision_sobre_prima_neta_emision_int,
	0) as float) as mto_comision_seguro_directo_emision_int
from
	bddltrn.cob_completas_emision z
left join bddldes.preparacion_emi_cob a on
	z.num_poliza = a.num_poliza
	and z.num_version = a.num_version
left join bddldes.insumos_autos_p13_union aa on
	z.num_poliza = aa.num_poliza
	and z.num_version = aa.num_version
	and z.cve_cobertura_tarificable = aa.cve_cobertura_tarificable
left join bddldes.contrato_coaseguro aaa on
	z.num_poliza = aaa.num_poliza
left join (
	select
		a.cve_contrato_coaseguro,
		a.cve_compania_coaseguradora,
		b.desc_compania_coaseguradora
	from
		bddldes.aut_companias_coaseguradora a
	left join bddldes.aut_nom_companias_coaseguradora b
		--(kcim44b1 )
 on
		a.cve_compania_coaseguradora = B.cve_compania_coaseguradora
	where
		B.cve_compania_coaseguradora is not null ) aaaa on
	aaa.cve_contrato_coaseguro = aaaa.cve_contrato_coaseguro
left join bddltrn.aut_insumos_sb1_bloques_eco_y_no_eco_emision_tmp_optimiz_cobe_4 b on
	z.num_poliza = b.num_poliza
	and z.num_version = b.num_version
	and z.cve_cobertura_tarificable = b.cve_cobertura ;
