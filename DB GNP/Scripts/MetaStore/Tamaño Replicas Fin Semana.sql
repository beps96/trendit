select
	db.NAME AS ESQUEMA,
	t.TBL_NAME AS TABLA,
	CASE WHEN t.TBL_TYPE = 'VIRTUAL_VIEW' THEN 'VISTA'
	WHEN t.TBL_TYPE = 'MANAGED_TABLE' THEN 'INTERNA'
	WHEN t.TBL_TYPE = 'EXTERNAL' THEN 'EXTERNA'
	ELSE 'N/A' END AS TIPO_TABLA,
	str_to_date(from_unixtime(t.CREATE_TIME),
	'%Y-%m-%d') as CREATE_TIME,
	str_to_date(from_unixtime(dd.PARAM_VALUE),
	'%Y-%m-%d %h:%i:%s') as LAST_DDL_TIME,
	CAST(d.PARAM_VALUE AS UNSIGNED)/ 1099511627776 as SIZEREPLICAX1_TB,
	CAST(d.PARAM_VALUE AS UNSIGNED)* 3 / 1099511627776 as SIZEREPLICAX3_TB
from
	metastore.TBLS t
left join metastore.TABLE_PARAMS d on
	t.TBL_ID = d.TBL_ID
	and d.PARAM_KEY = 'totalSize'
left join metastore.TABLE_PARAMS dd on
	t.TBL_ID = dd.TBL_ID
	and dd.PARAM_KEY = 'transient_lastDdlTime'
left join metastore.DBS db on
	t.DB_ID = db.DB_ID
where
	t.TBL_NAME in ('cnm_prima_pagada_poliza_endoso',
	'aut_siniestro_cabina',
	'aut_metrica_poliza_mes',
	'ext_egresos_ingresos',
	'aut_agente_poliza',
	'aut_poliza',
	'aut_conexos',
	'aut_objeto_asegurado',
	'aut_elemento_cobertura',
	'aut_comisiones',
	'aut_elemento_objeto',
	'aut_elemento_poliza',
	'aut_ace_ondemand',
	'aut_metrica_poliza',
	'aut_siniestro',
	'opr_precalificados_emision',
	'aut_siniestro_cob',
	'aut_siniestro_rol',
	'cnm_prima_pagada_poliza_asegurado_cobertura',
	'aut_udis',
	'aut_primas',
	'aut_cobertura',
	'cnm_prima_emitida_poliza_asegurado_cobertura',
	'aut_poliza_mes')
	and db.NAME = 'bddlalm'
UNION
select
	db.NAME AS ESQUEMA,
	t.TBL_NAME AS TABLA,
	CASE WHEN t.TBL_TYPE = 'VIRTUAL_VIEW' THEN 'VISTA'
	WHEN t.TBL_TYPE = 'MANAGED_TABLE' THEN 'INTERNA'
	WHEN t.TBL_TYPE = 'EXTERNAL' THEN 'EXTERNA'
	ELSE 'N/A' END AS TIPO_TABLA,
	str_to_date(from_unixtime(t.CREATE_TIME),
	'%Y-%m-%d') as CREATE_TIME,
	str_to_date(from_unixtime(dd.PARAM_VALUE),
	'%Y-%m-%d %h:%i:%s') as LAST_DDL_TIME,
	CAST(d.PARAM_VALUE AS UNSIGNED)/ 1099511627776 as SIZEREPLICAX1_TB,
	CAST(d.PARAM_VALUE AS UNSIGNED)* 3 / 1099511627776 as SIZEREPLICAX3_TB
from
	metastore.TBLS t
left join metastore.TABLE_PARAMS d on
	t.TBL_ID = d.TBL_ID
	and d.PARAM_KEY = 'totalSize'
left join metastore.TABLE_PARAMS dd on
	t.TBL_ID = dd.TBL_ID
	and dd.PARAM_KEY = 'transient_lastDdlTime'
left join metastore.DBS db on
	t.DB_ID = db.DB_ID
where
	t.TBL_NAME in ('cnm_comprobante',
	'cnm_contratante',
	'cnm_cartas_autorizacion_dg',
	'cnm_actividad_deportiva',
	'cnm_cartas_autorizacion_gc',
	'cnm_padecimiento_declarado',
	'cnm_gasto_tabulado',
	'cnm_endoso_exclusion',
	'cnm_padecimiento',
	'cnm_poliza',
	'cnm_actividad_laboral',
	'cnm_proveedor_medico',
	'cnm_gmm_proyecto_strgd',
	'cnm_poliza_azul',
	'cnm_procedimiento',
	'cnm_proveedor_hospital',
	'fnz_polizario',
	'cnm_gmm_proyecto_gd',
	'fnz_prima_sin',
	'cnm_reclamacion_acumulado',
	'fnz_prima_sin_ppto_anio_ant',
	'cnm_proveedor_ser_aux',
	'fnz_poliza_prima_siniestro_etiquetas',
	'cnm_poliza_datos_adicionales',
	'cnm_reclamacion',
	'cnm_asegurado_datos_adicionales',
	'cnm_notas_med_adm',
	'cnm_direccion_cte_azul',
	'cnm_cobertura_datos_adicionales',
	'cnm_contratante_domicilio',
	'cnm_solicitud',
	'opr_ext_contratacion',
	'opr_primas_contratacion',
	'cnm_intermediario')
	and db.NAME = 'bddlapr'
UNION
select
	db.NAME AS ESQUEMA,
	t.TBL_NAME AS TABLA,
	CASE WHEN t.TBL_TYPE = 'VIRTUAL_VIEW' THEN 'VISTA'
	WHEN t.TBL_TYPE = 'MANAGED_TABLE' THEN 'INTERNA'
	WHEN t.TBL_TYPE = 'EXTERNAL' THEN 'EXTERNA'
	ELSE 'N/A' END AS TIPO_TABLA,
	str_to_date(from_unixtime(t.CREATE_TIME),
	'%Y-%m-%d') as CREATE_TIME,
	str_to_date(from_unixtime(dd.PARAM_VALUE),
	'%Y-%m-%d %h:%i:%s') as LAST_DDL_TIME,
	CAST(d.PARAM_VALUE AS UNSIGNED)/ 1099511627776 as SIZEREPLICAX1_TB,
	CAST(d.PARAM_VALUE AS UNSIGNED)* 3 / 1099511627776 as SIZEREPLICAX3_TB
from
	metastore.TBLS t
left join metastore.TABLE_PARAMS d on
	t.TBL_ID = d.TBL_ID
	and d.PARAM_KEY = 'totalSize'
left join metastore.TABLE_PARAMS dd on
	t.TBL_ID = dd.TBL_ID
	and dd.PARAM_KEY = 'transient_lastDdlTime'
left join metastore.DBS db on
	t.DB_ID = db.DB_ID
where
	t.TBL_NAME in ('krbm30h',
	'krbn50h',
	'kremb8t',
	'kremb9t',
	'krem24t',
	'krem19h',
	'kcimh1t',
	'kpem03t',
	'krema7t',
	'kcim16h',
	'kcim18h',
	'kpeh02t',
	'kpem08t',
	'kremb6t',
	'kremb7t',
	'con_trans_cec_mov',
	'nrltdgg0',
	'ktotcat',
	'ktoa41h',
	'nrltnot0',
	'ktoa13t',
	'ksin30t',
	'nrltrcl0',
	'nrltdog0',
	'nrltcbh0',
	'nrltaan0',
	'ktpa82t',
	'nrltcrq0',
	'nrltslr0',
	'kren19h',
	'ktom4bt',
	'kremc1t',
	'ktoa42t',
	'kcim23h',
	'kpem07t',
	'krbm20h',
	'kcim18t',
	'kmcht1t',
	'kremd5t',
	'ktom46h',
	'ktoa41t',
	'ktom13t',
	'kcim06t',
	'kcin42t',
	'con_trans_contable',
	'kcim04t',
	'krem19t',
	'ktom78t',
	'kren18t',
	'kcim45t',
	'krbm10t',
	'kcim16t',
	'kmctt5t',
	'ktom41h',
	'ktomrct',
	'ktom46t',
	'ktom42t',
	'kcgm15t',
	'krbm30t',
	'ktom41t',
	'krem18t',
	'kcim23t',
	'kton41h',
	'krbtgat',
	'krbn50t',
	'ktom60t',
	'kcimf3t',
	'krbm20t',
	'kton41t')
	and db.NAME = 'bddlcru'
UNION
select
	db.NAME AS ESQUEMA,
	t.TBL_NAME AS TABLA,
	CASE WHEN t.TBL_TYPE = 'VIRTUAL_VIEW' THEN 'VISTA'
	WHEN t.TBL_TYPE = 'MANAGED_TABLE' THEN 'INTERNA'
	WHEN t.TBL_TYPE = 'EXTERNAL' THEN 'EXTERNA'
	ELSE 'N/A' END AS TIPO_TABLA,
	str_to_date(from_unixtime(t.CREATE_TIME),
	'%Y-%m-%d') as CREATE_TIME,
	str_to_date(from_unixtime(dd.PARAM_VALUE),
	'%Y-%m-%d %h:%i:%s') as LAST_DDL_TIME,
	CAST(d.PARAM_VALUE AS UNSIGNED)/ 1099511627776 as SIZEREPLICAX1_TB,
	CAST(d.PARAM_VALUE AS UNSIGNED)* 3 / 1099511627776 as SIZEREPLICAX3_TB
from
	metastore.TBLS t
left join metastore.TABLE_PARAMS d on
	t.TBL_ID = d.TBL_ID
	and d.PARAM_KEY = 'totalSize'
left join metastore.TABLE_PARAMS dd on
	t.TBL_ID = dd.TBL_ID
	and dd.PARAM_KEY = 'transient_lastDdlTime'
left join metastore.DBS db on
	t.DB_ID = db.DB_ID
where
	t.TBL_NAME in ('crm_atraccion_retencion',
	'crm_etiquetas_finanzas',
	'crm_segmento_tom',
	'crm_perfil_tom_seg',
	'perfiles',
	'crm_polizas_tom',
	'crm_jerarquia',
	'dmstros_autos',
	'crm_tablero_integralidad',
	'fyfautosmoveco',
	'crm_segmento_negocio_tom',
	'crm_tablero_perfil')
	and db.NAME = 'bddldes';