select
	db.NAME as ESQUEMA,
	t.TBL_NAME as TABLA,
	t.OWNER as OWNER,
	dd.PARAM_VALUE as last_modified_by,
	str_to_date(from_unixtime(t.CREATE_TIME),
	'%Y-%m-%d') as CREATE_TIME,
	str_to_date(from_unixtime(d.PARAM_VALUE),
	'%Y-%m-%d') as LAST_DDL_TIME ,
	t.TBL_TYPE as TIPO_TABLA
from
	metastore.TBLS t
left join metastore.TABLE_PARAMS d on
	t.TBL_ID = d.TBL_ID
	and d.PARAM_KEY = 'transient_lastDdlTime'
left join metastore.TABLE_PARAMS dd on
	t.TBL_ID = dd.TBL_ID
	and dd.PARAM_KEY = 'last_modified_by'
left join metastore.DBS db on
	t.DB_ID = db.DB_ID
where
	t.TBL_NAME in ('adc_alertascumplimiento_beneficiarios',
	'adc_crelint_carga',
	'adc_t_crelint_hist',
	'adc_t_crm_agente',
	'adc_t_crm_agente_resto',
	'adc_t_crm_beneficiario',
	'adc_t_crm_medio_contacto',
	'adc_t_crm_contratante',
	'adc_t_crm_direccion',
	'adc_t_crm_direccion_resto',
	'adc_alertascumplimiento_golden_record',
	'adc_t_crm_participante',
	'adc_t_crm_participante_resto',
	'crm_poliza',
	'adc_t_crm_uni_poliza_version',
	'adc_alertascumplimiento_datos_info',
	'adc_t_inf_ingresos_declarado',
	'adc_t_inf_suma_asegurada',
	'adc_t_inf_universo_polizas',
	'adc_t_inf_uni_poliza_version',
	'adc_t_crm_datos_generales',
	'adc_t_inf_datos_generales',
	'adc_inf_suma_asegurada',
	'adc_t_crm_polizas');
