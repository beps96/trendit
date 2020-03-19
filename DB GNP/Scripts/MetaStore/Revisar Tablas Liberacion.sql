select
	db.NAME AS ESQUEMA,
	t.TBL_NAME AS TABLA,
	t.TBL_TYPE AS TIPO_TABLA
from
	metastore.TBLS t
left join hue.auth_user u on
	t.OWNER = u.username
left join metastore.DBS db on
	t.DB_ID = db.DB_ID
where t.TBL_NAME in ( 'cnm_poliza_datos_adicionales');