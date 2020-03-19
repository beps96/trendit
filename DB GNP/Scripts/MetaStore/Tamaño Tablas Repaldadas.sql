select
	db.NAME AS ESQUEMA,
	t.TBL_NAME AS TABLA,
	t.OWNER AS OWNER,
	u.first_name AS NOMBRE,
	u.last_name AS APELLIDOS,
	u.email AS EMAIL,
	str_to_date(from_unixtime(t.CREATE_TIME),
	'%Y-%m-%d') as CREATE_TIME,
	str_to_date(from_unixtime(dd.PARAM_VALUE),
	'%Y-%m-%d') as LAST_DDL_TIME ,
	CASE t.LAST_ACCESS_TIME WHEN 0 THEN 'UNKNOWN'
	ELSE str_to_date(from_unixtime(t.LAST_ACCESS_TIME),
	'%Y-%m-%d') END as LAST_ACCESS_TIME,
	t.TBL_TYPE AS TIPO_TABLA,
	CASE WHEN CAST(d.PARAM_VALUE AS UNSIGNED) <= 1000000000 THEN CONCAT(CAST(CAST(d.PARAM_VALUE AS UNSIGNED)/ 1048576 as CHAR(50)), ' MB')
	WHEN CAST(d.PARAM_VALUE AS UNSIGNED) > 1000000000
	and CAST(d.PARAM_VALUE AS UNSIGNED) < 1000000000000 THEN CONCAT(CAST(CAST(d.PARAM_VALUE AS UNSIGNED)/ 1073741824 as CHAR(50)), ' GB')
	WHEN CAST(d.PARAM_VALUE AS UNSIGNED) >= 1000000000000 THEN CONCAT(CAST(CAST(d.PARAM_VALUE AS UNSIGNED)/ 1099511627776 as CHAR(50)), ' TB') END as SizeReplicaX1,
	CASE WHEN CAST(d.PARAM_VALUE AS UNSIGNED)* 3 <= 1000000000 THEN CONCAT(CAST(CAST(d.PARAM_VALUE AS UNSIGNED)* 3 / 1048576 as CHAR(50)), ' MB')
	WHEN (CAST(d.PARAM_VALUE AS UNSIGNED)* 3) > 1000000000
	and CAST(d.PARAM_VALUE AS UNSIGNED)* 3 < 1000000000000 THEN CONCAT(CAST(CAST(d.PARAM_VALUE AS UNSIGNED)* 3 / 1073741824 as CHAR(50)), ' GB')
	WHEN CAST(d.PARAM_VALUE AS UNSIGNED)* 3 >= 1000000000000 THEN CONCAT(CAST(CAST(d.PARAM_VALUE AS UNSIGNED)* 3 / 1099511627776 as CHAR(50)), ' TB') END as SizeReplicaX3
from
	metastore.TBLS t
left join hue.auth_user u on
	t.OWNER = u.username
left join metastore.TABLE_PARAMS d on
	t.TBL_ID = d.TBL_ID
	and d.PARAM_KEY = 'totalSize'
left join metastore.TABLE_PARAMS dd on
	t.TBL_ID = dd.TBL_ID
	and (dd.PARAM_KEY = 'transient_lastDdlTime'
	or dd.PARAM_KEY is null)
left join metastore.DBS db on
	t.DB_ID = db.DB_ID
where
	t.TBL_NAME like '%rfc0050297%'
UNION
select
	'',
	'',
	'',
	'',
	'',
	'',
	'',
	'',
	'',
	'TOTAL SIZE',
	CONCAT(CAST(SUM(CAST(d.PARAM_VALUE AS UNSIGNED)) / 1099511627776 as CHAR(50)), ' TB') as SumSizeX1,
	CONCAT(CAST(SUM(CAST(d.PARAM_VALUE AS UNSIGNED))* 3 / 1099511627776 as CHAR(50)), ' TB') as SumSizeX3
from
	metastore.TBLS t
left join hue.auth_user u on
	t.OWNER = u.username
left join metastore.TABLE_PARAMS d on
	t.TBL_ID = d.TBL_ID
	and d.PARAM_KEY = 'totalSize'
left join metastore.TABLE_PARAMS dd on
	t.TBL_ID = dd.TBL_ID
	and (dd.PARAM_KEY = 'transient_lastDdlTime'
	or dd.PARAM_KEY is null)
left join metastore.DBS db on
	t.DB_ID = db.DB_ID
where
	t.TBL_NAME like '%rfc0050297%';