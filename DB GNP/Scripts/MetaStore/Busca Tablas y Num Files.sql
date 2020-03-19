select DISTINCT 
	db.NAME AS ESQUEMA,
	t.TBL_NAME AS TABLA,
	t.TBL_TYPE AS TIPO_TABLA,
	str_to_date(from_unixtime(t.CREATE_TIME),
	'%Y-%m-%d') as CREATE_TIME,
	str_to_date(from_unixtime(dd.PARAM_VALUE),
	'%Y-%m-%d') as LAST_DDL_TIME,
	d.PARAM_VALUE as NUMFILES,
	case
		when p.PART_ID is not null then 'PARTICIONADA'
		else 'NO PARTICIONADA' end as particionada
	from
		metastore.TBLS t
	left join metastore.TABLE_PARAMS d on
		t.TBL_ID = d.TBL_ID
		and d.PARAM_KEY = 'numFiles'
	left join metastore.TABLE_PARAMS dd on
		t.TBL_ID = dd.TBL_ID
		and dd.PARAM_KEY = 'transient_lastDdlTime'
	left join metastore.DBS db on
		t.DB_ID = db.DB_ID
	left join metastore.PARTITIONS p on
		t.TBL_ID = p.TBL_ID
	where
		t.TBL_NAME = 'alm_conta_siniestros_gmm'
		and db.NAME = 'bddlalm' /*UNION
SELECT
	'',
	'',
	'',
	'',
	'TOTAL FILES',
	CAST(SUM(CAST(d.PARAM_VALUE AS UNSIGNED)) as CHAR(50)) as NUMFILES
from
	metastore.TBLS t
left join metastore.TABLE_PARAMS d on
	t.TBL_ID = d.TBL_ID
	and d.PARAM_KEY = 'numFiles'
left join metastore.DBS db on
	t.DB_ID = db.DB_ID
left join metastore.PARTITIONS p on
	t.TBL_ID = p.TBL_ID
where
	t.TBL_NAME = 'cnm_conta_siniestros_gmm'
	and db.NAME = 'bddlalm'*/;
