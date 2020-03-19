select
	db.NAME as ESQUEMA,
	t.TBL_NAME as TABLA,
	t.TBL_TYPE as TIPO_TABLA,
	case
		when t.TBL_TYPE = 'EXTERNAL_TABLE' then concat("ALTER TABLE ", db.NAME, ".", t.TBL_NAME, " SET TBLPROPERTIES(' EXTERNAL '=' FALSE ');") end as TO_INTERNAL_HIVE,
		concat('DROP TABLE ', db.NAME, '.', t.TBL_NAME, ';') as DROP_HIVE
	from
		metastore.TBLS t
	left join metastore.DBS db on
		t.DB_ID = db.DB_ID
	where
		TBL_NAME like '%_2019%'
		or TBL_NAME like '%_2018%'
		or TBL_NAME like '%_2020%'
		or TBL_NAME like '%2019_%'
		or TBL_NAME like '%2018_%'
		or TBL_NAME like '%2020_%'
		or lower(TBL_NAME) like '%rfc%'
		or lower(TBL_NAME) like '%bkp%'
		or lower(TBL_NAME) like '%backup%'
		or lower(TBL_NAME) like '%respaldo%'
		or lower(TBL_NAME) like '%borra%'
		or lower(TBL_NAME) like '%dev%'
		or lower(TBL_NAME) like '%enero%'
		or lower(TBL_NAME) like '%febrero%'
		or lower(TBL_NAME) like '%marzo%'
		or lower(TBL_NAME) like '%abril%'
		or lower(TBL_NAME) like '%mayo%'
		or lower(TBL_NAME) like '%junio%'
		or lower(TBL_NAME) like '%julio%'
		or lower(TBL_NAME) like '%agosto%'
		or lower(TBL_NAME) like '%septiembre%'
		or lower(TBL_NAME) like '%octubre%'
		or lower(TBL_NAME) like '%noviembre%'
		or lower(TBL_NAME) like '%diciembre%';