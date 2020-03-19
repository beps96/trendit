select CONCAT(db.NAME,'.',t.TBL_NAME),t.TBL_TYPE as TIPO_TABLA
	-- REPLACE(UPPER(CONCAT(db.NAME,'.',t.TBL_NAME)),'_RFC0050297','') AS TABLA
from
	metastore.TBLS t
left join metastore.DBS db on
	t.DB_ID = db.DB_ID
where t.TBL_NAME like '%_rfc0050297';
and db.NAME ='bddltrn';

