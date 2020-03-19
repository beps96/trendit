select
	str_to_date(from_unixtime(SUBSTRING(event_time,1,10)),
	'%Y-%m-%d') as event_time, operation, operation_text
from
	nav.HIVE_AUDIT_EVENTS_2020_01_11
where
	UPPER(OPERATION_TEXT) like '%BDDLCRU.KCIM16H%'
UNION
select
	str_to_date(from_unixtime(SUBSTRING(event_time,1,10)),
	'%Y-%m-%d') as event_time, operation, operation_text
from
	nav.HIVE_AUDIT_EVENTS_2020_01_12
where
	UPPER(OPERATION_TEXT) like '%BDDLCRU.KCIM16H%'
UNION
select
	str_to_date(from_unixtime(SUBSTRING(event_time,1,10)),
	'%Y-%m-%d') as event_time, operation, operation_text
from
	nav.HIVE_AUDIT_EVENTS_2020_01_13
where
	UPPER(OPERATION_TEXT) like '%BDDLCRU.KCIM16H%'
UNION
select
	str_to_date(from_unixtime(SUBSTRING(event_time,1,10)),
	'%Y-%m-%d') as event_time, operation, operation_text
from
	nav.HIVE_AUDIT_EVENTS_2020_01_14
where
	UPPER(OPERATION_TEXT) like '%BDDLCRU.KCIM16H%'
UNION
select
	str_to_date(from_unixtime(SUBSTRING(event_time,1,10)),
	'%Y-%m-%d') as event_time, operation, operation_text
from
	nav.HIVE_AUDIT_EVENTS_2020_01_15
where
	UPPER(OPERATION_TEXT) like '%BDDLCRU.KCIM16H%'
UNION
select
	str_to_date(from_unixtime(SUBSTRING(event_time,1,10)),
	'%Y-%m-%d') as event_time, operation, operation_text
from
	nav.HIVE_AUDIT_EVENTS_2020_01_16
where
	UPPER(OPERATION_TEXT) like '%BDDLCRU.KCIM16H%'
UNION
select
	str_to_date(from_unixtime(SUBSTRING(event_time,1,10)),
	'%Y-%m-%d') as event_time, operation, operation_text
from
	nav.HIVE_AUDIT_EVENTS_2020_01_17
where
	UPPER(OPERATION_TEXT) like '%BDDLCRU.KCIM16H%'
UNION
select
	str_to_date(from_unixtime(SUBSTRING(event_time,1,10)),
	'%Y-%m-%d') as event_time, operation, operation_text
from
	nav.HIVE_AUDIT_EVENTS_2020_01_18
where
	UPPER(OPERATION_TEXT) like '%BDDLCRU.KCIM16H%'
UNION
select
	str_to_date(from_unixtime(SUBSTRING(event_time,1,10)),
	'%Y-%m-%d') as event_time, operation, operation_text
from
	nav.HIVE_AUDIT_EVENTS_2020_01_19
where
	UPPER(OPERATION_TEXT) like '%BDDLCRU.KCIM16H%'
UNION
select
	str_to_date(from_unixtime(SUBSTRING(event_time,1,10)),
	'%Y-%m-%d') as event_time, operation, operation_text
from
	nav.HIVE_AUDIT_EVENTS_2020_01_20
where
	UPPER(OPERATION_TEXT) like '%BDDLCRU.KCIM16H%'
UNION
select
	str_to_date(from_unixtime(SUBSTRING(event_time,1,10)),
	'%Y-%m-%d') as event_time, operation, operation_text
from
	nav.IMPALA_AUDIT_EVENTS_2020_01_11
where
	UPPER(OPERATION_TEXT) like '%BDDLCRU.KCIM16H%'
UNION
select
	str_to_date(from_unixtime(SUBSTRING(event_time,1,10)),
	'%Y-%m-%d') as event_time, operation, operation_text
from
	nav.IMPALA_AUDIT_EVENTS_2020_01_12
where
	UPPER(OPERATION_TEXT) like '%BDDLCRU.KCIM16H%'
UNION
select
	str_to_date(from_unixtime(SUBSTRING(event_time,1,10)),
	'%Y-%m-%d') as event_time, operation, operation_text
from
	nav.IMPALA_AUDIT_EVENTS_2020_01_13
where
	UPPER(OPERATION_TEXT) like '%BDDLCRU.KCIM16H%'
UNION
select
	str_to_date(from_unixtime(SUBSTRING(event_time,1,10)),
	'%Y-%m-%d') as event_time, operation, operation_text
from
	nav.IMPALA_AUDIT_EVENTS_2020_01_14
where
	UPPER(OPERATION_TEXT) like '%BDDLCRU.KCIM16H%'
UNION
select
	str_to_date(from_unixtime(SUBSTRING(event_time,1,10)),
	'%Y-%m-%d') as event_time, operation, operation_text
from
	nav.IMPALA_AUDIT_EVENTS_2020_01_15
where
	UPPER(OPERATION_TEXT) like '%BDDLCRU.KCIM16H%'
UNION
select
	str_to_date(from_unixtime(SUBSTRING(event_time,1,10)),
	'%Y-%m-%d') as event_time, operation, operation_text
from
	nav.IMPALA_AUDIT_EVENTS_2020_01_16
where
	UPPER(OPERATION_TEXT) like '%BDDLCRU.KCIM16H%'
UNION
select
	str_to_date(from_unixtime(SUBSTRING(event_time,1,10)),
	'%Y-%m-%d') as event_time, operation, operation_text
from
	nav.IMPALA_AUDIT_EVENTS_2020_01_17
where
	UPPER(OPERATION_TEXT) like '%BDDLCRU.KCIM16H%'
UNION
select
	str_to_date(from_unixtime(SUBSTRING(event_time,1,10)),
	'%Y-%m-%d') as event_time, operation, operation_text
from
	nav.IMPALA_AUDIT_EVENTS_2020_01_18
where
	UPPER(OPERATION_TEXT) like '%BDDLCRU.KCIM16H%'
UNION
select
	str_to_date(from_unixtime(SUBSTRING(event_time,1,10)),
	'%Y-%m-%d') as event_time, operation, operation_text
from
	nav.IMPALA_AUDIT_EVENTS_2020_01_19
where
	UPPER(OPERATION_TEXT) like '%BDDLCRU.KCIM16H%'
UNION
select
	str_to_date(from_unixtime(SUBSTRING(event_time,1,10)),
	'%Y-%m-%d') as event_time, operation, operation_text
from
	nav.IMPALA_AUDIT_EVENTS_2020_01_20
where
	UPPER(OPERATION_TEXT) like '%BDDLCRU.KCIM16H%';
