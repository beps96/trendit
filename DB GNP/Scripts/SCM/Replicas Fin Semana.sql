select
	replace(substring(substring_index(c.ARGUMENTS , 'scheduleId', -2), 3, 3),
	',',
	'') as ID,
	substring(substring(c.ARGUMENTS, locate('tables' , c.ARGUMENTS), locate('targetClientConfig' , c.ARGUMENTS) - locate('tables' , c.ARGUMENTS)), 11, 7) as esquema,
	substring(substring(c.ARGUMENTS, locate('tables' , c.ARGUMENTS)+ 2, locate('targetClientConfig' , c.ARGUMENTS) - locate('tables' , c.ARGUMENTS)), locate('[', substring(c.ARGUMENTS, locate('tables' , c.ARGUMENTS), locate('targetClientConfig' , c.ARGUMENTS) - locate('tables' , c.ARGUMENTS))), locate(']', substring(c.ARGUMENTS, locate('tables' , c.ARGUMENTS), locate('targetClientConfig' , c.ARGUMENTS) - locate('tables' , c.ARGUMENTS))) - locate('[', substring(c.ARGUMENTS, locate('tables' , c.ARGUMENTS), locate('targetClientConfig' , c.ARGUMENTS) - locate('tables' , c.ARGUMENTS)))-3) as tabla,
	from_unixtime(substring(c.start_instant, 1, 10)) as fecha_inicio,
	from_unixtime(substring(c.end_instant, 1, 10)) as fecha_fin,
	replace(substring(substring(c.ARGUMENTS, locate('numConcurrentMaps' , c.ARGUMENTS), locate('overwrite' , c.ARGUMENTS) - locate('numConcurrentMaps' , c.ARGUMENTS)), 20, 3),
	',',
	'') as numConcurrentMaps,
	replace(substring(substring(c.ARGUMENTS, locate('bandwidth' , c.ARGUMENTS), locate('copyListingOnSource' , c.ARGUMENTS) - locate('bandwidth' , c.ARGUMENTS)), 12, 3),
	',',
	'') as bandwidth
from
	commands c
where
	c.name = 'HiveReplicationCommand'
	and str_to_date(from_unixtime(substring(c.start_instant, 1, 10)),
	'%Y-%m-%d') = '2020-03-08'
order by
	4 asc;