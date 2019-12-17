Extracción de la totalidad de los usuarios en hadoop, así como todos y cada uno de los roles que tienen asociados

SELECT   username, first_name AS nombre, last_name AS apellido, email,
         NAME AS grupo, IF (is_active = 1, 'SI', 'NO') AS activo,
         IF (is_superuser = 1, 'SI', 'NO') AS full_administrator,
         concat_ws ('.', hp.app, hp.action) AS permiso
    FROM auth_user u LEFT JOIN auth_user_groups ug ON u.ID = ug.user_id
         LEFT JOIN auth_group g ON ug.GROUP_ID = g.ID
         LEFT JOIN useradmin_grouppermission ugp ON g.ID = ugp.GROUP_ID
         LEFT JOIN useradmin_huepermission hp ON ugp.hue_permission_id = hp.ID
ORDER BY username;




 mysql hue -e " SELECT   username, first_name AS nombre, last_name AS apellido, email, NAME AS grupo, IF (is_active = 1, 'SI', 'NO') AS activo, IF (is_superuser = 1, 'SI', 'NO') AS full_administrator, concat_ws ('.', hp.app, hp.action) AS permiso FROM auth_user u LEFT JOIN auth_user_groups ug ON u.ID = ug.user_id LEFT JOIN auth_group g ON ug.GROUP_ID = g.ID LEFT JOIN useradmin_grouppermission ugp ON g.ID = ugp.GROUP_ID LEFT JOIN useradmin_huepermission hp ON ugp.hue_permission_id = hp.ID ORDER BY username;" | sed 's/\t/\|/g' > /tmp/rep.csv



 10675322:25000



hdfs dfs -du -s -h /user/hive/warehouse/bddlalm.db/gmm_elemento_poliza
hdfs dfs -du -s -h /user/hive/warehouse/bddlalm.db/gmm_elemento_poliza_relleno
hdfs dfs -du -s -h /user/hive/warehouse/bddlalm.db/gmm_elemento_objeto
hdfs dfs -du -s -h /user/hive/warehouse/bddlalm.db/gmm_elemento_objeto_relleno
hdfs dfs -du -s -h /user/hive/warehouse/bddlalm.db/gmm_elemento_cobertura
hdfs dfs -du -s -h /user/hive/warehouse/bddlalm.db/gmm_elemento_cobertura_relleno


UPDATE
	auth_user
SET
	is_superuser = 0
WHERE
	is_superuser = 1
	AND username NOT IN ('hadoop-admin',
	'hive');


UPDATE
	auth_user
SET
	is_superuser = 1
WHERE
	username IN ('admin',
	'amancebo',
	'appadmin',
	'eargaez',
	'esantos',
	'hadoop-admin',
	'hive',
	'jfrias',
	'jgfrias',
	'jrpuente',
	'maguero',
	'ruriel',
	'user_adm_dev',
	'user_aut',
	'user_crm',
	'user_fyf',
	'user_fza',
	'user_gmm',
	'user_mant',
	'user_vid',
	'usrsup');
