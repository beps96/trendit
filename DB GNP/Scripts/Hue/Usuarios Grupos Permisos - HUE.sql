SELECT   username, first_name AS nombre, last_name AS apellido, email,
         NAME AS grupo, IF (is_active = 1, 'SI', 'NO') AS activo,
         IF (is_superuser = 1, 'SI', 'NO') AS full_administrator,
         concat_ws ('.', hp.app, hp.action) AS permiso
    FROM hue.auth_user u LEFT JOIN hue.auth_user_groups ug ON u.ID = ug.user_id
         LEFT JOIN hue.auth_group g ON ug.GROUP_ID = g.ID
         LEFT JOIN hue.useradmin_grouppermission ugp ON g.ID = ugp.GROUP_ID
         LEFT JOIN hue.useradmin_huepermission hp ON ugp.hue_permission_id = hp.ID
ORDER BY username;