select
	dd.name,
	dd.uuid,
	u.username, 
	dd.last_modified 
from
	hue.desktop_document2 dd
left join hue.auth_user u on
	dd.owner_id = u.id
where
	-- dd.name like '%WKF_GMM_CDC%'
	-- dd.uuid in ('f149db30-5507-2721-7b9e-6043db1d17eb-1-0')
	 UPPER(dd.name) in ('WKF_GMM_POLIZA_AZUL')
	-- and u.username = 'usrdes01'
	 and dd.is_history = 0
	 and dd.is_trashed = 0
order by
	dd.name asc;

