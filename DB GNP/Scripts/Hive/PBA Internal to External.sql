create EXTERNAL table if not exists pba_external (id int,
name string) location '/transf/almac/pba';

drop table pba_external;
drop table pba_external2;


DESCRIBE FORMATTED pba_external;
DESCRIBE FORMATTED pba_external2;

ALTER TABLE pba_external SET TBLPROPERTIES('EXTERNAL'='FALSE');
ALTER TABLE pba_external RENAME TO pba_external2


-- RESTORE
ALTER TABLE pba_external2 RENAME TO pba_external
ALTER TABLE pba_external SET TBLPROPERTIES('EXTERNAL'='TRUE');



describe formatted bddlalm.trn_reclamacion;
describe formatted bddlalm.alm_pagohis;

