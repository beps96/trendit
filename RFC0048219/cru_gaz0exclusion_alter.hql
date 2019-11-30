--ok-- bddlint.gaz0_exclusion
drop table if exists test_migracion.gaz0_exclusion_int;
create table test_migracion.gaz0_exclusion_int as select * from bddlint.gaz0_exclusion;
--ok-- bddlint.gaz0_exclusionhis
drop table if exists test_migracion.gaz0_exclusionhis_int;
create table test_migracion.gaz0_exclusionhis_int as select * from bddlint.gaz0_exclusionhis;
--ok-- bddlorg.gaz0_exclusion
drop table if exists test_migracion.gaz0_exclusion_org;
create table test_migracion.gaz0_exclusion_org as select * from bddlorg.gaz0_exclusion;
--ok-- bddlorg.gaz0_exclusion_tmp
drop table if exists test_migracion.gaz0_exclusion_tmp_org;
create table test_migracion.gaz0_exclusion_tmp_org as select * from bddlorg.gaz0_exclusion_tmp;
--oki-- bddlcru.gaz0_exclusion
drop table if exists test_migracion.gaz0_exclusion;
create table test_migracion.gaz0_exclusion as select * from bddlcru.gaz0_exclusion;
--oki-- bddlint.gaz0_exclusionsc
drop table if exists test_migracion.gaz0_exclusionsc;
create table test_migracion.gaz0_exclusionsc as select * from bddlint.gaz0_exclusionsc;
--oki-- bddlcru.gaz0_exclusionhis
drop table if exists test_migracion.gaz0_exclusionhis;
create table test_migracion.gaz0_exclusionhis as select * from bddlcru.gaz0_exclusionhis;


-- creaciones
drop table if exists bddlint.gaz0_exclusion;
CREATE TABLE bddlint.gaz0_exclusion (   pol_oficina DECIMAL(2,0),   pol_numero DECIMAL(6,0),   pol_ofi_renovac DECIMAL(2,0),   pol_num_renovac DECIMAL(6,0),   pol_fec_ini_vig DECIMAL(8,0),   pol_fec_fin_vig DECIMAL(8,0),   pol_status CHAR(2),   exc_d2_user_alta CHAR(6),   exc_d2_fec_movto DECIMAL(6,0),   exc_ofna DECIMAL(2,0),   exc_poliza DECIMAL(6,0),   exc_certificado CHAR(8),   exc_numero DECIMAL(6,0),   exc_tipo CHAR(1),   exc_texto DECIMAL(2,0),   exc_numtexto DECIMAL(2,0),   exc_consec DECIMAL(2,0),   exc_texto_armado CHAR(70),  exc_texto_descr_titulo  CHAR(60), exc_texto_descr_var1 CHAR(60), exc_texto_descr_var2 CHAR(60), exc_texto_descr_var3 CHAR(60), exc_texto_descr_var4 CHAR(60), exc_texto_descr_var5 CHAR(60), exc_texto_descr_var6 CHAR(60), exc_texto_descr_var7 CHAR(60), exc_texto_descr_var8 CHAR(60), exc_estatus_endoso CHAR(1), exc_observa CHAR(80),   id_hist STRING,   idmd5 VARCHAR(32) COMMENT 'IDMD5',   idmd5completo VARCHAR(32) COMMENT 'IDMD5COMPLETO',   fechaaud TIMESTAMP COMMENT 'FECHAAUD',   sistorig VARCHAR(20) COMMENT 'SISTORIG' ) STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileOutputFormat' LOCATION 'hdfs://nameservice1/user/hive/warehouse/bddlint.db/gaz0_exclusion';
drop table if exists bddlint.gaz0_exclusionhis;
CREATE TABLE bddlint.gaz0_exclusionhis (   pol_oficina DECIMAL(2,0),   pol_numero DECIMAL(6,0),   pol_ofi_renovac DECIMAL(2,0),   pol_num_renovac DECIMAL(6,0),   pol_fec_ini_vig DECIMAL(8,0),   pol_fec_fin_vig DECIMAL(8,0),   pol_status CHAR(2),   exc_d2_user_alta CHAR(6),   exc_d2_fec_movto DECIMAL(6,0),   exc_ofna DECIMAL(2,0),   exc_poliza DECIMAL(6,0),   exc_certificado CHAR(8),   exc_numero DECIMAL(6,0),   exc_tipo CHAR(1),   exc_texto DECIMAL(2,0),   exc_numtexto DECIMAL(2,0),   exc_consec DECIMAL(2,0),   exc_texto_armado CHAR(70),  exc_texto_descr_titulo  CHAR(60), exc_texto_descr_var1 CHAR(60), exc_texto_descr_var2 CHAR(60), exc_texto_descr_var3 CHAR(60), exc_texto_descr_var4 CHAR(60), exc_texto_descr_var5 CHAR(60), exc_texto_descr_var6 CHAR(60), exc_texto_descr_var7 CHAR(60), exc_texto_descr_var8 CHAR(60), exc_estatus_endoso CHAR(1), exc_observa CHAR(80),   id_hist STRING,   idmd5 VARCHAR(32) COMMENT 'IDMD5',   idmd5completo VARCHAR(32) COMMENT 'IDMD5COMPLETO',   fechaaud TIMESTAMP COMMENT 'FECHAAUD',   sistorig VARCHAR(20) COMMENT 'SISTORIG',   fechhist TIMESTAMP COMMENT 'FECHHIST' ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileOutputFormat' LOCATION 'hdfs://nameservice1/user/hive/warehouse/bddlint.db/gaz0_exclusionhis';
drop table if exists bddlint.gaz0_exclusionsc;
CREATE TABLE bddlint.gaz0_exclusionsc (   pol_oficina DECIMAL(2,0),   pol_numero DECIMAL(6,0),   pol_ofi_renovac DECIMAL(2,0),   pol_num_renovac DECIMAL(6,0),   pol_fec_ini_vig DECIMAL(8,0),   pol_fec_fin_vig DECIMAL(8,0),   pol_status CHAR(2),   exc_d2_user_alta CHAR(6),   exc_d2_fec_movto DECIMAL(6,0),   exc_ofna DECIMAL(2,0),   exc_poliza DECIMAL(6,0),   exc_certificado CHAR(8),   exc_numero DECIMAL(6,0),   exc_tipo CHAR(1),   exc_texto DECIMAL(2,0),   exc_numtexto DECIMAL(2,0),   exc_consec DECIMAL(2,0),   exc_texto_armado CHAR(70),  exc_texto_descr_titulo  CHAR(60), exc_texto_descr_var1 CHAR(60), exc_texto_descr_var2 CHAR(60), exc_texto_descr_var3 CHAR(60), exc_texto_descr_var4 CHAR(60), exc_texto_descr_var5 CHAR(60), exc_texto_descr_var6 CHAR(60), exc_texto_descr_var7 CHAR(60), exc_texto_descr_var8 CHAR(60), exc_estatus_endoso CHAR(1), exc_observa CHAR(80),   id_hist STRING,   idmd5 VARCHAR(32) COMMENT 'IDMD5',   idmd5completo VARCHAR(32) COMMENT 'IDMD5COMPLETO',   fechaaud TIMESTAMP COMMENT 'FECHAAUD',   sistorig VARCHAR(20) COMMENT 'SISTORIG' ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileOutputFormat' LOCATION 'hdfs://nameservice1/user/hive/warehouse/bddlint.db/gaz0_exclusionsc';

drop table if exists bddlcru.gaz0_exclusion;
CREATE TABLE bddlcru.gaz0_exclusion (   pol_oficina DECIMAL(2,0),   pol_numero DECIMAL(6,0),   pol_ofi_renovac DECIMAL(2,0),   pol_num_renovac DECIMAL(6,0),   pol_fec_ini_vig DECIMAL(8,0),   pol_fec_fin_vig DECIMAL(8,0),   pol_status CHAR(2),   exc_d2_user_alta CHAR(6),   exc_d2_fec_movto DECIMAL(6,0),   exc_ofna DECIMAL(2,0),   exc_poliza DECIMAL(6,0),   exc_certificado CHAR(8),   exc_numero DECIMAL(6,0),   exc_tipo CHAR(1),   exc_texto DECIMAL(2,0),   exc_numtexto DECIMAL(2,0),   exc_consec DECIMAL(2,0),   exc_texto_armado CHAR(70),  exc_texto_descr_titulo  CHAR(60), exc_texto_descr_var1 CHAR(60), exc_texto_descr_var2 CHAR(60), exc_texto_descr_var3 CHAR(60), exc_texto_descr_var4 CHAR(60), exc_texto_descr_var5 CHAR(60), exc_texto_descr_var6 CHAR(60), exc_texto_descr_var7 CHAR(60), exc_texto_descr_var8 CHAR(60), exc_estatus_endoso CHAR(1), exc_observa CHAR(80),   id_hist STRING,   idmd5 VARCHAR(32) COMMENT 'IDMD5',   idmd5completo VARCHAR(32) COMMENT 'IDMD5COMPLETO',   fechaaud TIMESTAMP COMMENT 'FECHAAUD',   sistorig VARCHAR(20) COMMENT 'SISTORIG' ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileOutputFormat' LOCATION 'hdfs://nameservice1/user/hive/warehouse/bddlcru.db/gaz0_exclusion';
drop table if exists bddlcru.gaz0_exclusionhis;
CREATE TABLE bddlcru.gaz0_exclusionhis (   pol_oficina DECIMAL(2,0),   pol_numero DECIMAL(6,0),   pol_ofi_renovac DECIMAL(2,0),   pol_num_renovac DECIMAL(6,0),   pol_fec_ini_vig DECIMAL(8,0),   pol_fec_fin_vig DECIMAL(8,0),   pol_status CHAR(2),   exc_d2_user_alta CHAR(6),   exc_d2_fec_movto DECIMAL(6,0),   exc_ofna DECIMAL(2,0),   exc_poliza DECIMAL(6,0),   exc_certificado CHAR(8),   exc_numero DECIMAL(6,0),   exc_tipo CHAR(1),   exc_texto DECIMAL(2,0),   exc_numtexto DECIMAL(2,0),   exc_consec DECIMAL(2,0),   exc_texto_armado CHAR(70),  exc_texto_descr_titulo  CHAR(60), exc_texto_descr_var1 CHAR(60), exc_texto_descr_var2 CHAR(60), exc_texto_descr_var3 CHAR(60), exc_texto_descr_var4 CHAR(60), exc_texto_descr_var5 CHAR(60), exc_texto_descr_var6 CHAR(60), exc_texto_descr_var7 CHAR(60), exc_texto_descr_var8 CHAR(60), exc_estatus_endoso CHAR(1), exc_observa CHAR(80),   id_hist STRING,   idmd5 VARCHAR(32) COMMENT 'IDMD5',   idmd5completo VARCHAR(32) COMMENT 'IDMD5COMPLETO',   fechaaud TIMESTAMP COMMENT 'FECHAAUD',   sistorig VARCHAR(20) COMMENT 'SISTORIG',   fechhist TIMESTAMP COMMENT 'FECHHIST' ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.RCFileOutputFormat' LOCATION 'hdfs://nameservice1/user/hive/warehouse/bddlcru.db/gaz0_exclusionhis';

drop table if exists bddlorg.gaz0_exclusion;
CREATE TABLE bddlorg.gaz0_exclusion (   pol_oficina STRING,   pol_numero STRING,   pol_ofi_renovac STRING,   pol_num_renovac STRING,   pol_fec_ini_vig STRING,   pol_fec_fin_vig STRING,   pol_status STRING,   exc_d2_user_alta STRING,   exc_d2_fec_movto STRING,   exc_ofna STRING,   exc_poliza STRING,   exc_certificado STRING,   exc_numero STRING,   exc_tipo STRING,   exc_texto STRING,   exc_numtexto STRING,   exc_consec STRING,   exc_texto_armado STRING, exc_texto_descr_titulo  STRING, exc_texto_descr_var1 STRING, exc_texto_descr_var2 STRING, exc_texto_descr_var3 STRING, exc_texto_descr_var4 STRING, exc_texto_descr_var5 STRING, exc_texto_descr_var6 STRING, exc_texto_descr_var7 STRING, exc_texto_descr_var8 STRING, exc_estatus_endoso STRING,  exc_observa STRING,   id_hist STRING,   idmd5 VARCHAR(32) COMMENT 'IDMD5',   idmd5completo VARCHAR(32) COMMENT 'IDMD5COMPLETO' ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ( 'field.delim'='|', 'serialization.format'='|') STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'LOCATION 'hdfs://nameservice1/user/hive/warehouse/bddlorg.db/gaz0_exclusion';
drop table if exists bddlorg.gaz0_exclusion_tmp;
CREATE TABLE bddlorg.gaz0_exclusion_tmp (   pol_oficina STRING,   pol_numero STRING,   pol_ofi_renovac STRING,   pol_num_renovac STRING,   pol_fec_ini_vig STRING,   pol_fec_fin_vig STRING,   pol_status STRING,   exc_d2_user_alta STRING,   exc_d2_fec_movto STRING,   exc_ofna STRING,   exc_poliza STRING,   exc_certificado STRING,   exc_numero STRING,   exc_tipo STRING,   exc_texto STRING,   exc_numtexto STRING,   exc_consec STRING,   exc_texto_armado STRING, exc_texto_descr_titulo  STRING, exc_texto_descr_var1 STRING, exc_texto_descr_var2 STRING, exc_texto_descr_var3 STRING, exc_texto_descr_var4 STRING, exc_texto_descr_var5 STRING, exc_texto_descr_var6 STRING, exc_texto_descr_var7 STRING, exc_texto_descr_var8 STRING, exc_estatus_endoso STRING,  exc_observa STRING,   id_hist STRING,   idmd5 VARCHAR(32) COMMENT 'IDMD5',   idmd5completo VARCHAR(32) COMMENT 'IDMD5COMPLETO' ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ( 'field.delim'='|', 'serialization.format'='|') STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'LOCATION 'hdfs://nameservice1/user/hive/warehouse/bddlorg.db/gaz0_exclusion_tmp';




