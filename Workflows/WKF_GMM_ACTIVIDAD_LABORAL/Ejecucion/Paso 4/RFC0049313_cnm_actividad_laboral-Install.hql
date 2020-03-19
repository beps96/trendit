--___________________________________________________________________________________________________________________________________________________________ 
--/                                                 #####    PROYECTO GMM - ACTIVIDAD LABORAL  ##### 
--| @FECHA DE CREACION:  # 04 de Diciembre del 2019
--| @DOMINIOS:           # INFO
--| @ARCHIVO:            # RFC004913_cnm_actividad_laboral-Install.hql
--| @AUTOR:              # creherna
--| @DESCRIPCION:        # Se crea script de pre-instalacion, realiza backup y despues elimina tablas. 
--| @PARAMETROS:         # N/A.
--| @CONSIDERACIONES: 
--|     # Genera tablas de respaldo con el sufijo del _rfc0049313_bkp.
--\_________________________________________________________________________________________________________________________________________________________/ 
CREATE TABLE IF NOT EXISTS bddltrn.alm_actividad_laboral_rfc0049313_bkp LIKE bddlalm.alm_actividad_laboral;
INSERT OVERWRITE TABLE bddltrn.alm_actividad_laboral_rfc0049313_bkp SELECT * FROM bddlalm.alm_actividad_laboral;

CREATE TABLE IF NOT EXISTS bddltrn.alm_actividad_laboralhis_rfc0049313_bkp LIKE bddlalm.alm_actividad_laboralhis;
INSERT OVERWRITE TABLE bddltrn.alm_actividad_laboralhis_rfc0049313_bkp SELECT * FROM bddlalm.alm_actividad_laboralhis;

CREATE TABLE IF NOT EXISTS bddltrn.stg_gmm_con_actividad_laboral_delta_extraccion_rfc0049313_bkp LIKE bddltrn.stg_gmm_con_actividad_laboral_delta_extraccion;

CREATE TABLE IF NOT EXISTS bddltrn.stg_gmm_con_actividad_laboral_delta_rfc0049313_bkp LIKE bddltrn.stg_gmm_con_actividad_laboral_delta;

CREATE TABLE IF NOT EXISTS bddltrn.stg_gmm_con_actividad_laboral_delta_reglas_duplicados_rfc0049313_bkp LIKE bddltrn.stg_gmm_con_actividad_laboral_delta_reglas_duplicados;


--Borra tablas Actuales
DROP TABLE IF EXISTS bddlalm.alm_actividad_laboral;
DROP TABLE IF EXISTS bddlalm.alm_actividad_laboralhis;
DROP TABLE IF EXISTS bddltrn.stg_gmm_con_actividad_laboral_delta_extraccion;
DROP TABLE IF EXISTS bddltrn.stg_gmm_con_actividad_laboral_delta;
DROP TABLE IF EXISTS bddltrn.stg_gmm_con_actividad_laboral_delta_reglas_duplicados;

--Borra tablas de desarrollo
DROP TABLE IF EXISTS bddlalm.alm_actividad_laboral_rfc0049313;
DROP TABLE IF EXISTS bddlalm.alm_actividad_laboral_rfc0049313his;
DROP TABLE IF EXISTS bddltrn.stg_gmm_con_actividad_laboral_delta_extraccion_rfc0049313;
DROP TABLE IF EXISTS bddltrn.stg_gmm_con_actividad_laboral_delta_relleno_rfc0049313;
DROP TABLE IF EXISTS bddltrn.stg_gmm_con_actividad_laboral_delta_rfc0049313;
DROP TABLE IF EXISTS bddltrn.stg_gmm_con_actividad_laboral_delta_reglas_duplicados_rfc0049313;