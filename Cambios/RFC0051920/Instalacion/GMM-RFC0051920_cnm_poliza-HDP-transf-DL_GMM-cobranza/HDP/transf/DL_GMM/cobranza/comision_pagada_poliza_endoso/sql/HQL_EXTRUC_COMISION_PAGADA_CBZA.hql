drop table if exists bddldes.enr_comision_pagada_poliza_endoso_cbza_base;
drop table if exists bddldes.stg_comision_pagada_poliza_endoso_cbza_idmd5;
drop table if exists bddldes.enr_comision_pagada_poliza_endoso_cbza_base_estadistico_p_cbza;
------------------------------------------------------------------------------------------------------------
create  table if not exists bddldes.enr_comision_pagada_poliza_endoso_cbza_base
(
am_contable	int	,
fch_contable	timestamp	,
num_poliza	varchar(14)	,
vigencia_poliza	smallint	,
num_version_poliza	smallint	,
num_poliza_cobranza	varchar(8)	,
num_endoso	varchar(6)	,
cve_cobertura_contable	varchar(3)	,
des_cobertura_contable	varchar(100)	,
num_consecutivo_movimiento_contable	smallint	,
cve_sistema	varchar(4)	,
cve_sistema_int	varchar(4)	,
cve_forma_pago	varchar(1)	,
des_forma_pago	varchar(100)	,
cve_tipo_movimiento	varchar(6)	,-------------
des_tipo_movimiento	varchar(50)	,
mto_comision_prima_neta	decimal(16,4)	,
mto_comision_derecho_poliza	decimal(16,4)	,
mto_comision_recargo_pago_fraccionado	decimal(16,4)	,
mto_isr_comisiones	decimal(16,4)	,
mto_importe_comision_total	decimal(16,4)	,
cve_unidad_medida	varchar(4)	,
des_unidad_medida	varchar(50)	,
pct_comision	decimal(16,4)	,
cve_tipo_intermediario	varchar(3)	,
des_tipo_intermediario	varchar(50)	,
fch_movimiento_comision	timestamp	,
fch_pago timestamp	,
canal_venta_estadistico	varchar(100)	,
segmento_venta_estadistico	varchar(100)	,
cve_segmento	varchar(10)	,
des_segmento	varchar(100)	,
cve_negocio_multinacional	varchar(10)	,
des_negocio_multinacional	varchar(100)	,
cve_tipo_bono	varchar(5)	,
des_tipo_bono	varchar(100)	,
afecto_plan_incentivo	varchar(100)	,
ban_poliza_contributoria	varchar(2)	,
pct_contribucion	decimal(16,4)	,
ban_poliza_prestacion	varchar(2)	,
cve_motivo_exclusion_plan_incentivo	varchar(5)	,
des_motivo_exclusion_plan_incentivo	varchar(100)	
) stored as rcfile;

-------------------------------------------------------------------------------------------------------
create  table if not exists bddldes.enr_comision_pagada_poliza_endoso_cbza_base_estadistico_p_cbza
(
am_contable	int	,
fch_contable	timestamp	,
num_poliza	varchar(14)	,
vigencia_poliza	smallint	,
num_version_poliza	smallint	,
num_poliza_cobranza	varchar(8)	,
num_endoso	varchar(6)	,
cve_cobertura_contable	varchar(3)	,
des_cobertura_contable	varchar(100)	,
num_consecutivo_movimiento_contable	smallint	,
cve_sistema	varchar(4)	,
cve_sistema_int	varchar(4)	,
cve_forma_pago	varchar(1)	,
des_forma_pago	varchar(100)	,
cve_tipo_movimiento	varchar(6)	,-------------
des_tipo_movimiento	varchar(50)	,
mto_comision_prima_neta	decimal(16,4)	,
mto_comision_derecho_poliza	decimal(16,4)	,
mto_comision_recargo_pago_fraccionado	decimal(16,4)	,
mto_isr_comisiones	decimal(16,4)	,
mto_importe_comision_total	decimal(16,4)	,
cve_unidad_medida	varchar(4)	,
des_unidad_medida	varchar(50)	,
pct_comision	decimal(16,4)	,
cve_tipo_intermediario	varchar(3)	,
des_tipo_intermediario	varchar(50)	,
fch_movimiento_comision	timestamp	,
fch_pago timestamp	,
canal_venta_estadistico	varchar(100)	,
segmento_venta_estadistico	varchar(100)	,
cve_segmento	varchar(10)	,
des_segmento	varchar(100)	,
cve_negocio_multinacional	varchar(10)	,
des_negocio_multinacional	varchar(100)	,
cve_tipo_bono	varchar(5)	,
des_tipo_bono	varchar(100)	,
afecto_plan_incentivo	varchar(100)	,
ban_poliza_contributoria	varchar(2)	,
pct_contribucion	decimal(16,4)	,
ban_poliza_prestacion	varchar(2)	,
cve_motivo_exclusion_plan_incentivo	varchar(5)	,
des_motivo_exclusion_plan_incentivo	varchar(100)	
) stored as rcfile;

---------------------------------------------------------------------------------------------------------
create  table if not exists bddldes.stg_comision_pagada_poliza_endoso_cbza_idmd5
(
am_contable	int	,
fch_contable	timestamp	,
num_poliza	varchar(14)	,
vigencia_poliza	smallint	,
num_version_poliza	smallint	,
num_poliza_cobranza	varchar(8)	,
num_endoso	varchar(6)	,
cve_cobertura_contable	varchar(3)	,
des_cobertura_contable	varchar(100)	,
num_consecutivo_movimiento_contable	smallint	,
cve_sistema	varchar(4)	,
cve_sistema_int	varchar(4)	,
cve_forma_pago	varchar(1)	,
des_forma_pago	varchar(100)	,
cve_tipo_movimiento	varchar(6)	,-------------
des_tipo_movimiento	varchar(50)	,
mto_comision_prima_neta	decimal(16,4)	,
mto_comision_derecho_poliza	decimal(16,4)	,
mto_comision_recargo_pago_fraccionado	decimal(16,4)	,
mto_isr_comisiones	decimal(16,4)	,
mto_importe_comision_total	decimal(16,4)	,
cve_unidad_medida	varchar(4)	,
des_unidad_medida	varchar(50)	,
pct_comision	decimal(16,4)	,
cve_tipo_intermediario	varchar(3)	,
des_tipo_intermediario	varchar(50)	,
fch_movimiento_comision	timestamp	,
fch_pago timestamp	,
canal_venta_estadistico	varchar(100)	,
segmento_venta_estadistico	varchar(100)	,
cve_segmento	varchar(10)	,
des_segmento	varchar(100)	,
cve_negocio_multinacional	varchar(10)	,
des_negocio_multinacional	varchar(100)	,
cve_tipo_bono	varchar(5)	,
des_tipo_bono	varchar(100)	,
afecto_plan_incentivo	varchar(100)	,
ban_poliza_contributoria	varchar(2)	,
pct_contribucion	decimal(16,4)	,
ban_poliza_prestacion	varchar(2)	,
cve_motivo_exclusion_plan_incentivo	varchar(5)	,
des_motivo_exclusion_plan_incentivo	varchar(100),
ts_alta_hdfs timestamp,
idmd5 varchar(32),
idmd5completo varchar(32)	
) stored as rcfile;


CREATE TABLE  if not exists bddldes.alm_comision_pagada_poliza_endoso
(
	am_contable	int
	,fch_contable	timestamp
	,num_poliza	varchar(14)
	,vigencia_poliza	smallint
	,num_version_poliza	smallint
	,num_poliza_cobranza	varchar(8)
	,num_endoso	varchar(6)
	,cve_cobertura_contable	varchar(3)
	,des_cobertura_contable	varchar(100)
	,num_consecutivo_movimiento_contable	smallint
	,cve_sistema	varchar(4)
	,cve_sistema_int	varchar(4)
	,cve_forma_pago	varchar(1)
	,des_forma_pago	varchar(100)
	,cve_tipo_movimiento	varchar(6)
	,des_tipo_movimiento	varchar(50)
	,mto_comision_prima_neta	decimal(16,4)
	,mto_comision_derecho_poliza	decimal(16,4)
	,mto_comision_recargo_pago_fraccionado	decimal(16,4)
	,mto_isr_comisiones	decimal(16,4)
	,mto_importe_comision_total	decimal(16,4)
	,cve_unidad_medida	varchar(4)
	,des_unidad_medida	varchar(50)
	,pct_comision	decimal(16,4)
	,cve_tipo_intermediario	varchar(3)
	,des_tipo_intermediario	varchar(50)
	,fch_movimiento_comision	timestamp
	,fch_pago	timestamp
	,canal_venta_estadistico	varchar(100)
	,segmento_venta_estadistico	varchar(100)
	,cve_segmento	varchar(10)
	,des_segmento	varchar(100)
	,cve_negocio_multinacional	varchar(10)
	,des_negocio_multinacional	varchar(100)
	,cve_tipo_bono	varchar(5)
	,des_tipo_bono	varchar(100)
	,afecto_plan_incentivo	varchar(100)
	,ban_poliza_contributoria	varchar(2)
	,pct_contribucion	decimal(16,4)
	,ban_poliza_prestacion	varchar(2)
	,cve_motivo_exclusion_plan_incentivo	varchar(5)
	,des_motivo_exclusion_plan_incentivo	varchar(100)
	,ts_alta_hdfs	timestamp
	,idmd5	varchar(32)
	,idmd5completo	varchar(32)
) stored as rcfile;