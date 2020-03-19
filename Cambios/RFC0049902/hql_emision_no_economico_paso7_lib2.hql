--#@@@@@@@@@@@@@@@@ Paso_7  *Este paso tiene todo lo referente a  bddlalm.aut_elemento_poliza_tp
--****AMH:20190613 Le faltan catalogos  Karlita se los va pegar


create table bddldes.insumos_p_personalizado as 
select distinct a.num_poliza,a.num_version,a.cve_negocio_operable,a.cve_codigo_promocion,a.cve_producto_personalizado
,b.ID_NEGOCIO, b.VERSION_NEGOCIO   
from 
bddldes.insumos_autos_p7 a 
inner join 
bddlorg.SCA_TESTRUCTURA_NEGOCIO AS b  on trim(a.cve_negocio_operable)=trim(b.ID_NEGOCIO_OPERABLE) 
inner join 
bddlorg.SCA_TNEGOCIO as c
 on 
  b.ID_NEGOCIO = c.ID_NEGOCIO
and b.VERSION_NEGOCIO =c.VERSION_NEGOCIO
 and ( 
 (a.fch_inicio_vigencia>= Cast(unix_timestamp(concat(substring(c.FECHA_INICIO,1,4),substring(c.FECHA_INICIO,6,2),substring(c.FECHA_INICIO,9,2),'000000'),'yyyyMMddHHmmss')  as timestamp) 
and   a.fch_inicio_vigencia <=Cast(unix_timestamp(concat(substring(c.FECHA_FIN,1,4),substring(c.FECHA_FIN,6,2),substring(c.FECHA_FIN,9,2),'000000'),'yyyyMMddHHmmss')  as timestamp) )
 or 

(a.fch_inicio_vigencia>= Cast(unix_timestamp(concat(substring(c.FECHA_INICIO,1,4),substring(c.FECHA_INICIO,6,2),substring(c.FECHA_INICIO,9,2),'000000'),'yyyyMMddHHmmss')  as timestamp) 
and  trim(FECHA_FIN)='null'
))


create table bddldes.insumos_p_personalizado2 as 
select b.*
from (
select num_poliza,num_version,max(version_negocio) as version_negocio
from bddldes.insumos_p_personalizado 
group by num_poliza,num_version ) a 
inner join bddldes.insumos_p_personalizado b
on 
trim(a.num_poliza)        =  trim(b.num_poliza)       and 
a.num_version       =  b.num_version      and 
a.version_negocio   =  b.version_negocio  



create table bddldes.insumos_p_personalizado3 as 
select a.num_poliza,a.num_version,a.cve_negocio_operable,a.cve_codigo_promocion
,a.ID_NEGOCIO, a.VERSION_NEGOCIO,b.ID_NODO,c.ID_DETALLE_DOMINIO,c.VERSION_DETALLE_DOMINIO,a.cve_producto_personalizado,
d.producto_personalizado_nombre
from bddldes.insumos_p_personalizado2  a
inner join   bddlorg.sca_tnodo  b
on trim(a.cve_codigo_promocion)=trim(coalesce(translate(b.CODIGO_PROMOCION,'null',''),'')) 
  and a.ID_NEGOCIO = b.ID_NEGOCIO
and a.VERSION_NEGOCIO =b.VERSION_NEGOCIO
inner join bddlorg.sca_tdetalle_dominio_nodo c
on 
     a.ID_NEGOCIO = c.ID_NEGOCIO
and a.VERSION_NEGOCIO = c.VERSION_NEGOCIO
and b.ID_NODO = c.ID_NODO
and c.ID_DOMINIO=1
inner join bddlorg.SCA_TDOMINIO_COMBINACION_PAQUETE d
on  
     c.ID_DETALLE_DOMINIO = d.ID_DETALLE_DOMINIO
and  c.VERSION_DETALLE_DOMINIO =d.VERSION_DETALLE_DOMINIO
and  TRIM(a.cve_producto_personalizado)=TRIM(d.PRODUCTO_PERSONALIZADO)


drop table bddldes.insumos_p_personalizado4 ;
create table bddldes.insumos_p_personalizado4 as 
select  a.producto_personalizado,min(a.producto_personalizado_nombre) as producto_personalizado_nombre 
from (
select distinct b.producto_personalizado,b.producto_personalizado_nombre 
from (
select producto_personalizado,min(ts_alta_audit) as ts_alta_audit  
from bddlorg.SCA_TDOMINIO_COMBINACION_PAQUETE 
group by producto_personalizado) a  
inner join bddlorg.SCA_TDOMINIO_COMBINACION_PAQUETE b 
on 
a.producto_personalizado=b.producto_personalizado and 
a.ts_alta_audit=b.ts_alta_audit) a 
group by a.producto_personalizado


--####################################################
--####################################################



drop table if exists ${var:OWNERDES};
create table ${var:OWNERDES} as 
select a.*
,case when cast(b.cve_agrupacion_polizas as int) in (1, 4, 5) then 'Individual' when cast(b.cve_agrupacion_polizas as int) in (2, 3) then 'Flotilla' else 'Flotilla' end as id_poliza
,b.cve_subramo_automovil
,b.desc_subramo_automovil
,coalesce(b.cve_registro,'') as registro_de_nota_tecnica_cnsf
,b.cve_negocio_operable
,neop.desc_negocio_operable
,b.cve_codigo_promocion
,copo.desc_codigo_promocion
,b.cve_prod_configurador as cve_producto_personalizado

,case 
      when PPSICA.producto_personalizado_nombre is not null then  PPSICA.producto_personalizado_nombre 
	  else 
	        case 
			    when PPSICA2.producto_personalizado_nombre is not null then  PPSICA2.producto_personalizado_nombre 
				else  pp.desc_producto_personalizado  
			end
 end as desc_producto_personalizado

--producto_personalizado_nombre
--,pp.desc_producto_personalizado
,b.cve_dep_gubernamental 
,depg.desc_dep_gubernamental
,b.cve_sub_dep_gubernamental
,sdepg.desc_sub_dep_gubernamental
,b.cve_marca_pyme
,mpy.desc_marca_pyme
,b.cve_vrs_tarifa as cve_version_tarifa   ----campo agregado julio 2019
,round(b.pct_valor_descuento_volumen,3) as descuento_por_volumen  ----campo agregado julio 2019

from ${var:OWNERORI} a 
left join bddlalm.aut_elemento_poliza_tp  b
 on a.num_poliza=b.num_poliza and a.num_version=b.num_version
left join ( select id_negocio_operable, negocio_operable as desc_negocio_operable from bddlcru.cdn_negocio_operable group by 
id_negocio_operable, negocio_operable ) neop on trim(neop.id_negocio_operable) = trim(b.cve_negocio_operable)
left join ( select id_negocio_operable, id_codigo_promocion, codigo_promocion_descr as desc_codigo_promocion from 
bddlcru.cdn_codigo_promocion group by id_negocio_operable, id_codigo_promocion, codigo_promocion_descr ) copo on 
trim(copo.id_negocio_operable) = trim(b.cve_negocio_operable) and trim(copo.id_codigo_promocion) = trim(b.cve_codigo_promocion)
left join (select id_producto_personalizado, producto_personalizado as desc_producto_personalizado from 
bddlorg.cdn_producto_personalizado group by id_producto_personalizado, producto_personalizado ) pp on 
trim(pp.id_producto_personalizado) = trim(b.cve_prod_configurador)

----SICA Nuevo Configurador---------------------------------
left join 
bddldes.insumos_p_personalizado3 PPSICA ----------------------------
		on trim(b.num_poliza)=trim(PPSICA.num_poliza) and b.num_version=PPSICA.num_version
		--on trim(b.cve_prod_configurador) = trim(PPSICA.cve_producto_personalizado)
left join 
bddldes.insumos_p_personalizado4 PPSICA2 ----------------------------
		on trim(b.cve_prod_configurador)=trim(PPSICA2.producto_personalizado)
		
---------------------------------------------------------------

left join ( select id_negocio_operable, id_dependencia_negocio, dependencia_negocio as desc_dep_gubernamental from 
bddlcru.cdn_dependencia_negocio group by id_negocio_operable, id_dependencia_negocio, dependencia_negocio ) depg on 
trim(depg.id_negocio_operable) = trim(b.cve_negocio_operable) and trim(depg.id_dependencia_negocio) = trim(b.cve_dep_gubernamental)
left join ( select cve_subdependencia, subdependencia as desc_sub_dep_gubernamental from bddlcru.cdn_subdependencia group by 
cve_subdependencia, subdependencia ) sdepg on trim(sdepg.cve_subdependencia) = trim(b.cve_sub_dep_gubernamental)
left join ( select trim(cdelemen) as cve_marca_pyme, trim(dselemen) as desc_marca_pyme from bddlcru.ktctget where trim(cdtabla) = 'KTPTGOG' AND 
trim(CDIDIOMA) ='ES' group by cdelemen, dselemen ) mpy on trim(b.cve_marca_pyme) = trim(mpy.cve_marca_pyme)