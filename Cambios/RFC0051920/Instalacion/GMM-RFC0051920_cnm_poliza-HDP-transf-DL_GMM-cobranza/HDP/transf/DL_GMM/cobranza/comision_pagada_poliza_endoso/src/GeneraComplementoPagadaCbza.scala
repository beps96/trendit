package ComisionesBase.cbza

import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import scala.collection.mutable.ListBuffer
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Period}

object GeneraComplementoPagadaCbza {


  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName("GeneraComplementoPagadaCbza")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

spark.conf.set( "spark.sql.crossJoin.enabled" , "true")

spark.sql(s"""TRUNCATE TABLE bddldes.enr_comision_pagada_poliza_endoso_cbza_base""")

spark.sql(s"""
INSERT INTO bddldes.enr_comision_pagada_poliza_endoso_cbza_base
select
    Cast  (am_contable                                  as INT)          	as am_contable                          
    ,Cast (fch_contable                                 as TIMESTAMP)    	as fch_contable                         
    ,Cast (num_poliza                                   as VARCHAR (14)) 	as num_poliza                           
    ,Cast (vigencia_poliza                              as SMALLINT)     	as vigencia_poliza                      
    ,Cast (num_version_poliza                           as SMALLINT)     	as num_version_poliza                   
    ,Cast (num_poliza_cobranza                          as varchar (8))  	as num_poliza_cobranza                  
    ,Cast (num_endoso                                   as varchar (6))  	as num_endoso                           
    ,Cast (cve_cobertura_contable                       as varchar (3))  	as cve_cobertura_contable               
    ,Cast (des_cobertura_contable                       as varchar (100))	as des_cobertura_contable               
    ,cast (num_consecutivo_movimiento_contable          as smallint )    	as num_consecutivo_movimiento_contable  
    ,cast (cve_sistema                                  as varchar (4))  	as cve_sistema                          
    ,cast (cve_sistema_int                              as varchar (4))  	as cve_sistema_int                      
    ,cast (cve_forma_pago                               as varchar (1))  	as cve_forma_pago                       
    ,cast (des_forma_pago                               as varchar (100))	as des_forma_pago                       
    ,cast (cve_tipo_movimiento 							as varchar (6))  	as cve_tipo_movimiento 					
    ,cast (des_tipo_movimiento 							as varchar (50)) 	as des_tipo_movimiento 					
    ,cast (sum(mto_comision_prima_neta) 				as decimal (16,4)) 	as mto_comision_prima_neta
	,cast (sum(mto_comision_derecho_poliza)             as decimal (16,4)) 	as mto_comision_derecho_poliza
    ,cast (sum(mto_comision_recargo_pago_fraccionado)   as decimal (16,4)) 	as mto_comision_recargo_pago_fraccionado
    ,cast (sum(mto_isr_comisiones)                      as decimal (16,4)) 	as mto_isr_comisiones
    ,cast (sum(mto_importe_comision_total)              as decimal(16,4)) 	as mto_importe_comision_total
    ,cast (cve_unidad_medida                            as varchar (4))   	as cve_unidad_medida      
    ,cast (des_unidad_medida                            as varchar (50))    as des_unidad_medida      
    ,cast (pct_comision                                 as decimal (16,4))  as pct_comision           
    ,cast (cve_tipo_intermediario                       as varchar (3))     as cve_tipo_intermediario 
    ,cast (des_tipo_intermediario                       as varchar (50))    as des_tipo_intermediario 
    ,cast (fch_movimiento_comision                      as timestamp)       as fch_movimiento_comision
	,cast (rem_fecha_pago_rec                           as timestamp)  		as fch_pago    
    ,cast (canal_venta_estadistico                      as varchar (100)) 	as canal_venta_estadistico            
    ,cast (segmento_venta_estadistico                   as varchar (100))   as segmento_venta_estadistico         
    ,cast (nvl(TRIM(EMAZ.SEGMENTO),'')					as varchar (10))    as cve_segmento                       
    ,cast (nvl(TRIM(EMAZ.desc_segmento),'')				as varchar (100))   as des_segmento                       
    ,cast (nvl(TRIM(EMAZ.NEGOCIO),'')            		as varchar (10))    as cve_negocio_multinacional          
    ,cast (nvl(TRIM(EMAZ.desc_negocio),'')            	as varchar (100))   as des_negocio_multinacional          
    ,cast (nvl(TRIM(EMAZ.BONOS),'')						as varchar (5))     as cve_tipo_bono                      
    ,cast (nvl(TRIM(regexp_replace(EMAZ.desc_bonos,'\\t','')),'')			as varchar (100))   as des_tipo_bono                      
    ,cast (nvl(TRIM(regexp_replace(EMAZ.PLAN_INCENTIVOS,'\\t','')),'')		as varchar (100))   as afecto_plan_incentivo              
    ,cast (nvl(TRIM(EMAZ.desc_POL_CONTR),'')            as varchar (2))		as ban_poliza_contributoria           
    ,cast (nvl(EMAZ.CONTRB,'')							as decimal (16,4))	as pct_contribucion                   
    ,cast (nvl(TRIM(EMAZ.desc_pol_prest),'')			as varchar (2))     as ban_poliza_prestacion              
    ,cast (nvl(TRIM(EMAZ.mot_exc),'')  					as varchar (5))		as cve_motivo_exclusion_plan_incentivo
    ,cast (nvl(TRIM(EMAZ.desc_mot_exc),'')  			as varchar (100))	as des_motivo_exclusion_plan_incentivo

FROM
(    
        SELECT
       
               cast(CONCAT(substr(cast(rem_fecha_sistema as string),1,4),substr(cast(rem_fecha_sistema as string),5,2)) as int)  as am_contable,
               IF(rem_fecha_movimiento IS NULL or rem_fecha_movimiento=0, cast('1400-01-01 00:00:00' as timestamp),cast( concat(substr(cast(rem_fecha_movimiento as string),1,4),"-",substr(cast(rem_fecha_movimiento as string),5,2),"-",substr(cast(rem_fecha_movimiento as string),7,2)) as timestamp)) as fch_contable,            
               poliza.num_poliza,            
               cast(-999 as smallint) as vigencia_poliza,
               cast(-999 as smallint) as NUM_VERSION_POLIZA,    
               cast( Concat(Lpad(Cast(rem_oficina AS STRING), 2, '0'),Lpad(Cast(rem_poliza_numero AS STRING), 6, '0')) as varchar(8)) num_poliza_cobranza,            
               cast(Lpad(Cast(rem_numero_endoso AS STRING), 6, '0') as varchar(6))num_endoso,
               poliza.cve_cobertura_contable,
               poliza.des_cobertura_contable,
               cast(-999 as SMALLINT) as num_consecutivo_movimiento_contable,        
               'CBZA' as CVE_SISTEMA,
               'AZUL' as CVE_SISTEMA_INT,            
               poliza.CVE_FORMA_PAGO,
               poliza.DES_FORMA_PAGO,    
               cast(rem_tipo_poliza as varchar(1)) as cve_tipo_movimiento,
               cast(cat_azul.des_atributo as varchar(50)) as des_tipo_movimiento, --pendiente
               cast((rem_imp_mov/100) as decimal(16,4)) as mto_comision_prima_neta,
               cast(0 as decimal(16,4)) as mto_comision_derecho_poliza, --pendiente              
               cast((rem_importe_comis_recargo/100) as decimal(16,4)) as mto_comision_recargo_pago_fraccionado,    
               cast(0 as decimal(16,4)) mto_isr_comisiones,
               cast( ((rem_imp_mov/100) + (rem_importe_comis_recargo/100) ) as decimal(16,4)) as mto_importe_comision_total,
               cast( REM_MONEDA as varchar(4)) as cve_unidad_medida,        
               cast(cat_moneda.DES_HOMOLOGADA as varchar(50))   as des_unidad_medida,
               cast(1 as decimal(16,4) ) as pct_comision,
               cast(cve_tipo_intermediario as varchar(3)) as cve_tipo_intermediario,
               cast(intermediario.des_tipo_intermediario as varchar(50)) as des_tipo_intermediario,
               IF(rem_fecha_movimiento IS NULL or rem_fecha_movimiento=0, cast('1400-01-01 00:00:00' as timestamp),cast( concat(substr(cast(rem_fecha_movimiento as string),1,4),"-",substr(cast(rem_fecha_movimiento as string),5,2),"-",substr(cast(rem_fecha_movimiento as string),7,2)) as timestamp)) as fch_movimiento_comision,
			   rem_fecha_pago_rec,
               poliza.canal_venta_estadistico,
               poliza.segmento_venta_estadistico               
        FROM ( Select *,'1' as cve_tipo_intermediario
               from bddlcru.sdremesas  
             )as rem
        LEFT JOIN (   select *
                    from BDDLALM.CAT_HOMOLOGADOS
                    where id_catalogo = 29
                            and cve_sistema = 'CBZA'
                    )AS CAT on
                  REM_MONEDA = cast(CAT.cve_atributo_org as SMALLINT)
                 
        LEFT join ( select *
                    from bddlapr.cnm_poliza as poliza
                    where cve_sistema_int = 'AZUL'
                   ) as poliza on  poliza.num_poliza_cobranza = Concat(Lpad(Cast(rem_oficina AS STRING), 2, '0'), Lpad(Cast(rem_poliza_numero AS STRING), 6, '0'))  
        left join (
                   Select  CAST(CDELEMEN AS STRING) as ELEMENTO  ,CAST(DSELEMEN AS STRING) as DES_TIPO_INTERMEDIARIO
                   from bddlcru.KTCTGET
                   where trim(CDEMPRES) = '0001'
                        and trim(CDIDIOMA) = 'ES'
                        and trim(CDTABLA) = 'KCIT41G'
                        and trim(CDELEMEN) = '1'    
                   ) as intermediario on trim(intermediario.ELEMENTO) = rem.cve_tipo_intermediario
        LEFT JOIN              
                (
                    SELECT trim(cve_atributo)as cve_atributo ,des_atributo
                    FROM bddlcru.cat_sistema_azul
                    WHERE id_catalogo = 2
                             and trim(nombre_catalogo) = 'CATALOGO_TIPO_MOVIMIENTO_AZUL'
                )as cat_azul on cat_azul.cve_atributo = cast(rem.rem_tipo_poliza as string)
               
         LEFT JOIN (      
                    SELECT CVE_ATRIBUTO_ORG, CVE_HOMOLOGADA, DES_HOMOLOGADA
                     FROM BDDLALM.CAT_HOMOLOGADOS
                     WHERE NOMBRE_CATALOGO = 'CAT_MONEDA'
                               AND CVE_RAMO = 'SA'
                               AND CVE_SISTEMA = 'CBZA'    
                    )as cat_moneda on   cast(rem.REM_MONEDA as string) =  trim(cat_moneda.CVE_ATRIBUTO_ORG )
)as comisiones_pagadas_poliza_endoso
		 LEFT JOIN 
					(
						select POLIZA, cobranza, '' as  SEGMENTO, NVL(SEGMENTO, '') AS desc_segmento, '' as NEGOCIO, NVL(NEGOCIO, '') AS desc_negocio, 
						'' AS BONOS, NVL(BONOS, '') as desc_bonos, NVL(PLAN_INCENTIVOS, '') AS PLAN_INCENTIVOS, '' as POL_CONTR, NVL(POL_CONTR, '') AS desc_POL_CONTR, 
						NVL(CONTRB, '') AS CONTRB, '' as POL_PREST, NVL(POL_PREST, '') AS desc_pol_prest, '' as mot_exc, '' as desc_mot_exc from BDDLCRU.EMAZETIQ 
					) EMAZ 
					on  comisiones_pagadas_poliza_endoso.num_poliza_cobranza =  trim(EMAZ.cobranza) 
					and EMAZ.POLIZA = comisiones_pagadas_poliza_endoso.num_poliza
  
group by am_contable ,fch_contable ,num_poliza ,vigencia_poliza ,num_version_poliza , num_poliza_cobranza ,num_endoso ,cve_cobertura_contable ,
    des_cobertura_contable ,num_consecutivo_movimiento_contable ,cve_sistema ,cve_sistema_int ,cve_forma_pago ,des_forma_pago ,
    cve_tipo_movimiento ,des_tipo_movimiento ,cve_unidad_medida ,des_unidad_medida ,pct_comision ,cve_tipo_intermediario ,
    des_tipo_intermediario ,fch_movimiento_comision ,rem_fecha_pago_rec,canal_venta_estadistico ,segmento_venta_estadistico, 
	EMAZ.SEGMENTO,EMAZ.desc_segmento,EMAZ.NEGOCIO,EMAZ.desc_negocio,EMAZ.BONOS,EMAZ.desc_bonos,EMAZ.PLAN_INCENTIVOS,EMAZ.desc_POL_CONTR,EMAZ.CONTRB,
	EMAZ.desc_pol_prest,EMAZ.mot_exc,EMAZ.desc_mot_exc
""")

spark.sql(s"""
CREATE TABLE if not exists bddldes.alm_comision_pagada_poliza_endoso_repro_cbza as 
select * from bddldes.alm_comision_pagada_poliza_endoso
where cve_sistema_int = 'INFO' """)

spark.sql(s""" Truncate table bddldes.alm_comision_pagada_poliza_endoso """)

spark.sql(s""" insert into bddldes.alm_comision_pagada_poliza_endoso
select * from bddldes.alm_comision_pagada_poliza_endoso_repro_cbza """)

spark.sql(s"""drop table if exists bddldes.alm_comision_pagada_poliza_endoso_repro_cbza""")
	}
}

