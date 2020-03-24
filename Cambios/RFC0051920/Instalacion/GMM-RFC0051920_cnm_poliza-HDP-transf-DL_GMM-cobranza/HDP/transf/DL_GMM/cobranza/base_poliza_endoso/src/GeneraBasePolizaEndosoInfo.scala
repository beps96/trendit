package ComisionesBase.info

import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import scala.collection.mutable.ListBuffer
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Period}

object GeneraBasePolizaEndosoInfo {


  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName("GeneraBasePolizaEndosoInfo")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")



//Asigna parametros de extraccion
val fechaActual : org.joda.time.DateTime = new DateTime()
val año : Int = fechaActual.getYear
val mes : Int = fechaActual.getMonthOfYear
val dia : Int = fechaActual.getDayOfMonth
val configFilePath : String = "/transf/DL_GMM/cobranza/base_poliza_endoso/config/paramEnrEmitidaPagadaPolizaEndoso.ini"
val configFile : org.apache.spark.rdd.RDD[String] = spark.sparkContext.textFile(configFilePath)
var tipoCargaParam : String = configFile.take(1).last

//Relleno
        def rPad(str : String, paddedLength : Int, ch : Char) : String = {
            var remLength = paddedLength - str.length
            if (remLength <= 0) {
                return str;
            }
            val builder = StringBuilder.newBuilder
            for( a <- 0 until remLength) {
                builder.append(ch)
            }
            builder.append(str);
        return builder.toString();
        }

//Rango de fechas
        def fechaRango(inicio : DateTime, fin : DateTime, salto : Period) : Iterator[DateTime] = {
            Iterator.iterate(inicio)(_.plus(salto)).takeWhile(!_.isAfter(fin))}
		
//Calcula tipo de carga
        var (tipoCarga : String, am_contable : String) = 
            if (!spark.catalog.tableExists("bddldes" + "." + "enr_comisiones_emitidas_pagadas_info") || tipoCargaParam.equals("CI")) {
				spark.sql(s"""truncate table bddldes.enr_comisiones_emitidas_pagadas_info""") 
                ("CI", "000000")
            } else if (dia < 4 && tipoCargaParam.equals("DE")) {
                val añoContable : String = fechaActual.minusMonths(1).getYear.toString
                val mesContable : String = fechaActual.minusMonths(1).getMonthOfYear.toString
                ("DE", añoContable + rPad(mesContable, 2, '0'))
				spark.sql(s"""truncate table bddldes.enr_comisiones_emitidas_pagadas_info""")
            } else if (tipoCargaParam.equals("RE")) {
                val rangoReproceso : Array[String] = configFile.take(5).last.split(",")
                val formato = DateTimeFormat.forPattern("yyyyMM")
                var añoMeses = new ListBuffer[org.joda.time.DateTime]()
		spark.sql(s"""truncate table bddldes.enr_comisiones_emitidas_pagadas_info_repro""")
                //val am_contable : Array[String] = 
                rangoReproceso.foreach {x => 
                        if(x.indexOfSlice("-") > 0 && x.length().equals(21)) {
                            fechaRango(formato.parseDateTime(x.split("-")(0).substring(4)), formato.parseDateTime(x.split("-")(1).substring(4)), new Period().withMonths(1))
                                .foreach(y => {añoMeses += y})
                        } else if(x.indexOfSlice("-") > 0 && x.length().equals(17)) {
                            fechaRango(formato.parseDateTime(x.split("-")(0).substring(4) + "01"), formato.parseDateTime(x.split("-")(1).substring(4) + "01"), new Period().withMonths(1))
                                .foreach(y => {añoMeses += y})
                        } else {
                            añoMeses += formato.parseDateTime(x.split("-")(0).substring(4))
                        }
                    }
                var am_contables = new ListBuffer[String]()
                añoMeses.foreach {am => 
                    am_contables += "'" + am.getYearOfEra.toString + rPad(am.getMonthOfYear.toString, 2, '0') + "'"
                }
                ("RE", am_contables.mkString.replace("''", "','"))
            } else {
                val añoContable : String = año.toString
                val mesContable : String = mes.toString
                ("DE", añoContable + rPad(mesContable, 2, '0'))
            }


val am_contable_str = am_contable.replace("'", "")
	

// filtro de fecha
var filtro_fecha = ""

if (tipoCarga.equals("DE")) { 

	filtro_fecha = (s"""and and cast(substring(cast(FECONTAB as String),1,6) as int) in (${am_contable_str}""")
}
else if (tipoCarga.equals("CI")){
  
	filtro_fecha = ""

} 
else if (tipoCarga.equals("RE")){
filtro_fecha = (s"""and cast(substring(cast(FECONTAB as String),1,6) as int) in (${am_contable_str})""")

spark.sql(s"""
		INSERT INTO bddldes.enr_comisiones_emitidas_pagadas_info_repro
		select * from bddldes.enr_comisiones_emitidas_pagadas_info
				where am_contable not in (${am_contable_str})				
""")

spark.sql(s"""truncate table bddldes.enr_comisiones_emitidas_pagadas_info""")

}
 

spark.sql(s"""CACHE TABLE CNM_POLIZA
 SELECT num_poliza,vigencia_poliza,num_version_poliza,cve_cobertura_contable,cve_forma_pago,des_forma_pago,cve_segmento,des_segmento,cve_negocio,cve_negocio_multinacional,des_negocio_multinacional,
	cve_tipo_bono,des_tipo_bono,afecto_plan_incentivo,ban_poliza_contributoria,pct_contribucion,ban_poliza_prestacion,
	cve_motivo_exclusion_plan_incentivo,des_motivo_exclusion_plan_incentivo,fch_emision,cve_sistema,
	substr(trim(des_tipo_movimiento),1,case when length(trim(des_tipo_movimiento))  <= 0 then 0 else length(trim(des_tipo_movimiento))-3 end) as des_tipo_mov
FROM BDDLAPR.CNM_POLIZA WHERE cve_sistema_int = 'INFO'	
""")

spark.sql(s""" CACHE TABLE DES_TIPO_MOVIMIENTO
	SELECT cdelemen,trim(dselemen) as dselemen
	FROM bddlcru.KTCTGET WHERE cdempres = '0001' and cdidioma = 'ES' AND TRIM(CDTABLA) in ('KTOT23S')
""")
  
spark.sql(s"""CACHE TABLE CAT_HOMO
	SELECT cve_homologada,des_homologada
	FROM BDDLALM.CAT_HOMOLOGADOS WHERE cve_ramo = 'SA'
	and nombre_catalogo = 'CAT_MONEDA' and cve_sistema = 'INFO'
""") 

spark.sql(s"""CACHE TABLE DES_INTERMEDIARIO
	SELECT cdelemen,trim(dselemen) as dselemen
	FROM bddlcru.KTCTGET WHERE cdempres = '0001'
	and cdidioma = 'ES' AND TRIM(CDTABLA) in ('KCIT41G')
""")

spark.sql(s""" CACHE TABLE COBERTURA_CONTABLE
select cve_subsubcuenta,subsubcuenta from bddlcru.con_cat_subsubcuenta 
where cve_unidad_negocio = 'G'
""")

spark.sql(s"""
INSERT INTO bddldes.enr_comisiones_emitidas_pagadas_info 
SELECT 
	 cast (base.AM_CONTABLE 										AS INT)				AS AM_CONTABLE
	,cast (base.fch_contable  										AS TIMESTAMP)		AS fch_contable
	,cast (base.NUM_POLIZA  										AS VARCHAR (14))	AS NUM_POLIZA
	,cast (nvl(CNM_POLIZA.vigencia_poliza,-999)						AS SMALLINT)		AS vigencia_poliza
	,cast (nvl(CNM_POLIZA.NUM_VERSION_POLIZA,-999) 					AS SMALLINT)		AS NUM_VERSION_POLIZA
	,cast (''  							                    		AS VARCHAR (8))		AS num_poliza_cobranza
	,cast (''              											AS VARCHAR (6))		AS num_endoso
	,cast (base.cve_cobertura_contable 								AS VARCHAR (3))		AS cve_cobertura_contable
	,cast (nvl(COBERTURA_CONTABLE.subsubcuenta,concat('Descripción no encontrada para: ',base.cve_cobertura_contable)) AS VARCHAR (100)) AS DES_COBERTURA_CONTABLE
	,cast (num_consecutivo_movimiento_contable						AS SMALLINT	)		AS num_consecutivo_movimiento_contable
	,cast (CNM_POLIZA.cve_sistema 									AS VARCHAR (4))		AS CVE_SISTEMA
	,cast ('INFO'                     								AS VARCHAR (4))		AS CVE_SISTEMA_INT
	,cast (CNM_POLIZA.cve_forma_pago								AS VARCHAR (1))		AS cve_forma_pago
	,cast (CNM_POLIZA.des_forma_pago 								AS VARCHAR (100))	AS des_forma_pago
	,cast (base.cve_tipo_movimiento 								AS VARCHAR (6))		AS cve_tipo_movimiento
	,trim(cast (nvl(concat(DES_TIPO_MOVIMIENTO.dselemen,'-',CNM_POLIZA.des_tipo_mov),concat('Descripción no encontrada para: ',base.cve_tipo_movimiento)) AS VARCHAR (50))) 	AS des_tipo_movimiento
	,cast (base.mto_comision_prima_neta 							AS DECIMAL (16,4))	AS mto_comision_prima_neta
	,cast (base.mto_comision_derecho_poliza							AS DECIMAL (16,4))	AS mto_comision_derecho_poliza
	,cast (base.mto_comision_recargo_pago_fraccionado				AS DECIMAL (16,4))	AS mto_comision_recargo_pago_fraccionado
	,cast (base.mto_isr_comisiones									AS DECIMAL (16,4))	AS mto_isr_comisiones
	,cast ((base.mto_comision_prima_neta + base.mto_comision_recargo_pago_fraccionado + base.mto_comision_derecho_poliza + base.mto_isr_comisiones) AS DECIMAL(16,4)) AS mto_importe_comision_total 
	,cast (base.cve_unidad_medida 									AS VARCHAR (4))		AS cve_unidad_medida
	,cast (NVL(CAT_HOMO.des_homologada, concat('Descripción no encontrada para: ',base.cve_unidad_medida)) 	AS VARCHAR (100)) 		AS des_unidad_medida
	,cast (0														AS DECIMAL (16,4))	AS pct_comision
	,cast (base.Tipo_comision  										AS VARCHAR (15))	AS Tipo_comision
	,cast (base.cve_tipo_intermediario 								AS VARCHAR (3))		AS cve_tipo_intermediario
	,cast (nvl(DES_INTERMEDIARIO.dselemen,concat('Descripción no encontrada para: ',base.cve_tipo_intermediario)) AS VARCHAR (50))	AS des_tipo_intermediario
	,cast (base.fch_movimiento_comision 							AS TIMESTAMP)		AS fch_movimiento_comision  
	,cast (base.fch_pago				 							AS TIMESTAMP)		AS fch_pago
	,cast (CNM_POLIZA.fch_emision									AS TIMESTAMP)		AS fch_emision
	,cast ('PENDIENTE' 												AS VARCHAR (100))	AS canal_venta_estadistico
	,cast ('PENDIENTE' 												AS VARCHAR (100))	AS Segmento_estadistico
	,cast (NVL(CNM_POLIZA.cve_segmento,'') 							AS VARCHAR (10))	AS cve_segmento
	,cast (NVL(CNM_POLIZA.des_segmento,'')							AS VARCHAR (100))	AS des_segmento
	,cast (NVL(CNM_POLIZA.cve_negocio_multinacional,'')  			AS VARCHAR (10))	AS cve_negocio_multinacional
	,cast (NVL(CNM_POLIZA.des_negocio_multinacional,'')				AS VARCHAR (100)) 	AS des_negocio_multinacional 
	,cast (NVL(CNM_POLIZA.cve_tipo_bono,'')	 						AS VARCHAR (5))		AS cve_tipo_bono
	,cast (NVL(CNM_POLIZA.des_tipo_bono,'')							AS VARCHAR (100))	AS des_tipo_bono
	,cast (NVL(CNM_POLIZA.afecto_plan_incentivo,'')					AS VARCHAR (100))	AS afecto_plan_incentivo
	,cast (NVL(CNM_POLIZA.ban_poliza_contributoria,'')  			AS VARCHAR (2))		AS ban_poliza_contributoria
	,cast (NVL(CNM_POLIZA.pct_contribucion,0)						AS DECIMAL (16,4))	AS pct_contribucion
	,cast (NVL(CNM_POLIZA.ban_poliza_prestacion,'')					AS VARCHAR (2))		AS ban_poliza_prestacion
	,cast (NVL(CNM_POLIZA.cve_motivo_exclusion_plan_incentivo,'')	AS VARCHAR (5))		AS cve_motivo_exclusion_plan_incentivo
	,cast (NVL(CNM_POLIZA.des_motivo_exclusion_plan_incentivo,'')	AS VARCHAR (100))	AS des_motivo_exclusion_plan_incentivo
	,cast (ID_SECUENCIA_ASEGURADO 									AS SMALLINT  ) 		AS ID_SECUENCIA_ASEGURADO
	,cast (CVE_COBERTURA_CONTRATADA 								AS VARCHAR(10)   )  AS CVE_COBERTURA_CONTRATADA
from (
select 
	tcreint   AS cve_tipo_intermediario
	,cdnumpol AS NUM_POLIZA
	,ctvrspol AS NUM_VERSION_POLIZA
	,case when from_unixtime(unix_timestamp(cast(FECONTAB as VARCHAR(8)), 'yyyyMMdd')) is null then cast ('1400-01-01' as timestamp) else cast(concat(substring(cast(FECONTAB as String),1,4),  "-",  substring(cast(FECONTAB as String),5,2),"-",  substring(cast(FECONTAB as String),7,2)) as timestamp) end  AS fch_contable
	,case when from_unixtime(unix_timestamp(cast(FECONTAB as VARCHAR(8)), 'yyyyMMdd')) is null then 140001 else cast(substring(cast(FECONTAB as String),1,6) as int)end  as AM_CONTABLE
	,cast(substr(trim(cdramdgs),2,4) as varchar (3)) AS cve_cobertura_contable
	,NUSECMVO 						AS num_consecutivo_movimiento_contable
	,tctitxct
	,ctsecobj  AS ID_SECUENCIA_ASEGURADO
	,tccopera AS cve_operacion
	,CDMCT AS CVE_COBERTURA_CONTRATADA
	,TCCDMONE as cve_unidad_medida					
	,concat(trim(tctitxct),trim(tccopera)) AS cve_tipo_movimiento
	,concat(trim(tcdomori),trim(tccopera)) AS DES_MOV
	,0 as pct_comision
	,case when cast(concat(substring(cast(fecmvto as String),1,4),  "-",  substring(cast(fecmvto as String),5,2),"-",  substring(cast(fecmvto as String),7,2)) as timestamp) is null then cast ('1400-01-01' as timestamp) else cast(concat(substring(cast(fecmvto as String),1,4),  "-",  substring(cast(fecmvto as String),5,2),"-",  substring(cast(fecmvto as String),7,2)) as timestamp) end  AS fch_movimiento_comision
	,case when cast(concat(substring(cast(FECTRAB as String),1,4),  "-",  substring(cast(FECTRAB as String),5,2),"-",  substring(cast(FECTRAB as String),7,2)) as timestamp) is null then cast ('1400-01-01' as timestamp) else cast(concat(substring(cast(FECTRAB as String),1,4),  "-",  substring(cast(FECTRAB as String),5,2),"-",  substring(cast(FECTRAB as String),7,2)) as timestamp) end  AS fch_pago
	,case when  (tccopera = '001' or tccopera = '003') then 'Emitida' else 'Pagada' end Tipo_comision
	,case when  tctitxct = '001'  and  (tccopera ='001' or tccopera ='004') then SUM(impcc701 + impcc702 + impcc707 + impcc708)   else 
	case when  tctitxct = '003'  and  (tccopera ='003' or tccopera ='005') then SUM(impcc701 + impcc702 + impcc707 + impcc708)   else SUM(impcc701 + impcc702 +impcc707 + impcc708)*-1 end end  as mto_comision_prima_neta					
	,case when  tctitxct = '001'  and  (tccopera ='001' or tccopera ='004') then SUM(impcc703 + impcc704 + impcc717 + impcc718)   else 
		case when  tctitxct = '003'  and  (tccopera ='003' or tccopera ='005') then SUM(impcc703 + impcc704 + impcc717 + impcc718)   else SUM(impcc703 + impcc704 + impcc717+ impcc718)*-1 end end  as mto_comision_recargo_pago_fraccionado					
	,case when  tctitxct = '001'  and  (tccopera ='001' or tccopera ='004') then SUM(impcc705 + impcc706)   else 
		case when  tctitxct = '003'  and  (tccopera ='003' or tccopera ='005') then SUM(impcc705 + impcc706)   else SUM(impcc705 + impcc706)*-1 end end  as mto_comision_derecho_poliza
	,case when  tctitxct = '001'  and  (tccopera ='001' or tccopera ='004') then SUM(impcc712 + impcc715)   else 
	case when  tctitxct = '003'  and  (tccopera ='003' or tccopera ='005') then SUM(impcc712 + impcc713 + impcc714 + impcc715)   else SUM(impcc712 + impcc713 + impcc714 + impcc715)*-1 end end  as mto_isr_comisiones
from bddlcru.kton41t
where cdciagru ='SA'  
	and tcdomori = '01'
	----and tcreint not in ('A','9','4')
	and tctitxct in ('001','003')
	and tccopera in ('001','003','004','005')
	${filtro_fecha}
group by tcreint,cdnumpol,ctvrspol,fecontab,cdramdgs,NUSECMVO,tctitxct,ctsecobj,tcdomori,tccopera,CDMCT,TCCDMONE,fecmvto,FECTRAB	
union all
select 
	tcreint   AS cve_tipo_intermediario
	,cdnumpol AS NUM_POLIZA
	,ctvrspol AS NUM_VERSION_POLIZA
	,case when from_unixtime(unix_timestamp(cast(FECONTAB as VARCHAR(8)), 'yyyyMMdd')) is null then cast ('1400-01-01' as timestamp) else cast(concat(substring(cast(FECONTAB as String),1,4),  "-",  substring(cast(FECONTAB as String),5,2),"-",  substring(cast(FECONTAB as String),7,2)) as timestamp) end  AS fch_contable
	,case when from_unixtime(unix_timestamp(cast(FECONTAB as VARCHAR(8)), 'yyyyMMdd')) is null then 140001 else cast(substring(cast(FECONTAB as String),1,6) as int)end  as AM_CONTABLE
	,cast(substr(trim(cdramdgs),2,4) as varchar (3)) AS cve_cobertura_contable
	,NUSECMVO 						AS num_consecutivo_movimiento_contable
	,tctitxct
	,ctsecobj AS ID_SECUENCIA_ASEGURADO
	,tccopera AS cve_operacion
	,CDMCT AS CVE_COBERTURA_CONTRATADA
	,TCCDMONE as cve_unidad_medida					
	,concat(trim(tctitxct),trim(tccopera)) AS cve_tipo_movimiento
	,concat(trim(tcdomori),trim(tccopera)) AS DES_MOV
	,0 as pct_comision
	,case when cast(concat(substring(cast(fecmvto as String),1,4),  "-",  substring(cast(fecmvto as String),5,2),"-",  substring(cast(fecmvto as String),7,2)) as timestamp) is null then cast ('1400-01-01' as timestamp) else cast(concat(substring(cast(fecmvto as String),1,4),  "-",  substring(cast(fecmvto as String),5,2),"-",  substring(cast(fecmvto as String),7,2)) as timestamp) end  AS fch_movimiento_comision
	,case when cast(concat(substring(cast(FECTRAB as String),1,4),  "-",  substring(cast(FECTRAB as String),5,2),"-",  substring(cast(FECTRAB as String),7,2)) as timestamp) is null then cast ('1400-01-01' as timestamp) else cast(concat(substring(cast(FECTRAB as String),1,4),  "-",  substring(cast(FECTRAB as String),5,2),"-",  substring(cast(FECTRAB as String),7,2)) as timestamp) end  AS fch_pago
	,case when  (tccopera = '001' or tccopera = '003') then 'Emitida' else 'Pagada' end Tipo_comision
	,case when  tctitxct = '001'  and  (tccopera ='001' or tccopera ='004') then SUM(impcc701 + impcc702 + impcc707 + impcc708)   else 
		case when  tctitxct = '003'  and  (tccopera ='003' or tccopera ='005') then SUM(impcc701 + impcc702 + impcc707 + impcc708)   else SUM(impcc701 + impcc702 +impcc707 + impcc708)*-1 end end  as mto_comision_prima_neta					
	,case when  tctitxct = '001'  and  (tccopera ='001' or tccopera ='004') then SUM(impcc703 + impcc704 + impcc717 + impcc718)   else 
		case when  tctitxct = '003'  and  (tccopera ='003' or tccopera ='005') then SUM(impcc703 + impcc704 + impcc717 + impcc718)   else SUM(impcc703 + impcc704 + impcc717+ impcc718)*-1 end end  as mto_comision_recargo_pago_fraccionado					
	,case when  tctitxct = '001'  and  (tccopera ='001' or tccopera ='004') then SUM(impcc705 + impcc706)   else 
		case when  tctitxct = '003'  and  (tccopera ='003' or tccopera ='005') then SUM(impcc705 + impcc706)   else SUM(impcc705 + impcc706)*-1 end end  as mto_comision_derecho_poliza
	,case when  tctitxct = '001'  and  (tccopera ='001' or tccopera ='004') then SUM(impcc712 + impcc715)   else 
		case when  tctitxct = '003'  and  (tccopera ='003' or tccopera ='005') then SUM(impcc712 + impcc713 + impcc714 + impcc715)   else SUM(impcc712 + impcc713 + impcc714 + impcc715)*-1 end end  as mto_isr_comisiones
from bddlcru.kton41h
where cdciagru ='SA'  
	and tcdomori = '01'
	-----and tcreint not in ('A','9','4')
	and tctitxct in ('001','003')
	and tccopera in ('001','003','004','005')
	${filtro_fecha}
group by tcreint,cdnumpol,ctvrspol,fecontab,cdramdgs,NUSECMVO,tctitxct,ctsecobj,tcdomori,tccopera,CDMCT,TCCDMONE,fecmvto,FECTRAB
	) base
LEFT JOIN
        COBERTURA_CONTABLE  ON trim(base.cve_cobertura_contable) = trim(COBERTURA_CONTABLE.cve_subsubcuenta)
LEFT JOIN
		CNM_POLIZA     ON base.NUM_POLIZA = CNM_POLIZA.num_poliza and base.NUM_VERSION_POLIZA = CNM_POLIZA.num_version_poliza
LEFT JOIN
        DES_TIPO_MOVIMIENTO ON base.DES_MOV = trim(DES_TIPO_MOVIMIENTO.cdelemen)        
LEFT JOIN
		CAT_HOMO   ON CAT_HOMO.cve_homologada = base.cve_unidad_medida
LEFT JOIN
        DES_INTERMEDIARIO	   ON trim(base.cve_tipo_intermediario) = trim(DES_INTERMEDIARIO.cdelemen)     
""")

if (tipoCarga.equals("RE")) {
spark.sql(s"""
		INSERT INTO bddldes.enr_comisiones_emitidas_pagadas_info
	    SELECT * FROM bddldes.enr_comisiones_emitidas_pagadas_info_repro
""")
}
	}
}
