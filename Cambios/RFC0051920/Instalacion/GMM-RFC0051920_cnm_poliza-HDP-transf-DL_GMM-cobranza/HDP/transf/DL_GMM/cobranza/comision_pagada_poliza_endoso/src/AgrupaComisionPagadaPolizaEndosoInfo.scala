package ComisionesPagadas.info

import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import scala.collection.mutable.ListBuffer
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Period}

object AgrupaComisionPagadaPolizaEndosoInfo {


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
            if (!spark.catalog.tableExists("bddldes" + "." + "stg_comision_pagada_poliza_endoso") || tipoCargaParam.equals("CI")) {
				spark.sql(s"""truncate table bddldes.stg_comision_pagada_poliza_endoso""")
				("CI", "000000")
            } else if (dia < 4 && tipoCargaParam.equals("DE")) {
                val añoContable : String = fechaActual.minusMonths(1).getYear.toString
                val mesContable : String = fechaActual.minusMonths(1).getMonthOfYear.toString
                ("DE", añoContable + rPad(mesContable, 2, '0'))
				spark.sql(s"""truncate table bddldes.stg_comision_pagada_poliza_endoso""")
            } else if (tipoCargaParam.equals("RE")) {
                val rangoReproceso : Array[String] = configFile.take(2).last.split(",")
                val formato = DateTimeFormat.forPattern("yyyyMM")
                var añoMeses = new ListBuffer[org.joda.time.DateTime]()
				spark.sql(s"""truncate table bddldes.stg_comision_pagada_poliza_endoso""")
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

	filtro_fecha = (s"""and am_contable in (${am_contable_str}""")
}
else if (tipoCarga.equals("CI")){
  
	filtro_fecha = ""

} 
else if (tipoCarga.equals("RE")){
filtro_fecha = (s"""and am_contable in (${am_contable_str})""")
}

spark.sql(s"""
INSERT INTO bddldes.stg_comision_pagada_poliza_endoso
select 
	am_contable
	,fch_contable
	,num_poliza
	,vigencia_poliza
	,num_version_poliza
	,num_poliza_cobranza
	,num_endoso
	,cve_cobertura_contable
	,des_cobertura_contable
	,num_consecutivo_movimiento_contable
	,cve_sistema
	,cve_sistema_int
	,cve_forma_pago
	,des_forma_pago
	,cve_tipo_movimiento
	,des_tipo_movimiento
	,sum(mto_comision_prima_neta) 				as mto_comision_prima_neta
	,sum(mto_comision_derecho_poliza) 			as mto_comision_derecho_poliza
	,sum(mto_comision_recargo_pago_fraccionado) as mto_comision_recargo_pago_fraccionado
	,sum(mto_isr_comisiones) 					as mto_isr_comisiones
	,sum(mto_importe_comision_total) 			as mto_importe_comision_total
	,cve_unidad_medida
	,des_unidad_medida
	,pct_comision
	,cve_tipo_intermediario
	,des_tipo_intermediario
	,fch_movimiento_comision
	,fch_pago
	,canal_venta_estadistico
	,segmento_estadistico
	,cve_segmento
	,des_segmento
	,cve_negocio_multinacional
	,des_negocio_multinacional
	,cve_tipo_bono
	,des_tipo_bono
	,afecto_plan_incentivo
	,ban_poliza_contributoria
	,pct_contribucion
	,ban_poliza_prestacion
	,cve_motivo_exclusion_plan_incentivo
	,des_motivo_exclusion_plan_incentivo
from bddldes.enr_comisiones_emitidas_pagadas_info
where tipo_comision = 'Pagada'
${filtro_fecha}
group by am_contable,fch_contable,num_poliza,vigencia_poliza,num_version_poliza,num_poliza_cobranza,num_endoso,cve_cobertura_contable,des_cobertura_contable,num_consecutivo_movimiento_contable,cve_sistema,cve_sistema_int,cve_forma_pago,des_forma_pago,cve_tipo_movimiento
	,des_tipo_movimiento,cve_unidad_medida,des_unidad_medida,pct_comision,cve_tipo_intermediario,des_tipo_intermediario,fch_movimiento_comision,fch_pago,canal_venta_estadistico,segmento_estadistico,cve_segmento
	,des_segmento,cve_negocio_multinacional,des_negocio_multinacional,cve_tipo_bono,des_tipo_bono,afecto_plan_incentivo,ban_poliza_contributoria,pct_contribucion,ban_poliza_prestacion,cve_motivo_exclusion_plan_incentivo,des_motivo_exclusion_plan_incentivo

""")

if (tipoCarga.equals("RE")) {

spark.sql(s"""
CREATE TABLE if not exists bddldes.alm_comision_pagada_poliza_endoso_repro as 
select * from bddldes.alm_comision_pagada_poliza_endoso
where cve_sistema_int = 'INFO'
AND  am_contable NOT IN (${am_contable_str})
UNION ALL
select * from bddldes.alm_comision_pagada_poliza_endoso
where cve_sistema_int = 'AZUL' """)

spark.sql(s""" Truncate table bddldes.alm_comision_pagada_poliza_endoso """)

spark.sql(s""" insert into bddldes.alm_comision_pagada_poliza_endoso
			select * from bddldes.alm_comision_pagada_poliza_endoso_repro """)

spark.sql(s"""drop table if exists bddldes.alm_comision_pagada_poliza_endoso_repro""")
			
}
	
	}
}
