package ComisionesBase.cbza

import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import scala.collection.mutable.ListBuffer
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Period}

object GeneraComplementoEmitidaCbza {


  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName("GeneraComplementoEmitidaCbza")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")


spark.conf.set( "spark.sql.crossJoin.enabled" , "true")

//Asigna parametros de extraccion
val fechaActual : org.joda.time.DateTime = new DateTime()
val año : Int = fechaActual.getYear
val mes : Int = fechaActual.getMonthOfYear
val dia : Int = fechaActual.getDayOfMonth
val configFilePath : String = "/transf/DL_GMM/cobranza/comision_emitida_poliza_endoso/config/paramComisionEmitidaPolEnd.ini"
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
            if (!spark.catalog.tableExists("bddldes" + "." + "stg_comision_emitida_poliza_endoso_cbza") || tipoCargaParam.equals("CI")) {
				spark.sql(s"""truncate table bddldes.stg_comision_emitida_poliza_endoso_cbza""")
				("CI", "000000")
            } else if (dia < 4 && tipoCargaParam.equals("DE")) {
                val añoContable : String = fechaActual.minusMonths(1).getYear.toString
                val mesContable : String = fechaActual.minusMonths(1).getMonthOfYear.toString
                ("DE", añoContable + rPad(mesContable, 2, '0'))
				spark.sql(s"""truncate table bddldes.stg_comision_emitida_poliza_endoso_cbza""")
            } else if (tipoCargaParam.equals("RE")) {
                val rangoReproceso : Array[String] = configFile.take(2).last.split(",")
                val formato = DateTimeFormat.forPattern("yyyyMM")
                var añoMeses = new ListBuffer[org.joda.time.DateTime]()
				spark.sql(s"""truncate table bddldes.stg_comision_emitida_poliza_endoso_cbza""")
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

	filtro_fecha = (s""" AND cast(substring(cast(fch_contable as String),1,6) as int) in (${am_contable_str})""")
}
else if (tipoCarga.equals("CI")){

	filtro_fecha = ""

} 
else if (tipoCarga.equals("RE")){
filtro_fecha = (s""" AND cast(substring(cast(fch_contable as String),1,6) as int) in (${am_contable_str})""")
}




spark.sql(s""" CACHE TABLE  cat_homo
SELECT cve_atributo_org,des_atributo_org
FROM   bddlalm.cat_homologados
WHERE  id_catalogo = 29
AND    cve_sistema = 'CBZA'
""") 

spark.sql(s""" CACHE TABLE EMAZ
SELECT TRIM(poliza) AS poliza,TRIM(cobranza) AS cobranza,'' AS segmento,nvl(segmento, '') AS desc_segmento,'' AS negocio,
 nvl(negocio, '') AS desc_negocio, '' AS bonos, nvl(bonos, '') AS desc_bonos,
 nvl(plan_incentivos, '') AS plan_incentivos, '' AS pol_contr, nvl(pol_contr, '') AS desc_pol_contr,
 nvl(contrb, '') AS contrb, '' AS pol_prest, nvl(pol_prest, '') AS desc_pol_prest,
 '' AS mot_exc,'' AS desc_mot_exc
FROM   bddlcru.emazetiq 
""")

spark.sql(s"""CACHE TABLE ktctget
SELECT trim(cdelemen) AS cdelemen,trim(dselemen) AS des_tipo_intermediario
FROM   bddlcru.ktctget
WHERE  trim(cdempres) = '0001'
	AND    trim(cdidioma) = 'ES'
	AND    trim(cdtabla) = 'KCIT41G'
""")


spark.sql(s"""
INSERT INTO bddldes.stg_comision_emitida_poliza_endoso_cbza 
SELECT    Cast (base.am_contable 							AS	INT)			AS am_contable ,
          Cast (base.fch_contable 							AS	TIMESTAMP)		AS fch_contable ,
          Cast (base.num_poliza 							AS	VARCHAR (14)) 	AS num_poliza ,
          Cast (-999 										AS	SMALLINT)      	AS vigencia_poliza ,
          Cast (-999 										AS	SMALLINT)      	AS num_version_poliza ,
          Cast (base.num_poliza_cobranza 					AS	VARCHAR (8))   	AS num_poliza_cobranza ,
          Cast (base.num_endoso 							AS	VARCHAR (6))   	AS num_endoso ,
          Cast (base.cve_cobertura_contable 				AS	VARCHAR (3))   	AS cve_cobertura_contable ,
          Cast (base.des_cobertura_contable 				AS	VARCHAR (100)) 	AS des_cobertura_contable ,
          cast (0                              				AS	smallint )     	AS num_consecutivo_movimiento_contable ,
          cast ('CBZA' 										AS	varchar (4))   	AS cve_sistema ,
          cast ('AZUL' 										AS	varchar (4))   	AS cve_sistema_int ,
          cast (base.cve_forma_pago 						AS	varchar (1))   	AS cve_forma_pago ,
          cast (base.des_forma_pago 						AS	varchar (100)) 	AS des_forma_pago ,
          cast (base.cve_tipo_movimiento 					AS	varchar (6))   	AS cve_tipo_movimiento ,
          cast (CASE WHEN cve_tipo_movimiento='A' THEN 'Aumento' ELSE 'Disminución'END AS varchar (50))   AS des_tipo_movimiento ,
          cast (base.mto_comision_prima_neta 				AS	decimal (16,4)) AS mto_comision_prima_neta ,
          cast (base.mto_comision_derecho_poliza 			AS	decimal (16,4)) AS mto_comision_derecho_poliza ,
          cast (base.mto_comision_recargo_pago_fraccionado 	AS 	decimal (16,4)) AS mto_comision_recargo_pago_fraccionado ,
          cast (base.mto_isr_comisiones 					AS	decimal (16,4)) AS mto_isr_comisiones ,
          cast (base.mto_importe_comision_total 			AS	decimal(16,4))  AS mto_importe_comision_total ,
          cast (base.cve_unidad_medida 						AS	varchar (4))    AS cve_unidad_medida ,
          cast (cat_homo.des_atributo_org  					AS	varchar (50))  	AS des_unidad_medida ,
          cast (base.pct_comision 							AS	decimal (16,4)) AS pct_comision ,
          cast (base.cve_tipo_inter 						AS	varchar (3))   	AS cve_tipo_intermediario,
          cast (ktctget.des_tipo_intermediario 				AS	varchar (50))   AS des_tipo_intermediario  ,
          cast (base.fch_contable 							AS	timestamp)		AS fch_movimiento_comision ,
          cast (base.fch_emision 							AS	timestamp)		AS fch_emision ,
          cast ('PENDIENTE' 								AS	varchar (100))  AS canal_venta_estadistico ,
          cast ('PENDIENTE' 								AS	varchar (100))  AS segmento_estadistico, 
          cast (nvl(TRIM(emaz.segmento),'') 				AS	varchar (10))   AS cve_segmento ,
          cast (nvl(TRIM(emaz.desc_segmento),'') 			AS	varchar (100))  AS des_segmento ,
          cast (nvl(TRIM(emaz.negocio),'') 					AS	varchar (10))   AS cve_negocio_multinacional ,
          cast (nvl(TRIM(emaz.desc_negocio),'') 			AS	varchar (100))  AS des_negocio_multinacional ,
          cast (nvl(TRIM(emaz.bonos),'') 					AS	varchar (5))    AS cve_tipo_bono ,
          cast (nvl(TRIM(regexp_replace(EMAZ.desc_bonos,'\\t','')),'')       	AS varchar (100))                                 AS des_tipo_bono ,
          cast (nvl(TRIM(regexp_replace(EMAZ.PLAN_INCENTIVOS,'\\t','')),'')  	AS varchar (100))                                 AS afecto_plan_incentivo ,
          cast (nvl(TRIM(emaz.desc_pol_contr),'') 			AS	varchar (2))	AS ban_poliza_contributoria ,
          cast (nvl(emaz.contrb,'') 						AS	decimal (16,4))	AS pct_contribucion ,
          cast (nvl(TRIM(emaz.desc_pol_prest),'') 			AS	varchar (2))	AS ban_poliza_prestacion ,
          cast (nvl(TRIM(emaz.mot_exc),'') 					AS	varchar (5))	AS cve_motivo_exclusion_plan_incentivo ,
          cast (nvl(TRIM(emaz.desc_mot_exc),'') 			AS	varchar (100))	AS des_motivo_exclusion_plan_incentivo
FROM      (

SELECT    
		case when from_unixtime(unix_timestamp(cast(fch_contable as VARCHAR(8)), 'yyyyMMdd')) is null then 140001 else cast(substring(cast(fch_contable as String),1,6) as int) end  as AM_CONTABLE,		
		IF(fch_contable IS NULL or fch_contable=0, cast('1400-01-01 00:00:00' as timestamp),cast( concat(substr(cast(fch_contable as string),1,4),"-",substr(cast(fch_contable as string),5,2),"-",substr(cast(fch_contable as string),7,2)) as timestamp)) as fch_contable,
		CNM_POLIZA.num_poliza,
		ENR.num_poliza_cobranza ,
		num_endoso ,
		CNM_POLIZA.cve_cobertura_contable,
        CNM_POLIZA.des_cobertura_contable,
		CNM_POLIZA.cve_forma_pago ,
		CNM_POLIZA.des_forma_pago,
		cve_tipo_endoso AS cve_tipo_movimiento ,
		NVL(mto_comision_prima_neta,0) as mto_comision_prima_neta ,
		NVL(mto_comision_derecho_poliza,0) as mto_comision_derecho_poliza ,
		NVL(mto_comision_recargo_pago_fraccionado,0) as mto_comision_recargo_pago_fraccionado ,
		NVL(mto_isr_comisiones,0) as mto_isr_comisiones ,
		NVL(mto_importe_comision_total,0) as mto_importe_comision_total ,
		ENR.cve_unidad_medida ,
		pct_comision ,
		cast('1' AS varchar(3)) AS cve_tipo_inter,
		CNM_POLIZA.fch_emision
FROM      bddlenr.enr_cobranza_emitida_pol_end_base ENR
LEFT JOIN  bddlapr.cnm_poliza CNM_POLIZA
ON CNM_POLIZA.num_poliza_cobranza = ENR.num_poliza_cobranza
WHERE CNM_POLIZA.cve_sistema_int = 'AZUL'
	${filtro_fecha}
			) base
LEFT JOIN
          EMAZ 		 ON base.num_poliza_cobranza = emaz.cobranza AND base.num_poliza = EMAZ.POLIZA
LEFT JOIN
		   cat_homo  ON base.cve_unidad_medida = cat_homo.cve_atributo_org 	
LEFT JOIN
			ktctget ON ktctget.cdelemen = base.cve_tipo_inter
""")

if (tipoCarga.equals("RE")) {
spark.sql(s"""
CREATE TABLE if not exists bddldes.alm_comision_emitida_poliza_endoso_repro_cbza as 
select * from bddldes.alm_comision_emitida_poliza_endoso
where cve_sistema_int = 'AZUL'
AND  am_contable NOT IN (${am_contable_str})
UNION ALL
select * from bddldes.alm_comision_emitida_poliza_endoso
where cve_sistema_int = 'INFO'
""")

spark.sql(s""" Truncate table bddldes.alm_comision_emitida_poliza_endoso """)

spark.sql(s""" insert into bddldes.alm_comision_emitida_poliza_endoso
select * from bddldes.alm_comision_emitida_poliza_endoso_repro_cbza """)

spark.sql(s"""drop table if exists ebddldes.alm_comision_emitida_poliza_endoso_repro_cbza""")
}
	}
}
