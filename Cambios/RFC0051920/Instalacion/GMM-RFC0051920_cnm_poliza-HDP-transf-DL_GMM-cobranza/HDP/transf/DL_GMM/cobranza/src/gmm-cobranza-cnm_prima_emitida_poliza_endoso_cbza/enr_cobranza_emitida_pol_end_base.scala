import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.sql.{Connection, DriverManager}
import org.joda.time.DateTime
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lit, col, coalesce}
import org.apache.spark.sql.expressions.Window
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.text.Normalizer.{normalize => jnormalize, _}

object enr_cobranza_emitida_pol_end_base {
    def main(args: Array[String]){
        //Spark session
        val spark = 
            SparkSession
                .builder()
                .appName("GMM_DLK_Cobranza_Cobranza_Emitida_Poliza_Endoso_Base")
                .enableHiveSupport()
                .getOrCreate

        spark.conf.set( "spark.sql.crossJoin.enabled" , "true")

        //Parametros
        //val configFilePath : String = "/transf/DL_GMM/cobranza/base_poliza_endoso/config/paramEnrCobranzaEmitidaPolEndBase.ini"
        val configFilePath : String = args(0)

        //Lectura de parametros
        val configFile : org.apache.spark.rdd.RDD[String] = spark.sparkContext.textFile(configFilePath)
        var catColumnas : Array[String] = configFile.take(2).last.split(";")
        var tipoCargaParam : String = configFile.take(3).last

        //Variables
        val bddlcru : String = "bddlcru"
        val bddldes : String = "bddldes"
        val bddlenr :  String = "bddlenr"
        val bddltrn :  String = "bddltrn"
        val bddlalm : String = "bddlalm"
        val bddlapr : String = "bddlapr"
        val sdpoliza : String = "sdpoliza"
        val sdhistend : String = "sdhistend"
        val sdendoso : String = "sdendoso"
        val cat_homologados : String = "cat_homologados"
        val enr_cobranza_emitida_pol_end_base : String = "enr_cobranza_emitida_pol_end_base"
        val enr_cobranza_emitida_pol_end_base_tmp : String = enr_cobranza_emitida_pol_end_base + "_tmp"
        val emazetiq : String = "emazetiq"

        //Asigna parametros de extraccion
        val fechaActual : org.joda.time.DateTime = new DateTime()
        val año : Int = fechaActual.getYear
        val mes : Int = fechaActual.getMonthOfYear
        val dia : Int = fechaActual.getDayOfMonth

        //Filtros de extraccion
        val cve_sistema : String = "CBZA"

        //Valida tabla destino
        var estructuraDestino : String = configFile.take(1).last
        val configEstructuraFile : org.apache.spark.rdd.RDD[String] = spark.sparkContext.textFile(estructuraDestino)
        var columnas : StringBuilder = new StringBuilder("")
        var addColumnas : String = configEstructuraFile.collect.addString(columnas).toString
        var dropTable : String = 
            s"""DROP TABLE IF EXISTS 
                ${bddltrn}.${enr_cobranza_emitida_pol_end_base_tmp} 
                PURGE"""
        var createTable : String = 
            s"""CREATE TABLE IF NOT EXISTS  
                ${bddltrn}.${enr_cobranza_emitida_pol_end_base_tmp} (${addColumnas}) 
                ROW FORMAT SERDE 
                    'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe' 
                STORED AS INPUTFORMAT 
                    'org.apache.hadoop.hive.ql.io.RCFileInputFormat' 
                OUTPUTFORMAT 
                    'org.apache.hadoop.hive.ql.io.RCFileOutputFormat'"""
        spark.sql(dropTable)
        spark.sql(createTable)

        //Valida tabla hash
        var audit : String = "ts_alta_hdfs timestamp, idmd5 varchar(32), idmd5completo varchar(32)"
        var auditHash : String = addColumnas + ", " + audit
        var dropTableHash : String = 
            s"""DROP TABLE IF EXISTS 
                ${bddlenr}.${enr_cobranza_emitida_pol_end_base} 
                PURGE"""
        var createTableHash : String = 
            s"""CREATE TABLE IF NOT EXISTS  
                ${bddlenr}.${enr_cobranza_emitida_pol_end_base} (${auditHash}) 
                    ROW FORMAT SERDE 
                        'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe' 
                    STORED AS INPUTFORMAT 
                        'org.apache.hadoop.hive.ql.io.RCFileInputFormat' 
                    OUTPUTFORMAT 
                        'org.apache.hadoop.hive.ql.io.RCFileOutputFormat'"""
        spark.sql(dropTableHash)
        spark.sql(createTableHash)

        //Pad
        def rPad(str:String, paddedLength:Int, ch:Char) : String = {
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

        val reglasNegocio : Seq[String] = 
            Seq(
                s"""num_poliza_cobranza<>concat(lpad(cast(coalesce(ofna, 0) as varchar(2)), 2, '0'), lpad(cast(coalesce(poliza, 0) as varchar(6)), 6, '0'))""",
                s"""num_endoso<>lpad(numendoso, 6, '0')""",
                s"""am_contable<>substring(cast(fechemis as string), 1, 6)""",
                s"""fch_contable<>fechemis""",
                s"""cve_forma_pago<>formapago""",
                s"""cve_unidad_medida<>moneda""",
                s"""cve_tipo_endoso<>tipoendoso""",
                s"""mto_prima_neta<>case when tabla = 'sdpoliza' then prineta/100 else case when trim(tipoendoso) = 'D' then (prineta/100) * -1 else (prineta/100) end end""",
                s"""mto_recargo_descuento<>case when tabla = 'sdpoliza' then descvol/100 else case when trim(tipoendoso) = 'D' then (descvol/100) * -1 else (descvol/100) end end""",
                s"""mto_derecho_poliza<>case when tabla = 'sdpoliza' then gastos/100 else case when trim(tipoendoso) = 'D' then (gastos/100) * -1 else (gastos/100) end end""",
                s"""mto_recargo_pago_fraccionado<>case when tabla = 'sdpoliza' then recargo/100 else case when trim(tipoendoso) = 'D' then (recargo/100) * -1 else (recargo/100) end end""",
                s"""mto_iva_poliza<>case when tabla = 'sdpoliza' then imporimp/100 else case when trim(tipoendoso) = 'D' then (imporimp/100) * -1 else (imporimp/100) end end""",
                s"""mto_prima_total<>((case when tabla = 'sdpoliza' then prineta/100 else case when trim(tipoendoso) = 'D' then (prineta/100) * -1 else (prineta/100) end end) + (case when tabla = 'sdpoliza' then recargo/100 else case when trim(tipoendoso) = 'D' then (recargo/100) * -1 else (recargo/100) end end) + (case when tabla = 'sdpoliza' then imporimp/100 else case when trim(tipoendoso) = 'D' then (imporimp/100) * -1 else (imporimp/100) end end) + (case when tabla = 'sdpoliza' then gastos/100 else case when trim(tipoendoso) = 'D' then (gastos/100) * -1 else (gastos/100) end end))""",
                s"""mto_comision_prima_neta<>case when tabla = 'sdpoliza' then porcomis/100 else case when trim(tipoendoso) = 'D' then (porcomis/100) * -1 else (porcomis/100) end end""",
                s"""mto_comision_derecho_poliza<>case when tabla = 'sdpoliza' then comisgas/100 else case when trim(tipoendoso) = 'D' then (comisgas/100) * -1 else (comisgas/100) end end""",
                s"""mto_comision_recargo_pago_fraccionado<>case when tabla = 'sdpoliza' then comisrec/100 else case when trim(tipoendoso) = 'D' then (comisrec/100) * -1 else (comisrec/100) end end""",
                s"""mto_isr_comisiones<>0.00""",
                s"""mto_importe_comision_total<>case when tabla = 'sdpoliza' then impcomis/100 else case when trim(tipoendoso) = 'D' then (impcomis/100) * -1 else (impcomis/100) end end"""
            )
            
        //Calcula tipo de carga
        var (tipoCarga : String, am_contable : String) = 
            if (!spark.catalog.tableExists(bddlenr + "." + enr_cobranza_emitida_pol_end_base) || tipoCargaParam.equals("CI")) {
                ("CI", "000000")
            } else if (dia < 4) {
                val añoContable : String = fechaActual.minusMonths(1).getYear.toString
                val mesContable : String = fechaActual.minusMonths(1).getMonthOfYear.toString
                ("DELTA", añoContable + rPad(mesContable, 2, '0'))
            } else {
                val añoContable : String = año.toString
                val mesContable : String = mes.toString
                ("DELTA", añoContable + rPad(mesContable, 2, '0'))
            }

        //Consultas
        val bddlcru_sdpoliza : String = 
            if (tipoCarga.equals("CI")) {
                s"""select 'sdpoliza' as tabla, ofna_poliza as ofna, poliza_poliza as poliza, '000000' as numendoso, fechemipoliza as fechemis, formapago_poliza 
                as formapago, moneda_poliza as moneda, 'A' as tipoendoso, prineta_poliza as prineta, descvol_poliza as descvol, 
                gastos_poliza as gastos, recargo_poliza as recargo, imporimp_poliza as imporimp, 0 as primatot, porcomis_poliza as porcomis, 
                comisgas_poliza as comisgas, comisrec_poliza as comisrec, 0 as isr, impcomis_poliza as impcomis, max(id_hist) as id_hist 
                from ${bddlcru}.${sdpoliza} group by tabla, ofna, poliza, numendoso, fechemis, formapago, moneda, tipoendoso, prineta, descvol, 
                gastos, recargo, imporimp, primatot, porcomis, comisgas, comisrec, isr, impcomis"""
            } else {
                s"""select cast(polori_poliza as string) as polori_poliza, concat(lpad(cast(coalesce(ofna_poliza ,0) as varchar(2)),2,'0'), 
                lpad(cast(coalesce(poliza_poliza,0) as varchar(6)),6,'0')) as cobranza, ofna_poliza, poliza_poliza, formapago_poliza, 
                descvol_poliza, prineta_poliza, recargo_poliza, gastos_poliza, imporimp_poliza, fechemipoliza, subramo_poliza, 
                max(id_hist) as id_hist from ${bddlcru}.${sdpoliza} group by polori_poliza, ofna_poliza, poliza_poliza, formapago_poliza, 
                descvol_poliza, prineta_poliza, recargo_poliza, gastos_poliza, imporimp_poliza, fechemipoliza, subramo_poliza"""
            }
        val bddlcru_sdendoso : String = 
            s"""select 'sdendoso' as tabla, ofna_endoso as ofna, poliza_endoso as poliza, numendoso_endoso as numendoso, fechemis_endoso as fechemis, forpagend_endoso 
            as formapago, moneda_endoso as moneda, tipoendoso_endoso as tipoendoso, prineta_endoso as prineta, descvol_endoso as descvol, 
            gastos_endoso as gastos, recargo_endoso as recargo, imporimp_endoso as imporimp, primatot_endoso as primatot, porcomis_endoso 
            as porcomis, comisrec_endoso as comisgas, comisgas_endoso as comisrec, 0 as isr, impcomis_endoso as impcomis, max(id_hist) as id_hist 
            from ${bddlcru}.${sdendoso} group by tabla, ofna, poliza, numendoso, fechemis, formapago, moneda, tipoendoso, prineta, descvol, 
            gastos, recargo, imporimp, primatot, porcomis, comisgas, comisrec, isr, impcomis"""
        val bddlcru_sdhistend : String =
            s"""select 'sdhistend' as tabla, ofna_histend as ofna, poliza_histend as poliza, numendoso_histend as numendoso, fechemis_histend as fechemis, forpagend_histend 
            as formapago, moneda_histend as moneda, tipendoso_histend as tipoendoso, prineta_histend as prineta, descvol_histend as descvol, 
            gastos_histend as gastos, recargo_histend as recargo, imporimp_histend as imporimp, primatot_histend as primatot, porcomis_histend 
            as porcomis, comisgas_histend as comisgas, comisrec_histend as comisrec, 0 as isr, impcomis_histend as impcomis, max(id_hist) as id_hist 
            from ${bddlcru}.${sdhistend} group by tabla, ofna, poliza, numendoso, fechemis, formapago, moneda, tipoendoso, prineta, descvol, 
            gastos, recargo, imporimp, primatot, porcomis, comisgas, comisrec, isr, impcomis"""
        val bddlcru_emazetiq : String =
            s"""select trim(poliza) as poliza, trim(cobranza) as cobranza, '' as  segmento, nvl(segmento, '') as desc_segmento, '' as negocio, 
            nvl(negocio, '') as desc_negocio, '' as bonos, nvl(bonos, '') as desc_bonos, nvl(plan_incentivos, '') as plan_incentivos, 
            '' as pol_contr, nvl(pol_contr, '') as desc_pol_contr, nvl(contrb, '') as contrb, '' as pol_prest, nvl(pol_prest, '') 
            as desc_pol_prest, '' as mot_exc, '' as desc_mot_exc from ${bddlcru}.${emazetiq}"""

        //Lectura
        val targetDf : org.apache.spark.sql.DataFrame = 
            spark.table(bddltrn + "." + enr_cobranza_emitida_pol_end_base_tmp)
        var sdpolizaDf : org.apache.spark.sql.DataFrame = 
            spark.sql(bddlcru_sdpoliza)
        var sdhistendDf : org.apache.spark.sql.DataFrame = 
            spark.sql(bddlcru_sdhistend)
        var sdendosoDf : org.apache.spark.sql.DataFrame = 
            spark.sql(bddlcru_sdendoso)
        var emazetiqDf : org.apache.spark.sql.DataFrame = 
            spark.sql(bddlcru_emazetiq)

        //Logica
        //rellenar num_endoso a 6
        //cruzar tablas
        /*var tablaBaseDf : org.apache.spark.sql.DataFrame =
            sdpolizaDf.union(sdendosoDf).union(sdhistendDf)*/

        var tablaBaseDf : org.apache.spark.sql.DataFrame = 
            sdpolizaDf
            .union(sdpolizaDf.as("sdpoliza")
                .join(sdhistendDf.as("sdhistend"), col("sdpoliza.ofna") === col("sdhistend.ofna") 
                    && col("sdpoliza.poliza") === col("sdhistend.poliza"), "inner").select("sdhistend.*"))
            .union(sdpolizaDf.as("sdpoliza")
                .join(sdendosoDf.as("sdendoso"), col("sdpoliza.ofna") === col("sdendoso.ofna") 
                    && col("sdpoliza.poliza") === col("sdendoso.poliza"), "inner").select("sdendoso.*"))

        if(!tablaBaseDf.rdd.isEmpty) {
            var tablaBaseReglasNegocioDf : org.apache.spark.sql.DataFrame = reglasNegocio
                .foldLeft(tablaBaseDf)((newDf, reglaNegocio) => newDf.withColumn(reglaNegocio.split("<>")(0), expr(reglaNegocio.split("<>")(1))))
 
            val formatoTargetDf : Array[org.apache.spark.sql.Column] = targetDf.schema.fields.map { 
                campo => 
                    if (tablaBaseReglasNegocioDf.columns.contains(campo.name)) 
                        col(campo.name).cast(campo.dataType).alias(campo.name) 
                    else 
                        lit(null).cast(campo.dataType).alias(campo.name) 
                }
            
            val targetTable : org.apache.spark.sql.DataFrame = tablaBaseReglasNegocioDf.select(formatoTargetDf: _*)
                
            targetTable.distinct().write.format("hive").mode(SaveMode.Append).saveAsTable(bddltrn + "." + enr_cobranza_emitida_pol_end_base_tmp)
        } else {
            println("No hay informacion para el delta.")
            }
        }
}