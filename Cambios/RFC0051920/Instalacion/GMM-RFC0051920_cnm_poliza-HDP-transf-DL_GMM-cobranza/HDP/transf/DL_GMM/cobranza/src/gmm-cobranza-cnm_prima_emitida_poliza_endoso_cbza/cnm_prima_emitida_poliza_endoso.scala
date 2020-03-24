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

object cnm_prima_emitida_poliza_endoso {
    def main(args: Array[String]){
        //Spark session
        val spark = 
            SparkSession
                .builder()
                .appName("GMM_DLK_Prima_Emitida_Poliza_Endoso_Cobranza")
                .enableHiveSupport()
                .getOrCreate

        //spark.conf.set( "spark.sql.crossJoin.enabled" , "true")

        //Parametros
        //val configFilePath : String = "/transf/DL_GMM/cobranza/prima_emitida_poliza_endoso/config/paramCnmPrimaEmitidaPolizaEndoso.ini"
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
        val cnm_poliza : String = "cnm_poliza"
        val cat_homologados : String = "cat_homologados"
        val enr_cobranza_emitida_pol_end_base : String = "enr_cobranza_emitida_pol_end_base"
        val cnm_prima_emitida_poliza_endoso : String = "cnm_prima_emitida_poliza_endoso"
        val cnm_prima_emitida_poliza_endoso_tmp : String = cnm_prima_emitida_poliza_endoso + "_tmp"
        val cnm_prima_emitida_poliza_endoso_hash : String = cnm_prima_emitida_poliza_endoso + "_hash"
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
                ${bddltrn}.${cnm_prima_emitida_poliza_endoso_tmp} 
                PURGE"""
        var createTable : String = 
            s"""CREATE TABLE IF NOT EXISTS  
                ${bddltrn}.${cnm_prima_emitida_poliza_endoso_tmp} (${addColumnas}) 
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
                ${bddlenr}.${cnm_prima_emitida_poliza_endoso_hash} 
                PURGE"""
        var createTableHash : String = 
            s"""CREATE TABLE IF NOT EXISTS  
                ${bddlenr}.${cnm_prima_emitida_poliza_endoso_hash} (${auditHash}) 
                    ROW FORMAT SERDE 
                        'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe' 
                    STORED AS INPUTFORMAT 
                        'org.apache.hadoop.hive.ql.io.RCFileInputFormat' 
                    OUTPUTFORMAT 
                        'org.apache.hadoop.hive.ql.io.RCFileOutputFormat'"""
        spark.sql(dropTableHash)
        spark.sql(createTableHash)

        //Funciones 
        //Agrega valores de catalogos
        def add_catalogues(originalDf : org.apache.spark.sql.DataFrame, catalogoDf : org.apache.spark.sql.DataFrame, elementos : Array[String], origen : String) : org.apache.spark.sql.DataFrame = { 
            val elementosExistentes : Array[String] = elementos
                .map { field => if(originalDf.columns
                    .map(_.toLowerCase).contains(field.split("-")(0).toLowerCase)) field.toString else lit("no_existe").toString}.filter(_ != "no_existe")
            val addElementoCatalogueDf = if(origen.equals("KTCTGET")) {
                elementosExistentes.foldLeft(originalDf)((originalDf, elementosExistentes) => 
                    originalDf.alias("p")
                        .join(broadcast(catalogoDf
                        .filter(elementosExistentes.split("-")(1)).select(col("cdelemen"), trim(col("dselemen")).as("dselemen")).alias("c")), trim(col("p." + elementosExistentes.split("-")(0))) === trim(col("c.cdelemen")), "left_outer")
                    .withColumnRenamed("dselemen", "des_" + elementosExistentes.split("-")(0))
                    .drop("cdelemen"))
            } else if(origen.equals("CAT_HOMOLOGADOS")) {
                elementosExistentes.foldLeft(originalDf)((originalDf, elementosExistentes) => 
                    originalDf.alias("p")
                        .join(broadcast(catalogoDf
                        .filter(elementosExistentes.split("-")(1)).select(col("cve_atributo_org"), col("cve_homologada"), col("des_homologada")).alias("c")), col("p." + elementosExistentes.split("-")(0)) === col("c.cve_atributo_org"), "left_outer")
                    .withColumnRenamed("cve_homologada", "cve_homologada_" + elementosExistentes.split("-")(0))
                    .withColumnRenamed("des_homologada", "des_homologada_" + elementosExistentes.split("-")(0))
                    .drop("cve_atributo_org"))
            } else {
                originalDf
            }
            return addElementoCatalogueDf
        }

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
                s"""num_consecutivo_movimiento_contable<>0""",
                s"""cve_sistema<>'CBZA'""",
                s"""cve_sistema_int<>'AZUL'""",
                s"""mto_cesion_comision<>0.00""",
                s"""num_version_poliza<>-999""",
                s"""des_cobertura_contable<>nvl(des_cobertura_contable, '')""",
                s"""cve_forma_pago<>nvl(cve_forma_pago, '')""",
                s"""des_forma_pago<>nvl(des_forma_pago, '')""",
                s"""segmento_estadistico<>''""", //pendiente
                s"""cve_tipo_movimiento<>cve_tipo_endoso""",
                s"""des_tipo_movimiento<>case when cve_tipo_endoso = 'A' then 'Aumento' else 'Disminución' end""",
                s"""cve_unidad_medida<>cve_unidad_medida""",
                s"""des_unidad_medida<>nvl(des_homologada_cve_unidad_medida, '')""",
                s"""am_contable<>am_contable""",
                s"""pct_pago_prima<>1.00""",
                s"""mto_prima_neta_sin_nvo_met_pago<>mto_prima_neta * 1""",
                s"""mto_recargo_nvo_met_pago<>mto_prima_neta - (mto_prima_neta * 1)""",
                s"""fch_contable<>cast(concat(substring(cast(fch_contable as string), 1, 4), "-", substring(cast(fch_contable as string), 5, 2),"-", substring(cast(fch_contable as string),7,2) ) as timestamp)""",
                s"""num_poliza<>num_poliza""",
                s"""num_poliza_cobranza<>num_poliza_cobranza""",
                s"""num_endoso<>num_endoso""",
                s"""cve_cobertura_contable<>nvl(cve_cobertura_contable, '')""",
                s"""mto_prima_tecnica<>mto_prima_neta""",
                s"""mto_recargo_descuento<>mto_recargo_descuento""",
                s"""mto_prima_neta<>mto_prima_neta""",
                s"""mto_recargo_pago_fraccionado<>mto_recargo_pago_fraccionado""",
                s"""mto_derecho_poliza<>mto_derecho_poliza""",
                s"""mto_iva_poliza<>mto_iva_poliza""",
                s"""mto_prima_total<>mto_prima_total""",
                s"""canal_venta_estadistico<>''""",
                s"""cve_segmento<>''""",
                s"""des_segmento<>NVL(TRIM(SEGMENTO), '')""",
                s"""cve_negocio_multinacional<>''""",
                s"""des_negocio_multinacional<>NVL(TRIM(NEGOCIO), '')""",
                s"""cve_tipo_bono<>''""",
                s"""des_tipo_bono<>NVL(TRIM(BONOS), '')""",
                s"""afecto_plan_incentivo<>NVL(TRIM(PLAN_INCENTIVOS), '')""",
                s"""ban_poliza_contributoria<>NVL(TRIM(POL_CONTR), '')""",
                s"""pct_contribucion<>CAST(NVL(TRIM(CONTRB), '0') AS INT) / 100""",
                s"""ban_poliza_prestacion<>NVL(TRIM(POL_PREST), '')""",
                s"""cve_motivo_exclusion_plan_incentivo<>''""",
                s"""des_motivo_exclusion_plan_incentivo<>''"""
            )
            
        //Calcula tipo de carga
        var (tipoCarga : String, am_contable : String) = 
            if (!spark.catalog.tableExists(bddltrn + "." + cnm_prima_emitida_poliza_endoso) || tipoCargaParam.equals("CI")) {
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
        val bddlenr_enr_cobranza_emitida_pol_end_base : String = 
            if (tipoCarga.equals("CI")) {
                s"""select * from ${bddlenr}.${enr_cobranza_emitida_pol_end_base}"""
            } else {
                s"""select * from ${bddlenr}.${enr_cobranza_emitida_pol_end_base} where am_contable = cast(${am_contable} as int)"""
            }
        val bddlapr_cnm_poliza : String = 
            s"""select * from ${bddlapr}.${cnm_poliza} where cve_sistema = '${cve_sistema}'"""            

        //Lectura
        val targetDf : org.apache.spark.sql.DataFrame = 
            spark.table(bddltrn + "." + cnm_prima_emitida_poliza_endoso_tmp)
        var enr_cobranza_emitida_pol_end_baseDf : org.apache.spark.sql.DataFrame = 
            spark.sql(bddlenr_enr_cobranza_emitida_pol_end_base)
        var cnmPolizaDf : org.apache.spark.sql.DataFrame = 
            spark.sql(bddlapr_cnm_poliza)
        var cat_homologadosDf : org.apache.spark.sql.DataFrame =
            spark.table(bddlalm + "." + cat_homologados)
        var emazetiqDf : org.apache.spark.sql.DataFrame = 
            spark.table(bddlcru + "." + emazetiq)

        //Logica
        if(!enr_cobranza_emitida_pol_end_baseDf.rdd.isEmpty) {
            var enrCobranzaEmitidaPolEndBaseCnmPolizaDf : org.apache.spark.sql.DataFrame = 
                enr_cobranza_emitida_pol_end_baseDf.as("base")
                    .join(cnmPolizaDf.as("cnmPoliza"), col("base.num_poliza_cobranza") === col("cnmPoliza.num_poliza_cobranza"), "left")
                    .drop(col("cnmPoliza.num_poliza_cobranza"))
                    .drop(col("base.cve_forma_pago"))
            var enrCobranzaEmitidaPolEndBaseEmazetiqDf : org.apache.spark.sql.DataFrame = 
                enrCobranzaEmitidaPolEndBaseCnmPolizaDf.as("gaz0")
                    .join(emazetiqDf.as("emazetiq"), col("gaz0.num_poliza") === col("emazetiq.poliza") && col("gaz0.num_poliza_cobranza") === col("emazetiq.cobranza"), "left")
                    .drop(col("poliza"))
                    .drop(col("cobranza"))

            var enrCobranzaEmitidaPolEndBaseCatHomologadosDf : org.apache.spark.sql.DataFrame =
                add_catalogues(enrCobranzaEmitidaPolEndBaseEmazetiqDf, cat_homologadosDf, catColumnas, "CAT_HOMOLOGADOS")

            var tablaBaseReglasNegocioDf : org.apache.spark.sql.DataFrame = reglasNegocio
                .foldLeft(enrCobranzaEmitidaPolEndBaseCatHomologadosDf)((newDf, reglaNegocio) => newDf.withColumn(reglaNegocio.split("<>")(0), expr(reglaNegocio.split("<>")(1))))
 
            val formatoTargetDf : Array[org.apache.spark.sql.Column] = targetDf.schema.fields.map { 
                campo => 
                    if (tablaBaseReglasNegocioDf.columns.contains(campo.name)) 
                        col(campo.name).cast(campo.dataType).alias(campo.name) 
                    else 
                        lit(null).cast(campo.dataType).alias(campo.name) 
                }
            
            val targetTable : org.apache.spark.sql.DataFrame = tablaBaseReglasNegocioDf.select(formatoTargetDf: _*)
                
            targetTable.distinct().write.format("hive").mode(SaveMode.Append).saveAsTable(bddltrn + "." + cnm_prima_emitida_poliza_endoso_tmp)
        } else {
            println("No hay informacion para el delta.")
            }
        }
}