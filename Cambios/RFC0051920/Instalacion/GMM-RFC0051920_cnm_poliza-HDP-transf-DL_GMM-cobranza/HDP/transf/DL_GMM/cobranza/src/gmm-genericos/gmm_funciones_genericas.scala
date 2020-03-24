package mx.com.gnp.gmm.cobranza.genericos

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
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Period}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lit, col, coalesce}
import org.apache.spark.sql.expressions.Window
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.text.Normalizer.{normalize => jnormalize, _}
import scala.collection.mutable.ListBuffer

object gmm_funciones_genericas {
    val spark = 
        SparkSession
            .builder()
            .appName("GMM_DLK_Cobranza_Genericos")
            .enableHiveSupport()
            .getOrCreate

    //Relleno de cadenas
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

    //Generador de rangos de fechas
    def fechaRango(inicio : DateTime, fin : DateTime, salto : Period) : Iterator[DateTime] = {
        Iterator.iterate(inicio)(_.plus(salto)).takeWhile(!_.isAfter(fin))
    }

    //Calcula reprocesos o tipo de carga
    def calculaCarga(esquemaDestino: String, tablaDestino : String, pathParamFile : String) : (String, String) = {
        val fechaActual : org.joda.time.DateTime = new DateTime()
        val año : Int = fechaActual.getYear
        val mes : Int = fechaActual.getMonthOfYear
        val dia : Int = fechaActual.getDayOfMonth
        val configFile : org.apache.spark.rdd.RDD[String] = spark.sparkContext.textFile(pathParamFile)
        var tipoCargaParam : String = configFile.take(3).last
        var (tipoCarga : String, am_contable : String) = 
            if (!spark.catalog.tableExists(esquemaDestino + "." + tablaDestino) || tipoCargaParam.equals("CI")) {
                ("CI", "000000")
            } else if (dia < 4 && tipoCargaParam.equals("DE")) {
                val añoContable : String = fechaActual.minusMonths(1).getYear.toString
                val mesContable : String = fechaActual.minusMonths(1).getMonthOfYear.toString
                ("DE", añoContable + rPad(mesContable, 2, '0'))
            } else if (tipoCargaParam.equals("RE")) {
                val rangoReproceso : Array[String] = configFile.take(4).last.split(",")
                val formato = DateTimeFormat.forPattern("yyyyMM")
                var añoMeses = new ListBuffer[org.joda.time.DateTime]()
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
                ("RE", am_contables.mkString.replace("''", ", ").replace("'", ""))
                } else {
                    val añoContable : String = año.toString
                    val mesContable : String = mes.toString
                    ("DE", añoContable + rPad(mesContable, 2, '0'))
            }
        return (tipoCarga, am_contable)
        }

    //Agrega descripciones de catalogos
    def agregaCatalogos(originalDf : org.apache.spark.sql.DataFrame, catalogoDf : org.apache.spark.sql.DataFrame, elementos : Array[String], origen : String) : org.apache.spark.sql.DataFrame = { 
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
                    .filter(elementosExistentes.split("-")(1)).select(col("cve_atributo_org"), col("cve_homologada"), col("des_homologada")).alias("c")), trim(col("p." + elementosExistentes.split("-")(0))) === trim(col("c.cve_atributo_org")), "left_outer")
                .withColumnRenamed("cve_homologada", "cve_homologada_" + elementosExistentes.split("-")(0))
                .withColumnRenamed("des_homologada", "des_homologada_" + elementosExistentes.split("-")(0))
                .drop("cve_atributo_org"))
            } else {
                originalDf
            }
        return addElementoCatalogueDf
        }

    //Crea estructuras de paso
    def creaEstructuras(esquema : String, tabla : String, archivoEstructura : String, agregaAudit : String, tablaPaso : String) = {
        val configEstructuraFile : org.apache.spark.rdd.RDD[String] = spark.sparkContext.textFile(archivoEstructura)
        val audit : String = if(agregaAudit.equals("SI")) ", ts_alta_hdfs timestamp, idmd5 varchar(32), idmd5completo varchar(32)" else ""
        var columnas : StringBuilder = new StringBuilder("")
        var addColumnas : String = configEstructuraFile.collect.addString(columnas).toString + audit
        var dropTable : String = 
            s"""DROP TABLE IF EXISTS 
                ${esquema}.${tabla} 
                PURGE"""
        var createTable : String = 
            s"""CREATE TABLE IF NOT EXISTS  
                ${esquema}.${tabla} (${addColumnas}) 
                ROW FORMAT SERDE 
                    'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe' 
                STORED AS INPUTFORMAT 
                    'org.apache.hadoop.hive.ql.io.RCFileInputFormat' 
                OUTPUTFORMAT 
                    'org.apache.hadoop.hive.ql.io.RCFileOutputFormat'"""
        if (tablaPaso.equals("SI")) {
            spark.sql(dropTable)
            spark.sql(createTable)
        } else {
            spark.sql(createTable)
        }
        }
    }