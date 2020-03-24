package mx.com.gnp.gmm
import org.apache.spark.sql.SparkSession
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

object Prima_pagada_poliza_endoso_cnmFinal {

  def main(args: Array[String]): Unit = {



    print("INICIA PROCESO DE PRIMA PAGADA INFO / COBRANZA")
    val spark = SparkSession.builder().appName("GMM_genera_cnm_Prima_pagada_poliza_endoso").enableHiveSupport().getOrCreate
    print("tamaño   " + args.length)
    print(args.toList)


    val configFilePath: String = args(0)

    print("configFilePath" + configFilePath)


    val configFile: org.apache.spark.rdd.RDD[String] = spark.sparkContext.textFile(configFilePath)
    print(" PARAM 1" + configFile.take(1).last)
    print(" PARAM 2 " + configFile.take(2).last)
    print(" PARAM 3 " + configFile.take(3).last)
    print(" PARAM 4 " + configFile.take(4).last)
    print(" PARAM 5 " + configFile.take(5).last)

    var tctitxct = configFile.take(1).last
    var tccopera = configFile.take(2).last
    var ramo = configFile.take(3).last
    var schemaTablaFinal = configFile.take(4).last
    var tablaFinal = configFile.take(5).last
    var schemaTablaPaso = configFile.take(6).last
    var tablaPaso = configFile.take(7).last


    var fecIni = args(1)
    var fecFin = args(2)

    //spark.sql( "truncate table " + schemaTablaFinal + "." + tablaFinal)

    var df_prima_pagada_pol_end_delta = spark.sql("Select * from  BDDLDES.PRIMA_PAGADA_POLIZA_ENDOSO_BASE_MD5")

    df_prima_pagada_pol_end_delta.write.mode("append").insertInto(schemaTablaFinal + "." + tablaFinal)



  }

}
