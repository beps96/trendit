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
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lit, col, coalesce}
import org.apache.spark.sql.expressions.Window
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.text.Normalizer.{normalize => jnormalize, _}
import scala.collection.mutable.ListBuffer

object ventaEstadistico {
    def main(args: Array[String]){
        //Spark session
        val spark = 
            SparkSession
                .builder()
                .appName("GMM_DLK_VentaEstadistico")
                .enableHiveSupport()
                .getOrCreate

        //Parametros
        val sufijo : String = args(0)
        //val campoAmMes : String = args(1)
        val esquemaTablaSrc : String = args(1)
        val campoAñoMes : String = args(2)
        /*val sufijo : String = "info_ppac"
        val campoAmMes : String = "am_contable"
        val esquemaTablaSrc : String = "bddltrn.alm_prima_pagada_asegurado_cobertura_base_info"
        val campoAñoMes : String = "am_contable"*/

        //Variables
        val bddldes :  String = "bddldes"
        val bddltrn :  String = "bddltrn"
        val tablaFuente : String = esquemaTablaSrc
        val sufijoInfo : String = "i"
        val sufijoCbza : String = "c"
        val sufijoAzul : String = "a"
        val tablaStgInfo : String = bddltrn + ".stg_estadistico_" + sufijoInfo + sufijo
        val tablaStgCbza : String = bddltrn + ".stg_estadistico_" + sufijoCbza + sufijo
        val tablaStgAzul : String = bddltrn + ".stg_estadistico_" + sufijoAzul + sufijo
        val camposEstadisticos : Seq[String] = Seq(
            "num_poliza", 
            "num_version_poliza", 
            "vigencia_poliza", 
            "num_poliza_cobranza", 
            "canal_venta_estadistico", 
            "segmento_venta_estadistico",
            "cve_sistema",
            "cve_sistema_int",
            "anio_mes")

        //Lectura
        val esquemaTablaSrcDf : org.apache.spark.sql.DataFrame = 
            spark.table(esquemaTablaSrc)
        val tablaStgInfoDf : org.apache.spark.sql.DataFrame = 
            spark.table(tablaStgInfo)
                .select(camposEstadisticos.head, camposEstadisticos.tail : _*)
        val tablaStgCbzaDf : org.apache.spark.sql.DataFrame = 
            spark.table(tablaStgCbza)
                .select(camposEstadisticos.head, camposEstadisticos.tail : _*)
        val tablaStgAzulDf : org.apache.spark.sql.DataFrame = 
            spark.table(tablaStgAzul)
                .select(camposEstadisticos.head, camposEstadisticos.tail : _*)

        //Logica
        val tablaStgDf : org.apache.spark.sql.DataFrame =
            tablaStgInfoDf
                .union(tablaStgCbzaDf)
                .union(tablaStgAzulDf)
        val tgtDf : org.apache.spark.sql.DataFrame = 
            esquemaTablaSrcDf.as("source")
                .join(tablaStgDf.as("estadistico"), 
                    trim(col("source.num_poliza")) === trim(col("estadistico.num_poliza")) && 
                    col("source.num_version_poliza") === col("estadistico.num_version_poliza") && 
                    col("source.vigencia_poliza") === col("estadistico.vigencia_poliza") && 
                    trim(col("source.num_poliza_cobranza")) === trim(col("estadistico.num_poliza_cobranza")) && 
                    trim(col("source.cve_sistema")) === trim(col("estadistico.cve_sistema")) && 
                    trim(col("source.cve_sistema_int")) === trim(col("estadistico.cve_sistema_int")) && 
                    col(s"""source.${campoAñoMes}""") === col("estadistico.anio_mes"),
                    "left")
                .drop(col("source.canal_venta_estadistico"))
                .drop(col("source.segmento_venta_estadistico"))
                .drop(col("estadistico.num_poliza"))
                .drop(col("estadistico.num_version_poliza"))
                .drop(col("estadistico.vigencia_poliza"))
                .drop(col("estadistico.num_poliza_cobranza"))
                .drop(col("estadistico.cve_sistema"))
                .drop(col("estadistico.cve_sistema_int"))

        val reemplazaNulos : Seq[String] = 
            Seq(
                s"""canal_venta_estadistico_1<>nvl(canal_venta_estadistico, '')""",
                s"""segmento_venta_estadistico_1<>nvl(segmento_venta_estadistico, '')"""
            )

        var tgtSinNulosDf: org.apache.spark.sql.DataFrame = reemplazaNulos
            .foldLeft(tgtDf)((newDf, reglaNegocio) => newDf.withColumn(reglaNegocio.split("<>")(0), expr(reglaNegocio.split("<>")(1))))
                .drop(col("canal_venta_estadistico"))
                .drop(col("segmento_venta_estadistico"))
                .withColumnRenamed("canal_venta_estadistico_1", "canal_venta_estadistico")
                .withColumnRenamed("segmento_venta_estadistico_1", "segmento_venta_estadistico")

        val formatoTargetDf : Array[org.apache.spark.sql.Column] = esquemaTablaSrcDf.schema.fields.map { 
            campo => 
                if (tgtSinNulosDf.columns.contains(campo.name)) 
                    col(campo.name).cast(campo.dataType).alias(campo.name) 
                else 
                    lit(null).cast(campo.dataType).alias(campo.name) 
            }
         
        val targetTable : org.apache.spark.sql.DataFrame = tgtSinNulosDf.select(formatoTargetDf: _*)

        spark.sql(s"""truncate table ${esquemaTablaSrc}_${sufijo}""")

        targetTable
            .write
            .format("hive")
            .mode(SaveMode.Append)
            .saveAsTable(esquemaTablaSrc + "_" + sufijo)
    }
}