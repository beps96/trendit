/**
  *
  * @author Miguel Ivan Aguilar Mondragon
  * @note parametros de entrada en shell
  * @param1 ruta del documento de parametros
  * @param2 fecha de inicio para procesar bloque
  * @param3 fecha fin para procesar bloque
  * @forExample "/transf/DL_GMM/cobranza/config/configPrimaPagadaPolizaEndoso.ini"  20070101 20170101
  *
  */


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
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SparkSession

object Prima_emitida_poliza_endoso_base {

  def main(args: Array[String]) {


    /** ************** COMMENTAR AL GENERAR JAR ******************************/
    /*
     var args = new Array[String](20)
     args(0) = "/transf/DL_GMM/cobranza/config/configPrimaEmitidaPolizaEndoso.ini"
        args(1) = "20200301"
        args(2) = "20200331"
     */
    /** ********************************************/

    print("INICIA PROCESO DE PRIMA EMITIDA INFO / COBRANZA")
    val spark = SparkSession.builder().appName("GMM_Prima_emitida_poliza_endoso").enableHiveSupport().getOrCreate
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
    var schemaTablaSegmento = configFile.take(8).last
    var tablaSegmento = configFile.take(9).last
    var schemaVista = "bddlapr" //configFile.take(10).last
    var nombreVista = "cnm_prima_pagada_poliza_endoso" //configFile.take(11).last


    var fecIni = args(1)
    var fecFin = args(2)


    print("  tctitxct " + tctitxct + "  tccopera " + tccopera + "  ramo " + ramo + "  fecIni " + fecIni + "  fecFin " + fecFin + "  schemaTablaFinal " + schemaTablaFinal + " tablaFinal " + tablaFinal)


    crea_tabla_paso_prima_emitida_poliza_endoso(spark, schemaTablaPaso, tablaPaso)

    crea_tabla_paso_prima_emitida_poliza_endoso(spark, schemaTablaSegmento, tablaSegmento)

    crea_tabla_cnm_prima_emitida_poliza_endoso(spark, schemaTablaFinal, tablaFinal)

    crea_view_cnm_prima_emitida_poliza_endoso(spark,schemaTablaFinal, tablaFinal, schemaVista, nombreVista)

    //DELTA
    var df_prima_emitida_poliza_endoso_info = get_prima_emitida_poliza_endoso_info(spark, tctitxct, tccopera, ramo, fecIni, fecFin, schemaTablaPaso, tablaPaso, schemaTablaFinal, tablaFinal)

    //historico
    var df_prima_emitida_poliza_endoso_info_historico = get_prima_pagada_poliza_endoso_info_historico(spark, tctitxct, tccopera, ramo, fecIni, fecFin, schemaTablaPaso, tablaPaso, schemaTablaFinal, tablaFinal)

    //HISTORICO
    spark.sql(" Truncate table  " + schemaTablaFinal + "." + tablaFinal)
    df_prima_emitida_poliza_endoso_info_historico.write.mode("append").insertInto(schemaTablaFinal + "." + tablaFinal)

    //DELTA
    spark.sql(" Truncate table  " + schemaTablaPaso + "." + tablaPaso)

    // var df_prima_emitida_poliza_endoso_info_fill = set_null_values_default(df_prima_emitida_poliza_endoso_info)

    df_prima_emitida_poliza_endoso_info.write.mode("append").insertInto(schemaTablaPaso + "." + tablaPaso)


    print("FINALIZA PROCESO DE PRIMA emitida INFO / CROBRANZA")


    spark.sql(" refresh  " + schemaTablaPaso + "." + tablaPaso)
    spark.sql(" refresh  " + schemaTablaSegmento + "." + tablaSegmento)
    spark.sql(" refresh  " + schemaTablaFinal + "." + tablaFinal)


  }


  def get_prima_pagada_poliza_endoso_info_historico(spark: SparkSession, tctitxct: String, tccopera: String, ramo: String, fecIni: String, fecFin: String, schemaTablaPaso: String, tablaPaso: String, schemaTablaFinal: String, tablaFinal: String): DataFrame = {


    var qry_reprocceso_historico =
      """    SELECT *
             FROM """ + schemaTablaFinal + "." + tablaFinal +
        """    WHERE cve_sistema = 'INFO' and
                   am_contable  not in ( SELECT distinct am_contable as am_contable
                                         FROM """ + schemaTablaFinal + "." + tablaFinal +
        """                              WHERE cve_sistema_int = 'INFO' and am_contable
                                                 BETWEEN cast(substring(cast(""" + fecIni +
        """ as string),1,6) as int) """ +
        """ and cast(substring(cast(""" + fecFin +
        """ as string),1,6) as int)
                                         )
              order by am_contable asc
             """

    var df_rep_hist = spark.sql(qry_reprocceso_historico)

    df_rep_hist

  }


  def get_prima_emitida_poliza_endoso_info(spark: SparkSession, tctitxct: String, tccopera: String, ramo: String, fecIni: String, fecFin: String, schemaTablaPaso: String, tablaPaso: String, schemaTablaFinal: String, tablaFinal: String): DataFrame = {

    var conteo_carga_inicial = valida_carga_inicial(spark, schemaTablaPaso, tablaPaso, schemaTablaFinal, tablaFinal)

    var rango_fechas = ""
    if (conteo_carga_inicial > 0) {
      rango_fechas = " AND FECONTAB between " + fecIni + "  AND  " + fecFin
    }

    var queryktom41t = " SELECT   cdnumpol,ctvrspol,fecontab ,nusecmvo ,tctitxct ,tccopera ,impcec01,impcec02 ,impcec03,impcec04,impcec05,impcec06 ,impcec07 ,impcec09,impcec19,impcec20, TCCDMONE,TCDOMORI,SUBSTR(trim(cdramdgs),2,3) as cdramdgs " +
      " FROM BDDLCRU.KTOM41T " +
      " WHERE FECONTAB <> 0 and tctitxct in (" + tctitxct + ")  AND TCCOPERA in (" + tccopera + ")  AND CDCIAGRU='" + ramo + "'  AND length(cast(fecontab as string)) = 8 " + rango_fechas

    var queryktom41h = " SELECT   cdnumpol,ctvrspol,fecontab ,nusecmvo ,tctitxct ,tccopera ,impcec01,impcec02 ,impcec03,impcec04,impcec05,impcec06 ,impcec07 ,impcec09,impcec19,impcec20, TCCDMONE,TCDOMORI , SUBSTR(trim(cdramdgs),2,3) as cdramdgs" +
      " FROM BDDLCRU.KTOM41H " +
      " WHERE FECONTAB <> 0 and tctitxct in (" + tctitxct + ")  AND TCCOPERA in (" + tccopera + ")  AND CDCIAGRU='" + ramo + "'  AND length(cast(fecontab as string)) = 8 " + rango_fechas

    var Ktom41tDF = spark.sql(queryktom41t)
    var Ktom41hDF = spark.sql(queryktom41h)


    var querykcim06t =
      """ SELECT DISTINCT trim(cdnumpol) AS cdnumpol_06,ctvrspol as ctvrspol_06,inancaco,tcforpag,femision,feeftona,inpolpai
            FROM bddlcru.kcim06t
            WHERE cdciagru='SA'
                  AND cdprodte LIKE 'G%'
                  union all
          SELECT DISTINCT trim(cdnumpol) AS cdnumpol_06,ctvrspol as ctvrspol_06,inancaco,tcforpag,femision,feeftona,inpolpai
           FROM bddlcru.kcim06h
           WHERE cdciagru='SA'
                  AND cdprodte LIKE 'G%'
                   """


    var kcim06tdf = spark.sql(querykcim06t)

    var ktom41DF_union = Ktom41tDF.union(Ktom41hDF)

    var dfk41k06 = ktom41DF_union.join(kcim06tdf, trim(ktom41DF_union.col("CDNUMPOL")) === trim(kcim06tdf.col("cdnumpol_06")) and ktom41DF_union.col("CTVRSPOL") === kcim06tdf.col("ctvrspol_06"), "left")

    var dfpolizaquery =
      """  SELECT
                  num_poliza,num_version_poliza,cve_cobertura_contable,des_cobertura_contable,cve_forma_pago,des_forma_pago,cve_segmento,cve_canal_venta,
                    cve_negocio_multinacional,des_negocio_multinacional,cve_negocio_multinacional,cve_tipo_bono,des_tipo_bono,afecto_plan_incentivo,
                    ban_poliza_contributoria,pct_contribucion,ban_poliza_prestacion,cve_motivo_exclusion_plan_incentivo,des_motivo_exclusion_plan_incentivo,
                    segmento_venta_estadistico,canal_venta_estadistico,cve_sistema,cve_sistema_int,des_segmento,substring(DES_TIPO_MOVIMIENTO,1,char_length(DES_TIPO_MOVIMIENTO)-3) as DES_TIPO_MOVIMIENTO,
                    substring(trim(DES_TIPO_MOVIMIENTO),char_length( trim( DES_TIPO_MOVIMIENTO))-1 ,2 ) as CVE_TIPO_MOVIMIENTO ,
                     case when FCH_EMISION is null then cast('1400-00-00 00:00:00' as timestamp) else FCH_EMISION end as FCH_EMISION
              FROM   bddlalm.alm_poliza
              where  trim(cve_sistema_int) = 'INFO'
                      """
    var dfpoliza = spark.sql(dfpolizaquery)

    var dfk41k06poliza = dfk41k06.join(dfpoliza, trim(trim(dfk41k06.col("CDNUMPOL"))) === trim(dfpoliza.col("num_poliza")) and dfk41k06.col("CTVRSPOL") === dfpoliza.col("num_version_poliza"), "left")


    var dfktctget = spark.sql(
      """ Select  CAST(trim(CDELEMEN) AS STRING) as ELEMENTO  ,CAST(DSELEMEN AS STRING) as DES_TIPO_MOVIMIENTO_KTCTGET
                                 from bddlcru.KTCTGET
                                 where trim(CDEMPRES) = '0001'
                                      and trim(CDIDIOMA) = 'ES'
                                      and trim(CDTABLA) = 'KTOT23S' """)


    var dfk41k06polizaktctget = dfk41k06poliza.join(broadcast(dfktctget), concat(trim(dfk41k06poliza.col("TCDOMORI")), trim(dfk41k06poliza.col("TCCOPERA"))) === trim(dfktctget.col("ELEMENTO")), "left")


    var qry_ktctget_2 = spark.sql(
      """
                             Select  CAST(trim(CDELEMEN) AS STRING) as ELEMENTO_2  ,CAST(trim(DSELEMEN) AS STRING) as DES_TIPO_MOVIMIENTO_KTCTGET_2
                                 from bddlcru.KTCTGET
                                 where trim(CDEMPRES) = '0001'
                                      and trim(CDIDIOMA) = 'ES'
                                      and trim(CDTABLA) = 'KTOT51S'
                              """)


    var dfk41elemento_get = dfk41k06polizaktctget.join(broadcast(qry_ktctget_2), concat(trim(dfk41k06polizaktctget.col("TCDOMORI")), trim(dfk41k06polizaktctget.col("TCTITXCT"))) === trim(qry_ktctget_2.col("ELEMENTO_2")), "left")


    var dfelementopoliza = spark.sql(
      """  SELECT  case when  popagpri <> 0   then  popagpri/100  else 1 end  as pct_pago_prima ,num_poliza,num_version
                                        FROM    bddlalm.gmm_elemento_poliza
                                   """)

    var dfk41elemento = dfk41elemento_get.join(dfelementopoliza, trim(dfk41elemento_get.col("cdnumpol")) === trim(dfelementopoliza.col("num_poliza")) and dfk41elemento_get.col("ctvrspol") === dfelementopoliza.col("num_version"), "left")


    var dfcatmoneda = spark.sql(
      """ SELECT CVE_ATRIBUTO_ORG, CVE_HOMOLOGADA, DES_HOMOLOGADA
            FROM BDDLALM.CAT_HOMOLOGADOS
            WHERE NOMBRE_CATALOGO = 'CAT_MONEDA'
                             AND CVE_RAMO = 'SA'
                             AND CVE_SISTEMA = 'INFO'
                            """)

    var dfk41catmoneda = dfk41elemento.join(broadcast(dfcatmoneda), trim(dfk41elemento.col("TCCDMONE")) === trim(dfcatmoneda.col("CVE_ATRIBUTO_ORG")), "left")


    var dfcatSubcuenta = spark.sql(
      """ SELECT trim(cve_subsubcuenta) as cve_subsubcuenta ,trim(subsubcuenta) as subsubcuenta
            FROM bddlcru.CON_CAT_SUBSUBCUENTA
             WHERE cve_unidad_negocio = 'G'
         """)

    var dfk41catmoneda_catsubcuenta = dfk41catmoneda.join(broadcast(dfcatSubcuenta), trim(dfk41catmoneda.col("cdramdgs")) === trim(dfcatSubcuenta.col("cve_subsubcuenta")), "left")


    var dfimportes = dfk41catmoneda_catsubcuenta.withColumn("mto_prima_tecnica", expr(
      """
                  CASE
                            WHEN tctitxct = '003' THEN (
                                      CASE
                                                WHEN tccopera = '003'
                                                  THEN impcec01
                                                  ELSE impcec01 * -1
                                      END)
                            ELSE (
                                      CASE
                                                WHEN tccopera = '003'
                                                  THEN impcec01 * -1
                                                  ELSE  impcec01
                                      END)
                  END """))
      .withColumn("mto_recargo_descuento", expr(
        """
                 CASE
                            WHEN tctitxct = '003' THEN (
                                      CASE
                                                WHEN tccopera = '003'
                                                  THEN IMPCEC09
                                                  ELSE IMPCEC09  * -1
                                      END)
                            ELSE (
                                      CASE
                                                WHEN tccopera = '003'
                                                  THEN IMPCEC09    * -1
                                                  ELSE IMPCEC09
                                      END)
                  END

                  """))
      .withColumn("mto_prima_neta", expr(

        """ CASE
                            WHEN tctitxct = '003' THEN (
                                      CASE
                                                WHEN tccopera = '003'
                                                  THEN impcec19
                                                  ELSE impcec19 * -1
                                      END)
                            ELSE (
                                      CASE
                                                WHEN tccopera = '003'
                                                  THEN impcec19 * -1
                                                  ELSE impcec19
                                      END)
                  END"""))
      .withColumn("mto_recargo_pago_fraccionado", expr(
        """

        CASE
                            WHEN tctitxct = '003' THEN (
                                      CASE
                                                WHEN tccopera = '003'
                                                  THEN impcec03
                                                  ELSE impcec03  * -1
                                      END ) * -1
                            ELSE (
                                      CASE
                                                WHEN tccopera = '003'
                                                  THEN impcec03 * -1
                                                  ELSE impcec03
                                      END)
                  END

                  """))
      .withColumn("mto_derecho_poliza", expr(
        """

                  CASE
                            WHEN tctitxct = '003' THEN (
                                      CASE
                                                WHEN tccopera = '003'
                                                  THEN impcec04
                                                  ELSE impcec04 * -1
                                      END)
                            ELSE (
                                      CASE
                                                WHEN tccopera = '003'
                                                  THEN impcec04 * -1
                                                  ELSE impcec04
                                      END)
                  END

                  """))

      .withColumn("mto_recargo_gastos", expr(
        """

                  CASE
                            WHEN tctitxct = '003' THEN (
                                      CASE
                                                WHEN tccopera = '003'
                                                THEN impcec07
                                                ELSE impcec07 * -1
                                      END)
                            ELSE (
                                      CASE
                                                WHEN tccopera = '003'
                                                  THEN impcec07 * -1
                                                  ELSE impcec07
                                      END)
                  END

                  """))

      .withColumn("mto_iva_poliza", expr(
        """

                  CASE
                            WHEN tctitxct = '003' THEN (
                                      CASE
                                                WHEN tccopera = '003'
                                                  THEN (IMPCEC05 + IMPCEC06)
                                                  ELSE (IMPCEC05 + IMPCEC06) * -1
                                      END)
                            ELSE (
                                      CASE
                                                WHEN tccopera = '003'
                                                THEN (IMPCEC05 + IMPCEC06) * -1
                                                ELSE (IMPCEC05 + IMPCEC06)
                                      END)
                  END

                  """))

      .withColumn("mto_iva_gastos", expr(
        """

                  CASE
                            WHEN tctitxct = '003' THEN (
                                      CASE
                                                WHEN tccopera = '003'
                                                THEN impcec06
                                                ELSE impcec06 * -1
                                      END)
                            ELSE (
                                      CASE
                                                WHEN tccopera = '003'
                                                  THEN impcec06 * -1
                                                  ELSE impcec06
                                      END)
                  END

                  """))
      .withColumn("mto_prima_total", expr(
        """

                  CASE
                            WHEN tctitxct = '003' THEN (
                                      CASE
                                                WHEN tccopera = '003'
                                                THEN impcec20
                                                ELSE impcec20 * -1
                                      END)
                            ELSE (
                                      CASE
                                                WHEN tccopera = '003'
                                                  THEN impcec20 * -1
                                                  ELSE impcec20
                                      END)
                  END

                  """))
      .withColumn("mto_cesion_comision", expr(
        """

                   CASE
                            WHEN tctitxct = '003' THEN (
                                      CASE
                                                WHEN tccopera = '003' THEN (IMPCEC02 ) * -1
                                                ELSE (IMPCEC02)
                                      END) * -1
                            ELSE (
                                      CASE
                                                WHEN tccopera = '003' THEN (IMPCEC02 ) * -1
                                                ELSE (IMPCEC02)
                                      END)
                  END

                  """))




    var dfcolumnas = dfimportes.select(substring(col("fecontab"), 1, 6).as("am_contable"),
      concat(substring(col("fecontab"), 1, 4), lit("-"), substring(col("fecontab"), 5, 2), lit("-"), substring(col("fecontab"), 7, 2)).as("fch_contable"),
      col("CDNUMPOL").as("num_poliza"),
      col("inancaco").cast("Int").as("vigencia_poliza"),
      col("ctvrspol").cast("Int").as("num_version_poliza"),
      set_null_values_default(col("cdramdgs"), "","cve_cobertura_contable" ) ,
      set_null_values_default(col("subsubcuenta"), "SIN DESCRIPCION","des_cobertura_contable" ) ,
      col("nusecmvo").cast("Int").as("num_consecutivo_movimiento_contable"),
      col("cve_forma_pago"),
      col("des_forma_pago"),
      concat(trim(col("CVE_TIPO_MOVIMIENTO")), trim(col("TCCOPERA"))).alias("CVE_TIPO_MOVIMIENTO"),
      concat(trim(col("DES_TIPO_MOVIMIENTO_KTCTGET")), lit("-"), trim(col("DES_TIPO_MOVIMIENTO"))).as("DES_TIPO_MOVIMIENTO_KTCTGET"),
      col("mto_prima_tecnica"),
      col("mto_recargo_descuento"),
      col("mto_prima_neta"),
      col("mto_recargo_pago_fraccionado"),
      col("mto_derecho_poliza"),
      col("mto_cesion_comision"),
      col("mto_recargo_gastos"),
      col("mto_iva_poliza"),
      col("mto_iva_gastos"),
      col("mto_prima_total"),
      when(col("pct_pago_prima").isNull, 1).otherwise(col("pct_pago_prima")).alias("pct_pago_prima"),
      set_null_values_default(col("CVE_HOMOLOGADA"), "","CVE_UNIDAD_MEDIDA" ),
      set_null_values_default(col("DES_HOMOLOGADA"), "SIN DESCRIPCION","DES_UNIDAD_MEDIDA" ) ,
      col("canal_venta_estadistico"),
      col("segmento_venta_estadistico"), //poliza quitar
      col("FCH_EMISION"),
      col("cve_segmento"),
      col("des_segmento").alias("des_segmento"),
      col("cve_negocio_multinacional"),
      col("des_negocio_multinacional"),
      col("cve_tipo_bono"),
      col("des_tipo_bono"),
      col("afecto_plan_incentivo"),
      col("ban_poliza_contributoria"),
      //when(col("pct_contribucion").isNull, 1).otherwise(col("pct_contribucion")).alias("pct_contribucion"),
      col("pct_contribucion"),
      col("ban_poliza_prestacion"),
      col("cve_motivo_exclusion_plan_incentivo"),
      col("des_motivo_exclusion_plan_incentivo")
    ).withColumn("num_poliza_cobranza", lit("-999"))
      .withColumn("num_endoso", lit("-999"))
      .withColumn("cve_sistema", lit("INFO"))
      .withColumn("cve_sistema_int", lit("INFO"))
      .withColumn("mto_prima_neta_sin_nvo_met_pago", when(col("pct_pago_prima").isNull, col("mto_prima_neta").cast("double") * 1
      ) otherwise (col("pct_pago_prima") * col("mto_prima_neta")))
      .withColumn("mto_recargo_nvo_met_pago", when(col("pct_pago_prima").isNull, col("mto_prima_neta") - (col("mto_prima_neta").cast("double") * 1)
      ) otherwise (col("mto_prima_neta") - (col("pct_pago_prima") * col("mto_prima_neta"))))

    var dfcodfcolumnas_rename = dfcolumnas.withColumnRenamed("DES_TIPO_MOVIMIENTO_KTCTGET", "DES_TIPO_MOVIMIENTO")


    var df_prima_emitida_groupby = dfcodfcolumnas_rename.groupBy("am_contable", "fch_contable", "num_poliza", "vigencia_poliza", "num_version_poliza", "num_poliza_cobranza",
      "num_endoso", "cve_cobertura_contable", "des_cobertura_contable", "num_consecutivo_movimiento_contable",
      "cve_sistema", "cve_sistema_int", "cve_forma_pago", "des_forma_pago", "cve_tipo_movimiento", "DES_TIPO_MOVIMIENTO",
      "pct_pago_prima", "cve_unidad_medida", "des_unidad_medida", "canal_venta_estadistico", "segmento_venta_estadistico", "fch_emision", "cve_segmento",
      "des_segmento", "cve_negocio_multinacional", "des_negocio_multinacional", "cve_tipo_bono", "des_tipo_bono", "afecto_plan_incentivo",
      "ban_poliza_contributoria", "pct_contribucion", "ban_poliza_prestacion", "cve_motivo_exclusion_plan_incentivo", "des_motivo_exclusion_plan_incentivo"
    )
      .agg(sum("mto_prima_tecnica").alias("mto_prima_tecnica"), sum("mto_recargo_descuento").alias("mto_recargo_descuento"),
        sum("mto_prima_neta").alias("mto_prima_neta"), sum("mto_recargo_nvo_met_pago").alias("mto_recargo_nvo_met_pago"),
        sum("mto_prima_neta_sin_nvo_met_pago").alias("mto_prima_neta_sin_nvo_met_pago"), sum("mto_recargo_pago_fraccionado").alias("mto_recargo_pago_fraccionado"),
        sum("mto_derecho_poliza").alias("mto_derecho_poliza"), sum("mto_cesion_comision").alias("mto_cesion_comision"), sum("mto_iva_poliza").alias("mto_iva_poliza"),
        sum("mto_prima_total").alias("mto_prima_total")
      )


    var df_prima_emitida_formatos = df_prima_emitida_groupby.select("am_contable", "fch_contable", "num_poliza", "vigencia_poliza", "num_version_poliza", "num_poliza_cobranza",
      "num_endoso", "cve_cobertura_contable", "des_cobertura_contable", "num_consecutivo_movimiento_contable", "cve_sistema",
      "cve_sistema_int", "cve_forma_pago", "des_forma_pago", "cve_tipo_movimiento", "des_tipo_movimiento", "mto_prima_tecnica",
      "mto_recargo_descuento", "mto_prima_neta", "mto_recargo_nvo_met_pago", "mto_prima_neta_sin_nvo_met_pago", "mto_recargo_pago_fraccionado",
      "mto_derecho_poliza", "mto_cesion_comision", "mto_iva_poliza", "mto_prima_total", "pct_pago_prima", "cve_unidad_medida", "des_unidad_medida",
      "canal_venta_estadistico", "segmento_venta_estadistico", "fch_emision", "cve_segmento", "des_segmento", "cve_negocio_multinacional", "des_negocio_multinacional",
      "cve_tipo_bono", "des_tipo_bono", "afecto_plan_incentivo", "ban_poliza_contributoria", "pct_contribucion", "ban_poliza_prestacion", "cve_motivo_exclusion_plan_incentivo",
      "des_motivo_exclusion_plan_incentivo"
    )

    //retorno_prima_emitida_info
    df_prima_emitida_formatos

  }

  def valida_carga_inicial(spark: SparkSession, schemaTablaPaso: String, tablaPaso: String, schemaTablaFinal: String, tablaFinal: String): Int = {

    var df_carga_inicial = spark.sql("Select count(*) from " + schemaTablaFinal + "." + tablaFinal)
    var conteo = df_carga_inicial.head
    conteo.get(0).toString.toInt

  }

  def crea_tabla_paso_prima_emitida_poliza_endoso(spark: SparkSession, schemaTabla: String, tabla: String): Unit = {


    var sql_creata_tabla =
      """

        CREATE TABLE if not exists """ + schemaTabla + "." + tabla +
        """(
            am_contable	int	,
            fch_contable	timestamp	,
            num_poliza	varchar(14)	,
            vigencia_poliza	smallint	,
            num_version_poliza	smallint	,
            num_poliza_cobranza	varchar(8)	,
            num_endoso	varchar(6)	,
            cve_cobertura_contable	varchar(3)	,
            des_cobertura_contable	varchar(100)	,
            num_consecutivo_movimiento_contable	smallint	,
            cve_sistema	varchar(4)	,
            cve_sistema_int	varchar(4)	,
            cve_forma_pago	varchar(1)	,
            des_forma_pago	varchar(100)	,
            cve_tipo_movimiento	varchar(6)	,
            des_tipo_movimiento	varchar(50)	,
            mto_prima_tecnica	decimal(16,4)	,
            mto_recargo_descuento	decimal(16,4)	,
            mto_prima_neta	decimal(16,4)	,
            mto_recargo_nvo_met_pago	decimal(16,4)	,
            mto_prima_neta_sin_nvo_met_pago	decimal(16,4)	,
            mto_recargo_pago_fraccionado	decimal(16,4)	,
            mto_derecho_poliza	decimal(16,4)	,
            mto_cesion_comision	decimal(16,4)	,
            mto_iva_poliza	decimal(16,4)	,
            mto_prima_total	decimal(16,4)	,
            pct_pago_prima	decimal(16,8)	,
            cve_unidad_medida	varchar(4)	,
            des_unidad_medida	varchar(50)	,
            canal_venta_estadistico	varchar(100)	,
            segmento_venta_estadistico	varchar(100)	,
            fch_emision        timestamp,
            cve_segmento	varchar(10)	,
            des_segmento	varchar(100)	,
            cve_negocio_multinacional	varchar(10)	,
            des_negocio_multinacional	varchar(100)	,
            cve_tipo_bono	varchar(5)	,
            des_tipo_bono	varchar(100)	,
            afecto_plan_incentivo	varchar(100)	,
            ban_poliza_contributoria	varchar(2)	,
            pct_contribucion	decimal(16,4)	,
            ban_poliza_prestacion	varchar(2)	,
            cve_motivo_exclusion_plan_incentivo	varchar(5)	,
            des_motivo_exclusion_plan_incentivo	varchar(100)


           )
  ROW FORMAT SERDE
                     'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
                 STORED AS INPUTFORMAT
                     'org.apache.hadoop.hive.ql.io.RCFileInputFormat'
                 OUTPUTFORMAT
                     'org.apache.hadoop.hive.ql.io.RCFileOutputFormat'


    """


    spark.sql(sql_creata_tabla)

  }


  def crea_tabla_cnm_prima_emitida_poliza_endoso(spark: SparkSession, schemaTabla: String, tabla: String): Unit = {


    var sql_creata_tabla =
      """

        CREATE TABLE if not exists """ + schemaTabla + "." + tabla +
        """(
            am_contable	int	,
            fch_contable	timestamp	,
            num_poliza	varchar(14)	,
            vigencia_poliza	smallint	,
            num_version_poliza	smallint	,
            num_poliza_cobranza	varchar(8)	,
            num_endoso	varchar(6)	,
            cve_cobertura_contable	varchar(3)	,
            des_cobertura_contable	varchar(100)	,
            num_consecutivo_movimiento_contable	smallint	,
            cve_sistema	varchar(4)	,
            cve_sistema_int	varchar(4)	,
            cve_forma_pago	varchar(1)	,
            des_forma_pago	varchar(100)	,
            cve_tipo_movimiento	varchar(6)	,
            des_tipo_movimiento	varchar(50)	,
            mto_prima_tecnica	decimal(16,4)	,
            mto_recargo_descuento	decimal(16,4)	,
            mto_prima_neta	decimal(16,4)	,
            mto_recargo_nvo_met_pago	decimal(16,4)	,
            mto_prima_neta_sin_nvo_met_pago	decimal(16,4)	,
            mto_recargo_pago_fraccionado	decimal(16,4)	,
            mto_derecho_poliza	decimal(16,4)	,
            mto_cesion_comision	decimal(16,4)	,
            mto_iva_poliza	decimal(16,4)	,
            mto_prima_total	decimal(16,4)	,
            pct_pago_prima	decimal(16,8)	,
            cve_unidad_medida	varchar(4)	,
            des_unidad_medida	varchar(50)	,
            canal_venta_estadistico	varchar(100)	,
            segmento_venta_estadistico	varchar(100)	,
            fch_emision        timestamp,
            cve_segmento	varchar(10)	,
            des_segmento	varchar(100)	,
            cve_negocio_multinacional	varchar(10)	,
            des_negocio_multinacional	varchar(100)	,
            cve_tipo_bono	varchar(5)	,
            des_tipo_bono	varchar(100)	,
            afecto_plan_incentivo	varchar(100)	,
            ban_poliza_contributoria	varchar(2)	,
            pct_contribucion	decimal(16,4)	,
            ban_poliza_prestacion	varchar(2)	,
            cve_motivo_exclusion_plan_incentivo	varchar(5)	,
            des_motivo_exclusion_plan_incentivo	varchar(100),
            ts_alta_hdfs timestamp,
            idmd5 varchar(32),
            idmd5completo varchar(32)


           )
  ROW FORMAT SERDE
                     'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
                 STORED AS INPUTFORMAT
                     'org.apache.hadoop.hive.ql.io.RCFileInputFormat'
                 OUTPUTFORMAT
                     'org.apache.hadoop.hive.ql.io.RCFileOutputFormat'


    """


    spark.sql(sql_creata_tabla)

  }



  def crea_view_cnm_prima_emitida_poliza_endoso(spark: SparkSession, schemaTabla: String, tabla: String, schemaVista: String, vista: String): Unit ={


    var sql_vista_tabla =
      """

          create view if not exists  """ + schemaVista + "." + vista + """
           as
           select
            am_contable		,
             fch_contable		,
             num_poliza	,
             vigencia_poliza		,
             num_version_poliza		,
             num_poliza_cobranza		,
             num_endoso		,
             cve_cobertura_contable		,
             des_cobertura_contable		,
             num_consecutivo_movimiento_contable		,
             cve_sistema		,
             cve_sistema_int		,
             cve_forma_pago		,
             des_forma_pago		,
             cve_tipo_movimiento		,
             des_tipo_movimiento		,
             mto_prima_tecnica		,
             mto_recargo_descuento		,
             mto_prima_neta		,
             mto_recargo_nvo_met_pago		,
             mto_prima_neta_sin_nvo_met_pago		,
             mto_recargo_pago_fraccionado		,
             mto_derecho_poliza	,
             mto_cesion_comision		,
             mto_iva_poliza	,
             mto_prima_total		,
             pct_pago_prima	,
             cve_unidad_medida		,
             des_unidad_medida	,
             canal_venta_estadistico		,
             segmento_venta_estadistico		,
             fch_emision        ,
             cve_segmento		,
             des_segmento		,
             cve_negocio_multinacional	,
             des_negocio_multinacional		,
             cve_tipo_bono		,
             des_tipo_bono	,
             afecto_plan_incentivo		,
             ban_poliza_contributoria		,
             pct_contribucion	,
             ban_poliza_prestacion		,
             cve_motivo_exclusion_plan_incentivo		,
             des_motivo_exclusion_plan_incentivo

        from """ + schemaTabla + "." + tabla

    spark.sql(sql_vista_tabla)

  }

   def set_null_values_default(columna: Column, valor_default: String, alias_columna: String): Column = {

    when(trim(columna) === "" || columna.isNull, lit(valor_default)).otherwise(columna).alias(alias_columna)

  }

   def set_null_values_default(columna: Column, valor_default: Int, alias_columna: String): Column = {

    when(trim(columna) === "" || columna.isNull, lit(valor_default)).otherwise(columna).alias(alias_columna)

  }


}
