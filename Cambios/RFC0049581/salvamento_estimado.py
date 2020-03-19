# coding: utf-8
from pyspark               import SparkContext
from pyspark.sql           import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types     import DoubleType
from pyspark.sql.types     import IntegerType
from pyspark.sql.types     import StringType
from pyspark.sql.types     import StructField
from pyspark.sql.types     import StructType
from pyspark.sql.types     import TimestampType
from pyspark.sql.types     import *
import sys 
reload(sys)
sys.setdefaultencoding('UTF-8')


spark_context = SparkContext.getOrCreate()
hive_context  = HiveContext(spark_context)

SCHEMA_CRU = str(sys.argv[1]) 
SCHEMA_INT = str(sys.argv[2]) 
SCHEMA_DES = str(sys.argv[3]) 
INPUT_TABLE_NAME1 = str(sys.argv[4])#KTCTGET
INPUT_TABLE_NAME2 = str(sys.argv[5])#coberturas_afectas
INPUT_TABLE_NAME3 = str(sys.argv[6])#fnz_tipo_cambio
INPUT_TABLE_NAME4 = str(sys.argv[7])#ksim61h
INPUT_TABLE_NAME5 = str(sys.argv[8])#ksim61t
INPUT_TABLE_NAME6 = str(sys.argv[9])#ksim10

structuraF = StructType([
	StructField("cve_siniestro"    						, StringType()	, True),	
	StructField("num_afectado"    						, StringType(), 	True),
	StructField("num_movimiento"    						, StringType()	, True),
	StructField("num_mov_economico"    						, StringType(), 	True),	
	StructField("cve_cobertura"    							, StringType()	, True),
	StructField("desc_cobertura" , StringType()	, True), 	
	StructField("cve_cobertura_afecta"    							, StringType(), 	True),
	StructField("desc_cobertura_afecta"    	, StringType()	, True), 
	StructField("cve_cobertura_afecta_agrup"   						, StringType(), 	True),
	StructField("fch_operacion"    					, TimestampType(), 	True),
	StructField("cve_tipo_mov"    					, StringType()	, True), 	
	StructField("desc_tipo_mov"    					, StringType(), 	True),
	StructField("cve_moneda"    						, StringType(), 	True),
	StructField("tipo_cambio"    						, FloatType()	, True),
	StructField("mto_mov_ori"    						, FloatType(), 		True),
	StructField("mto_mov_int"    					, FloatType()	, True), 	
	StructField("mto_mov_calc"    					, FloatType(), 		True),
	StructField("mto_mov_calc_int"    				, FloatType()	, True), 	
	StructField("mto_estimado_ori"    						, FloatType(), 		True),
	StructField("mto_estimado_int"    					, FloatType()	, True), 	
	StructField("mto_estimado_calc"    					, FloatType(), 		True),
	StructField("mto_estimado_calc_int" 				, FloatType()	, True),	
	StructField("mto_salvado_ori"    						, FloatType(), 		True),
	StructField("mto_salvado_int"    					, FloatType()	, True), 	
	StructField("mto_salvado_calc"    					, FloatType(), 		True),
	StructField("mto_salvado_calc_int" 				, FloatType()	, True),
	StructField("mto_por_salvar_ori"    						, FloatType(), 		True),
	StructField("mto_por_salvar_int"    					, FloatType()	, True), 	
	StructField("mto_por_salvar_calc"    					, FloatType(), 		True),
	StructField("mto_por_salvar_calc_int" 				, FloatType()	, True), 
	StructField("cve_reintegro_salvamento"    						, StringType(), 	True),
	StructField("desc_reintegro_salvamento"    			, StringType()	, True), 	
	StructField("cve_factura_salvamento"    						, StringType(), 	True),
	StructField("desc_factura_salvamento"    				, StringType()	, True), 	
	StructField("tabla_origen"    						, StringType()	, True), 
	StructField("sistorig"    						, StringType()	, True)
])

structuraD = StructType([
	StructField("cdsinies"    						, StringType()	, True), 	StructField("ctnupoin"    						, StringType(), 	True),
	StructField("cdmct"    	  						, StringType()	, True), 	StructField("cdmcr"    							, StringType(), 	True),
	StructField("numovimi"    						, StringType()	, True), 	StructField("feoperac_fch"    					, TimestampType(), 	True),
	StructField("tctipmov"    						, StringType()	, True), 	StructField("tccdmone"    						, StringType(), 	True),
	StructField("imestima"    						, FloatType()	, True), 	StructField("imsalvad"    						, FloatType(), 		True),
	StructField("imporsal"    						, FloatType()	, True), 	StructField("immovimi"    						, FloatType(), 		True),
	StructField("inreisal"    						, StringType()	, True), 	StructField("desc_inreisal_calc"    			, StringType(), 	True),
	StructField("infacsal"    						, StringType()	, True), 	StructField("desc_infacsal_cal"    				, StringType(), 	True),
	StructField("numoveco"    						, StringType()	, True), 	StructField("tabori"    						, StringType(), 	True),
])	

get = hive_context.table(SCHEMA_CRU+"."+INPUT_TABLE_NAME1).filter((trim(col("cdtabla")) == 'KSITMSG') & (trim(col("cdidioma")) == "ES")).select(
	trim(col("cdtabla")).alias("cdtabla"), 	trim(col("cdelemen")).alias("tctipmov_salv"), 	trim(col("dselemen")).alias("dstipmov_salv")
	)

cobafe = hive_context.table(SCHEMA_DES+"."+INPUT_TABLE_NAME2).select(
	trim(col("clave_cobertura_tarificable")).alias("clave_cobertura_tarificable"), 	trim(col("nombre_cobertura_tarificable")).alias("nombre_cobertura_tarificable"),
	trim(col("descripcion_cobertura_tarificable")).alias("descripcion_cobertura_tarificable"), 	trim(col("clave_cobertura_afecta")).alias("clave_cobertura_afecta"),
	trim(col("nombre_cobertura_afecta")).alias("nombre_cobertura_afecta"), 	trim(col("descripcion_cobertura_afecta")).alias("descripcion_cobertura_afecta"),
	trim(col("nueva_agrupacion")).alias("cdmcr_agr")
	)
	
tipcamb = hive_context.table(SCHEMA_INT+"."+INPUT_TABLE_NAME3).filter(col("from_cur").isNotNull()).select(
	trim(col("from_cur")).alias("from_cur"), 	trim(col("rate_mult")).cast("float").alias("rate_mult2"),  	col("fecha")
	)
 			
m61h=hive_context.table(SCHEMA_CRU+"."+INPUT_TABLE_NAME4).select(
	col("cdsinies"), 	col("ctnupoin"), 	col("cdmct"), 	col("cdmcr"), 	col("numovimi"),
	concat_ws("-",substring(col("feoperac"),1,4),substring(col("feoperac"),5,2),substring(col("feoperac"),7,2)).cast("timestamp").alias("feoperac_fch"),
	col("tctipmov"), 	trim(col("tccdmone")).alias("tccdmone"), 	col("imestima").cast("float").alias("imestima"),
	col("imsalvad").cast("float").alias("imsalvad"), 	col("imporsal").cast("float").alias("imporsal"),
	col("immovimi").cast("float").alias("immovimi"), 	col("inreisal"), 	col("infacsal"), 	col("numoveco"), 	col("sistorig")
	)
m61h = m61h.withColumn("indori", lit("h"))

m61t=hive_context.table(SCHEMA_CRU+"."+INPUT_TABLE_NAME5).select(
	col("cdsinies"), 	col("ctnupoin"),	col("cdmct"),	col("cdmcr"),	col("numovimi"),
	concat_ws("-",substring(col("feoperac"),1,4),substring(col("feoperac"),5,2),substring(col("feoperac"),7,2)).cast("timestamp").alias("feoperac_fch"),
	col("tctipmov"), 	trim(col("tccdmone")).alias("tccdmone"),	col("imestima").cast("float").alias("imestima"),
	col("imsalvad").cast("float").alias("imsalvad"),	col("imporsal").cast("float").alias("imporsal"),	col("immovimi").cast("float").alias("immovimi"),
	col("inreisal"), 	col("infacsal"),	col("numoveco"), 	col("sistorig")
	)
m61t = m61t.withColumn("indori", lit("t")) 

def loadUniverse(name):
		t = hive_context.table(name+"t")
		h = hive_context.table(name+"h")
		u = h.unionAll(t)
		return u	

m10 = loadUniverse(SCHEMA_CRU+"."+INPUT_TABLE_NAME6)

m10=m10.select (
	col("cdsinies").alias("cdsinies10")
).distinct()

ab = [m61t.cdsinies == m10.cdsinies10]
a =  m61t.join(m10,ab,"inner")
a = a.drop(col("cdsinies10"))
bc = [m61h.cdsinies == m10.cdsinies10]
b =  m61h.join(m10,bc,"inner")
b = b.drop(col("cdsinies10"))
c = a.unionAll(b)
	
		
def origen (x):
	desc_inreisal_calc = ""
	if x["inreisal"] == "S":
		desc_inreisal_calc = "Alta reintegro/salvamento"
	elif x["inreisal"] == "N":
		desc_inreisal_calc = "Anulacion reintegro"
	elif x["inreisal"] == "A":
		desc_inreisal_calc = "Anulacion salvamento"
	desc_infacsal_cal = ""	
	if x["infacsal"] == "N":
		desc_inreisal_calc = "Factura cancelada"
	elif x["infacsal"] == "S":
		desc_inreisal_calc = "Factura"
	elif x["infacsal"] == "C":
		desc_inreisal_calc = "Factura cobrada"
	tabori = "ksim"+x["indori"]
	return(
		x["cdsinies"],
		x["ctnupoin"],
		x["cdmct"],
		x["cdmcr"],
		x["numovimi"],
		x["feoperac_fch"],
		x["tctipmov"],
		x["tccdmone"],
		float(x["imestima"]),
		float(x["imsalvad"]),
		float(x["imporsal"]),
		float(x["immovimi"]),
		x["inreisal"],
		desc_inreisal_calc,
		x["infacsal"],
		desc_infacsal_cal,
		x["numoveco"],
		tabori
	)
d = c.rdd.map(origen)
e = hive_context.createDataFrame(d,structuraD)

ktc = [e.tctipmov == get.tctipmov_salv]
ktctget = e.join(get,ktc,"left")
#ktctget = ktctget.drop(col("cdtabla"))


cob  = [ktctget.cdmct == cobafe.clave_cobertura_tarificable,ktctget.cdmcr ==  cobafe.clave_cobertura_afecta] 
coberturas  = ktctget.join(cobafe,cob,"left")
coberturas  = coberturas.drop(col("nombre_cobertura_afecta"))
tip = [coberturas.tccdmone == tipcamb.from_cur, coberturas.feoperac_fch == tipcamb.fecha]
tipoCab = coberturas.join(tipcamb,tip,"left")
#tipoCab = tipoCab.filter(col("cdsinies") == "0004457099")
def imCalc (y):
	immovimi_calc = 0.0
	if y["inreisal"] == "N" and y["tctipmov"] in ("3","4","6"):
		immovimi_calc = float(y["immovimi"]) * -1.0

	imestima_calc = 0.0
	if y["inreisal"] == "N" and y["tctipmov"] in ("3","4","6"):
		imestima_calc = float(y["imestima"]) * -1.0	

	imsalvad_calc = 0.0
	if y["inreisal"] == "N" and y["tctipmov"] in ("3","4","6"):
		imsalvad_calc = float(y["imsalvad"]) * -1.0	

	imporsal_calc = 0.0
	if y["inreisal"] == "N" and y["tctipmov"] in ("3","4","6"):
		imporsal_calc = float(y["imporsal"]) * -1.0		
	

	rate_mult = y["rate_mult2"]
	if y["tccdmone"] == "MXP":
		rate_mult = 1.0

	immovimi_int  = float(y["immovimi"]) * rate_mult
	immovimi_calc_int = immovimi_calc *  rate_mult
	imestima_int = float(y["imestima"]) * rate_mult
	imestima_calc_int = imestima_calc * rate_mult
	imsalvad_int = float(y["imsalvad"]) * rate_mult
	imsalvad_calc_int = imsalvad_calc * rate_mult
	imporsal_int  = float(y["imporsal"]) * rate_mult
	imporsal_calc_int = imporsal_calc * rate_mult
	sistorig = "INFO"
	return(
		y["cdsinies"],
		y["ctnupoin"],
		y["numovimi"],
		y["numoveco"],
		y["cdmct"],
		y["descripcion_cobertura_tarificable"],
		y["cdmcr"],
		y["descripcion_cobertura_afecta"],
		y["cdmcr_agr"],
		y["feoperac_fch"],
		y["tctipmov_salv"],
		y["dstipmov_salv"],
		y["tccdmone"],
		rate_mult,
		float(y["immovimi"]),
		immovimi_int,
		immovimi_calc,
		immovimi_calc_int,
		float(y["imestima"]),
		imestima_int,
		imestima_calc,
		imestima_calc_int,
		float(y["imsalvad"]),
		imsalvad_int,
		imsalvad_calc,
		imsalvad_calc_int,
		float(y["imporsal"]),
		imporsal_int,
		imporsal_calc,
		imporsal_calc_int,
		y["inreisal"],
		y["desc_inreisal_calc"],
		y["infacsal"],
		y["desc_infacsal_cal"],
		y["tabori"],
		sistorig
	)

f = tipoCab.rdd.map(imCalc)
g = hive_context.createDataFrame(f,structuraF)
g.show()
#g.write.format("hive").mode("append").saveAsTable("bddldes.aut_salvamento_estimado_tst")
