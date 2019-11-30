##########################################################################################
#                                                                                         #
#                            GRUPO NACIONAL PROVINCIAL S.A.B.                             #
#                 Direccion de Sistemas - Subdireccion de Analiticos y BI                 #
#                                                                                         #
#  NOMBRE DEL ENTREGABLE: Salvamentos estimados para siniestros autos (basado en la       #
#                            tabla KSIM61) de INFO.                                       #
#  PLATAFORMA DE INSTALACION: N/A                                                         #
#  AUTOR(ES):           Roman Rodriguez Cabrera                                           #
#  FECHA DE CREACION:   2018-Sep-03                                                       #
#  ULTIMA MODIFICACION: 2019-jul-30                                                       #
#                                                                                         #
# #########################################################################################

# encoding=utf8
import sys
from pyspark                import SparkContext
from pyspark.sql            import HiveContext
import pyspark.sql.functions as F
from pyspark.sql.types     import *
from pyspark.sql.functions import from_utc_timestamp 
import loadUniverse
import KTCTMNG
#import parametros

reload(sys)
sys.setdefaultencoding('utf8')
sc = SparkContext.getOrCreate()
hc = HiveContext(sc) 

def addString2Name(DF, s):
	l = []
	for c in DF.columns:
		l.append(c+s)
	return DF.toDF(*l)

t24=loadUniverse.TH(str(sys.argv[1])) #"bddlcru.krem24")
m13=hc.table(str(sys.argv[2])).filter((F.col("cdfiliac") != "0013071808") & (F.col("popacoac") !="0"))#"bddlcru.kcgm13t"
isinn=hc.table(str(sys.argv[3])) #Regla 9 bddlapr.aut_siniestro_cob_afecta #la buena"bddlapr.aut_siniestro_cob_afecta_insumo"
m49=hc.table(str(sys.argv[4])) #"bddlalm.aut_mec_reserva_siniestro"
m61=hc.table(str(sys.argv[5])).filter(F.col("cve_tipo_mov").isin(['1','2','3','6','9'])) #"Bddldes.aut_salvamento_estimado_cpg" """
SinCob=hc.table(str(sys.argv[3])) #"bddlapr.aut_siniestro_cob_afecta"
m20 = hc.table(str(sys.argv[6])) 
m08 = hc.table(str(sys.argv[7]))
get = hc.table(str(sys.argv[8]))
aut = hc.table(str(sys.argv[9]))
aut2 = hc.table(str(sys.argv[9]))
aut3 = hc.table(str(sys.argv[9]))
aut4 = hc.table(str(sys.argv[6]))
cvt = hc.table(str(sys.argv[10]))
ht08 = hc.table(str(sys.argv[7]))
ktpt = hc.table(str(sys.argv[11]))
ha3 = hc.table(str(sys.argv[12]))
	
#queda pendiente es la parte del coaseguro
t24=t24.select(t24.cdsinies, t24.inticoas, t24.pocoaseg).distinct()
t13=m13.filter((F.col("cdfiliac") != '0013071808') & (F.col("popacoac") != 0)).select(m13.cdsinies, m13.cdfiliac,m13.popacoac).distinct()
cond = [F.trim(t24.cdsinies) == F.trim(t13.cdsinies)]
reco = t24.join(t13,cond,"Inner").select(t24.cdsinies, t24.pocoaseg, t13.cdfiliac,
								F.when((t13.cdfiliac == '0002103504') & (F.trim(t24.inticoas ) == 'C'), m13.popacoac).otherwise(0).alias("porc_para_tomado"),
								F.when((t13.cdfiliac.isin(['0002103504','0009899754'])) & (F.trim(t24.inticoas ) == 'C'), m13.popacoac).otherwise(0).alias("porc_para_cedido")).distinct()


#Esto con las nuevas modificaciones de candido
m49 = m49.select(m49.cve_siniestro.alias("cve_siniestro1"), m49.num_movimiento.alias("num_movimiento1"), m49.fch_contable.alias("fch_contable"), m49.referencia_detalle.substr(7,10).alias("ref_cabecera"),m49.cve_cve_operacion_contable.alias("cve_operacion_contable"), m49.desc_operacion_contable.alias("desc_operacion_contable"), m49.cve_tipo_transaccion_contable.alias("cve_tipo_transaccion_contable"), m49.desc_tipo_transaccion_contable.alias("desc_tipo_transaccion_contable"),m49.cve_cobertura.alias("cve_cobertura1"),m49.cve_cobertura_afecta.alias("cve_cobertura_afecta1"),m49.num_afectado.alias("num_afectado1"),m49.cve_moneda.alias("cve_moneda1")).filter(F.col("cve_tipo_reserva")== "S")
#m49 = m49.filter(F.col("cve_siniestro1")== "0084420546").show()
#.filter(F.col("cve_siniestro") == "0084420546").show()
m61 = KTCTMNG.ktc(str(sys.argv[5]))
m61=m61.drop(m61.cve_moneda1).drop(m61.mto_mov_calc).drop(m61.mto_mov_calc_int).drop(m61.mto_estimado_ori).drop(m61.mto_estimado_int).drop(m61.mto_estimado_calc).drop(m61.mto_estimado_calc_int).drop(m61.mto_salvado_ori).drop(m61.mto_salvado_int).drop(m61.mto_salvado_calc).drop(m61.mto_salvado_calc_int).drop(m61.mto_por_salvar_ori).drop(m61.mto_por_salvar_int).drop(m61.mto_por_salvar_calc).drop(m61.mto_por_salvar_calc_int).drop(m61.cve_factura_salvamento).drop(m61.desc_factura_salvamento).drop(m61.tabla_origen).drop(m61.sistorig).distinct()

cond = [m61.cve_siniestro == m49.cve_siniestro1, m61.num_movimiento == m49.num_movimiento1, m61.cve_cobertura == m49.cve_cobertura1, m61.cve_cobertura_afecta == m49.cve_cobertura_afecta1, m61.num_afectado == m49.num_afectado1, m61.cve_moneda == m49.cve_moneda1]
datos = m61.join(m49,cond,"left") 
#datos.select(datos.cve_siniestro,datos.num_movimiento,datos.fch_contable).distinct().show()
datos=datos.drop(datos.cve_siniestro1).drop(datos.num_movimiento1).drop(datos.cve_cobertura1).drop(datos.cve_cobertura_afecta1).drop(datos.num_afectado1).drop(datos.cve_moneda1)

cond = [F.trim(datos.cve_siniestro) == F.trim(reco.cdsinies)]
datos = datos.join(reco,cond,"Left")
datos = datos.drop(datos.cdsinies)

m20 = m20.select(m20.cve_siniestro,m20.num_afectado,F.trim(m20.cve_cliente_origen).alias("cve_afectado"),m20.cve_tipo_afectado,F.trim(m20.descripcion_del_objeto).alias("desc_afectado2"))
m08 = m08.select(m08.cdfiliac,F.concat_ws(" ",F.col("dnap1rzm"),F.col("dnap2rzm"),F.col("dnnomrzm")).alias("desc_afectado1"))
ktc = get.select(F.trim(get.cdelemen).alias("tcperobj"),F.trim(get.dselemen).alias("descr_tipo_afectado")).filter((F.trim(F.col("CDTABLA"))== "KSITTIG") & (F.trim(F.col("CDEMPRES"))== "0001") & (F.trim(F.col("CDIDIOMA"))== "ES"))
cond = [F.trim(m20.cve_afectado) == F.trim(m08.cdfiliac)]
desafec = m20.join(m08,cond,"left")
desafec = desafec.drop(desafec.cdfiliac)
cond  = [F.trim(desafec.cve_tipo_afectado) == F.trim(ktc.tcperobj)]
desafec = desafec.join(ktc,cond,"left")
desafec = desafec.drop(desafec.tcperobj)
desafec = desafec.select(desafec.cve_siniestro.alias("cve_siniestro3"),desafec.num_afectado.alias("num_afectado3"),F.trim(desafec.cve_tipo_afectado).alias("cve_tipo_afectado"),F.trim(desafec.descr_tipo_afectado).alias("descr_tipo_afectado"),desafec.cve_afectado,F.when((desafec.desc_afectado1.isNotNull()),desafec.desc_afectado1).otherwise(desafec.desc_afectado2).alias("desc_afectado")).distinct()			

cond = [F.trim(datos.cve_siniestro) == F.trim(desafec.cve_siniestro3),  datos.num_afectado  == desafec.num_afectado3 ]
datos = datos.join(desafec,cond,"left")
datos = datos.drop(datos.cve_siniestro3).drop(datos.num_afectado3)


aut2 = aut2.groupBy(aut2.cve_siniestro).agg(F.max(aut2.ts_ultima_modificacion).alias("ts_ultima_modificacion"))
aut = aut.select(aut.cve_siniestro.alias("cve_siniestro5"),aut.cve_situacion_siniestro,aut.ts_ultima_modificacion.alias("ts_ultima_modificacion5"))
cond = [F.trim(aut.cve_siniestro5) == F.trim(aut2.cve_siniestro),aut.ts_ultima_modificacion5.cast("string") == aut2.ts_ultima_modificacion.cast("string")]
aut_union  = aut2.join(aut,cond,"left")
aut_union = aut_union.drop(aut_union.cve_siniestro5). drop(aut_union.ts_ultima_modificacion5).drop(aut_union.ts_ultima_modificacion)


est = get.select(F.trim(get.cdelemen).alias("estsin"),F.trim(get.dselemen).alias("desc_estatus_siniestro")).filter((F.trim(F.col("CDTABLA"))== "KSITSSG") & (F.trim(F.col("CDIDIOMA"))== "ES"))
cond = [F.trim(aut_union.cve_situacion_siniestro) == F.trim(est.estsin)]
est_sin = aut_union.join(est,cond,"left")
est_sin = est_sin.drop(est_sin.estsin)
est_sin = est_sin.select(est_sin.cve_siniestro.alias("cve_siniestro4"),est_sin.cve_situacion_siniestro,F.trim(est_sin.desc_estatus_siniestro).alias("desc_estatus_siniestro"))

cond = [F.trim(datos.cve_siniestro) == F.trim(est_sin.cve_siniestro4)]
datos = datos.join(est_sin,cond,"left")
datos = datos.drop(datos.cve_siniestro4)
#.filter(F.col("cve_siniestro")== "0002892701").show()
#Resuelve unicamente los campos que obtendremos de insumos 



SinCob=SinCob.groupBy(
						SinCob.cve_siniestro.alias("cve_siniestro1"),																	
						#SinCob.num_afectado.alias("num_afectado1"),										        
						SinCob.cve_cobertura_tarificable.alias("cve_cobertura_tarificable1"),																	
						SinCob.cve_cobertura_afecta.alias("cve_cobertura_afecta1"),
						
						

					).agg(	
						#F.max(SinCob.cve_tipo_afectado).alias("cve_tipo_afectado"), 							
						#F.max(SinCob.descr_tipo_afectado).alias("descr_tipo_afectado"), 
						#F.max(SinCob.cve_afectado).alias("cve_afectado"), 										
						#F.max(SinCob.desc_afectado).alias("desc_afectado"), 
						F.max(SinCob.num_poliza).alias("num_poliza"), 											F.max(SinCob.num_version).alias("num_version"), 
						F.max(SinCob.desc_siniestro).alias("desc_siniestro"), 									
						#F.max(SinCob.indicador_tipo_perdida_total).alias("indicador_tipo_perdida_total"), 
						F.max(SinCob.fch_decretacion_pt).alias("fch_decretacion_pt"),							F.max(SinCob.cve_causa_siniestro).alias("cve_causa_siniestro"), 	
						F.max(SinCob.desc_causa_siniestro).alias("desc_causa_siniestro"), 						#F.max(SinCob.fecomuni_fch).alias("fecomuni_fch"), 
						F.max(from_utc_timestamp(SinCob.feocusin_fch, "GMT+0")).alias("feocusin_fch"),										F.max(SinCob.hora_ocurrencia_siniestro).alias("hora_ocurrencia_siniestro"), 
						#F.max(SinCob.cve_situacion_siniestro).alias("cve_situacion_siniestro"), 				
						#F.max(SinCob.desc_estatus_siniestro).alias("desc_estatus_siniestro"), 						
						F.max(SinCob.cob_contab).alias("cob_contab"), 											F.max(SinCob.desc_cob_contab).alias("desc_cob_contab"),		
						F.max(SinCob.cve_tipo_mov_siniestro).alias("cve_tipo_mov_siniestro"), 					F.max(SinCob.desc_tipo_mov_siniestro).alias("desc_tipo_mov_siniestro"), 
						F.max(SinCob.mto_tipo_mov_sin_ori).alias("mto_tipo_mov_sin_ori"), 						F.max(SinCob.mto_tipo_mov_sin_int).alias("mto_tipo_mov_sin_int"), 
				        F.max(SinCob.desc_cobertura_tarificable).alias("desc_cobertura_tarificable"), 			F.max(SinCob.cve_cobertura_afecta_agrup).alias("cve_cobertura_afecta_agrup"),					
						F.max(SinCob.cve_clase_reserva).alias("cve_clase_reserva"),							F.max(SinCob.desc_clase_reserva).alias("desc_clase_reserva"),
						F.max(SinCob.cve_tipo_mov_reserva).alias("cve_tipo_mov_reserva"),						F.max(SinCob.desc_tipo_mov_reserva).alias("desc_tipo_mov_reserva"),
						F.max(SinCob.cve_ingreso).alias("cve_ingreso"),										F.max(SinCob.tipo_ingreso).alias("tipo_ingreso"),
						F.max(SinCob.importe_recuperacion).alias("importe_recuperacion"),						F.max(SinCob.operador_ingreso).alias("operador_ingreso"),
						F.max(SinCob.num_de_recuperacion).alias("num_de_recuperacion"),						F.max(SinCob.cve_estado_recuperacion).alias("cve_estado_recuperacion"),
						F.max(SinCob.desc_estado_recuperacion).alias("desc_estado_recuperacion"),				F.max(SinCob.nom_pagador).alias("nom_pagador"),
						F.max(SinCob.num_pago).alias("num_pago"),												F.max(SinCob.cve_codigo_pago).alias("cve_codigo_pago"),
						F.max(SinCob.desc_codigo_pago).alias("desc_codigo_pago"),								F.max(SinCob.cve_tipo_pago).alias("cve_tipo_pago"),
						F.max(SinCob.desc_tipo_pago).alias("desc_tipo_pago"),									F.max(SinCob.tipo_beneficiario).alias("tipo_beneficiario"),
						F.max(SinCob.cod_beneficiario).alias("cod_beneficiario"),								F.max(SinCob.beneficiario_pago).alias("beneficiario_pago"),
						F.max(SinCob.cve_medio_pago).alias("cve_medio_pago"),									F.max(SinCob.desc_medio_pago).alias("desc_medio_pago"),
						F.max(SinCob.num_medio_pago).alias("num_medio_pago"),									F.max(SinCob.texto_pago).alias("texto_pago"),
						F.max(SinCob.importe_total_pago).alias("importe_total_pago"),							F.max(SinCob.importe_total_pago_int).alias("importe_total_pago_int"),
						F.max(SinCob.clave_de_egreso).alias("clave_de_egreso"),								F.max(SinCob.clave_de_tipo_egreso).alias("clave_de_tipo_egreso"),
						F.max(SinCob.tipo_de_egreso).alias("tipo_de_egreso"),									F.max(SinCob.fecha_emision_pago).alias("fecha_emision_pago"),
						F.max(SinCob.operador_egreso).alias("operador_egreso"),								F.max(SinCob.num_factura).alias("num_factura"),	
						F.max(SinCob.cve_subtipo_mov_siniestro).alias("cve_subtipo_mov_siniestro"),			F.max(SinCob.desc_subtipo_mov_siniestro).alias("desc_subtipo_mov_siniestro"),
						F.max(SinCob.cve_tipo_pago_siniestro).alias("cve_tipo_pago_siniestro"),				F.max(SinCob.desc_tipo_pago_siniestro).alias("desc_tipo_pago_siniestro"),
						F.max(SinCob.cve_pago).alias("cve_pago"),												F.max(SinCob.desc_pago).alias("desc_pago"),
						F.max(SinCob.cve_cve_operacion_contable).alias("cve_cve_operacion_contable"),			F.max(SinCob.cve_estatus_mov).alias("cve_estatus_mov"),
						F.max(SinCob.cve_moneda_pagos).alias("cve_moneda_pagos"),								F.max(SinCob.cve_tipo_transaccion_contable).alias("cve_tipo_transaccion_contable"),
						F.max(SinCob.desc_estatus_mov).alias("desc_estatus_mov"),								F.max(SinCob.desc_operacion_contable).alias("desc_operacion_contable"),
						F.max(SinCob.desc_situacion_mov).alias("desc_situacion_mov"),							F.max(SinCob.desc_tipo_transaccion_contable).alias("desc_tipo_transaccion_contable"),
						F.max(SinCob.fch_operacion_mov).alias("fch_operacion_mov"),							F.max(SinCob.mto_gasto_bancario).alias("mto_gasto_bancario"), 				
						F.max(SinCob.desc_armadora_tercero).alias("desc_armadora_tercero"), 					F.max(SinCob.cve_categoria_vehiculo_tercero).alias("cve_categoria_vehiculo_tercero"), 
						#F.max(SinCob.desc_categoria_vehiculo_tercero).alias("desc_categoria_vehiculo_tercero"),
						#F.max(SinCob.cve_uso_vehiculo_tercero).alias("cve_uso_vehiculo_tercero"), 
						#F.max(SinCob.desc_uso_vehiculo_tercero).alias("desc_uso_vehiculo_tercero"),			
						F.max(SinCob.cve_rfc_afectado_beneficiario).alias("cve_rfc_afectado_beneficiario"), 	
						F.max(SinCob.cve_situacion_mov).alias("cve_situacion_mov"), 															
						F.max(SinCob.subconcepto).alias("subconcepto"),										F.max(SinCob.desc_subconcepto).alias("desc_subconcepto"),
						F.max(SinCob.cve_concepto_movimiento).alias("cve_concepto_movimiento"),				F.max(SinCob.desc_concepto_movimiento).alias("desc_concepto_movimiento"),
						F.max(SinCob.cve_codigo_recuperacion).alias("cve_codigo_recuperacion"),				F.max(SinCob.desc_codigo_recuperacion).alias("desc_codigo_recuperacion"),
						F.max(SinCob.fch_cambio_mov).alias("fch_cambio_mov"),								F.max(SinCob.num_movimiento).alias("num_movimiento1"),
						#F.max(SinCob.cve_armadora_tercero).alias("cve_armadora_tercero"),					
						F.max(SinCob.ban_litigio).alias("ban_litigio"),
						#F.max(SinCob.cve_tipo_vehiculo_tercero).alias("cve_tipo_vehiculo_tercero"),
						#F.max(SinCob.desc_tipo_vehiculo_tercero).alias("desc_tipo_vehiculo_tercero"),
						#F.max(SinCob.cve_marca_vehiculo_tercero).alias("cve_marca_vehiculo_tercero"),					
						F.max(SinCob.desc_marca_vehiculo_tercero).alias("desc_marca_vehiculo_tercero"),
						#F.max(SinCob.cve_carroceria_tercero).alias("cve_carroceria_tercero"),							F.max(SinCob.desc_carroceria_tercero).alias("desc_carroceria_tercero"),							
						#F.max(SinCob.cve_modelo_vehiculo_tercero).alias("cve_modelo_vehiculo_tercero"),
						#F.max(SinCob.desc_vehiculo_tercero).alias("desc_vehiculo_tercero"),							
						#F.max(SinCob.serie_tercero).alias("serie_tercero"),											    
						F.max(SinCob.num_placas_vehiculo_tercero).alias("num_placas_vehiculo_tercero"),
						F.max(SinCob.volante_admision_tercero).alias("volante_admision_tercero"),						F.max(SinCob.danos_vehiculo_tercero).alias("danos_vehiculo_tercero"),
						F.max(SinCob.fecha_valuacion_tercero).alias("fecha_valuacion_tercero"),						    
						#F.max(SinCob.total_valuacion_tercero).alias("total_valuacion_tercero"),
						F.max(SinCob.pct_danos_tercero).alias("pct_danos_tercero"),										F.max(SinCob.numero_motor_tercero).alias("numero_motor_tercero"),
						F.max(SinCob.cve_sipac).alias("cve_sipac"),
						F.max(SinCob.cve_tipo_via).alias("cve_tipo_via"),							
						F.max(SinCob.desc_tipo_via).alias("desc_tipo_via"),												F.max(SinCob.nom_poblacion).alias("nom_poblacion"),
						F.max(SinCob.nom_provincia).alias("nom_provincia"),												F.max(SinCob.cve_postal).alias("cve_postal"),
						F.max(SinCob.cve_catastrofico).alias("cve_catastrofico"),										F.max(SinCob.desc_evento_catastrofico).alias("desc_evento_catastrofico"),	
						F.max(SinCob.ban_responsabilidad).alias("ban_responsabilidad"),
						F.max(SinCob.volante_entregado_recibido).alias("volante_entregado_recibido"),					F.max(SinCob.tipo_orden).alias("tipo_orden"),
						#F.max(SinCob.cia_aseguradora_tercero).alias("cia_aseguradora_tercero"),							
						F.max(SinCob.num_presiniestro).alias("num_presiniestro"),
						F.max(SinCob.ban_migracion_convivencia).alias("ban_migracion_convivencia"),						F.max(SinCob.fch_migracion_convivencia).alias("fch_migracion_convivencia"),
						F.max(SinCob.desc_cobertura_afecta).alias("desc_cobertura_afecta"),								F.max(SinCob.cve_cobertura_afecta_agrup).alias("cve_cobertura_afecta_agrup"),
					 	F.max(SinCob.operador_reserva).alias("operador_reserva"),										
																														F.max(from_utc_timestamp(SinCob.fecomuni_fch, "GMT+0")).alias("fecomuni_fch"),
						F.max(SinCob.operador_ingreso).alias("operador_ingreso"),										F.max(SinCob.supervisoria_ingreso_egreso).alias("supervisoria_ingreso_egreso")
						)

cond = [F.trim(datos.cve_siniestro) == F.trim(SinCob.cve_siniestro1), F.trim(datos.cve_cobertura) == F.trim(SinCob.cve_cobertura_tarificable1), F.trim(datos.cve_cobertura_afecta) == F.trim(SinCob.cve_cobertura_afecta1)]
salvamentos = datos.join(SinCob,cond,"left")
#salvamentos.select(salvamentos.fch_contable).distinct().show()
salvamentos = salvamentos.drop(salvamentos.cve_cobertura_afecta1).drop(salvamentos.cve_cobertura_tarificable1).drop(salvamentos.cve_siniestro1).drop(salvamentos.num_movimiento1).drop(salvamentos.pocoaseg)

#salvamentos = salvamentos.filter(F.col("cve_siniestro") == "0084420546").show()
#salvamentos.show()

#togas = togas.select(togas.cve_siniestro5,togas.num_afectado5,togas.cve_tipo_vehiculo_tercero,togas.desc_tipo_vehiculo_tercero,togas.cve_marca_vehiculo_tercero,togas.cve_modelo_vehiculo_tercero,
#					togas.carroceria,togas.desc_vehiculo_tercero,togas.cve_armadora_tercero,togas.cve_uso_vehiculo_tercero,togas.desc_uso_vehiculo_tercero,togas.num_serie_vehiculo_tercero,
#					togas.cve_filiacion_compania_contraria,togas.desc_categoria_vehiculo_tercero,togas.cia_aseguradora_tercero,togas.desc_carroceria_vehiculo_tercero,togas.mto_total_valuacion_tercero)

 
#final = salvamentos.join(togas,cond,"left")
#final.show()

aut4 = aut4.groupBy(aut4.cve_siniestro.alias("cve_siniestro5"),aut4.num_afectado.alias("num_afectado5")).agg(F.max(aut4.cve_tipo_vehiculo).alias("cve_tipo_vehiculo_tercero"),F.max(aut4.descr_tipo_vehiculo).alias("desc_tipo_vehiculo_tercero"),
				F.concat_ws("",F.trim(F.max(aut4.cve_tipo_vehiculo)),F.trim(F.max(aut4.clave_armadora)),F.trim(F.max(aut4.carroceria)),F.trim(F.max(aut4.cve_version_vehiculo))).alias("cve_marca_vehiculo_tercero"),F.max(aut4.modelo_vehiculo).alias("cve_modelo_vehiculo_tercero"),
				F.trim(F.max(aut4.carroceria)).alias("carroceria"),F.max(aut4.descripcion_vehiculo).alias("desc_vehiculo_tercero"),F.max(aut4.clave_armadora).alias("cve_armadora_tercero"),F.max(aut4.cve_uso_vehiculo).alias("cve_uso_vehiculo_tercero"),
				F.max(aut4.descr_uso_vehiculo).alias("desc_uso_vehiculo_tercero"),F.max(aut4.num_serie).alias("num_serie_vehiculo_tercero"),F.max(aut4.cve_filiacion_compania_contraria).alias("cve_filiacion_compania_contraria"),F.max(aut4.ban_activa_inactiva).alias("indicador_tipo_perdida_total"),)

cvt = cvt.select(cvt.tctipveh,cvt.cdarmad,cvt.cdcarro,cvt.dscatveh.alias("desc_categoria_vehiculo_tercero")).filter(F.trim(F.col("insistem"))== "INFO")
cond = [F.trim(aut4.cve_tipo_vehiculo_tercero) == F.trim(cvt.tctipveh),F.trim(aut4.cve_armadora_tercero) == F.trim(cvt.cdarmad),F.trim(aut4.carroceria) == F.trim(cvt.cdcarro)]
cat = aut4.join(cvt,cond,"left")
cat = cat.drop(cat.tctipveh).drop(cat.cdarmad).drop(cat.cdcarro)

ht08 =  ht08.select(ht08.cdfiliac,F.concat_ws(" ",F.trim(F.col("dnap1rzm")),F.trim(F.col("dnap2rzm")),F.trim(F.col("dnnomrzm"))).alias("cia_aseguradora_tercero"))
cond = [F.trim(cat.cve_filiacion_compania_contraria) == F.trim(ht08.cdfiliac)]
cia = cat.join(ht08,cond,"left")
cia = cia.drop(cia.cdfiliac) 


ktpt = ktpt.select(ktpt.tctipveh,ktpt.cdarmad,ktpt.cdcarro,F.trim(ktpt.dscarro).alias("desc_carroceria_vehiculo_tercero"))
cond = [F.trim(cia.cve_tipo_vehiculo_tercero) == F.trim(ktpt.tctipveh),F.trim(cia.cve_armadora_tercero) == F.trim(ktpt.cdarmad),F.trim(cia.carroceria) == F.trim(ktpt.cdcarro)] 
dcar = cia.join(ktpt,cond,"left")
dcar  = dcar.drop(dcar.tctipveh).drop(dcar.cdarmad).drop(dcar.cdcarro)

ha3 = ha3.select(ha3.cdsinies,ha3.ctnupoin,ha3.imtotgas.alias("mto_total_valuacion_tercero").cast("float")).distinct()
cond = [F.trim(dcar.cve_siniestro5) == F.trim(ha3.cdsinies), dcar.num_afectado5 == ha3.ctnupoin]
togas = dcar.join(ha3,cond,"left")
togas = togas.drop(togas.cdsinies).drop(togas.ctnupoin)
#togas.show()
cond = [F.trim(salvamentos.cve_siniestro) == F.trim(togas.cve_siniestro5), salvamentos.num_afectado == togas.num_afectado5]
fin = salvamentos.join(togas,cond,"left")
fin = fin.drop(fin.cve_siniestro5).drop(fin.num_afectado5)


def printSalvamentos (x):
	mto_tipo_mov_sin_ori = x["mto_mov_ori"] * -1 if x["cve_tipo_mov"] in ('1','2') else x["mto_mov_ori"]
	mto_tipo_mov_sin_int = x["mto_mov_int"] * -1 if x["cve_tipo_mov"] in ('1','2') else x["mto_mov_int"]
	mto_sin_coaseg_cedido_ori = 0.0
	if x["mto_mov_ori"] != 0 and x["porc_para_cedido"] > 0:
		if  x["cve_tipo_mov"] in ('1','2'):
			mto_sin_coaseg_cedido_ori = float( x["mto_mov_ori"] * -1) * float( x["porc_para_cedido"]/100) 
		else:
			mto_sin_coaseg_cedido_ori = float(x["mto_mov_ori"]) * float(x["porc_para_cedido"]/100)
	
	mto_sin_coaseg_cedido_int = 0.0 
	if x["mto_mov_int"] != 0 and x["porc_para_cedido"] > 0:
		if  x["cve_tipo_mov"] in ('1','2'):
			mto_sin_coaseg_cedido_int = float( x["mto_mov_int"] * -1) * float(x["porc_para_cedido"]/100) 
		else:
			mto_sin_coaseg_cedido_int = float(x["mto_mov_int"]) * float(x["porc_para_cedido"]/100)
	
	mto_sin_sincoaseg_cedido_ori = 0.0
	if x["mto_mov_ori"] != 0 and x["porc_para_cedido"] > 0:
		if  x["cve_tipo_mov"] in ('1','2'):
			mto_sin_sincoaseg_cedido_ori = float(x["mto_mov_ori"] * -1) - float((x["mto_mov_ori"] * -1) * float(x["porc_para_cedido"]/100)) 
		else: 
			mto_sin_sincoaseg_cedido_ori = float(x["mto_mov_ori"])  - (float(x["mto_mov_ori"]) * float(x["porc_para_cedido"]/100))
	
	mto_sin_sincoaseg_cedido_int = 0.0
	if x["mto_mov_int"] != 0 and x["porc_para_cedido"] > 0:
		if  x["cve_tipo_mov"] in ('1','2'):
			mto_sin_sincoaseg_cedido_int = float(x["mto_mov_int"] * -1) - float((x["mto_mov_int"] * -1) * float(x["porc_para_cedido"]/100)) 
		else: 
			mto_sin_sincoaseg_cedido_int = float(x["mto_mov_int"])  - (float(x["mto_mov_int"]) * float(x["porc_para_cedido"]/100))
	mto_sin_tomado_ori = 0.0
	if x["mto_mov_ori"] != 0 and x["porc_para_cedido"] > 0:
		if  x["cve_tipo_mov"] in ('1','2'):
			mto_sin_tomado_ori = float(x["mto_mov_ori"] * -1) * float(x["porc_para_cedido"]/100)
		else: 
			mto_sin_tomado_ori = float(x["mto_mov_ori"]) * float(x["porc_para_cedido"]/100) 	
			
	mto_sin_tomado_int = 0.0
	if x["mto_mov_int"] != 0 and x["porc_para_cedido"] > 0:
		if  x["cve_tipo_mov"] in ('1','2'):
			mto_sin_tomado_int = float(x["mto_mov_int"] * -1) * float(x["porc_para_cedido"]/100)
		else:
			mto_sin_tomado_int = float(x["mto_mov_int"]) * float(x["porc_para_cedido"]/100) 
	fecha_emision_pago = None
	indicador_tipo_perdida_total = 'S' if x["indicador_tipo_perdida_total"] in ('C','D') else 'N'
	return (	

			x["cve_siniestro"],
			x["num_afectado"],
			'R',
			'Reserva',
			mto_tipo_mov_sin_ori,
			mto_tipo_mov_sin_int,
			x["cve_tipo_afectado"],
			x["descr_tipo_afectado"],
			x["cve_afectado"],
			x["desc_afectado"],
			20000+x["num_movimiento"],
			x["cve_cobertura"],
			x["desc_cobertura"],
			x["num_poliza"],
			x["num_version"],
			x["desc_siniestro"],
			indicador_tipo_perdida_total,
			x["fch_decretacion_pt"],
			x["cve_sipac"],
			"N",
			x["cve_causa_siniestro"],
			x["desc_causa_siniestro"],
			x["fecomuni_fch"],
			x["feocusin_fch"],
			x["hora_ocurrencia_siniestro"],
			x["cve_tipo_via"],
			x["desc_tipo_via"],
			x["nom_poblacion"],
			x["nom_provincia"],
			x["cve_postal"],
			x["cve_situacion_siniestro"],
			x["desc_estatus_siniestro"],
			x["cve_catastrofico"],
			x["desc_evento_catastrofico"],
			x["cve_cobertura_afecta"],
			x["desc_cobertura_afecta"],
			x["cve_cobertura_afecta_agrup"],
			x["cob_contab"],
			x["desc_cob_contab"],
			x["fch_contable"],
			x["cve_moneda"],
			x["desc_moneda"],
			x["tipo_cambio"],
			x["ref_cabecera"],
			x["cve_tipo_vehiculo_tercero"],
			x["desc_tipo_vehiculo_tercero"],
			x["cve_marca_vehiculo_tercero"],
			x["desc_marca_vehiculo_tercero"],
			x["cve_modelo_vehiculo_tercero"],
			x["carroceria"],
			x["desc_carroceria_vehiculo_tercero"],
			x["desc_vehiculo_tercero"],
			x["numero_motor_tercero"],
			x["num_serie_vehiculo_tercero"],
			x["num_placas_vehiculo_tercero"],
			x["volante_admision_tercero"],
			x["danos_vehiculo_tercero"],
			x["fecha_valuacion_tercero"],
			x["mto_total_valuacion_tercero"],
			x["pct_danos_tercero"],
			x["ban_responsabilidad"],
			"",
			"",
			x["volante_entregado_recibido"],
			x["tipo_orden"],
			mto_sin_coaseg_cedido_ori,
			mto_sin_coaseg_cedido_int,
			mto_sin_sincoaseg_cedido_ori,
			mto_sin_sincoaseg_cedido_int,
			mto_sin_tomado_ori,
			mto_sin_tomado_int,
			'S',
			'Salvamento',
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			0.0,
			"",
			"",
			0,
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			0.0,
			0.0,
			"",
			"",
			"",
			fecha_emision_pago,
			x["cia_aseguradora_tercero"],
			"",
			"",
			"",
			x["cve_tipo_mov"],
			x["desc_tipo_mov"],
			x["cve_armadora_tercero"],
			x["desc_armadora_tercero"],
			x["cve_categoria_vehiculo_tercero"],
			x["desc_categoria_vehiculo_tercero"],
			x["cve_uso_vehiculo_tercero"],
			x["desc_uso_vehiculo_tercero"],
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"RS",
			"Reserva de Salvamento",
			x["ban_migracion_convivencia"],
			x["cve_cve_operacion_contable"],
			"",
			"",
			x["cve_rfc_afectado_beneficiario"],
			x["cve_situacion_mov"],
			x["cve_tipo_transaccion_contable"],
			"",
			x["desc_operacion_contable"],
			"",
			"",
			x["fch_cambio_mov"],
			x["fch_migracion_convivencia"],
			x["fch_operacion"],
			0.0,
			0.0,
			0.0,
			0.0,
			0.0,
			0.0,
			0.0,
			0.0,
			0.0,
			0.0,
			0.0,
			0.0,
			0.0,
			0.0,
			0,
			0,
			0,
			"",
			"",
			0,
			'INFO',
			""
	)

f = fin.rdd.map(printSalvamentos)
g = hc.createDataFrame(f,isinn.schema)

#g = g.filter(F.col("cve_siniestro") == "0000297366")
#g.show()
#get.show(15)
#m61 = m61.filter(F.col("cve_siniestro")== "0002892701").show()
#tipoCab = tipoCab.filter(col("cdsinies") == "0004457099")
#g.show(10)
#print f.take(10)
g.write.format("parquet").mode("overwrite").saveAsTable("bddldes.aut_reserva_salvamento_011119_s17")
