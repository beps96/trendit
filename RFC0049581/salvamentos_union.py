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


reload(sys)
sys.setdefaultencoding('utf8')
sc = SparkContext.getOrCreate()
hc = HiveContext(sc)

rva_sal = hc.table(str(sys.argv[1]))
ins_nvo = hc.table(str(sys.argv[2]))
rva_comp = rva_sal.unionAll(ins_nvo)
rva_comp.write.format("parquet").mode("overwrite").saveAsTable("bddldes.aut_siniestro_cob_afecta_insumo_comp")
