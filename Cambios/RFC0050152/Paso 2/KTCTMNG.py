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
import pyspark
from pyspark                import SparkContext
from pyspark.sql            import HiveContext
import pyspark.sql.functions as F


reload(sys)
sys.setdefaultencoding('utf8')
conf = pyspark.SparkConf().setAll([("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true"),("spark.sql.session.timeZone", "America/Mexico_City")])
sc = SparkContext.getOrCreate(conf=conf)
hc = HiveContext(sc)

def ktc(tabla):
    a = hc.table("bddlcru.ktctget").filter(F.trim(F.col("cdtabla")) == 'KTCTMNG').select(F.col("cdelemen").alias("cve_moneda1"), F.col("dselemen").alias("desc_moneda"))
    b = hc.table(tabla)
    c = [F.trim(b.cve_moneda) == F.trim(a.cve_moneda1)]
    u = b.join(a, c, "Left").distinct()
    return u