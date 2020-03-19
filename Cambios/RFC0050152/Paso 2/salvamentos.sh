#######################################################################################
# Archivo    : ejecuta_wf_auto.sh
# Autor      : Roman Rodriguez 
# Proposito  : Proceso de Salvamentos
# Parametros : 
# Ejecucion  : ****Preguntar la fecha y hora de ejecucion***
# Historia            : 
# Observaciones       : 
# Dependencias        : 
#######################################################################################

EJECUTA_FLUJO()

{
echo "Proceso en e j e c u c i o n ............ "

mkdir /tmp/paso
HADOOP_USER_NAME=hdfs hdfs dfs -copyToLocal /user/usrdes01/Scripts_usrdes01/Siniestros/python/Salvamentos_v3.py /tmp/paso/
HADOOP_USER_NAME=hdfs hdfs dfs -copyToLocal /user/usrdes01/Scripts_usrdes01/Siniestros/python/KTCTMNG.py /tmp/paso/
HADOOP_USER_NAME=hdfs hdfs dfs -copyToLocal /user/usrdes01/Scripts_usrdes01/Siniestros/python/loadUniverse.py /tmp/paso/
chmod 777 /tmp/paso/Salvamentos_v3.py
chmod 777 /tmp/paso/KTCTMNG.py
chmod 777 /tmp/paso/loadUniverse.py
HADOOP_USER_NAME=usrdes01 spark2-submit --master yarn --driver-memory 32G --executor-memory 32G --conf spark.yarn.executor.memoryOverhead=10G --conf spark.yarn.driver.memoryOverhead=10G --num-executors 32 --executor-cores 5 --driver-class-path /etc/hive/conf/ /tmp/paso/Salvamentos_v3.py bddlcru.krem24 bddlcru.kcgm13t bddldes.aut_siniestro_cob_afecta_insumo_nvos_parquet bddldes.aut_mec_reserva_siniestro_parquet bddldes.aut_salvamento_estimado bddlalm.aut_siniestro_afectado bddlcru.kpem08t bddlcru.ktctget bddlalm.aut_siniestro bddlcru.ksincvt bddlcru.ktpt2it bddlcru.ksim3ah bddlalm aut_reserva_salvamentos


estado=$?


ls /tmp/paso
rm -r /tmp/paso

if [ $estado -ne 0 ]; then
echo "---------------ERROR---------------"
echo "Ocurrio un error en la ejecucion del proceso "
echo "Codigo de error: $estado"

##detalleError $estado 


exit 1
else
echo "             "
echo "             "
echo "La ejecucion fue correcta"

	fi
}

# Funcion principal del shell script 
main_EJEC()
{	obtieneHoraInicio
	EJECUTA_FLUJO
	obtieneHoraFin
	}

#Llamado a ejecución inicial.
main_EJEC

echo 'Finaliza ejecucion'

exit 0
