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
HADOOP_USER_NAME=hdfs hdfs dfs -copyToLocal /user/usrdes01/Scripts_usrdes01/Siniestros/python/salvamentos_union.py /tmp/paso/
chmod 777 /tmp/paso/salvamentos_union.py
HADOOP_USER_NAME=usrdes01 spark2-submit --master yarn --driver-memory 2G --executor-memory 4G --num-executors 30 --driver-class-path /etc/hive/conf/ /tmp/paso/salvamentos_union.py bddlalm.aut_reserva_salvamentos bddldes.aut_siniestro_cob_afecta_insumo_nvos_parquet bddldes aut_siniestro_cob_afecta_insumo_comp


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