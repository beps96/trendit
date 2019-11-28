set -xv
#!/bin/bash
######################################################################
# Nombre del shell: sh_mail_cifras_control.sh			     #
# Objetivo: Invoca y Ejecuta WKF de oozie               	     #
# Autor:  Arturo Ramirez                                             #
# Vía de ejecución: sh/Control M (Bash sh)                           #
# Fecha de creación: 27 de Noviembre de 2019                         #
# Area: Soporte                                                      #
# Salida: Email con cifras Control				     #
######################################################################


###########################################################################
#Definicion de Consultas
###########################################################################
export TMP=/archivos-desa/AUTOS/DL_AUTOS/aut_maestra_coberturas_y_fechas_contables/tmp
TABLAS=(bddlapr.aut_siniestro_poliza bddlapr.aut_siniestro_cob_afecta)
CAMPO=fch_contable

touch $TMP/cc_html.txt
cat /dev/null > $TMP/cc_html.txt


for TABLA in ${TABLAS[@]}; do
echo "<tr><th>$TABLA</th>" >> $TMP/cc_html.txt
hive --silent -e "SELECT CONCAT ('<th>',CAST(format_number (COUNT (1), 0) AS STRING),'</th>') FROM $TABLA;" > $TMP/cifras_rows.txt
awk 'NR==1' $TMP/cifras_rows.txt >> $TMP/cc_html.txt
hive --silent -e "SELECT CONCAT ('<th>',CAST(DATE_FORMAT(MAX($CAMPO), 'YYYY-MM-d') AS STRING),'</th>') FROM $TABLA;" > $TMP/cifras_date.txt
awk 'NR==1' $TMP/cifras_date.txt >> $TMP/cc_html.txt
done


###########################################################################
#Variables de ambiente para obtener parametros necesarios en la ejecucion #
###########################################################################

export INI=/archivos-desa/AUTOS/DL_AUTOS/aut_maestra_coberturas_y_fechas_contables/shells
cd ${INI}
export OOZIE_URL="http://10.67.53.21:11000/oozie"
export CFG=$(cd ..; pwd)/config/
export WSPACE="1574813752.67"
export FECHA=$(date)
export DB_BDDLALM=bddlalm
export T_aut_primas=aut_primas
export T_aut_coaseguro=aut_coaseguro
export T_aut_comision=aut_comision
export T_aut_servicio_conexo=aut_servicio_conexo
export T_aut_udi=aut_udi
export T_aut_udis_conexos=aut_udis_conexos
export T_aut_comision_servicios_conexos=aut_comision_servicios_conexos
export DB_BDDLTRN=bddltrn
export T_aut_maestra_coberturas_y_fechas_contables_spark_ant=aut_maestra_coberturas_y_fechas_contables_spark_ant
#export MAIL=arturo.ramirezhurtado@gnp.com.mx,alan.esquivelreyes@gnp.com.mx,gustavo.victoria@gnp.com.mx
export MAIL=arturo.ramirezhurtado@gnp.com.mx
export ELEMENTO=DBM4
export SUBJECT="Cifras Control $ELEMENTO"
export CUERPO=`cat $TMP/cc_html.txt`


##############################################################################################
#Funcion que genera el archivo job.properties con los parametros necesarios para su ejecucion#
##############################################################################################

parametriza()
{
echo "oozie.use.system.libpath=True" > ../config/job.properties
echo "security_enabled=False" >> ../config/job.properties
echo "dryrun=False" >> ../config/job.properties
echo "nameNode=hdfs://nameservice1" >> ../config/job.properties
echo "jobTracker=yarnRM" >> ../config/job.properties
echo "FECHA"=$FECHA >> ../config/job.properties
echo "DB_BDDLALM"=$DB_BDDLALM >> ../config/job.properties
echo "T_aut_primas"=$T_aut_primas >> ../config/job.properties
echo "T_aut_coaseguro"=$T_aut_coaseguro >> ../config/job.properties
echo "T_aut_comision"=$T_aut_comision >> ../config/job.properties
echo "T_aut_servicio_conexo"=$T_aut_servicio_conexo >> ../config/job.properties
echo "T_aut_udi"=$T_aut_udi >> ../config/job.properties
echo "T_aut_udis_conexos"=$T_aut_udis_conexos >> ../config/job.properties
echo "T_aut_comision_servicios_conexos"=$T_aut_comision_servicios_conexos >> ../config/job.properties
echo "DB_BDDLTRN"=$DB_BDDLTRN >> ../config/job.properties
echo "T_aut_maestra_coberturas_y_fechas_contables_spark_ant"=$T_aut_maestra_coberturas_y_fechas_contables_spark_ant >> ../config/job.properties
echo "MAIL"=$MAIL >> ../config/job.properties
echo "ELEMENTO"=$ELEMENTO >> ../config/job.properties
echo "SUBJECT"=$SUBJECT >> ../config/job.properties
echo "CUERPO"=$CUERPO >> ../config/job.properties
}


############################################
# Funcion que ejecuta el workflow de oozie #
############################################

ejecuta()
{
parametriza
echo "El proceso inicia: " $FECHA
export OBTENJOB=$(oozie job -oozie http://10.67.53.21:11000/oozie  -run -D oozie.wf.application.path hdfs://nameservice1/user/hue/oozie/workspaces/hue-oozie-${WSPACE}/workflow.xml -config ${CFG}job.properties -run)

echo "El id del workflow es: " $OBTENJOB
echo $OBTENJOB > $TMP/oozie_job.txt

export OOZIEJOB=$(cat $TMP/oozie_job.txt | awk -F' ' '{ print $2 }' | tr -d '[:space:]' )
echo "El id del workflow es: " $OOZIEJOB
echo "Esperaremos 3 segundos " 
sleep 3
}

###########################################################################################################
# Funcion que monitorea la ejecucion del workflow de oozie y espera la finalizacion para terminar el shell#
###########################################################################################################
monitor()
{
jobOozie=$1


oozie job -info ${jobOozie} | tail -3 | head -1 > $TMP/estatus.txt

estatusJob=$(cat $TMP/estatus.txt | awk -F' ' '{ print $2 }' | tr -d '[:space:]' )

while [ "$estatusJob" == "RUNNING" ];
do
       oozie job -info ${jobOozie} | head -5 | tail -1 > $TMP/estatus.txt
       estatusJob=$(cat $TMP/estatus.txt | awk -F' ' '{ print $3 }' | tr -d '[:space:]') 
##En caso de seguir el job en ejecucion se tendra una espera de 20seg para pregunatr nuevamente
       echo "El workflow sigue corriendo... tendrás que esperar " `date +%Y-%m-%d_%H:%M`
       sleep 120
done

if [ "$estatusJob" == "SUCCEEDED" ] 
then 
echo "El Job Termino Correctamente con STATUS: " $estatusJob
exit 0 
fi

if [ "$estatusJob" == "SUSPENDED" ] 
then 
echo "Se ha producido un error STATUS: " $estatusJob
exit 1
fi

if [ "$estatusJob" == "KILLED" ] 
then 
echo "Se ha producido un error STATUS: " $estatusJob
exit 1
fi

if [ "$estatusJob" == "FAILED" ] 
then 
echo "Se ha producido un error STATUS: " $estatusJob
exit 1
fi

echo "El estatus es : " $estatusJob
echo "El workflow termina " `date +%Y-%m-%d_%H:%M`

########################################
#Obtiene el estatus final del workflow #
########################################

oozie job -info ${jobOozie} | sed -n 's/Status\(.*\): \(.*\)/\2/p' > $TMP/resultado.txt
       estatusJob=$(grep SUCCEEDED $TMP/resultado.txt)

if [ "$estatusJob" != "SUCCEEDED" ];
then 

  echo "El proceso NO se ejecuto correctamente. El estatus es:" $(cat $TMP/resultado.txt)
  exit 1
fi

echo "El estatus es : " $estatusJob
echo "El workflow termina " `date +%Y-%m-%d_%H:%M`

}
###########################################################################
# Inicia la orquestacion del proceso de ejecucion del workflow de oozie   #
###########################################################################
ejecuta
monitor $OOZIEJOB
