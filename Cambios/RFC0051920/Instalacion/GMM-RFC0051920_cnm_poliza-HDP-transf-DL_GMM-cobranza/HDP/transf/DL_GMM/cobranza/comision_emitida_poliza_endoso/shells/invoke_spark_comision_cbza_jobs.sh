#!/bin/bash
####################################################################################
## Script para obtener los parametros de ejecucion de Taller de Producto de GMM   ##
## DATE: 06/Septiembre/2019                                                       ##
## UPDATE: 08/Noviembre/2019                                                      ##
## AUTHOR: Luis Fernando Tejas Hernandez                                          ##
## USAGE: execTallerProductoGMM.sh <mainClass>                                    ##
####################################################################################
set -x
HADOOP_USER_NAME="${usario:=$2}"
export HADOOP_USER_NAME



if [[ ! ("$#" == 2 ) ]]; then
        echo 'Pase 2 argumentos  que sea el nombre de clase de jar por ejemplo cobertura.cbza.ComplementoCNMCobertura  y nombre de usario de hadoop como mssahusa'
        exit 1
fi

ComisionPolEndCbzaHomePath="hdfs://nameservice1/transf/DL_GMM/cobranza/comision_emitida_poliza_endoso"

if [ ${1} == "ComisionesBase.cbza.GeneraComplementoEmitidaCbza" ]
then
    #Parametros de ejecucion


    #Asignacion de recursos de ejecucion
    executorMemory=16G
    numExecutors=2
    executorCores=5
	


	spark2-submit --class ComisionesBase.cbza.GeneraComplementoEmitidaCbza\
                        --master yarn \
			--queue root.gmm\
                        --num-executors ${numExecutors} \
                        --executor-cores ${executorCores}  --executor-memory ${executorMemory}\
                        --driver-class-path /etc/hive/conf  \
                        --conf "spark.shuffle.service.enabled=true" \
                        ${ComisionPolEndCbzaHomePath}/spark/gmm-cobranza-comisiones_poliza_endoso_cbza_2.11-1.0.jar




elif [ ${1} == "ComisionesBase.cbza.GeneraComplementoPagadaCbza" ]
then

    #Parametros de ejecucion


    #Asignacion de recursos de ejecucion
    executorMemory=16G
    numExecutors=2
    executorCores=5
	

        spark2-submit --class ComisionesBase.cbza.GeneraComplementoPagadaCbza\
                        --master yarn \
			--queue root.gmm\
                        --num-executors ${numExecutors} \
                        --executor-cores ${executorCores}  --executor-memory ${executorMemory}\
                        --driver-class-path /etc/hive/conf  \
                        --conf "spark.shuffle.service.enabled=true" \
						${ComisionPolEndCbzaHomePath}/spark/gmm-cobranza-comisiones_poliza_endoso_cbza_2.11-1.0.jar



else
    echo "errorGetContext=CLASS_NOT_FOUND"
    exit 1
fi
