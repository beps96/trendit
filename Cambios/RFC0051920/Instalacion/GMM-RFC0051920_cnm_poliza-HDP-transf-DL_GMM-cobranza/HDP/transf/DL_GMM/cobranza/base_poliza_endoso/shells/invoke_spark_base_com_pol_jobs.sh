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

ComisionPolEndHomePath="hdfs://nameservice1/transf/DL_GMM/cobranza/base_poliza_endoso"

if [ ${1} == "ComisionesBase.info.GeneraBasePolizaEndosoInfo" ]
then
    #Parametros de ejecucion


    #Asignacion de recursos de ejecucion
    executorMemory=16G
    numExecutors=2
    executorCores=5
    maxExecutors=25	


	spark2-submit --class ComisionesBase.info.GeneraBasePolizaEndosoInfo\
                        --master yarn \
			--queue root.gmm\
                        --num-executors ${numExecutors} \
                        --executor-cores ${executorCores}  --executor-memory ${executorMemory} --conf spark.dynamicAllocation.maxExecutors=${maxExecutors} \
                        --driver-class-path /etc/hive/conf  \
                        --conf "spark.shuffle.service.enabled=true" \
                        ${ComisionPolEndHomePath}/spark/gmm-cobranza-enr_comisiones_emitidas_pagadas_info_2.11-1.0.jar




elif [ ${1} == "ComisionesEmitidas.info.AgrupaComisionEmitidaPolizaEndosoInfo" ]
then

    #Parametros de ejecucion


    #Asignacion de recursos de ejecucion
    executorMemory=16G
    numExecutors=2
    executorCores=5
    maxExecutors=25

        spark2-submit --class ComisionesEmitidas.info.AgrupaComisionEmitidaPolizaEndosoInfo\
                        --master yarn \
			--queue root.gmm\
                        --num-executors ${numExecutors} \
                        --executor-cores ${executorCores}  --executor-memory ${executorMemory} --conf spark.dynamicAllocation.maxExecutors=${maxExecutors} \
                        --driver-class-path /etc/hive/conf  \
                        --conf "spark.shuffle.service.enabled=true" \
						${ComisionPolEndHomePath}/spark/gmm-cobranza-enr_comisiones_emitidas_pagadas_info_2.11-1.0.jar



elif [ ${1} == "ComisionesPagadas.info.AgrupaComisionPagadaPolizaEndosoInfo" ]
then

    #Parametros de ejecucion


    #Asignacion de recursos de ejecucion
    executorMemory=16G
    numExecutors=2
    executorCores=5
   maxExecutors=25

        spark2-submit --class ComisionesPagadas.info.AgrupaComisionPagadaPolizaEndosoInfo\
                        --master yarn \
			--queue root.gmm\
                        --num-executors ${numExecutors} \
                        --executor-cores ${executorCores}  --executor-memory ${executorMemory} --conf spark.dynamicAllocation.maxExecutors=${maxExecutors} \
                        --driver-class-path /etc/hive/conf  \
                        --conf "spark.shuffle.service.enabled=true" \
                        ${ComisionPolEndHomePath}/spark/gmm-cobranza-enr_comisiones_emitidas_pagadas_info_2.11-1.0.jar



else
    echo "errorGetContext=CLASS_NOT_FOUND"
    exit 1
fi

