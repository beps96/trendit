#!/bin/bash
####################################################################################
## Script para obtener los parametros de ejecucion de primas de GMM               ##
## DATE: 23/Enero/2020                                                            ##
## UPDATE:                                                                        ##
## AUTHOR: Luis Fernando Tejas Hernandez                                          ##
## USAGE: execSparkPrimas.sh <mainClass>                                          ##
####################################################################################
set -e
HADOOP_USER_NAME=lttejast
export HADOOP_USER_NAME
if [ ${1} = "enr_cobranza_emitida_pol_end_base" ]
then
    #Parametros de ejecucion
    configFilePath=/transf/DL_GMM/cobranza/base_poliza_endoso/config/paramEnrCobranzaEmitidaPolEndBase.ini

    #Asignacion de recursos de ejecucion
    driverMemory=20G
    executorMemory=16G
    numExecutors=20
    executorCores=3
    driverCores=4
    maxExecutors=25
    maxResultSize=0

    #Parametros de configuracion
    appName=GMM_DLK_Cobranza_Prima_Pagada_Poliza_Asegurado_Cobertura_INFO
    mainClass=cnm_prima_pagada_poliza_asegurado_cobertura_info
elif [ ${1} = "cnm_prima_pagada_poliza_asegurado_cobertura_cbza" ]
then
    #Parametros de ejecucion
    configFilePath=/transf/DL_GMM/cobranza/prima_pagada_asegurado_cobertura/config/paramCbzaCnmPrimaPagadaPolizaAseguradoCobertura.ini

    #Asignacion de recursos de ejecucion
    driverMemory=20G
    executorMemory=16G
    numExecutors=20
    executorCores=3
    driverCores=4
    maxExecutors=25
    maxResultSize=0

    #Parametros de configuracion
    appName=GMM_DLK_Cobranza_Prima_Pagada_Poliza_Asegurado_Cobertura_CBZA
    mainClass=cnm_prima_pagada_poliza_asegurado_cobertura_cbza
elif [ ${1} = "cnm_prima_pagada_poliza_asegurado_cobertura_union" ]
then
    #Parametros de ejecucion
    configFilePath=/transf/DL_GMM/cobranza/prima_pagada_asegurado_cobertura/config/paramCnmPrimaPagadaPolizaAseguradoCobertura.ini

    #Asignacion de recursos de ejecucion
    driverMemory=20G
    executorMemory=16G
    numExecutors=20
    executorCores=3
    driverCores=4
    maxExecutors=25
    maxResultSize=0

    #Parametros de configuracion
    appName=GMM_DLK_Cobranza_Prima_Pagada_Poliza_Asegurado_Cobertura_Union
    mainClass=cnm_prima_pagada_poliza_asegurado_cobertura_union
else
    echo "errorGetContext=CLASS_NOT_FOUND"
    exit 1
fi

echo "appName=$appName"
echo "mainClass=$mainClass"

localPath=/etc/hive/conf/
scriptSpark=hdfs://nameservice1/transf/DL_GMM/cobranza/base_poliza_endoso/spark/gmm-cobranza-cobranzaemitidapolizaendosobase_2.11-1.0.jar

echo "spark2-submit --queue root.gmm --class "${1}" --driver-class-path ${localPath} --master yarn --deploy-mode cluster --driver-memory ${driverMemory} --num-executors ${numExecutors} --executor-memory ${executorMemory} --executor-cores ${executorCores} --driver-cores ${driverCores} --conf spark.dynamicAllocation.maxExecutors=${maxExecutors} --conf spark.driver.maxResultSize=${maxResultSize} --conf spark.shuffle.service.enabled=true ${scriptSpark} ${configFilePath}"

spark2-submit --queue root.gmm --class "${1}" --driver-class-path ${localPath} --master yarn --deploy-mode cluster --driver-memory ${driverMemory} --num-executors ${numExecutors} --executor-memory ${executorMemory} --executor-cores ${executorCores} --driver-cores ${driverCores} --conf spark.dynamicAllocation.maxExecutors=${maxExecutors} --conf spark.driver.maxResultSize=${maxResultSize} --conf spark.shuffle.service.enabled=true ${scriptSpark} ${configFilePath}
