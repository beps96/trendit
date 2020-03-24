#!/bin/bash
####################################################################################
## Script para obtener los parametros de ejecucion de primas de GMM               ##
## DATE: 13/Enero/2020                                                            ##
## UPDATE:                                                                        ##
## AUTHOR: Luis Fernando Tejas Hernandez                                          ##
## USAGE: execSparkPrimas.sh <mainClass> <user>                                   ##
####################################################################################
set -e
#HADOOP_USER_NAME=lttejast
HADOOP_USER_NAME=$2
export HADOOP_USER_NAME
if [ ${1} = "mx.com.gnp.gmm.prima.pagada.PolizaAseguradoCobertura.info.cnm_prima_pagada_poliza_asegurado_cobertura_info" ]
then
    #Parametros de ejecucion
    configFilePath=/transf/DL_GMM/cobranza/prima_pagada_asegurado_cobertura/config/paramInfoCnmPrimaPagadaPolizaAseguradoCobertura.ini

    #Asignacion de recursos de ejecucion
    driverMemory=20G
    executorMemory=16G
    numExecutors=20
    executorCores=4
    driverCores=4
    maxExecutors=60
    maxResultSize=0

    #Parametros de configuracion
    appName=GMM_DLK_Cobranza_Prima_Pagada_Poliza_Asegurado_Cobertura_INFO
    mainClass=mx.com.gnp.gmm.prima.pagada.PolizaAseguradoCobertura.info.cnm_prima_pagada_poliza_asegurado_cobertura_info
    scriptSpark=hdfs://nameservice1/transf/DL_GMM/cobranza/prima_pagada_asegurado_cobertura/spark/gmm-cobranza-cnmprimapagadapolizaaseguradocobertura_2.11-1.0.jar
elif [ ${1} = "mx.com.gnp.gmm.prima.pagada.PolizaAseguradoCobertura.cbza.cnm_prima_pagada_poliza_asegurado_cobertura_cbza" ]
then
    #Parametros de ejecucion
    configFilePath=/transf/DL_GMM/cobranza/prima_pagada_asegurado_cobertura/config/paramCbzaCnmPrimaPagadaPolizaAseguradoCobertura.ini

    #Asignacion de recursos de ejecucion
    driverMemory=20G
    executorMemory=16G
    numExecutors=20
    executorCores=4
    driverCores=4
    maxExecutors=40
    maxResultSize=0

    #Parametros de configuracion
    appName=GMM_DLK_Cobranza_Prima_Pagada_Poliza_Asegurado_Cobertura_CBZA
    mainClass=mx.com.gnp.gmm.prima.pagada.PolizaAseguradoCobertura.cbza.cnm_prima_pagada_poliza_asegurado_cobertura_cbza
    scriptSpark=hdfs://nameservice1/transf/DL_GMM/cobranza/prima_pagada_asegurado_cobertura/spark/gmm-cobranza-cnmprimapagadapolizaaseguradocobertura_2.11-1.0.jar
elif [ ${1} = "mx.com.gnp.gmm.prima.pagada.PolizaAseguradoCobertura.union.cnm_prima_pagada_poliza_asegurado_cobertura_union" ]
then
    #Parametros de ejecucion
    configFilePath=/transf/DL_GMM/cobranza/prima_pagada_asegurado_cobertura/config/paramCnmPrimaPagadaPolizaAseguradoCobertura.ini

    #Asignacion de recursos de ejecucion
    driverMemory=20G
    executorMemory=16G
    numExecutors=20
    executorCores=4
    driverCores=4
    maxExecutors=40
    maxResultSize=0

    #Parametros de configuracion
    appName=GMM_DLK_Cobranza_Prima_Pagada_Poliza_Asegurado_Cobertura_Union
    mainClass=mx.com.gnp.gmm.prima.pagada.PolizaAseguradoCobertura.union.cnm_prima_pagada_poliza_asegurado_cobertura_union
    scriptSpark=hdfs://nameservice1/transf/DL_GMM/cobranza/prima_pagada_asegurado_cobertura/spark/gmm-cobranza-cnmprimapagadapolizaaseguradocobertura_2.11-1.0.jar
elif [ ${1} = "mx.com.gnp.gmm.prima.pagada.PolizaAseguradoCobertura.info.delta.cnm_prima_pagada_poliza_asegurado_cobertura_info_delta" ]
then
    #Parametros de ejecucion
    configFilePath=/transf/DL_GMM/cobranza/prima_pagada_asegurado_cobertura/config/paramInfoCnmPrimaPagadaPolizaAseguradoCobertura.ini

    #Asignacion de recursos de ejecucion
    driverMemory=20G
    executorMemory=16G
    numExecutors=20
    executorCores=4
    driverCores=4
    maxExecutors=40
    maxResultSize=0

    #Parametros de configuracion
    appName=GMM_DLK_Cobranza_Prima_Pagada_Poliza_Asegurado_Cobertura_Info_Delta
    mainClass=mx.com.gnp.gmm.prima.pagada.PolizaAseguradoCobertura.info.delta.cnm_prima_pagada_poliza_asegurado_cobertura_info_delta
    scriptSpark=hdfs://nameservice1/transf/DL_GMM/cobranza/prima_pagada_asegurado_cobertura/spark/gmm-cobranza-cnmprimapagadapolizaaseguradocobertura_2.11-1.0.jar
elif [ ${1} = "mx.com.gnp.gmm.prima.pagada.PolizaAseguradoCobertura.cbza.delta.cnm_prima_pagada_poliza_asegurado_cobertura_cbza_delta" ]
then
    #Parametros de ejecucion
    configFilePath=/transf/DL_GMM/cobranza/prima_pagada_asegurado_cobertura/config/paramCbzaCnmPrimaPagadaPolizaAseguradoCobertura.ini

    #Asignacion de recursos de ejecucion
    driverMemory=20G
    executorMemory=16G
    numExecutors=20
    executorCores=4
    driverCores=4
    maxExecutors=40
    maxResultSize=0

    #Parametros de configuracion
    appName=GMM_DLK_Cobranza_Prima_Pagada_Poliza_Asegurado_Cobertura_Cbza_Delta
    mainClass=mx.com.gnp.gmm.prima.pagada.PolizaAseguradoCobertura.cbza.delta.cnm_prima_pagada_poliza_asegurado_cobertura_cbza_delta
    scriptSpark=hdfs://nameservice1/transf/DL_GMM/cobranza/prima_pagada_asegurado_cobertura/spark/gmm-cobranza-cnmprimapagadapolizaaseguradocobertura_2.11-1.0.jar
elif [ ${1} = "mx.com.gnp.gmm.base.emitida.cbza.enr_cobranza_emitida_pol_end_base" ]
then
    #Parametros de ejecucion
    configFilePath=/transf/DL_GMM/cobranza/base_poliza_endoso/config/paramEnrCobranzaEmitidaPolEndBase.ini

    #Asignacion de recursos de ejecucion
    driverMemory=20G
    executorMemory=16G
    numExecutors=20
    executorCores=4
    driverCores=4
    maxExecutors=40
    maxResultSize=0

    #Parametros de configuracion
    appName=GMM_DLK_Cobranza_Emitida_Pol_End_Base
    mainClass=mx.com.gnp.gmm.base.emitida.cbza.enr_cobranza_emitida_pol_end_base
    scriptSpark=hdfs://nameservice1/transf/DL_GMM/cobranza/spark/gmm-cobranza-cnm_prima_emitida_poliza_endoso_cbza_2.11-1.0.jar
elif [ ${1} = "mx.com.gnp.gmm.base.emitida.cbza.delta.enr_cobranza_emitida_pol_end_base_delta" ]
then
    #Parametros de ejecucion
    configFilePath=/transf/DL_GMM/cobranza/base_poliza_endoso/config/paramEnrCobranzaEmitidaPolEndBase.ini

    #Asignacion de recursos de ejecucion
    driverMemory=20G
    executorMemory=16G
    numExecutors=20
    executorCores=4
    driverCores=4
    maxExecutors=40
    maxResultSize=0

    #Parametros de configuracion
    appName=GMM_DLK_Cobranza_Emitida_Pol_End_Base_Delta
    mainClass=mx.com.gnp.gmm.base.emitida.cbza.delta.enr_cobranza_emitida_pol_end_base_delta
    scriptSpark=hdfs://nameservice1/transf/DL_GMM/cobranza/spark/gmm-cobranza-cnm_prima_emitida_poliza_endoso_cbza_2.11-1.0.jar
elif [ ${1} = "mx.com.gnp.gmm.prima.emitida.PolizaEndoso.cbza.cnm_prima_emitida_poliza_endoso_cbza" ]
then
    #Parametros de ejecucion
    configFilePath=/transf/DL_GMM/cobranza/prima_emitida_poliza_endoso/config/paramCnmPrimaEmitidaPolizaEndoso.ini

    #Asignacion de recursos de ejecucion
    driverMemory=20G
    executorMemory=16G
    numExecutors=20
    executorCores=4
    driverCores=4
    maxExecutors=40
    maxResultSize=0

    #Parametros de configuracion
    appName=GMM_DLK_Cobranza_Prima_Emitida_Poliza_Endoso_CBZA
    mainClass=mx.com.gnp.gmm.prima.emitida.PolizaEndoso.cbza.cnm_prima_emitida_poliza_endoso_cbza
    scriptSpark=hdfs://nameservice1/transf/DL_GMM/cobranza/spark/gmm-cobranza-cnm_prima_emitida_poliza_endoso_cbza_2.11-1.0.jar
elif [ ${1} = "mx.com.gnp.gmm.cobranza.genericos.ventaEstadistico" ]
then
    #Parametros de ejecucion
    echo "Parametro 4: $4"
    configFilePath="${4} ${3} ${6}"

    #Asignacion de recursos de ejecucion
    driverMemory=20G
    executorMemory=16G
    numExecutors=20
    executorCores=4
    driverCores=4
    maxExecutors=40
    maxResultSize=0

    #Parametros de configuracion
    appName=GMM_DLK_VentaEstadistico
    mainClass=mx.com.gnp.gmm.cobranza.genericos.ventaEstadistico
    scriptSpark=hdfs://nameservice1/transf/DL_GMM/cobranza/spark/gmm-cobranza-genericos_2.11-1.0.jar
else
    echo "errorGetContext=CLASS_NOT_FOUND"
    exit 1
fi

echo "appName=$appName"
echo "mainClass=$mainClass"
echo "configFilePath=$configFilePath"

localPath=/etc/hive/conf/
jarGMMGenerico=hdfs://nameservice1/transf/DL_GMM/cobranza/spark/gmm-cobranza-genericos_2.11-1.0.jar

echo "spark2-submit --queue root.gmm --class "${1}" --driver-class-path ${localPath} --master yarn --deploy-mode cluster --driver-memory ${driverMemory} --num-executors ${numExecutors} --executor-memory ${executorMemory} --executor-cores ${executorCores} --driver-cores ${driverCores} --conf spark.dynamicAllocation.maxExecutors=${maxExecutors} --conf spark.driver.maxResultSize=${maxResultSize} --conf spark.shuffle.service.enabled=true --jars ${jarGMMGenerico} ${scriptSpark} ${configFilePath}"

spark2-submit --queue root.gmm --class "${1}" --driver-class-path ${localPath} --master yarn --deploy-mode cluster --driver-memory ${driverMemory} --num-executors ${numExecutors} --executor-memory ${executorMemory} --executor-cores ${executorCores} --driver-cores ${driverCores} --conf spark.dynamicAllocation.maxExecutors=${maxExecutors} --conf spark.driver.maxResultSize=${maxResultSize} --conf spark.shuffle.service.enabled=true --jars ${jarGMMGenerico} ${scriptSpark} ${configFilePath}
