#!/bin/bash
####################################################################################
## Script para obtener los parametros de ejecucion de Taller de Producto de GMM   ##
## DATE: 06/Septiembre/2019                                                       ##
## UPDATE: 26/Noviembre/2019                                                      ##
## AUTHOR: Luis Fernando Tejas Hernandez                                          ##
## USAGE: execTallerProductoGMM.sh <mainClass>                                    ##
####################################################################################
set -e
HADOOP_USER_NAME=usrsup
export HADOOP_USER_NAME
if [ ${1} = "PolizasDC" ]
then
    #Parametros de ejecucion
    configFilePath=/transf/DL_TallerProd/config/paramPolizaGMM.txt
    esquemaOrigen=BDDLCRU
    tablaOrigenT=KCIM16T
    tablaOrigenH=KCIM16H
    tablaCatalogo=KTPA82T
    esquemaTemporales=BDDLTRN
    esquemaDestino=BDDLALM
    tablaDestino=GMM_ELEMENTO_POLIZA

    #Asignacion de recursos de ejecucion
    driverMemory=40G
    executorMemory=16G
    numExecutors=30
    executorCores=3
    driverCores=4
    maxExecutors=100
    maxResultSize=0

    #Parametros de configuracion
    appName=GMM_DLK_TallerProducto_Polizas
    mainClass=PolizasDC
elif [ ${1} = "PolizasDC_Relleno" ]
then
    #Parametros de ejecucion
    esquemaOrigen=BDDLCRU
    tablaOrigen=KCIM06T
    esquemaDestino=BDDLALM
    tablaBase=GMM_ELEMENTO_POLIZA
    tablaDestino=GMM_ELEMENTO_POLIZA_RELLENO
    ramo=G

    #Asignacion de recursos de ejecucion
    driverMemory=20G
    executorMemory=16G
    numExecutors=20
    executorCores=3
    driverCores=4
    maxExecutors=25
    maxResultSize=0

    #Parametros de configuracion
    appName=GMM_DLK_TallerProducto_Relleno_Polizas
    mainClass=PolizasDC_Relleno
elif [ ${1} = "ObjetosDC" ]
then
    #Parametros de ejecucion
    configFilePath=/transf/DL_TallerProd/config/paramAseguradoGMM.txt
    esquemaOrigen=BDDLCRU
    tablaOrigenT=KCIM18T
    tablaOrigenH=KCIM18H
    tablaCatalogo=KTPA82T
    esquemaTemporales=BDDLTRN
    esquemaDestino=BDDLALM
    tablaDestino=GMM_ELEMENTO_OBJETO

    #Asignacion de recursos de ejecucion
    driverMemory=40G
    executorMemory=16G
    numExecutors=30
    executorCores=4
    driverCores=4
    maxExecutors=100
    maxResultSize=0

    #Parametros de configuracion
    appName=GMM_DLK_TallerProducto_Objetos
    mainClass=ObjetosDC
elif [ ${1} = "ObjetosDC_Relleno" ]
then
    #Parametros de ejecucion
    esquemaOrigen=BDDLCRU
    tablaOrigen=KCIM06T
    esquemaDestino=BDDLALM
    tablaBase=GMM_ELEMENTO_OBJETO
    tablaDestino=GMM_ELEMENTO_OBJETO_RELLENO
    ramo=G

    #Asignacion de recursos de ejecucion
    driverMemory=20G
    executorMemory=16G
    numExecutors=20
    executorCores=3
    driverCores=4
    maxExecutors=25
    maxResultSize=0

    #Parametros de configuracion
    appName=GMM_DLK_TallerProducto_Relleno_Objetos
    mainClass=ObjetosDC_Relleno
elif [ ${1} = "CoberturasDC" ]
then
    #Parametros de ejecucion
    configFilePath=/transf/DL_TallerProd/config/paramCoberturaGMM.txt
    esquemaOrigen=BDDLCRU
    tablaOrigenT=KCIM23T
    tablaOrigenH=KCIM23H
    tablaCatalogo=KTPA82T
    esquemaTemporales=BDDLTRN
    esquemaDestino=BDDLALM
    tablaDestino=GMM_ELEMENTO_COBERTURA

    #Asignacion de recursos de ejecucion
    driverMemory=40G
    executorMemory=16G
    numExecutors=30
    executorCores=4
    driverCores=4
    maxExecutors=100
    maxResultSize=0

    #Parametros de configuracion
    appName=GMM_DLK_TallerProducto_Coberturas
    mainClass=CoberturasDC
elif [ ${1} = "CoberturasDC_Relleno" ]
then
    #Parametros de ejecucion
    esquemaOrigen=BDDLCRU
    tablaOrigen=KCIM06T
    esquemaDestino=BDDLALM
    tablaBase=GMM_ELEMENTO_COBERTURA
    tablaDestino=GMM_ELEMENTO_COBERTURA_RELLENO
    ramo=G

    #Asignacion de recursos de ejecucion
    driverMemory=25G
    executorMemory=16G
    numExecutors=20
    executorCores=4
    driverCores=4
    maxExecutors=25
    maxResultSize=0

    #Parametros de configuracion
    appName=GMM_DLK_TallerProducto_Relleno_Coberturas
    mainClass=CoberturasDC_Relleno
else
    echo "errorGetContext=CLASS_NOT_FOUND"
    exit 1
fi

echo "appName=$appName"
echo "mainClass=$mainClass"

localPath=/etc/hive/conf/
scriptSpark=hdfs://nameservice1/transf/DL_TallerProd/tallerprodc_2.11-2.0.jar

if [ ${1} = "PolizasDC" ] || [ ${1} = "ObjetosDC" ] || [ ${1} = "CoberturasDC" ]
then
    echo "spark2-submit --queue root.gmm --class "${1}" --driver-class-path ${localPath} --master yarn --deploy-mode cluster --driver-memory ${driverMemory} --num-executors ${numExecutors} --executor-memory ${executorMemory} --executor-cores ${executorCores} --driver-cores ${driverCores} --conf spark.dynamicAllocation.maxExecutors=${maxExecutors} --conf spark.driver.maxResultSize=${maxResultSize} --conf spark.shuffle.service.enabled=true ${scriptSpark} ${configFilePath} ${esquemaOrigen} ${tablaOrigenT} ${tablaOrigenH} ${tablaCatalogo} ${esquemaTemporales} ${esquemaDestino} ${tablaDestino}"
    spark2-submit --queue root.gmm --class "${1}" --driver-class-path ${localPath} --master yarn --deploy-mode cluster --driver-memory ${driverMemory} --num-executors ${numExecutors} --executor-memory ${executorMemory} --executor-cores ${executorCores} --driver-cores ${driverCores} --conf spark.dynamicAllocation.maxExecutors=${maxExecutors} --conf spark.driver.maxResultSize=${maxResultSize} --conf spark.shuffle.service.enabled=true ${scriptSpark} ${configFilePath} ${esquemaOrigen} ${tablaOrigenT} ${tablaOrigenH} ${tablaCatalogo} ${esquemaTemporales} ${esquemaDestino} ${tablaDestino} 
elif [ ${1} = "PolizasDC_Relleno" ] || [ ${1} = "ObjetosDC_Relleno" ] || [ ${1} = "CoberturasDC_Relleno" ]
    echo "Return spark2-submit $?"
then
    echo "spark2-submit --queue root.gmm --class "${1}" --driver-class-path ${localPath} --master yarn --deploy-mode cluster --driver-memory ${driverMemory} --num-executors ${numExecutors} --executor-memory ${executorMemory} --executor-cores ${executorCores} --driver-cores ${driverCores} --conf spark.dynamicAllocation.maxExecutors=${maxExecutors} --conf spark.driver.maxResultSize=${maxResultSize} --conf spark.shuffle.service.enabled=true ${scriptSpark}  ${esquemaOrigen} ${tablaOrigen} ${esquemaDestino} ${tablaBase} ${tablaDestino} ${ramo}"
    spark2-submit --queue root.gmm --class "${1}" --driver-class-path ${localPath} --master yarn --deploy-mode cluster --driver-memory ${driverMemory} --num-executors ${numExecutors} --executor-memory ${executorMemory} --executor-cores ${executorCores} --driver-cores ${driverCores} --conf spark.dynamicAllocation.maxExecutors=${maxExecutors} --conf spark.driver.maxResultSize=${maxResultSize} --conf spark.shuffle.service.enabled=true ${scriptSpark}  ${esquemaOrigen} ${tablaOrigen} ${esquemaDestino} ${tablaBase} ${tablaDestino} ${ramo}
    echo "Return spark2-submit $?"
else
    echo "errorGetContext=CLASS_NOT_FOUND"
    exit 1
fi
