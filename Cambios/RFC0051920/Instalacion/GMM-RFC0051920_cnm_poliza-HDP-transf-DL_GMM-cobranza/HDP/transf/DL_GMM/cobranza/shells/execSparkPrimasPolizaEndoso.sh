#!/bin/bash
####################################################################################
## Script para obtener los parametros de ejecucion de primas de GMM               ##
## DATE: 13/Enero/2020                                                            ##
## UPDATE:                                                                        ##
## AUTHOR: MIGUEL IVAN AGUILAR MONDRAGON                                          ##
## USAGE: execSparkPrimasPolizaEndoso.sh  <mainClass> <user>   <configFilePath>   ##
####################################################################################
set -e
#HADOOP_USER_NAME=mamondra
    #Parametros de configuracion

appName=GMM_Prima_genera_primas
mainClass=${1}
HADOOP_USER_NAME=$2
configFilePath=$3
export HADOOP_USER_NAME
fecha_inicial_int=20090101
fecha_final_int=20191231
localPath=/etc/hive/conf/
scriptSpark=hdfs://nameservice1/transf/DL_GMM/cobranza/spark/primas_poliza_endoso_base_1.0.0.jar
driverMemory=15G
executorMemory=16G
numExecutors=25
executorCores=4
driverCores=6
maxExecutors=40
maxResultSize=0

calculafechas(){

      datefull=$(date +%Y-%m-%d)
      day=$(date +%d)
      month=$(date +%m)
      year=$(date +%Y)

      LAST_MONTH=`date +'%m' -d 'last month'`
      LAST_YEAR=`date +'%Y' -d 'last year'`
      FIRST_DAY=01
      LAST_DAY=31



      if [ $day -lt 4 ] #menor al dia 4
      then
        fecha_final=$(date -d "$(date -d `date +%Y-%m-%d` +%Y-%m-01) -1 day" +%Y-%m-%d)
        LAST_YEAR=$(date -d "$(date -d `date +%Y-%m-%d` +%Y-%m-01) -1 day" +%Y)
        LAST_MONTH=$(date -d "$(date -d `date +%Y-%m-%d` +%Y-%m-01) -1 day" +%m)
        fecha_inicial="$LAST_YEAR-$LAST_MONTH-01"
        fecha_final_int=$(date -d "$(date -d `date +%Y-%m-%d` +%Y-%m-01) -1 day" +%Y%m%d)
        LAST_YEAR_INT=$(date -d "$(date -d `date +%Y-%m-%d` +%Y%m01) -1 day" +%Y)
        LAST_MONTH_INT=$(date -d "$(date -d `date +%Y-%m-%d` +%Y%m01) -1 day" +%m)
        fecha_inicial_int="$LAST_YEAR$LAST_MONTH_INT$FIRST_DAY"
       anio_mes_partition="$LAST_YEAR$LAST_MONTH_INT"
      else

        LAST_DATE=$datefull
         fecha_inicial="$year-$month-$FIRST_DAY"
         fecha_inicial_int="$year$month$FIRST_DAY"
        fecha_final="$year-$month-$LAST_DAY"
        fecha_final_int="$year$month$LAST_DAY"
        anio_mes_partition="$year$month"
      fi

      echo "LAST_DATE $LAST_DATE"
      echo "fecha_inicial $fecha_inicial"
      echo "fecha_inicial_int  $fecha_inicial_int"
      echo "fecha_final  $fecha_final"
      echo "fecha_final_int  $fecha_final_int"

}

calculafechas

echo "fecha_final_int  $fecha_final_int"
echo "fecha_inicial_int  $fecha_inicial_int"
echo "appName=$appName"
echo "mainClass=$mainClass"

echo " spark2-submit --queue root.gmm --class mx.com.gnp.gmm."${1}" --driver-class-path ${localPath} --master yarn --deploy-mode cluster --driver-memory ${driverMemory} --num-executors ${numExecutors} --executor-memory ${executorMemory} --executor-cores ${executorCores} --driver-cores ${driverCores} --conf spark.dynamicAllocation.maxExecutors=${maxExecutors} --conf spark.driver.maxResultSize=${maxResultSize} --conf spark.shuffle.service.enabled=true ${scriptSpark}  ${configFilePath} ${fecha_inicial_int}  ${fecha_final_int} "


spark2-submit --queue root.gmm --class mx.com.gnp.gmm."${1}" --driver-class-path ${localPath} --master yarn --deploy-mode cluster --driver-memory ${driverMemory} --num-executors ${numExecutors} --executor-memory ${executorMemory} --executor-cores ${executorCores} --driver-cores ${driverCores} --conf spark.dynamicAllocation.maxExecutors=${maxExecutors} --conf spark.driver.maxResultSize=${maxResultSize} --conf spark.shuffle.service.enabled=true ${scriptSpark}  ${configFilePath} ${fecha_inicial_int} ${fecha_final_int}
#fecha_inicial_int  fecha_final_int