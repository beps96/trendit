#!/bin/bash
cd /temp/VDI/DLK_VDI/src/vdi_reportes/
etiqueta=$1
spark2-submit --class vdi_rep_siniestralidad_p1 --queue root.vida --master yarn --driver-memory 20G --executor-memory 16G --num-executors 25 --executor-cores 6 --driver-cores 6 --conf spark.dynamicAllocation.maxExecutors=30 --conf spark.shuffle.service.enabled=true --conf spark.driver.maxResultSize=0 vdi_rep_siniestralidad_p1_2.11-1.0.jar ${etiqueta} && spark2-submit --class vdi_rep_siniestralidad_p2 --queue root.vida --master yarn --driver-memory 20G --executor-memory 16G --num-executors 25 --executor-cores 6 --driver-cores 6 --conf spark.dynamicAllocation.maxExecutors=30 --conf spark.shuffle.service.enabled=true --conf spark.driver.maxResultSize=0 vdi_rep_siniestralidad_p2_2.11-1.0.jar && spark2-submit --class vdi_rep_siniestralidad_p3 --queue root.vida --master yarn --driver-memory 20G --executor-memory 16G --num-executors 25 --executor-cores 6 --driver-cores 6 --conf spark.dynamicAllocation.maxExecutors=30 --conf spark.shuffle.service.enabled=true --conf spark.driver.maxResultSize=0 vdi_rep_siniestralidad_p3_2.11-1.0.jar && spark2-submit --class vdi_rep_siniestralidad_p4 --queue root.vida --master yarn --driver-memory 20G --executor-memory 16G --num-executors 25 --executor-cores 6 --driver-cores 6 --conf spark.dynamicAllocation.maxExecutors=30 --conf spark.shuffle.service.enabled=true --conf spark.driver.maxResultSize=0 vdi_rep_siniestralidad_p4_2.11-1.0.jar ${etiqueta} && spark2-submit --class vdi_rep_siniestro_pago --queue root.vida --master yarn --driver-memory 20G --executor-memory 16G --num-executors 25 --executor-cores 6 --driver-cores 6 --conf spark.dynamicAllocation.maxExecutors=30 --conf spark.shuffle.service.enabled=true --conf spark.driver.maxResultSize=0 vdi_rep_siniestro_pago_2.11-1.0.jar ${etiqueta}

if [ $etiqueta = "S" ]
then
sh /temp/VDI/DLK_VDI/shells/bq_vdi.sh bddlapr vdi_rep_siniestro_pago_s bq_carga_reppag_s.csv
sh /temp/VDI/DLK_VDI/shells/bq_vdi.sh bddlapr vdi_rep_siniestralidad_s bq_carga_repsin_s.csv
fi
if [ $etiqueta = "M" ]
then
sh /temp/VDI/DLK_VDI/shells/bq_vdi.sh bddlapr vdi_rep_siniestro_pago_s bq_carga_reppag_m.csv
sh /temp/VDI/DLK_VDI/shells/bq_vdi.sh bddlapr vdi_rep_siniestralidad_m bq_carga_repsin_m.csv
fi
if [ $etiqueta = "A" ]
then
sh /temp/VDI/DLK_VDI/shells/bq_vdi.sh bddlapr vdi_rep_siniestro_pago_s bq_carga_reppag_a.csv
sh /temp/VDI/DLK_VDI/shells/bq_vdi.sh bddlapr vdi_rep_siniestralidad_a bq_carga_repsin_a.csv
fi

