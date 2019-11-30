#!/bin/bash 

touch rep_respaldos.csv

echo "ESQUEMA,TABLA,USUARIO,GRUPO,PESO" > enc_respaldos.csv

ESQUEMAS=(base_afinidad bddladm bddlalm bddlapr bddlcru bddldes bddlenr bddling bddlint bddlorg bddltrn bddltrnbkp qa test test_migracion test_mrc test_omr)
PATRONES=([0-9][0-9][0-9][0-9][0-9][0-9] bkp backup resp borra dev enero febrero marzo abril mayo junio julio agosto septiembre octubre noviembre diciembre)


for ESQUEMA in ${ESQUEMAS[@]}; do for PATRON in ${PATRONES[@]}; do hdfs dfs -ls -h /user/hive/warehouse/$ESQUEMA.db | grep -iE $PATRON | while IFS='\n' read LINE; do USUARIO=$(echo $LINE | awk '{ print $3 }'); GRUPO=$(echo $LINE | awk '{ print $4 }'); TABLA=$(echo $LINE | awk '{ print $8 }' | sed 's/.*\///g'); PESO=$(echo $LINE | awk '{ print $8}' | xargs -I {} hdfs dfs -du -s -h {} | awk '{ print $1 $2}'); echo "$ESQUEMA , $TABLA , $USUARIO , $GRUPO , $PESO"; done ; done; done >> rep_respaldos.csv


#sort rep_respaldos.csv > sort_respaldos.csv
#uniq -d 

cat enc_respaldos.csv rep_respaldos.csv > respaldos.csv

sort rep_respaldos.csv > sort_respaldos.csv
uniq -d sort_respaldos.csv > uniq_respaldos.csv
uniq -u sort_respaldos.csv >> uniq_respaldos.csv

cat enc_respaldos.csv uniq_respaldos.csv > respaldos.csv

