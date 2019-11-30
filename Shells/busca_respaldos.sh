#!/bin/bash

##### BUSCA BKP #####

hdfs dfs -find /user/hive/warehouse/ -name '*BKP*' 2>/dev/null | xargs hdfs dfs -du -h -s |  awk '{split($0,a,"/"); print a[5] "," a[6] "," a[1]}' > respaldos.csv

hdfs dfs -find /user/hive/warehouse/ -name '*bkp*' 2>/dev/null | xargs hdfs dfs -du -h -s |  awk '{split($0,a,"/"); print a[5] "," a[6] "," a[1]}' >> respaldos.csv

hdfs dfs -find /user/hive/warehouse/ -name '*Bkp*' 2>/dev/null | xargs hdfs dfs -du -h -s |  awk '{split($0,a,"/"); print a[5] "," a[6] "," a[1]}' >> respaldos.csv


##### BUSCA BORRA #####

hdfs dfs -find /user/hive/warehouse/ -name '*BORRA*' 2>/dev/null | xargs hdfs dfs -du -h -s |  awk '{split($0,a,"/"); print a[5] "," a[6] "," a[1]}' >> respaldos.csv

hdfs dfs -find /user/hive/warehouse/ -name '*borra*' 2>/dev/null | xargs hdfs dfs -du -h -s |  awk '{split($0,a,"/"); print a[5] "," a[6] "," a[1]}' >> respaldos.csv

hdfs dfs -find /user/hive/warehouse/ -name '*Borra*' 2>/dev/null | xargs hdfs dfs -du -h -s |  awk '{split($0,a,"/"); print a[5] "," a[6] "," a[1]}' >> respaldos.csv


##### BUSCA BACKUP #####

hdfs dfs -find /user/hive/warehouse/ -name '*BACKUP*' 2>/dev/null | xargs hdfs dfs -du -h -s |  awk '{split($0,a,"/"); print a[5] "," a[6] "," a[1]}' >> respaldos.csv

hdfs dfs -find /user/hive/warehouse/ -name '*backup*' 2>/dev/null | xargs hdfs dfs -du -h -s |  awk '{split($0,a,"/"); print a[5] "," a[6] "," a[1]}' >> respaldos.csv

hdfs dfs -find /user/hive/warehouse/ -name '*Backup*' 2>/dev/null | xargs hdfs dfs -du -h -s |  awk '{split($0,a,"/"); print a[5] "," a[6] "," a[1]}' >> respaldos.csv


##### BUSCA RESP #####

hdfs dfs -find /user/hive/warehouse/ -name '*RESP*' 2>/dev/null | xargs hdfs dfs -du -h -s |  awk '{split($0,a,"/"); print a[5] "," a[6] "," a[1]}' >> respaldos.csv

hdfs dfs -find /user/hive/warehouse/ -name '*resp*' 2>/dev/null | xargs hdfs dfs -du -h -s |  awk '{split($0,a,"/"); print a[5] "," a[6] "," a[1]}' >> respaldos.csv

hdfs dfs -find /user/hive/warehouse/ -name '*Resp*' 2>/dev/null | xargs hdfs dfs -du -h -s |  awk '{split($0,a,"/"); print a[5] "," a[6] "," a[1]}' >> respaldos.csv


sed -e '/^,/d' respaldos.csv > respaldos_aux.csv
mv respaldos_aux.csv respaldos.csv 

