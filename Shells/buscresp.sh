#!/bin/bash

export var1=`echo $1 | cut -d:  -f1`

var1=$var1
echo "Buscando el patron: " $var1

#touch respaldos.csv

hdfs dfs -find /user/hive/warehouse/ -iname "*$var1*" 2>/dev/null | xargs hdfs dfs -du -h -s |  awk '{split($0,a,"/"); print a[5] "," a[6] "," a[1]}' >> respaldos.csv

