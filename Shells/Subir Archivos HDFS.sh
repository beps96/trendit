su hdfs

hdfs dfs -copyFromLocal DL_Au2018_InsumosIBNR_03_KTOM49_MecResrvSin_v3p1.sql /user/user_aut/usrdes01/Scripts_usrdes01/Siniestros/hql/.

hdfs dfs -chown user_aut:supergroup /user/user_aut/usrdes01/Scripts_usrdes01/Siniestros/hql/DL_Au2018_InsumosIBNR_03_KTOM49_MecResrvSin_v3p1.sql
hdfs dfs -chmod 777 /user/user_aut/usrdes01/Scripts_usrdes01/Siniestros/hql/DL_Au2018_InsumosIBNR_03_KTOM49_MecResrvSin_v3p1.sql



hdfs dfs -copyFromLocal Salvamentos_v3.py /user/user_aut/usrdes01/Scripts_usrdes01/Siniestros/pyspark/.
hdfs dfs -copyFromLocal salvamentos_union.py /user/user_aut/usrdes01/Scripts_usrdes01/Siniestros/pyspark/.
hdfs dfs -copyFromLocal salvamento_estimado.py /user/user_aut/usrdes01/Scripts_usrdes01/Siniestros/pyspark/.
hdfs dfs -copyFromLocal loadUniverse.py /user/user_aut/usrdes01/Scripts_usrdes01/Siniestros/pyspark/.
hdfs dfs -copyFromLocal KTCTMNG.py /user/user_aut/usrdes01/Scripts_usrdes01/Siniestros/pyspark/.

hdfs dfs -chown user_aut:supergroup /user/user_aut/usrdes01/Scripts_usrdes01/Siniestros/pyspark/Salvamentos_v3.py
hdfs dfs -chmod 777 /user/user_aut/usrdes01/Scripts_usrdes01/Siniestros/pyspark/Salvamentos_v3.py

hdfs dfs -chown user_aut:supergroup /user/user_aut/usrdes01/Scripts_usrdes01/Siniestros/pyspark/salvamentos_union.py
hdfs dfs -chmod 777 /user/user_aut/usrdes01/Scripts_usrdes01/Siniestros/pyspark/salvamentos_union.py

hdfs dfs -chown user_aut:supergroup /user/user_aut/usrdes01/Scripts_usrdes01/Siniestros/pyspark/salvamento_estimado.py
hdfs dfs -chmod 777 /user/user_aut/usrdes01/Scripts_usrdes01/Siniestros/pyspark/salvamento_estimado.py

hdfs dfs -chown user_aut:supergroup /user/user_aut/usrdes01/Scripts_usrdes01/Siniestros/pyspark/loadUniverse.py
hdfs dfs -chmod 777 /user/user_aut/usrdes01/Scripts_usrdes01/Siniestros/pyspark/loadUniverse.py

hdfs dfs -chown user_aut:supergroup /user/user_aut/usrdes01/Scripts_usrdes01/Siniestros/pyspark/KTCTMNG.py
hdfs dfs -chmod 777 /user/user_aut/usrdes01/Scripts_usrdes01/Siniestros/pyspark/KTCTMNG.py


