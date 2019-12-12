#RFC: RFC0049313 
#Respaldo

su usrsup

hdfs dfs -ls /transf/DL_TallerProd/config/execTallerProductoGMM.sh
hdfs dfs -ls /transf/DL_TallerProd/config/paramPolizaGMM.txt
hdfs dfs -ls /transf/DL_TallerProd/config/paramAseguradoGMM.txt
hdfs dfs -ls /transf/DL_TallerProd/config/paramCoberturaGMM.txt

hdfs dfs -mv /transf/DL_TallerProd/config/execTallerProductoGMM.sh /transf/DL_TallerProd/config/execTallerProductoGMM.sh_bkp_RFC0049313
hdfs dfs -mv /transf/DL_TallerProd/config/paramPolizaGMM.txt /transf/DL_TallerProd/config/paramPolizaGMM.txt_bkp_RFC0049313
hdfs dfs -mv /transf/DL_TallerProd/config/paramAseguradoGMM.txt /transf/DL_TallerProd/config/paramAseguradoGMM.txt_bkp_RFC0049313
hdfs dfs -mv /transf/DL_TallerProd/config/paramCoberturaGMM.txt /transf/DL_TallerProd/config/paramCoberturaGMM.txt_bkp_RFC0049313


hdfs dfs -ls /transf/DL_TallerProd/tallerprodc_2.11-2.0.jar

hdfs dfs -mv /transf/DL_TallerProd/tallerprodc_2.11-2.0.jar /transf/DL_TallerProd/tallerprodc_2.11-2.0.jar_bkp_RFC0049313