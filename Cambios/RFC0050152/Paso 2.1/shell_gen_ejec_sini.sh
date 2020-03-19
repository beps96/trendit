SQL_EJEC=${1}
FF_TEMP=${2}
PATH_RAIZ=/tmp/dl_autos/
PATH_RAIZ_HQL=/user/usrdes01/Scripts_usrdes01/Siniestros/hql



mkdir -p ${PATH_RAIZ}${FF_TEMP}
chmod 777 ${PATH_RAIZ}${FF_TEMP}
export PYTHON_EGG_CACHE=${PATH_RAIZ}${FF_TEMP}

hadoop fs -get /user/usrdes01/Scripts_usrdes01/Siniestros/hql/${SQL_EJEC}  .
impala-shell -i gnpmcdes11.cloudera.des.gnp.mx  -f ${SQL_EJEC}
estado=$?
echo "Codigo error: " ${estado}
rm -fr ${PATH_RAIZ}${FF_TEMP}

if [ $estado -ne 0 ]; then
echo "---------------ERROR---------------"
echo "Ocurrio un error en la ejecucion del proceso"
echo "Codigo de error: $estado"

exit 1
else
echo "             "
echo "             "
echo   " La ejecucion hacia la tabla fue correcta " 
	fi