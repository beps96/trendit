TRUNCATE TABLE bddltrn.stg_gmm_pol_azul_formato;
INSERT INTO  bddltrn.stg_gmm_pol_azul_formato
SELECT  
	 cast( trim(TEM.num_poliza )                 			AS varchar(14)) 	   as  num_poliza                  		
	,cast( trim(TEM.num_poliza_cobranza )        			AS varchar(8))		   as  num_poliza_cobranza         		
	,cast( trim(TEM.cve_sistema)                 			AS varchar(4))         as  cve_sistema                 		
	,cast( trim(TEM.cve_sistema_int)             			AS varchar(4))         as  cve_sistema_int             		
	,cast( trim(TEM.cve_cobertura_contable)      			AS varchar(3))         as  cve_cobertura_contable      		
	,cast( trim(conta.subsubcuenta)      			AS varchar(100))       as  des_cobertura_contable      		
	,cast( TRIM(TEM.cve_estatus_poliza)          			AS varchar(2))         as  cve_estatus_poliza          		
	,cast( trim(TEM.des_estatus_poliza)          	AS varchar(100))       as  des_estatus_poliza          		
	,cast( TRIM(TEM.cve_forma_pago)              			AS varchar(1))         as  cve_forma_pago              		
	,cast( trim(TEM.des_forma_pago)              	AS varchar(100))       as  des_forma_pago              		
	,cast( TRIM(TEM.clave_intermediario_principal)     	AS varchar(10))        as  clave_intermediario_principal     
	,cast( trim(TEM.nombre_intermediario_principal) AS varchar(150))       as  nombre_intermediario_principal    
	,cast( TEM.fch_ini_vigencia            			AS timestamp)          as  fch_ini_vigencia            		
	,cast( TEM.fch_fin_vigencia            			AS timestamp)          as  fch_fin_vigencia            		
	,cast( TRIM(TEM.rfc_contratante )            			AS varchar(13))        as  rfc_contratante             		
	,cast( trim(TEM.nombre_contratante)          	AS varchar(150))       as  nombre_contratante          		
	,cast( TRIM(TEM.cve_circulo_medico_contratado)   		AS varchar(10))        as  cve_circulo_medico_contratado   	
	,cast( trim(TEM.des_circulo_medico_contratado)  AS varchar(100))       as  des_circulo_medico_contratado   	
	,cast( Fecha_min.min_pol_fec_ini_vig   			AS timestamp)          as  min_pol_fec_ini_vig   		
	,cast( TEM.fch_emision                 			AS timestamp)          as  fch_emision                 		
	,cast( TEM.am_emision                  			AS int)                as  am_emision                  		
	,cast( TRIM(TEM.cve_experiencia)             			AS varchar(5))         as  cve_experiencia             		
	,cast( trim(TEM.des_experiencia)             	AS varchar(50))        as  des_experiencia 
 	,cast( TRIM(substr(cve_pool,1,case when length(trim(TEM.cve_pool))<= 0 then 0 else length(cve_pool)-2 end)) AS varchar(10))  AS  cve_pool                    		
	,cast( trim(regexp_replace(TEM.des_pool,'01','')) AS varchar(100))     as  des_pool                    		
	,cast( TRIM(TEM.cve_subramo_admon)           			AS varchar(2))         as  cve_subramo_admon           		
	,cast( trim(TEM.des_subramo_admon)           	AS varchar(100))       as  des_subramo_admon           		
	,cast( TRIM(TEM.cve_plan)                    			AS varchar(5))         as  cve_plan                    		
	,cast( trim(TEM.des_plan)                    	AS varchar(100))       as  des_plan                    		
	,cast( TRIM(TEM.ban_coaseguro_cero )         			AS varchar(2))         as  ban_coaseguro_cero          		
	,cast( TRIM(NVL(us.cve_usuario,''))          			AS varchar(10))        as  cve_usuario                  		
	,cast( TRIM(NVL(us.nom_usuario,'No existe valor para este endoso')) AS varchar(100))       as  nom_usuario                 		
	,cast( trim(TEM.nivel_hospital)          	    AS varchar(100))       as  des_nivel_hospital          		
	,cast( TRIM(TEM.cve_admon_poliza )           			AS varchar(5))         as  cve_admon_poliza            		
	,cast( trim(TEM.des_admon_poliza)            	AS varchar(100))       as  des_admon_poliza            		
	,cast( TRIM(TEM.gama_poliza)                 			AS varchar(100))       as  gama_poliza                 		
	,cast( TRIM(TEM.plan_gama_nivel_poliza)      			AS varchar(100))       as  plan_gama_nivel_poliza      		
	,cast( TRIM(TEM.territorialidad_poliza)      			AS varchar(100))       as  territorialidad_poliza      		
	,cast( TRIM(TEM.canal_venta_estadistico)     			AS varchar(100))       as  canal_venta_estadistico     		
	,cast( TRIM(TEM.segmento_venta_estadistico)  			AS varchar(100))       as  segmento_venta_estadistico  		
	,cast( TEM.cve_dir_plaza               			AS varchar(5))         as  cve_dir_plaza               		
	,cast( trim(TEM.des_dir_plaza)               	AS varchar(100))       as  des_dir_plaza               		
	,cast( TEM.cve_edo_dir_plaza           			AS varchar(5))         as  cve_edo_dir_plaza           		
	,cast( trim(TEM.des_edo_dir_plaza)           	AS varchar(100))       as  des_edo_dir_plaza           		
	,cast( TEM.cve_direccion_agencia       			AS varchar(5))         as  cve_direccion_agencia       		
	,cast( trim(TEM.des_direccion_agencia)       	AS varchar(100))       as  des_direccion_agencia       		
	,cast( TEM.cve_oficina_intermediario   			AS smallint)           as  cve_oficina_intermediario   		
	,cast( TRIM(TEM.cve_red_territorial)         			AS varchar(5))         as  cve_red_territorial         		
	,cast( trim(TEM.des_red_territorial)         	AS varchar(100))       as  des_red_territorial         		
	,cast( TRIM(TEM.ban_preexistencia)           			AS varchar(2))         as  ban_preexistencia           		
	,cast( TEM.fch_registro_producto       			AS timestamp)          as  fch_registro_producto       		
	,cast( TRIM(TEM.num_registro_producto)       			AS varchar(100))       as  num_registro_producto       		
	,cast( TEM.num_aseg_dependientes       			AS int)                as  num_aseg_dependientes       		
	,cast( TEM.num_aseg_titulares          			AS int)                as  num_aseg_titulares          		
	,cast( TRIM(TEM.registro_producto_recas)     			AS varchar(50))        as  registro_producto_recas     		
	,cast( TRIM(TEM.cve_segmento)                			AS varchar(10))        as  cve_segmento                		
	,cast( trim(TEM.des_segmento)                	AS varchar(100))       as  des_segmento                		
	,cast( TRIM(TEM.cve_negocio_multinacional)   			AS varchar(10))        as  cve_negocio_multinacional   		
	,cast( trim(TEM.des_negocio_multinacional)   	AS varchar(100))       as  des_negocio_multinacional   		
	,cast( TRIM(TEM.cve_tipo_bono)               			AS varchar(5))         as  cve_tipo_bono               		
	,cast( trim(TEM.des_tipo_bono)               	AS varchar(100))       as  des_tipo_bono               		
	,cast( TRIM(TEM.afecto_plan_incentivo )      			AS varchar(100))       as  afecto_plan_incentivo       		
	,cast( TRIM(TEM.ban_poliza_contributoria )   			AS varchar(2))         as  ban_poliza_contributoria    		
	,NVL (cast( pct_contribucion        			AS decimal(16,4)),0)   as  pct_contribucion        			
	,cast( TEM.ban_poliza_prestacion       			AS varchar(2))         as  ban_poliza_prestacion       		
	,cast( TEM.cve_motivo_exclusion_plan_incentivo 	AS varchar(5))         as  cve_motivo_exclusion_plan_incentivo
	,cast( trim(TEM.des_motivo_exclusion_plan_incentivo) 	AS varchar(100))        as  des_motivo_exclusion_plan_incentivo
FROM  bddltrn.stg_gmm_poliza_azul_temporal TEM
			LEFT JOIN
			( 
				SELECT CONCAT(LPAD(CAST(COALESCE(pol_oficina,0) AS VARCHAR(2)),2,'0'), LPAD(CAST(COALESCE(pol_numero,0) AS VARCHAR(6)),6,'0')) AS num_poliza_fin, 
						min(pol_fec_ini_vig) AS min_pol_fec_ini_vig
				FROM  bddltrn.agrupado_gaz0_polizahis_duplicados
				GROUP BY CONCAT(LPAD(CAST(COALESCE(pol_oficina,0) AS VARCHAR(2)),2,'0'), LPAD(CAST(COALESCE(pol_numero,0) AS VARCHAR(6)),6,'0'))
			)Fecha_min
		ON TEM.num_poliza = Fecha_min.num_poliza_fin
	LEFT JOIN
		     (SELECT pol_ofi_renovac,pol_num_renovac,pol_oficina,pol_numero,cve_usuario,nom_usuario
				FROM bddlCRU.cat_gmm_usuario_azul WHERE POL_ENDOSO = 0 ) us
			ON   cast( TEM.num_poliza AS varchar(14)) = CONCAT(LPAD(cast(us.pol_oficina     as varchar(2)),2,'0'), LPAD(cast(us.pol_numero      as varchar(6)),6,'0'))
			AND  cast( TEM.num_poliza_cobranza AS varchar(8)) = CONCAT(LPAD(cast(us.pol_ofi_renovac as varchar(2)),2,'0'), LPAD(cast(us.pol_num_renovac as varchar(6)),6,'0'))
	LEFT JOIN
			(select cve_subsubcuenta,subsubcuenta from BDDLCRU.CON_CAT_SUBSUBCUENTA WHERE cve_unidad_negocio = 'G') as conta
			on trim(TEM.cve_cobertura_contable) = trim(conta.cve_subsubcuenta)
