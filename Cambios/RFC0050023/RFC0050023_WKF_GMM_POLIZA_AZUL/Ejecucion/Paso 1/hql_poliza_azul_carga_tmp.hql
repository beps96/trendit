drop table if exists bddltrn.stg_gmm_poliza_azul_temporal;
create table bddltrn.stg_gmm_poliza_azul_temporal as
    SELECT DISTINCT
          CONCAT(LPAD(CAST(COALESCE(gaz.pol_oficina,0) AS VARCHAR(2)),2,'0'), LPAD(CAST(COALESCE(gaz.pol_numero,0) AS VARCHAR(6)),6,'0')) AS NUM_POLIZA
        , CONCAT(LPAD(CAST(COALESCE(gaz.pol_ofi_renovac ,0) AS VARCHAR(2)),2,'0'), LPAD(CAST(COALESCE(gaz.pol_num_renovac,0) AS VARCHAR(6)),6,'0'))AS NUM_POLIZA_COBRANZA
        , 'AZUL'                                                    AS CVE_SISTEMA
        , 'AZUL'                                                    AS CVE_SISTEMA_INT
        , nvl(CAT_CVE_CONTABLE.cve_cobertura_contable,'')           AS CVE_COBERTURA_CONTABLE 
        , nvl(CAT_CVE_CONTABLE.descripcion_cobertura_contable,concat('Descripción no encontrada para  ',gaz.pol_sramo_cont))   AS DES_COBERTURA_CONTABLE 
        , nvl(STATUS.cve_homologada,'')                             AS CVE_ESTATUS_POLIZA 
        , nvl(STATUS.des_homologada,concat('Descripción no encontrada para  ',nvl(gaz.pol_status_emis,''))) AS DES_ESTATUS_POLIZA
        , nvl(for_pago.cve_homologada,'')                           AS CVE_FORMA_PAGO 
        , nvl(for_pago.des_homologada,concat('Descripción no encontrada para  ',nvl(gaz.pol_cve_pago,''))) AS DES_FORMA_PAGO
        , nvl(gaz.pol_cve_agente,'')                                AS clave_intermediario_principal
        , nvl(agente.dsnombre,'')                                   AS nombre_intermediario_principal 
        , NVL(
            CAST(CAST(CONCAT(SUBSTR(CAST(gaz.pol_fec_ini_vig AS STRING),1,4),'-',SUBSTR(CAST(gaz.pol_fec_ini_vig AS STRING),5,2), '-', SUBSTR(CAST(gaz.pol_fec_ini_vig AS STRING),7,2)) AS DATE) AS TIMESTAMP),
            cast(from_unixtime(unix_timestamp(TO_DATE('1400-01-01'), 'yyyy-MM-dd')) as TIMESTAMP)) AS FCH_INI_VIGENCIA
        , NVL(
            CAST(CAST(CONCAT(SUBSTR(CAST(gaz.pol_fec_fin_vig AS STRING),1,4),'-',SUBSTR(CAST(gaz.pol_fec_fin_vig AS STRING),5,2), '-', SUBSTR(CAST(gaz.pol_fec_fin_vig AS STRING),7,2)) AS DATE) AS TIMESTAMP),
            cast(from_unixtime(unix_timestamp(TO_DATE('1400-01-01'), 'yyyy-MM-dd')) as TIMESTAMP)) AS FCH_FIN_VIGENCIA
        , gaz.pol_rfc                                               AS RFC_CONTRATANTE
        , gaz.pol_razon_soc                                         AS NOMBRE_CONTRATANTE 
        , nvl(CAT_CIRCULO.cve_homologada,'')                        AS cve_circulo_medico_contratado
        , nvl(CAT_CIRCULO.cirind,concat('Descripción no encontrada para  ',nvl(gaz.pol_num_tipo,''))) AS des_circulo_medico_contratado
        , CAST(CAST('1400-01-01' AS DATE) AS TIMESTAMP)             AS fch_ini_poliza
        , NVL(
            CAST(CAST(CONCAT(SUBSTR(CAST(gaz.pol_fec_emision AS STRING),1,4),'-',SUBSTR(CAST(gaz.pol_fec_emision AS STRING),5,2), '-', SUBSTR(CAST(gaz.pol_fec_emision AS STRING),7,2)) AS DATE) AS TIMESTAMP),
            cast(from_unixtime(unix_timestamp(TO_DATE('1400-01-01'), 'yyyy-MM-dd')) as TIMESTAMP)) AS fch_emision
        , NVL(
            CAST(CONCAT(SUBSTR(CAST(gaz.pol_fec_emision AS STRING),1,4),SUBSTR(CAST(gaz.pol_fec_emision AS STRING),5,2) ) AS INT),140001) AS am_emision
        , nvl(CAT_EXPERI.cve_atributo_org,'')                       AS cve_experiencia
        , nvl(CAT_EXPERI.des_homologada,concat('Descripción no encontrada para  ',nvl(gaz.pol_cve_experiencia,''))) AS des_experiencia
        , nvl(CAT_POOL.cve_pool,'')                                      AS cve_pool
        , nvl(CAT_POOL.pool,concat('Descripción no encontrada para  ',nvl(gaz.pol_pool,''))) AS des_pool
        , nvl(cat_sramo.cve_homologada,'')                                AS cve_subramo_admon
        , nvl(cat_sramo.des_homologada,concat('Descripción no encontrada para  ',nvl(gaz.pol_sramo_cont,'') )) AS des_subramo_admon
        , nvl(gaz.pol_num_tipo,'')                                  AS cve_plan
        , nvl(CAT_PLAN.plntpo,concat('Descripción no encontrada para  ',nvl(gaz.pol_num_tipo,''))) AS des_plan
        , case when gaz.pol_coaseguro1 = 0 then "SI" else "NO" end  AS ban_coaseguro_cero
        , ''                                                        AS cve_usuario
        , ''                                                        as des_usuario
        , nvl(CAT_TERRITO.nivhos,concat('Descripción no encontrada para  ',nvl(gaz.pol_num_tipo,''))) AS nivel_hospital
        , nvl(CAT_ADMIN.cve_homologada,'')                          AS cve_admon_poliza
        , nvl(CAT_ADMIN.des_homologada,concat('Descripción no encontrada para  ',nvl(gaz.pol_tipo_admon,''))) AS des_admon_poliza
        , nvl(CAT_TERRITO.PLNDSC,concat('Descripción no encontrada para  ', cast(gaz.POL_NUM_TIPO as string)))AS gama_poliza
        , nvl(CAT_CIRCULO.plndsc,concat('Descripción no encontrada para  ',nvl(gaz.pol_num_tipo,'')))  AS plan_gama_nivel_poliza
        , nvl(CAT_TERRITO.clspln,concat('Descripción no encontrada para  ', cast(gaz.POL_NUM_TIPO as string))) AS territorialidad_poliza
        , ''                                                        AS canal_venta_estadistico
        , ''                                                        as segmento_venta_estadistico
        , case  when agente.clnumfol is null  and gaz.pol_fec_emision_join <= obtiene_agente_min.am_proc then nvl(obtiene_agente_min.dirplz,0) 
			else  nvl(agente.dirplz,0) end AS cve_dir_plaza
        , case  when agente.clnumfol is null  and gaz.pol_fec_emision_join <= obtiene_agente_min.am_proc then nvl(obtiene_agente_min.dsdirplz,'') 
		else  nvl(agente.dsdirplz,'') end AS des_dir_plaza 
        , case  when agente.clnumfol is null  and gaz.pol_fec_emision_join <= obtiene_agente_min.am_proc then nvl(obtiene_agente_min.cdedoplz,0) 
		else  nvl(agente.cdedoplz,0) end AS cve_edo_dir_plaza 
        , case  when agente.clnumfol is null  and gaz.pol_fec_emision_join <= obtiene_agente_min.am_proc then nvl(obtiene_agente_min.dsedoplz,'') 
		else  nvl(agente.dsedoplz,'') end AS des_edo_dir_plaza 
        , case  when agente.clnumfol is null  and gaz.pol_fec_emision_join <= obtiene_agente_min.am_proc then nvl(obtiene_agente_min.GRZCVE,0) 
		else  nvl(agente.GRZCVE,0) end AS cve_direccion_agencia 
        , case  when agente.clnumfol is null  and gaz.pol_fec_emision_join <= obtiene_agente_min.am_proc then nvl(obtiene_agente_min.dsdirplz,'') 
		else  nvl(agente.dsdirplz,'') end AS des_direccion_agencia 
		,NVL(agente.ofncve,0)                                              AS cve_oficina_intermediario
        , case  when agente.clnumfol is null  and gaz.pol_fec_emision_join <= obtiene_agente_min.am_proc then nvl(CAT_DIRPLZ_MIN.cddirplz,'') 
		 else  nvl(CAT_DIRPLZ.cddirplz,'') end AS cve_red_territorial
        , case  when agente.clnumfol is null  and gaz.pol_fec_emision_join <= obtiene_agente_min.am_proc then nvl(CAT_DIRPLZ_MIN.red_terr,'') 
		else  nvl(CAT_DIRPLZ.red_terr,'') end AS des_red_territorial 
        , case when ( 
					case when (trim(pol_nombre_cob1) = 'GMM' or trim(pol_nombre_cob1) = 'CAB') and pol_preexistencia1 <> 10000 then 1 else 0 end 
					+case when (trim(pol_nombre_cob2) = 'GMM' or trim(pol_nombre_cob2) = 'CAB')and pol_preexistencia2 <> 10000 then 1 else 0 end 
					+case when (trim(pol_nombre_cob3) = 'GMM' or trim(pol_nombre_cob3) = 'CAB')and pol_preexistencia3 <> 10000 then 1 else 0 end 
					+case when (trim(pol_nombre_cob4) = 'GMM' or trim(pol_nombre_cob4) = 'CAB')and pol_preexistencia4 <> 10000 then 1 else 0 end 
					+case when (trim(pol_nombre_cob5) = 'GMM' or trim(pol_nombre_cob5) = 'CAB')and pol_preexistencia5 <> 10000 then 1 else 0 end 
					+case when (trim(pol_nombre_cob6) = 'GMM' or trim(pol_nombre_cob6) = 'CAB')and pol_preexistencia6 <> 10000 then 1 else 0 end 
					+case when (trim(pol_nombre_cob7) = 'GMM' or trim(pol_nombre_cob7) = 'CAB')and pol_preexistencia7 <> 10000 then 1 else 0 end 
					+case when (trim(pol_nombre_cob8) = 'GMM' or trim(pol_nombre_cob8) = 'CAB')and pol_preexistencia8 <> 10000 then 1 else 0 end 
					+case when (trim(pol_nombre_cob9) = 'GMM' or trim(pol_nombre_cob9) = 'CAB')and pol_preexistencia9 <> 10000 then 1 else 0 end 
					+case when (trim(pol_nombre_cob10) = 'GMM' or trim(pol_nombre_cob10) = 'CAB')and pol_preexistencia10 <> 10000 then 1 else 0 end 
					+case when (trim(pol_nombre_cob11) = 'GMM' or trim(pol_nombre_cob11) = 'CAB')and pol_preexistencia11 <> 10000 then 1 else 0 end
					+case when (trim(pol_nombre_cob12) = 'GMM' or trim(pol_nombre_cob12) = 'CAB')and pol_preexistencia12 <> 10000 then 1 else 0 end
					+case when (trim(pol_nombre_cob13) = 'GMM' or trim(pol_nombre_cob13) = 'CAB')and pol_preexistencia13 <> 10000 then 1 else 0 end
					+case when (trim(pol_nombre_cob14) = 'GMM' or trim(pol_nombre_cob14) = 'CAB')and pol_preexistencia14 <> 10000 then 1 else 0 end
					+case when (trim(pol_nombre_cob15) = 'GMM' or trim(pol_nombre_cob15) = 'CAB')and pol_preexistencia15 <> 10000 then 1 else 0 end) > 0 then 'SI' else 'NO' end AS ban_preexistencia
        , CAST(CAST('1400-01-01' AS DATE) AS TIMESTAMP)             AS fch_registro_producto
        , case when gaz.pol_fec_ini_vig >= 20130407 then nvl(concat(cast(sr.fch_registro_producto as char(8)),' ',sr.des_producto_cnsf), 'Descripción no encontrada para  ') else
						nvl(concat(cast(ra.fch_registro_producto as char(8)),' ',ra.des_producto_cnsf), 'Descripción no encontrada para  ') end  AS num_registro_producto
        , gaz.pol_num_asg_dep                                       AS num_aseg_dependientes
        , gaz.pol_num_asg_pri                                       AS num_aseg_titulares
        , ''                                                        AS registro_producto_recas
        ,nvl(EMAZ.segmento, '')                                     AS CVE_SEGMENTO
        ,nvl(EMAZ.desc_segmento,'')                                 AS DES_SEGMENTO
        ,nvl(EMAZ.negocio, '')                                      AS CVE_NEGOCIO_MULTINACIONAL
        ,nvl(EMAZ.desc_negocio,'')                                  AS DES_NEGOCIO_MULTINACIONAL
        ,nvl(EMAZ.bonos, '')                                        AS CVE_TIPO_BONO
        ,nvl(EMAZ.desc_bonos,'')                                    AS DES_TIPO_BONO
        ,nvl(EMAZ.plan_incentivos, '')                              AS AFECTO_PLAN_INCENTIVO
        ,nvl(EMAZ.desc_pol_contr, '')                               AS BAN_POLIZA_CONTRIBUTORIA
        ,nvl(EMAZ.contrb, '')                                       AS PCT_CONTRIBUCION
        ,nvl(EMAZ.desc_pol_prest, '')                               AS BAN_POLIZA_PRESTACION
        ,nvl(EMAZ.mot_exc, '')                                      AS CVE_MOTIVO_EXCLUSION_PLAN_INCENTIVO
        ,nvl(EMAZ.desc_mot_exc,'')                                  AS DES_MOTIVO_EXCLUSION_PLAN_INCENTIVO
        , current_timestamp()                                       AS ts_alta_hdfs
    FROM 
        (

            Select * from bddltrn.agrupado_gaz0_polizahis_duplicados
            
        ) gaz 
        LEFT OUTER JOIN 
        (
			SELECT 
                 cat.clnumfol as clnumfol
                , nvl(cat.dirplz,0) dirplz
                , nvl(cat.cdedoplz,0)  cdedoplz
                , nvl(cat.am_proc,140001) am_proc
                , nvl(plz.dsdirplz,'') dsdirplz
                , nvl(edoplz.dsedoplz,'') dsedoplz
                , cat.redterin
                ,cat.GRZCVE
				,cat.dsnombre
				,cat.ofncve
             FROM bddlcru.cat_cagente2 cat
                LEFT OUTER JOIN bddlcru.cddirplz plz 
                    ON cat.dirplz = plz.cddirplz
                LEFT OUTER JOIN bddlcru.cdedoplz edoplz
                    ON cat.cdedoplz = edoplz.cdedoplz
        ) agente
        ON gaz.pol_cve_agente = agente.clnumfol
        AND gaz.pol_fec_emision_join = agente.am_proc
        
---------------catalogo de estatus de la poliza
        LEFT JOIN
        (
		select cve_atributo_org,cve_homologada,des_homologada from bddlalm.cat_homologados where nombre_catalogo = 'CAT_ESTATUS_POLIZA' and cve_sistema = 'AZUL'  
        ) STATUS
        ON STATUS.cve_atributo_org = gaz.pol_status_emis

----------------catalogo de la forma de pago
        LEFT JOIN
        (
        select cve_atributo_org,cve_homologada,des_homologada from bddlalm.cat_homologados where nombre_catalogo = 'CAT_FORMA_PAGO' and cve_sistema = 'AZUL'
        ) for_pago 
        on for_pago.cve_atributo_org = gaz.pol_cve_pago

----------------catalogo experiencia de la poliza
        LEFT JOIN(
        select cve_atributo_org,cve_homologada,des_homologada from bddlalm.cat_homologados where nombre_catalogo = 'CAT_CLAVE_EXPERIENCIA_POLIZA' and cve_sistema = 'AZUL'
        )CAT_EXPERI
        ON gaz.pol_cve_experiencia=CAT_EXPERI.cve_atributo_org

-----------------Catalogo Tipo de Administracion de la polza
        LEFT JOIN
        (
		  select cve_atributo_org,cve_homologada,des_homologada from bddlalm.cat_homologados where nombre_catalogo = 'CAT_TIPO_ADMINISTRACION_POLIZA' and cve_sistema = 'AZUL'         
		) CAT_ADMIN
        ON CAT_ADMIN.cve_atributo_org = gaz.pol_tipo_admon

------------------Catalogo Pool
        LEFT JOIN
        (
        select trim(cve_pool) cve_pool, trim(pool) pool from bddlcru.cat_pool
        )CAT_POOL 
        ON gaz.pol_pool =  CAT_POOL.cve_pool

-------------------Catalogo dir plaza
        LEFT JOIN
        (
        select cddirplz, dsdirplz, red_terr  from  bddlcru.cat_cddirplz
        )CAT_DIRPLZ
        on agente.dirplz= CAT_DIRPLZ.cddirplz 

--------------------Catalogo clave territorialidad
        LEFT JOIN
        (
        select cdtiplan, clspln,PLNDSC,nivhos from bddlcru.cat_cplanes
        ) CAT_TERRITO
        ON CAT_TERRITO.cdtiplan =  cast(gaz.POL_NUM_TIPO as string)

----------------Catalogo Circulo Medico
        LEFT JOIN
        (
        select a.cdtiplan , b.cve_homologada , a.cirind, a.plndsc from bddlcru.cat_cplanes a inner join 
              bddlalm.cat_homologados b on a.cirind = b.des_homologada
			  WHERE b.nombre_catalogo = 'CAT_CIRCULOS_MEDICOS' and cve_sistema = 'AZUL'
        )CAT_CIRCULO  ON  gaz.pol_num_tipo = cast(CAT_CIRCULO.cdtiplan as decimal(3,0))

----------------Catalogo Cobertura Contable
        LEFT JOIN
        (
        select psramcob,subsubco cve_cobertura_contable ,subdessu descripcion_cobertura_contable from bddlcru.cat_subramo
        )CAT_CVE_CONTABLE 
        ON CAT_CVE_CONTABLE.psramcob=gaz.pol_sramo_cont
  

----------------Catalogo de Agenes con fecha minima
        LEFT JOIN
        (
        
                SELECT 
                                 cat2.clnumfol as clnumfol
                                , nvl(cat2.dirplz,0) as dirplz
                                , nvl(cat2.cdedoplz,0) as cdedoplz
                                , nvl(cat2.am_proc,140001) as am_proc
                                , nvl(plz.dsdirplz,'') as dsdirplz
                                , nvl(edoplz.dsedoplz,'') as dsedoplz
                                , cat2.redterin as redterin
                                ,cat2.GRZCVE
                                FROM 
                                ( select   clnumfol, dirplz,cdedoplz, A.am_proc, redterin, GRZCVE    from (
                    select clnumfol, dirplz,cdedoplz, am_proc, redterin, GRZCVE,
                    RANK () over(partition by clnumfol 
                order by am_proc asc)  as Posicion from bddlcru.cat_cagente2
                where clnumfol is not null 
                ) A where Posicion=1)
                                cat2
                LEFT OUTER JOIN bddlcru.cddirplz plz 
                    ON cat2.dirplz = plz.cddirplz
                LEFT OUTER JOIN bddlcru.cdedoplz edoplz
                    ON cat2.cdedoplz = edoplz.cdedoplz)  obtiene_agente_min
                on gaz.pol_cve_agente = obtiene_agente_min.clnumfol
               


-------------------Catalogo dir plaza con fecha minima
        LEFT JOIN
        (
        select cddirplz, dsdirplz, red_terr  from  bddlcru.cat_cddirplz
        )CAT_DIRPLZ_MIN
        on obtiene_agente_min.dirplz= CAT_DIRPLZ_MIN.cddirplz 

-------------------Catalogo dir plaza con fecha minima
        LEFT JOIN
        (  
			select cve_atributo_org,cve_homologada,des_homologada from bddlalm.cat_homologados where nombre_catalogo = 'CAT_CLAVE_SUBRAMO_ADMINISTRATIVO_POLIZA' and cve_sistema = 'AZUL'  
        )   CAT_SRAMO
        on gaz.pol_sramo_cont = cat_sramo.cve_atributo_org

--------------------Catalogo clave plan
        LEFT JOIN
        (
--select trim(cdtiplan) cdtiplan, trim(plntpo) plntpo from bddlcru.cat_cplanes
         select pla_plan as cdtiplan,  pla_nombre as plntpo from bddlcru.gaz0_tab_planes
        ) CAT_PLAN  
        ON CAT_PLAN.cdtiplan =  gaz.pol_num_tipo
--------------------Etiquetado de azul
        LEFT JOIN 
        (
            select POLIZA, cobranza, '' as  SEGMENTO, NVL(SEGMENTO, '') AS desc_segmento, '' as NEGOCIO, NVL(NEGOCIO, '') AS desc_negocio, '' AS BONOS, NVL(BONOS, '') as desc_bonos, NVL(PLAN_INCENTIVOS, '') AS PLAN_INCENTIVOS, '' as POL_CONTR, NVL(POL_CONTR, '') AS desc_POL_CONTR, NVL(CONTRB, '') AS CONTRB, '' as POL_PREST, NVL(POL_PREST, '') AS desc_pol_prest, '' as mot_exc, '' as desc_mot_exc from BDDLCRU.EMAZETIQ 
        ) EMAZ 
        on  CONCAT(LPAD(CAST(COALESCE(pol_oficina,0) AS VARCHAR(2)),2,'0'), LPAD(CAST(COALESCE(pol_numero,0) AS VARCHAR(6)),6,'0')) = trim(EMAZ.POLIZA) and CONCAT(LPAD(CAST(COALESCE(gaz.pol_ofi_renovac ,0) AS VARCHAR(2)),2,'0'), LPAD(CAST(COALESCE(gaz.pol_num_renovac,0) AS VARCHAR(6)),6,'0')) =  trim(EMAZ.cobranza)       
--------------------Catalogo SR 
        LEFT JOIN 
          bddlcru.cat_gmm_sr as sr
		on  sr.cve_plan = gaz.pol_num_tipo
		and sr.cve_experiencia = gaz.pol_cve_experiencia
--------------------Catalogo RA		
		LEFT JOIN 
		   bddlcru.cat_gmm_ra as ra
		on  ra.cve_plan = gaz.pol_num_tipo
		and ra.cve_experiencia = gaz.pol_cve_experiencia       		
		WHERE gaz.ultimo_reg = 1;