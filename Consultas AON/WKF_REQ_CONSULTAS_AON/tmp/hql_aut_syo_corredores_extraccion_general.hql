
 --  ###############################################################################################
 -- #                                                                                              #
 -- #                            GRUPO NACIONAL PROVINCIAL S.A.B.                                  #
 -- #                 Direccion de Sistemas - Subdireccion de Analiticos y BI                      #
 -- #                                                                                              #
 -- #  NOMBRE DEL ENTREGABLE: Generacion de la tabla bddlapr.aut_syo_corredores_extraccion_general #
 -- #  PLATAFORMA DE INSTALACION: N/A                                                              #
 -- #  AUTOR(ES):           Anaximandro Ivan Medrano Gutierrez.                                    #
 -- #  FECHA DE CREACION:   2019-Jul-12                                                            #
 -- #  ULTIMA MODIFICACION: 2019-Jul-31                                                            #
 -- #                                                                                              #
 -- # ##############################################################################################
 -- Nota: Para evitar conflictos de compatibilidad entre ASCII y otras codificaciones 
 --       se omiten de este script acentos y dieresis principalmente.
 --

    --- ################################################################################################### 
    --- PASO 1 .- Generar la tabla transformada que alimentara la tabla de campos solicitados
    ---           que radicara en APR.
    --- ################################################################################################### 
           
 drop table if exists bddltrn.trn_aut_syo_corredores_extraccion_general;

 set hive.execution.engine=spark;

    create table bddltrn.trn_aut_syo_corredores_extraccion_general as 
    select  
         aniomes
        ,fch_contable
        ,max(num_contrato                 ) as num_contrato
        ,max(cve_intermediario_principal  ) as cve_intermediario_principal
        ,max(num_poliza_padre             ) as num_poliza_padre
        ,num_poliza
        ,num_version
        ,max(referencia_1                 ) as referencia_1
        ,max(estatus_poliza_sistema       ) as estatus_poliza_sistema
        ,max(desc_estatus_poliza_sistema  ) as desc_estatus_poliza_sistema
        ,max(estatus_endoso               ) as estatus_endoso
        ,max(desc_estatus_endoso          ) as desc_estatus_endoso
        ,max(fch_inicio_vigencia          ) as fch_inicio_vigencia
        ,max(fch_fin_vigencia             ) as fch_fin_vigencia
        ,max(cve_contratante              ) as cve_contratante
        ,max(nombre_completo_contratante  ) as nombre_completo_contratante
        ,nombre_conductor
        ,max(referencia_2                 ) as referencia_2
        ,cve_vrs_tarifa
        ,pct_valor_descuento_volumen  
        ,sum(mto_gasto_emision_ori        ) as mto_gasto_emision_ori
        ,max(cve_tipo_vehiculo            ) as cve_tipo_vehiculo
        ,max(desc_tipo_vehiculo           ) as desc_tipo_vehiculo
        ,max(cve_marca                    ) as cve_marca
        ,max(desc_vehiculo                ) as desc_vehiculo
        ,max(modelo_vehiculo              ) as modelo_vehiculo
        ,max(num_serie                    ) as num_serie
        ,max(num_placas_vehiculo          ) as num_placas_vehiculo
        ,max(motor                        ) as motor
        ,max(paquete2                      ) as paquete 
        ,max(descr_paquete                ) as descr_paquete 
        ,max(cve_forma_pago               ) as cve_forma_pago
        ,max(desc_forma_pago              ) as desc_forma_pago
        ,max(cve_entidad_circulacion      ) as cve_entidad_circulacion
        ,max(desc_entidad_circulacion     ) as desc_entidad_circulacion
        ,max(cve_tipo_carga               ) as cve_tipo_carga
        ,valor_vehiculo2 as VALOR_FACTURA  
        ,'pendiente sistemas-sbi' as VALOR_ADAPTACION
        ,'pendiente sistemas-sbi' as Descripcion_Adaptacion
        ,cve_moneda_poliza
        ,desc_moneda_poliza
        ,tipo_cambio_emision
        ,max(case when cve_cobertura_tarificable = '0000001288'  then round(mto_prima_neta_ori_solo_cdopera001001,2) else 0 end) as DM_PERDIDA_TOTAL
        ,max(case when cve_cobertura_tarificable = '0000001289'  then round(mto_prima_neta_ori_solo_cdopera001001,2) else 0 end) as DM_PERDIDA_PARCIAL
        ,max(case when cve_cobertura_tarificable = '0000000916'  then round(mto_prima_neta_ori_solo_cdopera001001,2) else 0 end) as ROBO_TOTAL
        ,max(case when cve_cobertura_tarificable = '0000001273'  then round(mto_prima_neta_ori_solo_cdopera001001,2) else 0 end) as RC_DANIOS_A_TERCEROS
        ,max(case when cve_cobertura_tarificable = '0000001285'  then round(mto_prima_neta_ori_solo_cdopera001001,2) else 0 end) as PROTECCION_LEGAL
        ,max(case when cve_cobertura_tarificable = '0000000906'  then round(mto_prima_neta_ori_solo_cdopera001001,2) else 0 end) as GASTOS_MEDICOS_OCUP
        ,max(case when cve_cobertura_tarificable = '0000000904'  then round(mto_prima_neta_ori_solo_cdopera001001,2) else 0 end) as EXTENSION_DE_RC
        ,max(case when cve_cobertura_tarificable = '0000001452'  then round(mto_prima_neta_ori_solo_cdopera001001,2) else 0 end) as RC_POR_FALLECIMIENTO
        ,max(case when cve_cobertura_tarificable = '0000001453'  then round(mto_prima_neta_ori_solo_cdopera001001,2) else 0 end) as RC_OCUPANTES
        ,max(case when cve_cobertura_tarificable = '0000000900'  then round(mto_prima_neta_ori_solo_cdopera001001,2) else 0 end) as DANOS_POR_LA_CARGA
        ,max(case when cve_cobertura_tarificable = '0000000908'  then round(mto_prima_neta_ori_solo_cdopera001001,2) else 0 end) as MANIOBRAS_CARG_DES
        ,max(case when cve_cobertura_tarificable = '0000000912'  then round(mto_prima_neta_ori_solo_cdopera001001,2) else 0 end) as RC_ECOLOGICA
        ,max(case when cve_cobertura_tarificable = '0000000909'  then round(mto_prima_neta_ori_solo_cdopera001001,2) else 0 end) as PROTECCION_AUXILIAR
        ,max(case when cve_cobertura_tarificable = '0000000907'  then round(mto_prima_neta_ori_solo_cdopera001001,2) else 0 end) as GM_AL_CONDUCTOR
        ,max(case when cve_cobertura_tarificable = '0000001455'  then round(mto_prima_neta_ori_solo_cdopera001001,2) else 0 end) as asISTENCIA_VIAL_CAM
        ,max(case when cve_cobertura_tarificable = '0000001268'  then round(mto_prima_neta_ori_solo_cdopera001001,2) else 0 end) as CLUB_GNP
        ,max(case when cve_cobertura_tarificable = '0000001348'  then round(mto_prima_neta_ori_solo_cdopera001001,2) else 0 end) as AYUDA_PARA_PERD_TOT
        ,max(case when cve_cobertura_tarificable = '0000000893'  then round(mto_prima_neta_ori_solo_cdopera001001,2) else 0 end) as ACC_AL_CONDUCTOR
        ,max(case when cve_cobertura_tarificable = '0000000915'  then round(mto_prima_neta_ori_solo_cdopera001001,2) else 0 end) as RC_VIAJERO
        ,max(case when cve_cobertura_tarificable = '0000000894'  then round(mto_prima_neta_ori_solo_cdopera001001,2) else 0 end) as ADAP_CONV_Y_EQPO_ESP
        ,max(case when cve_cobertura_tarificable = '0000001457'  then round(mto_prima_neta_ori_solo_cdopera001001,2) else 0 end) as CERO_DED_PT_POR_DM
        ,round(sum(mto_prima_neta_ori_solo_cdopera001001 ),2) as mto_prima_neta_ori_solo_cdopera001001
        ,round(sum(mto_prima_total_ori_solo_cdopera001001),2) as mto_prima_total_ori_solo_cdopera001001
        ,ban_plan_piso2 as ban_plan_piso
        ,num_recibo 
        ,num_folio_fiscal 
        ,primer_recibo
        ,recibos_subsecuentes
    from (
         select a.*,
            case 
                when 
                    SUBSTR(trim(cve_prod_tecnico),1,8) = 'A2400ESP' and
                    SUBSTR(trim(cve_prod_comercial),1,5) in ('00002','00003','00004','00005') 
                then 'PL' 
                else '' 
            end as ban_plan_piso2,  
            b.mto_prima_neta_ori as mto_prima_neta_ori_solo_cdopera001001, 
            b.mto_prima_total_ori as mto_prima_total_ori_solo_cdopera001001,
            aep.cve_vrs_tarifa,
            aep.pct_valor_descuento_volumen,
            aeo.nombre_conductor,
            aeo.valor_vehiculo as valor_vehiculo2,
            paq.paquete as paquete2, 
            cat_pqagr.descr_paquete,
            r1.num_recibo,
            r1.num_folio_fiscal,
            r1.primer_recibo,       
            rsubs.recibos_subsecuentes
         from bddlapr.aut_insumos_sb1_emision_cobertura a
         inner join( 
            select
                num_poliza,
                num_version,
                min(fch_contable) as min_fch_contable_vers_0,
                cve_moneda_poliza
           from bddlapr.aut_insumos_sb1_emision_cobertura
              where 
               fch_contable is not null 
              and num_version = 0
            group by
                num_poliza,
                num_version,
                cve_moneda_poliza
        ) d on ( a.num_poliza    = d.num_poliza
             and a.num_version   = d.num_version
             and a.fch_contable  = d.min_fch_contable_vers_0
             and a.cve_moneda_poliza    = d.cve_moneda_poliza
             )
        left outer join ( 
            select distinct
                aep.num_poliza
                ,aep.num_version
                ,aep.cve_vrs_tarifa
                ,aep.pct_valor_descuento_volumen
            from bddlalm.aut_elemento_poliza_tp aep
            where  num_version = 0    
        ) aep
        on (a.num_poliza=aep.num_poliza and a.num_version=aep.num_version) 
        left outer join ( 
            select distinct
                aeo.num_poliza
                ,aeo.num_version
                ,aeo.nombre_conductor
                ,aeo.valor_vehiculo
            from bddlalm.aut_elemento_objeto_tp aeo
            where  num_version = 0    
        ) aeo
        on (a.num_poliza=aeo.num_poliza and a.num_version=aeo.num_version) 
        left outer join( 
              select num_poliza, num_version, cve_cobertura,
              cve_operacion, fch_contable, cve_moneda, mto_prima_neta_ori, mto_prima_total_ori
              from bddlalm.aut_primas
              where 
               fch_contable is not null 
              and cve_operacion = '001001'
              and num_version = 0   
        ) b
        on (    a.num_poliza    = b.num_poliza
            and a.num_version   = b.num_version
            and a.cve_cobertura_tarificable = b.cve_cobertura
            and a.fch_contable  = b.fch_contable
            and a.cve_moneda_poliza    = b.cve_moneda )
        left outer join (
            select distinct cdnumpol, ctvrspol, paquete
            from (SELECT cdnumpol, ctvrspol,  max(imcamp20) as paquete
                    FROM bddlcru.kcim23t
                    where cdprodte like 'A%' and ctvrspol = 0 
                    group by cdnumpol, ctvrspol
                 union 
                    SELECT cdnumpol, ctvrspol,  max(imcamp20) as paquete
                    FROM bddlcru.kcim23h
                    where cdprodte like 'A%' and ctvrspol = 0 
                    group by cdnumpol, ctvrspol
            ) a
        ) paq
        on (a.num_poliza=paq.CDNUMPOL and a.num_version=paq.CTVRSPOL)
        left outer join (
            select distinct
                cve_sist
               ,cast(paquete as int) as paquete
               ,descripcion          as descr_paquete
               ,paqueteagrupado      as paquete_agrupado
            from bddldes.cat_aut_paquete_agrupado
            where trim(cve_sist) = 'I' 
        ) cat_pqagr
        on (paq.paquete=cat_pqagr.paquete)
        left outer join (
                select distinct 
                    cdnumpol as num_poliza, 
                    ctvrspol as num_version, 
                    CDNUMREC as num_recibo, 
                    CDFOLFIS as num_folio_fiscal, 
                    imtotrec as primer_recibo
                from bddlcru.krbm10t
                where numfra = 1 and imtotrec <> 0
            union
                select distinct 
                    cdnumpol as num_poliza, 
                    ctvrspol as num_version, 
                    CDNUMREC as num_recibo, 
                    CDFOLFIS as num_folio_fiscal, 
                    imtotrec as primer_recibo
                from bddlcru.krbm10h
                where numfra = 1 and imtotrec <> 0
        ) r1 on (a.num_poliza = r1.num_poliza and a.num_version = r1.num_version)
        left outer join (
                select distinct 
                    cdnumpol as num_poliza, 
                    ctvrspol as num_version, 
                    imtotrec as recibos_subsecuentes
                from bddlcru.krbm10t  ---- recibos
                where numfra = 2 and imtotrec <> 0
            union
                select distinct 
                    cdnumpol as num_poliza, 
                    ctvrspol as num_version, 
                    imtotrec as recibos_subsecuentes
                from bddlcru.krbm10h
                where numfra = 2 and imtotrec <> 0
        ) rsubs on (a.num_poliza = rsubs.num_poliza and a.num_version = rsubs.num_version)
    ) c
    group by
        aniomes
        ,fch_contable
        ,num_poliza
        ,num_version
        ,ban_plan_piso
        ,nombre_conductor
        ,cve_vrs_tarifa
        ,pct_valor_descuento_volumen
        ,valor_vehiculo2
        ,cve_moneda_poliza
        ,desc_moneda_poliza
        ,tipo_cambio_emision
        ,num_recibo 
        ,num_folio_fiscal 
        ,primer_recibo
        ,recibos_subsecuentes
    having  mto_prima_neta_ori_solo_cdopera001001 > 0;


    
    ----------------------------------------------------------------------------
    --- Refrescar MetaData 

    invalidate metadata  bddltrn.trn_aut_syo_corredores_extraccion_general;
    
    --- ################################################################################################### 
    --- PASO 2 .- Generar la tabla que contendra solo atributos solicitados y la cual radicara en APR
    --- ################################################################################################### 


 	drop table if exists bddlapr.aut_syo_corredores_extraccion_general;

    create table bddlapr.aut_syo_corredores_extraccion_general as 
    select DISTINCT 
         num_contrato
        ,cve_intermediario_principal
        ,num_poliza_padre
        ,num_poliza
        ,num_version
        ,referencia_1
        ,estatus_poliza_sistema       as estatus
        ,desc_estatus_poliza_sistema  as desc_estatus
        ,fch_inicio_vigencia
        ,fch_fin_vigencia
        ,cve_contratante
        ,nombre_completo_contratante
        ,nombre_conductor
        ,referencia_2
        ,cve_vrs_tarifa
        ,pct_valor_descuento_volumen
        ,mto_gasto_emision_ori as mto_derechos_poliza
        ,cve_tipo_vehiculo
        ,desc_tipo_vehiculo
        ,cve_marca
        ,desc_vehiculo
        ,modelo_vehiculo
        ,num_serie
        ,num_placas_vehiculo
        ,motor
        ,paquete
        ,descr_paquete
        ,cve_forma_pago
        ,desc_forma_pago
        ,cve_entidad_circulacion
        ,desc_entidad_circulacion
        ,cve_tipo_carga
        ,valor_factura
        ,desc_moneda_poliza
        ,dm_perdida_total
        ,dm_perdida_parcial
        ,robo_total
        ,rc_danios_a_terceros
        ,proteccion_legal
        ,gastos_medicos_ocup
        ,extension_de_rc
        ,rc_por_fallecimiento
        ,rc_ocupantes
        ,danos_por_la_carga
        ,maniobras_carg_des
        ,rc_ecologica
        ,proteccion_auxiliar
        ,gm_al_conductor
        ,asistencia_vial_cam
        ,club_gnp
        ,ayuda_para_perd_tot
        ,acc_al_conductor
        ,rc_viajero
        ,adap_conv_y_eqpo_esp
        ,cero_ded_pt_por_dm
        ,mto_prima_neta_ori_solo_cdopera001001  as prima_neta
        ,mto_prima_total_ori_solo_cdopera001001 as prima_total
        ,ban_plan_piso
        ,primer_recibo
        ,recibos_subsecuentes
    from bddltrn.trn_aut_syo_corredores_extraccion_general;
    
    

    ----------------------------------------------------------------------------
    --- Refresca MetaData 

    invalidate metadata  bddlapr.aut_syo_corredores_extraccion_general;
    
