
 --  ##########################################################################################
 -- #                                                                                         #
 -- #                            GRUPO NACIONAL PROVINCIAL S.A.B.                             #
 -- #                 Direccion de Sistemas - Subdireccion de Analiticos y BI                 #
 -- #                                                                                         #
 -- #  NOMBRE DEL ENTREGABLE: Generacion de la tabla aut_syo_corredores_conciliacion_emision  #
 -- #  PLATAFORMA DE INSTALACION: N/A                                                         #
 -- #  AUTOR(ES):           Anaximandro Ivan Medrano Gutierrez.                               #
 -- #  FECHA DE CREACION:   2019-Jul-19                                                       #
 -- #  ULTIMA MODIFICACION: 2019-Jul-31                                                       #
 -- #                                                                                         #
 -- # #########################################################################################
 -- Nota: Para evitar conflictos de compatibilidad entre ASCII y otras codificaciones 
 --       se omiten de este script acentos y dieresis principalmente.
 --
            
    drop table if exists bddlapr.aut_syo_corredores_conciliacion_emision;

    create table bddlapr.aut_syo_corredores_conciliacion_emision stored as parquet as 
         select  
            num_contrato,
            referencia_1                as poliza_AON,
            a.num_poliza                as num_poliza_gnp,
            a.num_version,
			a.cve_marca,
			a.desc_vehiculo,
			a.num_serie,			
			a.modelo_vehiculo,
            estatus_poliza_sistema      as cve_status,
            desc_estatus_poliza_sistema as desc_status,  
            round(b.prima_neta ,2) as prima_neta,
            round(b.prima_total,2) as prima_total,     
            cve_forma_pago,
            desc_forma_pago,          
            r1.primer_recibo,       
            rsubs.recibos_subsecuentes,
            nurec.num_recibos,          
            fch_inicio_vigencia,
            fch_fin_vigencia,
            aep.cve_vrs_tarifa,
            aep.pct_valor_descuento_volumen,
            cat_pqagr.descr_larga_paquete
         from bddlapr.aut_insumos_sb1_emision_cobertura a
         inner join( 
            select
                num_poliza,
                 max(num_version) as max_num_version
           from bddlapr.aut_insumos_sb1_emision_cobertura
              where 
               fch_contable is not null 
            group by
                num_poliza
        ) d on ( a.num_poliza    = d.num_poliza
             and a.num_version   = d.max_num_version
             )
        left outer join ( 
            select distinct
                aep.num_poliza
                ,aep.num_version
                ,aep.cve_vrs_tarifa
                ,aep.pct_valor_descuento_volumen
            from bddlalm.aut_elemento_poliza_tp aep 
        ) aep
        on (a.num_poliza=aep.num_poliza and a.num_version=aep.num_version) 
        left outer join( 
            select
                num_poliza,
                      sum(mto_prima_neta_emision_ori) as prima_neta,
                      sum(mto_prima_total_emision_ori) as prima_total
           from bddlapr.aut_insumos_sb1_emision_cobertura
              where 
               fch_contable is not null 
               and num_version = 0
            group by
                num_poliza
        ) b
        on (    a.num_poliza    = b.num_poliza)
        left outer join (
            select distinct cdnumpol, ctvrspol, paquete
            from (SELECT cdnumpol, ctvrspol,  max(imcamp20) as paquete
                    FROM bddlcru.kcim23t
                    where cdprodte like 'A%' 
                    group by cdnumpol, ctvrspol
                 union 
                    SELECT cdnumpol, ctvrspol,  max(imcamp20) as paquete
                    FROM bddlcru.kcim23h
                    where cdprodte like 'A%'
                    group by cdnumpol, ctvrspol
            ) a
        ) paq
        on (a.num_poliza=paq.CDNUMPOL and a.num_version=paq.CTVRSPOL)
        left outer join (
             SELECT 
                 TRIM(CDTABLA) AS CDTABLA,
                 cast(TRIM(CDELEMEN) as int) AS paquete,
                 SUBSTR(DSELEMEN,1,20) as descr_corta_paquete,
                 SUBSTR(DSELEMEN,21,120) as descr_larga_paquete
                 FROM  BDDLCRU.KTCTGET
             WHERE TRIM(CDTABLA)='KTPT45S' AND CDIDIOMA ='ES'
        ) cat_pqagr
        on (paq.paquete=cat_pqagr.paquete)
        left outer join (
                select distinct cdnumpol, ctvrspol, imtotrec as primer_recibo
                from bddlcru.krbm10t
                where numfra = 1 and imtotrec <> 0
                   and ctvrspol = 0
            union
                select distinct cdnumpol, ctvrspol, imtotrec as primer_recibo
                from bddlcru.krbm10h
                where numfra = 1 and imtotrec <> 0
                   and ctvrspol = 0
        ) r1 on (a.num_poliza = r1.cdnumpol)
        left outer join (
                select distinct cdnumpol, ctvrspol, imtotrec as recibos_subsecuentes
                from bddlcru.krbm10t
                where numfra = 2 and imtotrec <> 0
                   and ctvrspol = 0
            union
                select distinct cdnumpol, ctvrspol, imtotrec as recibos_subsecuentes
                from bddlcru.krbm10h
                where numfra = 2 and imtotrec <> 0
                   and ctvrspol = 0
        ) rsubs on (a.num_poliza = rsubs.cdnumpol)
        left outer join (
            select cdnumpol, ctvrspol, count(*) as num_recibos
            from (
                    select cdnumpol, ctvrspol, numfra
                    from bddlcru.krbm10t
                    where imtotrec <> 0
                       and ctvrspol = 0
                    group by cdnumpol, ctvrspol, numfra
                union
                    select cdnumpol, ctvrspol, numfra
                    from bddlcru.krbm10h
                    where imtotrec <> 0
                       and ctvrspol = 0
                    group by cdnumpol, ctvrspol, numfra
            ) a
            group by cdnumpol, ctvrspol
        ) nurec on (a.num_poliza = nurec.cdnumpol)
     group by
        num_contrato,
        referencia_1,
        a.num_poliza,
        a.num_version,
		a.cve_marca,
		a.desc_vehiculo,
		a.num_serie,			
		a.modelo_vehiculo,  
        estatus_poliza_sistema,
        desc_estatus_poliza_sistema, 
        b.prima_neta, 
        b.prima_total,
        cve_forma_pago,
        desc_forma_pago,          
        r1.primer_recibo,       
        rsubs.recibos_subsecuentes,
        nurec.num_recibos,          
        fch_inicio_vigencia,
        fch_fin_vigencia,
        aep.cve_vrs_tarifa,
        aep.pct_valor_descuento_volumen,
        cat_pqagr.descr_larga_paquete;



    --------------------------------------------------------------------------------------
    --- Refresca MetaData

    invalidate metadata bddlapr.aut_syo_corredores_conciliacion_emision;


   
  
