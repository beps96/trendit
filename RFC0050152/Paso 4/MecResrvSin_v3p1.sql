 --  ##########################################################################################
 -- #                                                                                         #
 -- #                            GRUPO NACIONAL PROVINCIAL S.A.B.                             #
 -- #                 Direccion de Sistemas - Subdireccion de Analiticos y BI                 #
 -- #                                                                                         #
 -- #  NOMBRE DEL ENTREGABLE: Movimientos economicos contables de la reserva de siniestros    #
 -- #                          autos (basado en la KTOM49) de INFO.                           #
 -- #  PLATAFORMA DE INSTALACION: N/A                                                         #
 -- #  AUTOR(ES):           Anaximandro Ivan Medrano Gutierrez.                               #
 -- #  FECHA DE CREACION:   2018-Ago-24                                                       #
 -- #  ULTIMA MODIFICACION: 2018-Sep-21                                                       #
 -- #                                                                                         #
 -- # #########################################################################################
 -- Nota: Para evitar conflictos de compatibilidad entre ASCII y otras codificaciones 
 --       se omiten de este script acentos y dieresis principalmente.

   --ALTER TABLE bddldes.aut_mec_reserva_siniestro SET TBLPROPERTIES('EXTERNAL'='FALSE');
   --drop table bddldes.aut_mec_reserva_siniestro;

drop table bddldes.aut_mec_reserva_siniestro;
    CREATE TABLE bddldes.aut_mec_reserva_siniestro (
         cve_siniestro                      char(10)
        ,num_afectado                       int
        ,num_movimiento                     int    
        ,cve_cobertura                      char(10)
     -- ,desc_cobertura_info                char(50)    /* <== 20180920 AUT_ConsDef_CC001.- Se cambia origen de descripciones */
        ,desc_cobertura                     char(50)    /* <== 20180920 AUT_ConsDef_CC001.- Se cambia origen de descripciones */
        ,cve_cobertura_afecta               char(10)    /* <== 20180920 AUT_ConsDef_CC001.- Se cambia origen de descripciones */
        ,desc_cobertura_afecta              char(50)    /* <== 20180920 AUT_ConsDef_CC001.- Se cambia origen de descripciones */ 
        ,cve_cobertura_afecta_agrup         char(2)     /* <== 20180920 AUT_ConsDef_CC001.- Se cambia origen de descripciones */
        ,cve_tipo_reserva                   char(1)
        ,desc_tipo_reserva                  char(30)
        ,cve_tipo_mov_reserva               char(2)
        ,desc_tipo_mov_reserva              char(30)
        ,num_identificacion_trabajo         char(12)      /* <== 20180920 AUT_ConsDef_CC002.- Se descomenta campo */
        ,cve_dominio_origen_contable        char(2)
        ,desc_dominio_origen_contable       char(30)
        ,cve_tipo_transaccion_contable      char(3)
        ,desc_tipo_transaccion_contable     char(100)
        ,cve_caracteristica_t           char(3)       /* <== 20180920 AUT_ConsDef_CC003.- Se descomenta campo */
        ,cve_operacion_contable         char(3)
        ,desc_operacion_contable            char(60)
        ,cve_medio_pago                     char(2)
        ,desc_medio_pago                    char(50)
        ,cve_moneda                         char(3)
        ,tipo_cambio                        float  
        ,mto_mov_reserva_sini_ori           float  
        ,mto_mov_reserva_sini_int           float  
        ,fch_contable                       timestamp
        ,referencia_detalle                 char(16)
        ,num_apunte_contable                char(18)
        ,cve_tipo_concepto_contable         char(1)
        ,desc_tipo_concepto_contable        char(10)
        ,cve_tipo_mov_arbol                 char(1)
        ,desc_tipo_mov_arbol                char(30)
        ,tabla_origen                       char(8)
        ,sistorig                           char(4)
    ) STORED AS PARQUET;

    --  truncate table bddldes.aut_mec_reserva_siniestro;

    --select count(*) as cta from bddldes.aut_mec_reserva_siniestro; 
    --truncate table bddldes.aut_mec_reserva_siniestro;
    --select * from bddldes.aut_mec_reserva_siniestro limit 100;

    --- tablas:
    compute stats bddlcru.ktom49t;
    compute stats bddlcru.ktom49h;
    compute stats bddlint.fnz_tipo_cambio;

    --select count (*) --
    --from (
insert into table bddldes.aut_mec_reserva_siniestro
    select
        cast(cdsinies      as char(10))    as cve_siniestro,
        cast(ctnupoin      as int)         as num_afectado,
        cast(numovimi      as int)         as num_movimiento,
        cast(cdmct         as char(10))    as cve_cobertura,
     -- cast(DSMCT_info    as char(50))    as desc_cobertura_info,            /* <== 20180920 AUT_ConsDef_CC001.- Se cambia origen de descripciones */
        cast(desr_CDMCT    as char(20))    as desc_cobertura,                 /* <== 20180920 AUT_ConsDef_CC001.- Se cambia origen de descripciones */
        cast(cdmcr         as char(10))    as cve_cobertura_afecta,           /* <== 20180920 AUT_ConsDef_CC001.- Se cambia origen de descripciones */
        cast(desr_CDMCR    as char(50))    as desc_cobertura_afecta,          /* <== 20180920 AUT_ConsDef_CC001.- Se cambia origen de descripciones */ 
        cast(CDMCR_agr     as char(2))     as cve_cobertura_afecta_agrup,     /* <== 20180920 AUT_ConsDef_CC001.- Se cambia origen de descripciones */
        cast(tctipres      as char(1))     as cve_tipo_reserva,
        cast(dstipres      as char(30))    as desc_tipo_reserva,
        cast(tctipmor      as char(2))     as cve_tipo_mov_reserva,
        cast(dstipmor      as char(30))    as desc_tipo_mov_reserva,
        cast(numidtra      as char(12))    as num_identificacion_trabajo,      /* <== 20180920 AUT_ConsDef_CC002.- Se descomenta campo */
        cast(tcdomori      as char(2))     as cve_dominio_origen_contable,
        cast(dsdomori      as char(30))    as desc_dominio_origen_contable,
        cast(tctitxct      as char(3))     as cve_tipo_transaccion_contable,
        cast(dstitxct      as char(100))   as desc_tipo_transaccion_contable,
        cast(tccdcatr      as char(3))     as cve_caracteristica_t,        /* <== 20180920 AUT_ConsDef_CC003.- Se descomenta campo */
        cast(tccopera      as char(3))     as cve_operacion_contable,
        cast(dscopera      as char(60))    as desc_operacion_contable,
        cast(cdmedpag      as char(2))     as cve_medio_pago,
        cast(dsmedpag      as char(50))    as desc_medio_pago,
        cast(tccdmone      as char(3))     as cve_moneda,
        cast(rate_mult     as float)       as tipo_cambio, 
        cast(IMPCEC81      as float)       as mto_mov_reserva_sini_ori,
        cast(IMPCEC81_int  as float)       as mto_mov_reserva_sini_int,
        cast(fecontab_fch  as timestamp)   as fch_contable,
        cast(cdrefdet      as char(16))    as referencia_detalle,
        cast(nuapunte      as char(18))    as num_apunte_contable,
        cast(intcocon      as char(1))     as cve_tipo_concepto_contable,
        cast(dstcocon      as char(10))    as desc_tipo_concepto_contable,
        cast(tctipmoa      as char(1))     as cve_tipo_mov_arbol,
        cast(dstipmoa      as char(30))    as desc_tipo_mov_arbol,
        cast(tabori        as char(8))     as tabla_origen,
        cast(sistorig      as char(4))     as sistorig
    from  (
        select 
            CDSINIES, CTNUPOIN, CDMCT, NOMCT_info, DSMCT_info, nom_CDMCT, desr_CDMCT, CDMCR, nom_CDMCR, desr_CDMCR, CDMCR_agr,  /* <== 20180920 AUT_ConsDef_CC001.- Se cambia origen de descripciones */
             NUMOVIMI, TCTIPRES, dstipres, NUMIDTRA, TCTITXCT, DSTITXCT, TCCDCATR, /* <== 20180920 AUT_ConsDef_CC002, AUT_ConsDef_CC003.- Se descomenta campo */
             TCCOPERA, DSCOPERA, TCDOMORI, DSDOMORI,  
            /*a.TCTINEGO,*/ CDMEDPAG, DSMEDPAG, TCCDMONE, /*CDTIPCAM, */
             coalesce(rate_mult,1) as rate_mult, -- sustituyendo al CDTIPCAM vacio de info
            IMPCEC81,
            IMPCEC81 * coalesce(rate_mult,1)  as IMPCEC81_int,
            TCTIPMOR, DSTIPMOR, TCTIPMOA, DSTIPMOA, CDREFDET, /*FECONTAB,*/ fecontab_fch, 
             NUAPUNTE, INTCOCON, DSTCOCON, tabori, sistorig
        from  (
            select
                a.CDSINIES, a.CTNUPOIN, a.CDMCT, a.CDMCR, NUMOVIMI, a.TCTIPRES, NUMIDTRA, a.TCTITXCT, TCCDCATR,  /* <==  20180920 AUT_ConsDef_CC002, AUT_ConsDef_CC003.- Se descomenta campo */
                a.TCCOPERA, a.TCDOMORI, /*a.TCTINEGO,*/ a.CDMEDPAG, TCCDMONE, /*CDTIPCAM, */
                cast(CASE TCCDMONE 
                    WHEN 'MXP'  
                    THEN 1 
                    ELSE TC.rate_mult
                END as float) AS rate_mult, -- sustituyendo al CDTIPCAM vacio de info
                a.TCTIPMOR, 
                a.TCTIPMOA, 
                IMPCEC81, CDREFDET,
                /*FECONTAB,*/ fecontab_fch, NUAPUNTE, 
                coalesce(dstipres  ,'') as dstipres, 
                case trim(TCTIPMOR)
                   when '0'  then 'INICIAL'         
                   when '1'  then 'INCREMENTO'      
                   when '2'  then 'DECREMENTO'      
                   when '3'  then 'RB-FINAL'        
                   when '4'  then 'CAMBIO-PROMEDIO' 
                   when '5'  then 'CAMBIO-NORMAL'   
                   when '6'  then 'CAMBIO-PR-CON'
                   when '7'  then 'CAMBIO-NM-CON'   
                   when '8'  then 'CAMBIO-CON-PR'
                   when '9'  then 'CAMBIO-CON-NM'   
                   when '10' then 'CAMBIO-NOR-LIT'
                   when '11' then 'CAMBIO-NOR-CNS'  
                   when '12' then 'CAMBIO-LIT-NOR'
                   when '13' then 'CAMBIO-LIT-CON'
                   when '14' then 'CAMBIO-LIT-CNS'
                   when '15' then 'CAMBIO-CON-LIT'
                   when '16' then 'CAMBIO-CON-CNS'
                   when '17' then 'CAMBIO-CNS-NOR'
                   when '18' then 'CAMBIO-CNS-LIT'
                   else ''
                end as DSTIPMOR,
                ----------coalesce(dssitmov  ,'') as dssitmov, 
                coalesce(dsmedpag  ,'') as dsmedpag, 
                ------------coalesce(dstipasi  ,'') as dstipasi, 
                ------------coalesce(DSCDPAG   ,'') as DSCDPAG, 
                case trim(TCTIPMOA)
                    when '1' then 'INICIAL'             
                    when '2' then 'INCREMENTO-MANUAL'   
                    when '3' then 'DECREMENTO'          
                    when '4' then 'INCREMENT-AUTOMATICO'
                    when '5' then 'DECREMENT-AUTOMATICO'
                    when '6' then 'PAGO'                
                    when '7' then 'REINTEGRO'           
                    when '8' then 'PAGO-COASEGURO'      
                    when '9' then 'REINTEGRO-COASEGURO' 
                    when 'A' then 'INCREMENTO-AUT-CICOS'
                    when 'B' then 'DECREMENTO-AUT-CICOS'
                    when 'C' then 'PAGO-CICOS'          
                    when 'D' then 'REINTEGRO-CICOS'     
                    when 'E' then 'TERMINACION'         
                    when 'F' then 'PAGO-DIFERIDO'       
                    when 'G' then 'ANULA-PAGO'          
                    when 'H' then 'ANULA-REINTEGRO'     
                    when 'I' then 'PAGO-EXTRANJERO'     
                    when 'J' then 'REINTEGRO-EXTRANJERO'
                    when 'K' then 'CAMBIO-PROMEDIO'     
                    when 'L' then 'CAMBIO-NORMAL'
                    else ''
                end as DSTIPMOA,
                coalesce(DSTITXCT  ,'') as DSTITXCT, 
                coalesce(DSCOPERA  ,'') as DSCOPERA, 
                coalesce(DSDOMORI  ,'') as DSDOMORI, 
                coalesce(NOMCT_info ,'') as NOMCT_info,   /* <== 20180920 AUT_ConsDef_CC001.- Se cambia origen de descripciones */
                coalesce(DSMCT_info ,'') as DSMCT_info,   /* <== 20180920 AUT_ConsDef_CC001.- Se cambia origen de descripciones */
                coalesce(nom_CDMCT  ,'') as nom_CDMCT,    /* <== 20180920 AUT_ConsDef_CC001.- Se cambia origen de descripciones */
                coalesce(desr_CDMCT ,'') as desr_CDMCT,   /* <== 20180920 AUT_ConsDef_CC001.- Se cambia origen de descripciones */
                coalesce(nom_CDMCR  ,'') as nom_CDMCR,    /* <== 20180920 AUT_ConsDef_CC001.- Se cambia origen de descripciones */
                coalesce(desr_CDMCR ,'') as desr_CDMCR,   /* <== 20180920 AUT_ConsDef_CC001.- Se cambia origen de descripciones */
                coalesce(CDMCR_agr  ,'') as CDMCR_agr,    /* <== 20180920 AUT_ConsDef_CC001.- Se cambia origen de descripciones */
                a.INTCOCON, 
                coalesce(DSTCOCON     ,'') as DSTCOCON,
                tabori, sistorig
            from (  --- 
                    select 
                        CDSINIES, CTNUPOIN, CDMCT, CDMCR, NUMOVIMI, TCTIPRES, NUMIDTRA, TCTITXCT, TCCDCATR, TCCOPERA, TCDOMORI, TCTINEGO, CDMEDPAG, 
                        TCCDMONE, /*CDTIPCAM,*/ TCTIPMOR, TCTIPMOA, IMPCEC81, CDREFDET, /*FECONTAB,*/ fecontab_fch,
                        NUAPUNTE, INTCOCON,
                        CONCAT('ktom49', INDORI) AS tabori,
                        upper(sistorig) as sistorig
                    from (
                           select 
                                CDSINIES, CTNUPOIN, CDMCT, CDMCR, NUMOVIMI, TCTIPRES, NUMIDTRA, TCTITXCT, TCCDCATR, TCCOPERA, TCDOMORI, TCTINEGO, CDMEDPAG, 
                                TCCDMONE, CDTIPCAM, TCTIPMOR, TCTIPMOA, IMPCEC81, CDREFDET, FECONTAB as fecontab_fch, NUAPUNTE, 
                                INTCOCON, 't' as INDORI, 'INFO' AS sistorig
                            from bddlcru.ktom49t
                             where cdciagru = 'AU' and trim(cdsinies) <>''
                          --  ----------  muestra ejemplo  ----------------------------------------------------------------------------------------
                          --  and cdsinies in ('0004183174','0000429035','0002716645','0014398184','0071454730','0079342119','0050406990','0049725005',
                          --                   '0075842088','0063900674','0064612880','0067140475','0068016724','0043399450','0044233385','0042004747','0021538996','0044599934',
                          --                   '0042369785','0039068762','0041919481','0043229657','0045723137','0065068777','0059570283','0042219618','0042219618',
                          --                   '0044588176','0045973534','0042923326','0048020655','0042031781','0001005743','0002589919','0000695890','0002528131','0002276830',
                          --                   '0000801373','0002090439','0002009595','0000603928','0001972496'
                          --                   )
                          -----------------------------------------------------------------------------------------------------------------------
                     union
                            select
                                CDSINIES, CTNUPOIN, CDMCT, CDMCR, NUMOVIMI, TCTIPRES, NUMIDTRA, TCTITXCT, TCCDCATR, TCCOPERA, TCDOMORI, TCTINEGO, CDMEDPAG, 
                                TCCDMONE, CDTIPCAM, TCTIPMOR, TCTIPMOA, IMPCEC81, CDREFDET, /*FECONTAB,*/
                                FECONTAB as fecontab_fch,
                                NUAPUNTE, INTCOCON, 'h' as INDORI, 'INFO' AS sistorig
                            from bddlcru.ktom49h
                            where cdciagru = 'AU' and trim(cdsinies) <>''
                           -- ----------  muestra ejemplo  ----------------------------------------------------------------------------------------
                           -- and cdsinies in ('0004183174','0000429035','0002716645','0014398184','0071454730','0079342119','0050406990','0049725005',
                           --                  '0075842088','0063900674','0064612880','0067140475','0068016724','0043399450','0044233385','0042004747','0021538996','0044599934',
                           --                  '0042369785','0039068762','0041919481','0043229657','0045723137','0065068777','0059570283','0042219618','0042219618',
                           --                  '0044588176','0045973534','0042923326','0048020655','0042031781','0001005743','0002589919','0000695890','0002528131','0002276830',
                           --                  '0000801373','0002090439','0002009595','0000603928','0001972496'
                           --                  )
                           -- ---------------------------------------------------------------------------------------------------------------------
                        ) u
             ) a
            left outer join 
            (
                SELECT    --- Dominio contable    --ok
                    trim(CDELEMEN) AS tcdomori,
                    trim(DSELEMEN) as DSDOMORI
                FROM  BDDLCRU.KTCTGET
                WHERE TRIM(CDTABLA)='KTOT20G' AND CDIDIOMA ='ES'
            ) b
            on (a.tcdomori=b.tcdomori)
            left outer join 
            (
                SELECT    --- Tipo de transaccion contable   --ok
                    substr(TRIM(CDELEMEN),1,2) AS tcdomori,
                    substr(TRIM(CDELEMEN),3,3) AS TCTITXCT,
                    trim(DSELEMEN) as DSTITXCT
                FROM  BDDLCRU.KTCTGET
                WHERE TRIM(CDTABLA)='KTOT51S' AND CDIDIOMA ='ES'
            ) c
            on (a.tcdomori=c.tcdomori and a.TCTITXCT=c.TCTITXCT)
            left outer join 
            (
                SELECT     --- Codigo de operacion contable   --ok
                    substr(TRIM(CDELEMEN),1,2) AS tcdomori,
                    substr(TRIM(CDELEMEN),3,3) AS TCCOPERA,
                    substr(trim(DSELEMEN),1,60) as DSCOPERA
                FROM  BDDLCRU.KTCTGET
                WHERE TRIM(CDTABLA)='KTOT23S' AND CDIDIOMA ='ES'
            ) d
            on (a.tcdomori=d.tcdomori and a.TCCOPERA=d.TCCOPERA)
            left outer join 
            (
                SELECT     --- Codigo de operacion contable   --ok
                    trim(CDELEMEN) AS INTCOCON,
                    trim(DSELEMEN) as DSTCOCON
                FROM  BDDLCRU.KTCTGET
                WHERE TRIM(CDTABLA)='KSITIPG' AND CDIDIOMA ='ES'
            ) e
            on (a.INTCOCON=e.INTCOCON)
            left outer join 
            (
                SELECT --cve_tipo_reserva -- ok
                    TRIM(CDTABLA) AS CDTABLA,
                    trim(CDELEMEN) AS tctipres,
                    trim(DSELEMEN) as dstipres
                FROM  BDDLCRU.KTCTGET
                WHERE TRIM(CDTABLA)='KTOTTRG' AND CDIDIOMA ='ES'
            ) g
            on (a.tctipres=g.tctipres)
            left outer join 
            (
                SELECT   --- cve_medio_pago -- ok
                    substr(TRIM(CDELEMEN),1,2) AS tcdomori,
                    substr(TRIM(CDELEMEN),3,4) AS CDMEDPAG,
                    trim(DSELEMEN) as DSMEDPAG
                FROM  BDDLCRU.KTCTGET
                WHERE TRIM(CDTABLA)='KTOT04S' AND CDIDIOMA ='ES'
            ) i
            on (a.tcdomori=i.tcdomori and a.CDMEDPAG=i.CDMEDPAG)
            left outer join  /* <== 20180920 AUT_ConsDef_CC001.- Se cambia origen de descripciones */
            (  --- Nombre y descripcion de cobertura tarificable segun INFO
                SELECT CDMCT, regexp_replace(NOMCT,'DA#O','DAÑO') as NOMCT_info, regexp_replace(DSMCT,'DA#O','DAÑO') as DSMCT_info 
                FROM bddlcru.KTPM18T 
            ) k
            on (a.CDMCT=k.CDMCT)
            LEFT OUTER JOIN  /* <== 20180920 AUT_ConsDef_CC001.- Se cambia origen de descripciones */
            (--- Nombre, descripcion y cobertura agrupada de las coberturas tarificable y afecta segun el catalogo oficial del area tecnica de autos
                select 
                      clave_cobertura_tarificable, 
                      regexp_replace(nombre_cobertura_tarificable,'DA¬O','DAÑO')      as nom_CDMCT, 
                      regexp_replace(descripcion_cobertura_tarificable,'DA¬O','DAÑO') as desr_CDMCT,
                      clave_cobertura_afecta, 
                      regexp_replace(nombre_cobertura_afecta,'DA¬O','DAÑO')      as nom_CDMCR, 
                      regexp_replace(descripcion_cobertura_afecta,'DA¬O','DAÑO') as desr_CDMCR,
                      nueva_agrupacion as CDMCR_agr
                from bddldes.coberturas_afectas 
            ) n
            on (trim(n.clave_cobertura_tarificable) = trim(a.CDMCT) and trim(n.clave_cobertura_afecta) = trim(a.CDMCR))
            left outer join ( 
                select distinct trim(from_cur) as from_cur, cast(rate_mult as float) as rate_mult, fecha from bddlint.fnz_tipo_cambio
                where from_cur is not null 
            ) TC
            on (TC.from_cur = trim(a.TCCDMONE) and TC.fecha = a.fecontab_fch)
        )  d
    )  ktom49
    order by 
         CDSINIES, ctnupoin
    ;











