DROP TABLE IF EXISTS bddlalm.aut_siniestro_afectado;
CREATE TABLE bddlalm.aut_siniestro_afectado (
cve_siniestro    char(10)
,num_afectado    int
,cve_cliente_origen    char(10)
,cve_convenio    char(10)
,cve_tipo_afectado    char(3)
,descr_tipo_afectado    char(30)
,num_afectado_rel_padre    int
,ban_afectado_reservable    char(1)
,descripcion_del_objeto    char(40)
,ban_fraude    char(1)
,ban_culpa    char(1)
,fch_terminacion_afectado    timestamp
,fch_alta_afectado    timestamp
,cve_sit_tecnica_apertura    char(1)
,descr_sit_tecnica_apertura    char(30)
,cve_tipo_afe_aseg_contrario    char(1)
,descr_tipo_afe_aseg_contrario    char(20)
,cve_personalidad_juridica    char(1)
,descr_personalidad_juridica    char(10)
,cve_filiacion_compania_contraria    char(10)
,num_referencia_contraria    char(20)
,cve_tipo_objeto    char(10)
,num_num_contrario    int
,cve_uso_vehiculo    char(2)
,descr_uso_vehiculo    char(200)
,num_matricula_vehiculo    char(12)
,ban_prueba_alcoholismo    char(1)
,ban_asalariado    char(1)
,cve_icea_cia_contraria    char(4)
,ban_colision_directa    char(1)
,cve_tipo_via    char(2)
,descr_tipo_via    char(15)
,nombre_direccion_objeto_seguro    char(40)
,num_direccion_objeto_seguro    char(9)
,codigo_postal    char(5)
,nombre_poblacion_objeto    char(35)
,descripcion_conductor_contrario    char(120)
,cve_tipo_comunicacion_1    char(2)
,descr_tipo_comunicacion_1    char(20)
,cve_num_prefijo_pais_1    char(3)
,num_telefono_1    char(10)
,num_extension_telefono_1    char(10)
,fch_nacim_1er_conductor    timestamp
,fch_permiso_conductor    timestamp
,cve_cve_sexo    char(1)
,descripcion_contacto    char(145)
,clave_armadora    char(2)
,descr_armadora    char(20)
,carroceria    char(2)
,cve_version_vehiculo    char(2)
,descripcion_vehiculo    char(80)
,valor_vehiculo    float
,ban_activa_inactiva    char(1)
,ban_robo    char(1)
,fch_declaracion_robo    timestamp
,cve_tipo_vehiculo    char(3)
,descr_tipo_vehiculo    char(100)
,modelo_vehiculo    int
,num_serie    char(22)
,num_motor    char(22)
,registro_federal_vehicular    char(22)
,codigo_filiacion_propietario    char(10)
,cve_rfc_propietario    char(20)
,apellido1_o_razon_soc_propietario    char(30)
,apellido2_o_razon_soc_propietario    char(30)
,nombre_o_razon_social_propietario    char(40)
,cve_personalidad_jur_proietario    char(1)
,descr_personalidad_jur_proietario    char(10)
,tipo_docto_identif_proietario    char(1)
,num_prefijo_celular    char(3)
,ts_ultima_modificacion    timestamp
,sistorig    char(4)
,desc_carroceria    char(40))
STORED AS PARQUET;



INSERT INTO TABLE bddlalm.aut_siniestro_afectado 
    select
        cast(cdsinies      as char(10))    as cve_siniestro,
        cast(ctnupoin      as int)         as num_afectado,
        cast(cdfiliac      as char(10))    as cve_cliente_origen,
        cast(cdconven      as char(10))    as cve_convenio,
        cast(tcperobj      as char(3))     as cve_tipo_afectado,
        cast(dsperobj      as char(30))    as descr_tipo_afectado,
        cast(nupoirel      as int)         as num_afectado_rel_padre,
        cast(inpores       as char(1))     as ban_afectado_reservable,
        cast(dsobjpoi      as char(40))    as descripcion_del_objeto,
        cast(INFRAUDE_calc as char(1))     as ban_fraude,
        cast(INCULPA_calc  as char(1))     as ban_culpa,
    --- cast(intipant      as char(1))     as ban_tipo_informacion_pantalla,
    --- cast(tcsitper      as char(1))     as cve_situacion_afectado,  --- sin catalogo ktctget localizado
        cast(feterper_fch  as timestamp)   as fch_terminacion_afectado,
        cast(fealtper_fch  as timestamp)   as fch_alta_afectado,
        cast(tcsiteap      as char(1))     as cve_sit_tecnica_apertura,
        cast(dssiteap      as char(30))    as descr_sit_tecnica_apertura,
        cast(tcpoasco      as char(1))     as cve_tipo_afe_aseg_contrario,
        cast(dspoasco      as char(20))    as descr_tipo_afe_aseg_contrario,
        cast(tcpefiju      as char(1))     as cve_personalidad_juridica,
        cast(dspefiju      as char(10))    as descr_personalidad_juridica,
        cast(cdfilcco      as char(10))    as cve_filiacion_compania_contraria,
        cast(nurefcco      as char(20))    as num_referencia_contraria,
        cast(cdobjtp       as char(10))    as cve_tipo_objeto,
        cast(ctnuoco       as int)         as num_num_contrario,
        cast(cdusoveh      as char(2))     as cve_uso_vehiculo,
        cast(dsusoveh      as char(200))    as descr_uso_vehiculo,
        cast(numatvco      as char(12))    as num_matricula_vehiculo,
        cast(inpalcoc      as char(1))     as ban_prueba_alcoholismo,
        cast(indasala      as char(1))     as ban_asalariado,
        cast(cdiceaco      as char(4))     as cve_icea_cia_contraria,
        cast(incoldir      as char(1))     as ban_colision_directa,
        cast(tctipvia      as char(2))     as cve_tipo_via,
        cast(dstipvia      as char(15))    as descr_tipo_via,
        cast(nodirobj      as char(40))    as nombre_direccion_objeto_seguro,
        cast(nudirobj      as char(9))     as num_direccion_objeto_seguro,
        cast(tccopost      as char(5))     as codigo_postal,
        cast(nopobobj      as char(35))    as nombre_poblacion_objeto,
        cast(dsconcc       as char(120))   as descripcion_conductor_contrario,
        cast(tcticom1      as char(2))     as cve_tipo_comunicacion_1,
        cast(dsticom1      as char(20))    as descr_tipo_comunicacion_1,
        cast(tcprepa1      as char(3))     as cve_num_prefijo_pais_1,
        cast(nutelf1       as char(10))    as num_telefono_1,
        cast(nuextel1      as char(10))    as num_extension_telefono_1,
        cast(fenacco1_fch  as timestamp)   as fch_nacim_1er_conductor,
        cast(fepecon1_fch  as timestamp)   as fch_permiso_conductor,
        cast(tcsexo        as char(1))     as cve_cve_sexo,
        cast(dscontac      as char(145))   as descripcion_contacto,
        cast(tcarmado      as char(2))     as clave_armadora,
        cast(dsarmado      as char(20))    as descr_armadora,
        cast(tccarroc      as char(2))     as carroceria,
        cast(cdverveh      as char(2))     as cve_version_vehiculo,
        cast(dsvehicu      as char(80))    as descripcion_vehiculo,
        cast(vavehic       as float)       as valor_vehiculo,
        cast(inptact       as char(1))     as ban_activa_inactiva,
        cast(inderob       as char(1))     as ban_robo,
        cast(feinirob_fch  as timestamp)   as fch_declaracion_robo,
        cast(tctipveh      as char(3))     as cve_tipo_vehiculo,
        cast(dstipveh      as char(100))    as descr_tipo_vehiculo,
        cast(vamodelo      as int)         as modelo_vehiculo,
        cast(dsnumser      as char(22))    as num_serie,
        cast(dsnummot      as char(22))    as num_motor,
        cast(dsrfv         as char(22))    as registro_federal_vehicular,
        cast(cdfilprp      as char(10))    as codigo_filiacion_propietario,
        cast(cdidfprp      as char(20))    as cve_rfc_propietario,
        cast(dnap1prp      as char(30))    as apellido1_o_razon_soc_propietario,
        cast(dnap2prp      as char(30))    as apellido2_o_razon_soc_propietario,
        cast(dnnomprp      as char(40))    as nombre_o_razon_social_propietario,
        cast(tcpefprp      as char(1))     as cve_personalidad_jur_proietario,
        cast(dspefprp      as char(10))    as descr_personalidad_jur_proietario,
        cast(tctidprp      as char(1))     as tipo_docto_identif_proietario,
        cast(nuprecl1      as char(3))     as num_prefijo_celular,
        cast(tsultmod      as timestamp)     as ts_ultima_modificacion,
        cast(sistorig      as char(4))     as sistorig,
        cast(desc_carroceria as char(40)) as desc_carroceria
        
    from  (
          select
                ---- tabla 1 tcpefprp
                CDSINIES, 
                CTNUPOIN, 
                CDFILIAC, 
                CDCONVEN,
                a.TCPEROBJ, 
                coalesce(DSPEROBJ     ,'') as DSPEROBJ,
                NUPOIREL, 
                INPORES, 
                DSOBJPOI, 
                CDSECDOM, 
                INFRAUDE_calc, 
                INCULPA_calc, 
                feterper_fch, 
                fealtper_fch,
                a.TCSITEAP, 
                coalesce(dssiteap     ,'') as dssiteap,
                a.TCPOASCO, 
                coalesce(DSPOASCO     ,'') as DSPOASCO,
                a.TCPEFIJU, 
                coalesce(DSPEFIJU     ,'') as DSPEFIJU,
                CDFILCCO, 
                NUREFCCO, 
                CDOBJTP,
                CTNUOCO, 
                a.CDUSOVEH, 
                coalesce(dsusoveh     ,'') as dsusoveh,
                NUMATVCO, 
                INPALCOC, 
                INCONPOL, 
                INDASALA, 
                CDICEACO, 
                DSCOLVCO, 
                INCOLDIR, 
                a.TCTIPVIA, coalesce(dstipvia     ,'') as dstipvia,
                NODIROBJ, 
                NUDIROBJ, 
                TCCOPOST, 
                NOPOBOBJ, 
                DSCONCC, 
                a.TCTICOM1, 
                coalesce(dsticom1     ,'') as dsticom1,
                TCPREPA1, 
                NUTELF1, 
                NUEXTEL1, 
                fenacco1_fch, 
                fepecon1_fch, 
                TCSEXO, 
                DSCONTAC,
                a.TCARMADO, 
                coalesce(DSARMADO     ,'') as DSARMADO, 
                a.TCCARROC,
				coalesce(desc_carroceria     ,'') as desc_carroceria,  --- <--- 20191107 IMEDRANO : Esta es la posicion que debe ocupar en la tabla fisica productiva
                CDVERVEH, 
                DSVEHICU, 
                VAVEHIC, 
                INPTACT, 
                INDEROB, 
                feinirob_fch,
                a.TCTIPVEH, 
                coalesce(dstipveh     ,'') as dstipveh,
                VAMODELO,
                DSNUMSER, 
                DSNUMMOT, 
                DSRFV, 
                CDFILPRP, 
                CDIDFPRP, 
                DNAP1PRP, 
                DNAP2PRP, 
                DNNOMPRP,
                a.TCPEFPRP, 
                coalesce(dspefprp     ,'') as dspefprp,
               TCTIDPRP, 
               NUPRECL1, 
               TSULTMOD, 
               sistorig
            from (  --- 
                    select distinct
                        ---- tabla 1 
                        u1.CDSINIES, u1.CTNUPOIN, CDFILIAC, CDCONVEN, TCPEROBJ, /*INPERNOI, TCTIPOBJ,*/ NUPOIREL, INPORES, DSOBJPOI, CDSECDOM, 
                        case 
                            when trim(INFRAUDE) in ('N','0') then 'N' 
                            when trim(INFRAUDE) in ('S','1') then 'S' 
                            else INFRAUDE 
                       end as INFRAUDE_calc, 
                        case 
                            when trim(INCULPA) in ('N','0') then 'N' 
                            when trim(INCULPA) in ('S','1') then 'S' 
                            else INCULPA
                       end as INCULPA_calc, 
                        /*INTIPANT, TCSITPER, */
                        cast(concat(substr(cast (feterper as string),1,4),'-',substr(cast (feterper as string),5,2),'-',substr(cast (feterper as string),7,2)) as timestamp) as feterper_fch,
                        cast(concat(substr(cast (fealtper as string),1,4),'-',substr(cast (fealtper as string),5,2),'-',substr(cast (fealtper as string),7,2)) as timestamp) as fealtper_fch,
                        TCSITEAP, 
                        TCPOASCO, 
                        TCPEFIJU, 
                        CDFILCCO, NUREFCCO, CDOBJTP, 
                        ---- tabla 2
                        CTNUOCO, CDUSOVEH, trim(NUMATVCO) as NUMATVCO, INPALCOC, INCONPOL, INDASALA, CDICEACO, DSCOLVCO, /*INCICOS,*/ INCOLDIR, 
                        TCTIPVIA, trim(NODIROBJ) as NODIROBJ, NUDIROBJ, TCCOPOST, trim(NOPOBOBJ) as NOPOBOBJ, trim(DSCONCC) as DSCONCC, TCTICOM1, TCPREPA1, NUTELF1, NUEXTEL1, 
                        cast(concat(substr(cast (fenacco1 as string),1,4),'-',substr(cast (fenacco1 as string),5,2),'-',substr(cast (fenacco1 as string),7,2)) as timestamp) as fenacco1_fch,
                        cast(concat(substr(cast (fepecon1 as string),1,4),'-',substr(cast (fepecon1 as string),5,2),'-',substr(cast (fepecon1 as string),7,2)) as timestamp) as fepecon1_fch,
                        TCSEXO, trim(DSCONTAC) as DSCONTAC, TCARMADO, TCCARROC, CDVERVEH, trim(DSVEHICU) as DSVEHICU, VAVEHIC, INPTACT, INDEROB,
                        concat(trim(TCTIPVEH),trim(TCARMADO),trim(TCCARROC)) as cve_tac_vehiculo,
                        cast(concat(substr(cast (feinirob as string),1,4),'-',substr(cast (feinirob as string),5,2),'-',substr(cast (feinirob as string),7,2)) as timestamp) as feinirob_fch,
                        TCTIPVEH, VAMODELO,  trim(DSNUMSER) as DSNUMSER, trim(DSNUMMOT) as DSNUMMOT, trim(DSRFV) as DSRFV, CDFILPRP, trim(CDIDFPRP) as CDIDFPRP,
                        trim(DNAP1PRP) as DNAP1PRP, trim(DNAP2PRP) as DNAP2PRP, trim(DNNOMPRP) as DNNOMPRP, TCPEFPRP, TCTIDPRP, NUPRECL1, TSULTMOD,
                        'INFO' as sistorig
                    from (
                            select distinct
                                a.CDSINIES, a.CTNUPOIN, a.CDFILIAC, a.CDCONVEN, TCPEROBJ, /*INPERNOI, TCTIPOBJ,*/ 
                                NUPOIREL, INPORES, DSOBJPOI, CDSECDOM, INFRAUDE, INCULPA, /*a.INTIPANT, TCSITPER,*/ 
                                FETERPER, FEALTPER, TCSITEAP, TCPOASCO, TCPEFIJU, CDFILCCO, NUREFCCO, a.CDOBJTP, TSULTMOD
                            from (
                                         select 
                                             CDSINIES, CTNUPOIN, CDFILIAC, CDCONVEN, TCPEROBJ, /*INPERNOI, TCTIPOBJ,*/
                                             NUPOIREL, INPORES, DSOBJPOI, CDSECDOM, INFRAUDE, INCULPA,  /*INTIPANT, TCSITPER,*/
                                             FETERPER, FEALTPER, TCSITEAP, TCPOASCO, TCPEFIJU, CDFILCCO, NUREFCCO, CDOBJTP, TSULTMOD
                                         from bddlcru.ksim20t
                                          /* where cdsinies = '0089346035' and CTNUPOIN = 7 */
                                     union 
                                         select 
                                             CDSINIES, CTNUPOIN, CDFILIAC, CDCONVEN, TCPEROBJ, /*INPERNOI, TCTIPOBJ,*/
                                             NUPOIREL, INPORES, DSOBJPOI, CDSECDOM, INFRAUDE, INCULPA,  /*INTIPANT, TCSITPER,*/
                                             FETERPER, FEALTPER, TCSITEAP, TCPOASCO, TCPEFIJU, CDFILCCO, NUREFCCO, CDOBJTP, TSULTMOD
                                         from bddlcru.ksim20h
                                          /* where cdsinies = '0089346035' and CTNUPOIN = 7 */
                            ) a
                            inner join (select distinct CDSINIES 
                                       from (    
                                                select CDSINIES 
                                                  from bddlcru.ksim10t
                                                 where tctipsin = 'AU' 
                                                 /*  and cdsinies = '0089346035' */
                                            union 
                                                select  CDSINIES 
                                                  from bddlcru.ksim10h
                                                 where tctipsin = 'AU' 
                                                 /*  and cdsinies = '0089346035' */
                                          ) s 
                            ) b
                             on (b.CDSINIES = a.CDSINIES)
                        ) u1
                    left outer join (
                    
                            select distinct
                                CDSINIES, CTNUPOIN, CTNUOCO, CDUSOVEH, NUMATVCO, INPALCOC, INCONPOL, INDASALA, CDICEACO, DSCOLVCO, /*INCICOS,*/ INCOLDIR, 
                                TCTIPVIA, NODIROBJ, NUDIROBJ, TCCOPOST, NOPOBOBJ, DSCONCC, TCTICOM1, TCPREPA1, NUTELF1, NUEXTEL1, FENACCO1, FEPECON1, 
                                TCSEXO, DSCONTAC, TCARMADO, TCCARROC, CDVERVEH, DSVEHICU, VAVEHIC, INPTACT, INDEROB, FEINIROB, TCTIPVEH, VAMODELO, 
                                DSNUMSER, DSNUMMOT, DSRFV, CDFILPRP, CDIDFPRP, DNAP1PRP, DNAP2PRP, DNNOMPRP, TCPEFPRP, TCTIDPRP, NUPRECL1
                            from (
                    
                                        select 
                                            CDSINIES, CTNUPOIN, CTNUOCO, CDUSOVEH, NUMATVCO, INPALCOC, INCONPOL, INDASALA, CDICEACO, DSCOLVCO, /*INCICOS,*/ INCOLDIR, 
                                            TCTIPVIA, NODIROBJ, NUDIROBJ, TCCOPOST, NOPOBOBJ, DSCONCC, TCTICOM1, TCPREPA1, NUTELF1, NUEXTEL1, FENACCO1, FEPECON1, 
                                            TCSEXO, DSCONTAC, TCARMADO, TCCARROC, CDVERVEH, DSVEHICU, VAVEHIC, INPTACT, INDEROB, FEINIROB, TCTIPVEH, VAMODELO, 
                                            DSNUMSER, DSNUMMOT, DSRFV, CDFILPRP, CDIDFPRP, DNAP1PRP, DNAP2PRP, DNNOMPRP, TCPEFPRP, TCTIDPRP, NUPRECL1
                                        from bddlcru.ksia24t a  
                                         /* where cdsinies = '0089346035' and CTNUPOIN = 7 */
                                 union
                                        select 
                                            CDSINIES, CTNUPOIN, CTNUOCO, CDUSOVEH, NUMATVCO, INPALCOC, INCONPOL, INDASALA, CDICEACO, DSCOLVCO, /*INCICOS,*/ INCOLDIR, 
                                            TCTIPVIA, NODIROBJ, NUDIROBJ, TCCOPOST, NOPOBOBJ, DSCONCC, TCTICOM1, TCPREPA1, NUTELF1, NUEXTEL1, FENACCO1, FEPECON1, 
                                            TCSEXO, DSCONTAC, TCARMADO, TCCARROC, CDVERVEH, DSVEHICU, VAVEHIC, INPTACT, INDEROB, FEINIROB, TCTIPVEH, VAMODELO, 
                                            DSNUMSER, DSNUMMOT, DSRFV, CDFILPRP, CDIDFPRP, DNAP1PRP, DNAP2PRP, DNNOMPRP, TCPEFPRP, TCTIDPRP, NUPRECL1
                                        from bddlcru.ksia24h a 
                                         /* where cdsinies = '0089346035' and CTNUPOIN = 7 */
                                ) a
                        ) u2
                    on ( u1.CDSINIES=u2.CDSINIES and u1.CTNUPOIN=u2.CTNUPOIN)
             ) a
            --nuevo jc
            left outer join (
                select
                    TCTIPVEH as cve_tipo_vehiculo,
                    CDARMAD  as clave_armadora,
                    CDCARRO  as cve_carroceria,
                    DSCARRO  as desc_carroceria
                from  bddlcru.KTPT2IT ) as z
                on z.cve_tipo_vehiculo=a.tctipveh AND
                   z.clave_armadora=a.tcarmado AND
                   z.cve_carroceria=a.tccarroc
                
            left outer join 
            (
                SELECT --tipo de afectado   ---ok
                    TRIM(CDELEMEN) AS TCPEROBJ,
                    trim(DSELEMEN) as DSPEROBJ
                    FROM  BDDLCRU.KTCTGET
                WHERE TRIM(CDTABLA)='KSITTIG' AND trim(CDEMPRES) = '0001' AND CDIDIOMA ='ES'
            ) b
            on (a.TCPEROBJ=b.TCPEROBJ)
            left outer join 
            (
                SELECT -- situacion tecnica de apertura   ---ok
                    TRIM(CDELEMEN) AS tcsiteap,
                    trim(DSELEMEN) as dssiteap
                    FROM  BDDLCRU.KTCTGET
                WHERE TRIM(CDTABLA)='KSITSTG' AND CDIDIOMA ='ES'
            ) c
            on (a.tcsiteap=c.tcsiteap)
            left outer join 
            (
                SELECT -- tipo de afectado contrario   ---ok
                    TRIM(CDELEMEN) AS TCPOASCO,
                    trim(DSELEMEN) as DSPOASCO
                    FROM  BDDLCRU.KTCTGET
                WHERE TRIM(CDTABLA)='KSITACG' AND CDIDIOMA ='ES'
            ) d
            on (a.TCPOASCO=d.TCPOASCO)
            left outer join 
            (
                SELECT -- tipo de persona juridica   ---ok
                    TRIM(CDELEMEN) AS TCPEFIJU,
                    trim(DSELEMEN) as DSPEFIJU
                    FROM  BDDLCRU.KTCTGET
                WHERE TRIM(CDTABLA)='KPETCAG' AND CDIDIOMA ='ES'
            ) e
            on (a.TCPEFIJU=e.TCPEFIJU)
            left outer join 
            (
                SELECT -- tipo de via   ---ok
                    TRIM(CDELEMEN) AS tctipvia,
                    trim(DSELEMEN) as dstipvia
                    FROM  BDDLCRU.KTCTGET
                WHERE TRIM(CDTABLA)='KTCTTVG' AND CDIDIOMA ='ES'
            ) f
            on (a.tctipvia=f.tctipvia)
            left outer join 
            (
                SELECT -- tipo de comunicacion ---ok
                    TRIM(CDELEMEN) AS tcticom1,
                    trim(DSELEMEN) as dsticom1
                    FROM  BDDLCRU.KTCTGET
                WHERE TRIM(CDTABLA)='KSITACG' AND CDIDIOMA ='ES'
            ) g
            on (a.tcticom1=g.tcticom1)
            left outer join 
            (
                SELECT -- tipo de pvehiculo   ---ok
                    TRIM(CDELEMEN) AS tctipveh,
                    trim(DSELEMEN) as dstipveh
                    FROM  BDDLCRU.KTCTGET
                WHERE TRIM(CDTABLA)='KTPT2OG' AND CDIDIOMA ='ES'
            ) h
            on (a.tctipveh=h.tctipveh)
            left outer join 
            (
                SELECT --uso del vehiculo   ---ok
                    TRIM(CDELEMEN) AS cdusoveh,
                    trim(DSELEMEN) as dsusoveh
                    FROM  BDDLCRU.KTCTGET
                WHERE TRIM(CDTABLA)='KTPT2SG' AND CDIDIOMA ='ES'
            ) i
            on (a.cdusoveh=i.cdusoveh)
            left outer join 
            (
                SELECT -- armadora   ---ok
                    TRIM(CDELEMEN) AS TCARMADO,
                    trim(DSELEMEN) as DSARMADO
                    FROM  BDDLCRU.KTCTGET
                WHERE TRIM(CDTABLA)='KTPT3KG' AND CDIDIOMA ='ES'
            ) j
            on (a.TCARMADO=j.TCARMADO)
            left outer join 
            (
                SELECT -- tipo de persona juridica   ---ok
                    TRIM(CDELEMEN) AS tcpefprp,
                    trim(DSELEMEN) as dspefprp
                    FROM  BDDLCRU.KTCTGET
                WHERE TRIM(CDTABLA)='KPETCAG' AND CDIDIOMA ='ES'
            ) m
            on (a.tcpefprp=m.tcpefprp)
    )  ksim20
;