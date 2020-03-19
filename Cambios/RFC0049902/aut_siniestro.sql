DROP TABLE IF EXISTS bddlalm.aut_siniestro;
CREATE TABLE bddlalm.aut_siniestro  (

cve_siniestro char(10),
cve_poliza char(14),
num_version int,
desc_siniestro char(145),
cve_agrupacion_siniestro char(10),
cve_identifica_siniestro char(1),
cve_motivo_siniestro_c char(2),
cve_contrato char(5),
cve_tipo_objeto char(10),
num_secuencia_objeto int,
cve_producto_tecnico char(10),
cve_producto_comercial char(10),
cve_estructura_pol char(4),
cve_tipo_unidad_gestora_pol char(4),
cve_unidad_gestora_pol char(10),
cve_tipo_unidad_productora_pol char(4),
cve_unidad_productora_pol char(10),
cve_estructura_tramitacion char(4),
cve_tipo_unidad_tram char(4),
cve_unidad_tramitacion char(10),
cve_intermediario char(10),
cve_mediador_cobrador char(10),
cve_compania char(10),
ban_negocio_internacional char(1),
cve_tipo_reaseguro char(1),
cve_tipo_coaseguro char(1),
ban_gestion_unica char(1),
cve_factor_culpa char(1),
cve_estructura char(4),
cve_tipo_unidad_abridora char(4),
cve_unidad char(10),
cve_tipo_via char(2),
desc_tipo_via varchar(256),
lugar_ocurrencia_del_siniestro char(250),
num_via char(9),
nom_poblacion char(35),
cve_poblacion_ine char(11),
nom_provincia char(35),
cve_postal char(5),
cve_pais char(3),
desc_direccion_territorio char(20),
desc_observacion char(210),
ban_litigio char(1),
ban_existe_concurrencia char(1),
ban_existen_victimas char(1),
ban_consorcio char(1),
cve_identifica_fraude char(1),
ban_ocurrido_dia_festivo char(1),
ban_siniestro_migrado char(1),
cve_catastrofico char(6),
desc_catastrofico varchar(256),
desc_ref_cliente_reaseguro char(20),
feocusin_fch timestamp,
hora_ocurrencia_siniestro char(4),
feprimco_fch timestamp,
fecomuni_fch timestamp,
fereclam_fch timestamp,
feclaima_fch timestamp,
fehecaus_fch timestamp,
feaudit_fch timestamp,
feapertu_fch timestamp,
ban_hay_expediente char(1),
ban_si_existe_salvamento char(1),
cve_clase_siniestro char(2),
num_grupo_siniestro char(9),
cve_convenio char(10),
ferehabi_fch timestamp,
fetermin_fch timestamp,
cve_situacion_siniestro char(1),
cve_situacion_tecnica_siniestro char(1),
ban_tipo_informacion_pantalla char(1),
cve_causa_siniestro char(4),
desc_causa_siniestro varchar(256),
cve_motivo_terminacion_del_siniestro char(2),
cve_convenio_sac char(10),
version_del_convenio_sac int,
cve_estructura_operativa_siniestros char(4),
cve_tipo_unidad_operativa_siniestros char(4),
cve_unidad_operativa_siniestros char(10),
nom_persona_reporta_siniestro char(30),
cve_usuario char(10),
nom_programa char(8),
ban_siniestro_poliza_rdr_red char(1),
ban_aplica_stop_loss_cash_flow char(2),
ts_ultima_modificacion timestamp

 ) STORED AS PARQUET;


insert overwrite table bddlalm.aut_siniestro
select
cast(cdsinies as char(10))    as cve_siniestro,
--cast(tctipsin as char(2))     as cve_tipo_siniestro, 
cast(cdnumpol as char(14))    as cve_poliza,
cast(ctvrspol as int)         as num_version,
cast(dssinies as char(145))   as desc_siniestro,
cast(cdagrusi as char(10))    as cve_agrupacion_siniestro, -- a mi me suena que no va (Candido)
--cast(tccdmons as char(3))     as cve_moneda_siniestro,
cast(tcidensi as char(1))     as cve_identifica_siniestro, -- C y A pendiente Candido
-- pendiente
cast(tcmotic as char(2))      as cve_motivo_siniestro_c, -- 1S y 1N pendiente Candido
-- pendiente
cast(cdnumcon as char(5))     as cve_contrato,
cast(cdobjtp as char(10))     as cve_tipo_objeto,
cast(ctsecobj as int)         as num_secuencia_objeto,
cast(cdprodte as char(10))    as cve_producto_tecnico,
cast(cdprodco as char(10))    as cve_producto_comercial,
cast(cdestrct as char(4))     as cve_estructura_pol,
cast(cdtipges as char(4))     as cve_tipo_unidad_gestora_pol,
cast(cdunges as char(10))     as cve_unidad_gestora_pol,
cast(cdtipro as char(4))      as cve_tipo_unidad_productora_pol,
cast(cdunpro as char(10))     as cve_unidad_productora_pol,
cast(cdestram as char(4))     as cve_estructura_tramitacion,
cast(cdtitram as char(4))     as cve_tipo_unidad_tram,
cast(cduntram as char(10))    as cve_unidad_tramitacion,
cast(cdinterm as char(10))    as cve_intermediario,
cast(cdinteco as char(10))    as cve_mediador_cobrador,
cast(cdcompan as char(10))    as cve_compania,
cast(innegint as char(1))     as ban_negocio_internacional,
--cast(tcidenex as char(1))     as cve_identifica_extranjero, -- todo viene null
cast(tctiprea as char(1))     as cve_tipo_reaseguro, -- la mayoria tiene "N"
-- pendiente
cast(tctipcoa as char(1))     as cve_tipo_coaseguro, -- C y N pendiente Candido
-- pendiente
cast(ingesuni as char(1))     as ban_gestion_unica, -- pendiente varias tablas ****
cast(tcfaculp as char(1))     as cve_factor_culpa, -- T,C y A pendiente Candido
-- pendiente
--cast(cdsiniab as char(25))    as cve_siniestro_cia_abridora,
--cast(pocoaseg as float)       as porcentaje_coaseguro,
cast(cdestruc as char(4))     as cve_estructura,
cast(cdtipabr as char(4))     as cve_tipo_unidad_abridora,
cast(cdunabr as char(10))     as cve_unidad,
--cast(tctiplug as char(1))     as cve_tipo_lugar,
cast(tctipvia as char(2))     as cve_tipo_via,
cast(a04.desc_elem as varchar(256))     as desc_tipo_via,
cast(novidisi as char(250))   as lugar_ocurrencia_del_siniestro,
cast(nuvia as char(9))        as num_via,
cast(nopobl as char(35))      as nom_poblacion,
cast(tccopine as char(11))    as cve_poblacion_ine, -- desconcatenar el atributo Ivan
-- pendiente
cast(noprovin as char(35))    as nom_provincia,
cast(tccopost as char(5))     as cve_postal,
cast(tccopais as char(3))     as cve_pais,
cast(dslugacc as char(20))    as desc_direccion_territorio,
cast(dsobser as char(210))    as desc_observacion,
--cast(inintpol as char(1))     as ban_intervencion_policial,
cast(indenunc as char(1))     as ban_litigio,
cast(inexconc as char(1))     as ban_existe_concurrencia,
--cast(insintot as char(1))     as ban_siniestro_total,
--cast(imvalven as float)       as mto_valor_venal, --- ver si se calcula con el tipo de cambio
--cast(tccdmone as char(3))     as cve_moneda,
--cast(cdtipcam as char(1))     as cve_tipo_cambio,
--cast(concat(substr(cast (fecambio as string),1,4),'-',substr(cast (fecambio as string),5,2),'-',substr(cast (fecambio as string),7,2)) as timestamp) as fecambio_fch,
--cast(insinrel as char(1))     as ban_existe_sntro_relacionado,
--cast(inviajud as char(1))     as ban_existe_via_judicial,
--cast(intestig as char(1))     as ban_existe_testigo,
cast(invictim as char(1))     as ban_existen_victimas,
--cast(insinaud as char(1))     as ban_siniestro_auditado,
--cast(insinagr as char(1))     as ban_siniestro_agravado,
cast(inconsor as char(1))     as ban_consorcio,
--cast(intipape as char(2))     as ban_tipo_apertura,
cast(tcidenfr as char(1))     as cve_identifica_fraude,
--cast(infalsis as char(1))     as ban_fallo_sistemas_seguridad,
cast(inocfest as char(1))     as ban_ocurrido_dia_festivo,
cast(insimigr as char(1))     as ban_siniestro_migrado,
--cast(tcstarea as char(3))     as cve_area_del_siniestro_reaseguro,
cast(tccodcat as char(6))     as cve_catastrofico,
cast(a05.desc_elem as varchar(256))      as desc_catastrofico,
--cast(nucatast as char(9))     as num_catastrofico,
cast(dsrefcli as char(20))    as desc_ref_cliente_reaseguro,
cast(concat(substr(cast (feocusin as string),1,4),'-',substr(cast (feocusin as string),5,2),'-',substr(cast (feocusin as string),7,2)) as timestamp) as feocusin_fch,
cast(hoocsini as char(4))     as hora_ocurrencia_siniestro,
cast(concat(substr(cast (feprimco as string),1,4),'-',substr(cast (feprimco as string),5,2),'-',substr(cast (feprimco as string),7,2)) as timestamp) as feprimco_fch,
cast(concat(substr(cast (fecomuni as string),1,4),'-',substr(cast (fecomuni as string),5,2),'-',substr(cast (fecomuni as string),7,2)) as timestamp) as fecomuni_fch,
cast(concat(substr(cast (fereclam as string),1,4),'-',substr(cast (fereclam as string),5,2),'-',substr(cast (fereclam as string),7,2)) as timestamp) as fereclam_fch,
cast(concat(substr(cast (feclaima as string),1,4),'-',substr(cast (feclaima as string),5,2),'-',substr(cast (feclaima as string),7,2)) as timestamp) as feclaima_fch,
cast(concat(substr(cast (fehecaus as string),1,4),'-',substr(cast (fehecaus as string),5,2),'-',substr(cast (fehecaus as string),7,2)) as timestamp) as fehecaus_fch,
cast(concat(substr(cast (feaudit as string),1,4),'-',substr(cast (feaudit as string),5,2),'-',substr(cast (feaudit as string),7,2)) as timestamp) as feaudit_fch,
cast(concat(substr(cast (feapertu as string),1,4),'-',substr(cast (feapertu as string),5,2),'-',substr(cast (feapertu as string),7,2)) as timestamp) as feapertu_fch,
cast(inhayexp as char(1))     as ban_hay_expediente,
cast(insalvam as char(1))     as ban_si_existe_salvamento,
cast(tcclasin as char(2))     as cve_clase_siniestro, -- 00,01,10 y 11 pendiente Candido
-- pendiente
cast(nuclagro as char(9))     as num_grupo_siniestro,
cast(cdconven as char(10))    as cve_convenio,
cast(concat(substr(cast (ferehabi as string),1,4),'-',substr(cast (ferehabi as string),5,2),'-',substr(cast (ferehabi as string),7,2)) as timestamp) as ferehabi_fch,
cast(concat(substr(cast (fetermin as string),1,4),'-',substr(cast (fetermin as string),5,2),'-',substr(cast (fetermin as string),7,2)) as timestamp) as fetermin_fch,
cast(tcsitsin as char(1))     as cve_situacion_siniestro,
cast(tcsittec as char(1))     as cve_situacion_tecnica_siniestro, -- pendiente varias tablas ****
cast(intipant as char(1))     as ban_tipo_informacion_pantalla, -- B,I y P pendiente Candido
-- pendiente
cast(tccausin as char(4))     as cve_causa_siniestro,
cast(a03.desc_elem as varchar(256))      as desc_causa_siniestro, -- KSITCSS fala la obtener la desc (falta el cruce)
cast(tcmotter as char(2))     as cve_motivo_terminacion_del_siniestro,
--cast(clrefere as char(20))    as cve_referencia_externa, -- sin resultados
--cast(clreflib as char(20))    as cve_referencia_libre,
--cast(incondia as char(1))     as ban_condicionado_a,
--cast(insrbmod as char(1))     as ban_siniestro_con_robo_total_modificado,
--cast(insrbtra as char(1))     as ban_siniestro_con_robo_total_traspasado,
cast(cdconsac as char(10))    as cve_convenio_sac, -- sin resultados
cast(ctvrssac as int)         as version_del_convenio_sac,
cast(cdesopra as char(4))     as cve_estructura_operativa_siniestros,
cast(cdtiopra as char(4))     as cve_tipo_unidad_operativa_siniestros,
cast(cdunopra as char(10))    as cve_unidad_operativa_siniestros,
cast(txperrep as char(30))    as nom_persona_reporta_siniestro,
cast(cdusuari as char(10))    as cve_usuario,
--cast(cdempusu as char(4))     as cve_empresa_usuario,
cast(noprogra as char(8))     as nom_programa,
--cast(concat(substr(cast (tsultmod as string),1,4),'-',substr(cast (tsultmod as string),5,2),'-',substr(cast (tsultmod as string),7,2)) as timestamp) as tsultmod_fch,
cast(inrdr as char(1))        as ban_siniestro_poliza_rdr_red,
cast(inapslcf as char(2))     as ban_aplica_stop_loss_cash_flow,
--cast(tccdcasi as char(2))     as cve_categoria_siniestro,
--cast(cdmedpag as char(2))     as cve_medio_pago
TSULTMOD as ts_ultima_modificacion

from (
select 
ksim10t_.CDSINIES,TCTIPSIN,CDNUMPOL,CTVRSPOL,DSSINIES,CDAGRUSI,TCCDMONS,TCIDENSI,TCMOTIC,CDNUMCON,CDOBJTP,CTSECOBJ,CDPRODTE,CDPRODCO,CDESTRCT,CDTIPGES
,CDUNGES,CDTIPRO,CDUNPRO,CDESTRAM,CDTITRAM,CDUNTRAM,CDINTERM,CDINTECO,CDCOMPAN,INNEGINT,TCIDENEX,TCTIPREA,TCTIPCOA,INGESUNI,TCFACULP,CDSINIAB
,POCOASEG,CDESTRUC,CDTIPABR,CDUNABR,TCTIPLUG,TCTIPVIA,NOVIDISI,NUVIA,NOPOBL,TCCOPINE,NOPROVIN,TCCOPOST,TCCOPAIS,DSLUGACC,DSOBSER,ININTPOL
,INDENUNC,INEXCONC,INSINTOT,IMVALVEN,TCCDMONE,CDTIPCAM,FECAMBIO,INSINREL,INVIAJUD,INTESTIG,INVICTIM,INSINAUD,INSINAGR,INCONSOR,INTIPAPE
,TCIDENFR,INFALSIS,INOCFEST,INSIMIGR,TCSTAREA,TCCODCAT,NUCATAST,DSREFCLI,FEOCUSIN,HOOCSINI,FEPRIMCO,FECOMUNI,FERECLAM,FECLAIMA,FEHECAUS
,FEAUDIT,FEAPERTU,INHAYEXP,INSALVAM,TCCLASIN,NUCLAGRO,CDCONVEN,FEREHABI,FETERMIN,TCSITSIN,TCSITTEC,INTIPANT,TCCAUSIN,TCMOTTER,CLREFERE
,CLREFLIB,INCONDIA,INSRBMOD,INSRBTRA,CDCONSAC,CTVRSSAC,CDESOPRA,CDTIOPRA,CDUNOPRA,TXPERREP,CDUSUARI,CDEMPUSU,NOPROGRA,ksim10t_.TSULTMOD
,INRDR,INAPSLCF,TCCDCASI,CDMEDPAG
from (select
CDSINIES,TCTIPSIN,CDNUMPOL,CTVRSPOL,DSSINIES,CDAGRUSI,TCCDMONS,TCIDENSI,TCMOTIC,CDNUMCON,CDOBJTP,CTSECOBJ,CDPRODTE,CDPRODCO,CDESTRCT,CDTIPGES
,CDUNGES,CDTIPRO,CDUNPRO,CDESTRAM,CDTITRAM,CDUNTRAM,CDINTERM,CDINTECO,CDCOMPAN,INNEGINT,TCIDENEX,TCTIPREA,TCTIPCOA,INGESUNI,TCFACULP,CDSINIAB
,POCOASEG,CDESTRUC,CDTIPABR,CDUNABR,TCTIPLUG,TCTIPVIA,NOVIDISI,NUVIA,NOPOBL,TCCOPINE,NOPROVIN,TCCOPOST,TCCOPAIS,DSLUGACC,DSOBSER,ININTPOL
,INDENUNC,INEXCONC,INSINTOT,IMVALVEN,TCCDMONE,CDTIPCAM,FECAMBIO,INSINREL,INVIAJUD,INTESTIG,INVICTIM,INSINAUD,INSINAGR,INCONSOR,INTIPAPE
,TCIDENFR,INFALSIS,INOCFEST,INSIMIGR,TCSTAREA,TCCODCAT,NUCATAST,DSREFCLI,FEOCUSIN,HOOCSINI,FEPRIMCO,FECOMUNI,FERECLAM,FECLAIMA,FEHECAUS
,FEAUDIT,FEAPERTU,INHAYEXP,INSALVAM,TCCLASIN,NUCLAGRO,CDCONVEN,FEREHABI,FETERMIN,TCSITSIN,TCSITTEC,INTIPANT,TCCAUSIN,TCMOTTER,CLREFERE
,CLREFLIB,INCONDIA,INSRBMOD,INSRBTRA,CDCONSAC,CTVRSSAC,CDESOPRA,CDTIOPRA,CDUNOPRA,TXPERREP,CDUSUARI,CDEMPUSU,NOPROGRA,TSULTMOD
,INRDR,INAPSLCF,TCCDCASI,CDMEDPAG
from bddlcru.ksim10t  where 
 trim(TCTIPSIN)='AU'
union
select
CDSINIES,TCTIPSIN,CDNUMPOL,CTVRSPOL,DSSINIES,CDAGRUSI,TCCDMONS,TCIDENSI,TCMOTIC,CDNUMCON,CDOBJTP,CTSECOBJ,CDPRODTE,CDPRODCO,CDESTRCT,CDTIPGES
,CDUNGES,CDTIPRO,CDUNPRO,CDESTRAM,CDTITRAM,CDUNTRAM,CDINTERM,CDINTECO,CDCOMPAN,INNEGINT,TCIDENEX,TCTIPREA,TCTIPCOA,INGESUNI,TCFACULP,CDSINIAB
,POCOASEG,CDESTRUC,CDTIPABR,CDUNABR,TCTIPLUG,TCTIPVIA,NOVIDISI,NUVIA,NOPOBL,TCCOPINE,NOPROVIN,TCCOPOST,TCCOPAIS,DSLUGACC,DSOBSER,ININTPOL
,INDENUNC,INEXCONC,INSINTOT,IMVALVEN,TCCDMONE,CDTIPCAM,FECAMBIO,INSINREL,INVIAJUD,INTESTIG,INVICTIM,INSINAUD,INSINAGR,INCONSOR,INTIPAPE
,TCIDENFR,INFALSIS,INOCFEST,INSIMIGR,TCSTAREA,TCCODCAT,NUCATAST,DSREFCLI,FEOCUSIN,HOOCSINI,FEPRIMCO,FECOMUNI,FERECLAM,FECLAIMA,FEHECAUS
,FEAUDIT,FEAPERTU,INHAYEXP,INSALVAM,TCCLASIN,NUCLAGRO,CDCONVEN,FEREHABI,FETERMIN,TCSITSIN,TCSITTEC,INTIPANT,TCCAUSIN,TCMOTTER,CLREFERE
,CLREFLIB,INCONDIA,INSRBMOD,INSRBTRA,CDCONSAC,CTVRSSAC,CDESOPRA,CDTIOPRA,CDUNOPRA,TXPERREP,CDUSUARI,CDEMPUSU,NOPROGRA,TSULTMOD
,INRDR,INAPSLCF,TCCDCASI,CDMEDPAG
from bddlcru.ksim10h  where 
 trim(TCTIPSIN)='AU') ksim10t_

inner join 
(
select cdsinies,max(tsultmod) as tsultmod from ( select cdsinies,tsultmod  from bddlcru.ksim10t  union   select cdsinies,tsultmod  from bddlcru.ksim10h) a group by cdsinies) maxtsultmod

on ksim10t_.CDSINIES = maxtsultmod.CDSINIES and   ksim10t_.tsultmod = maxtsultmod.tsultmod

) as ksim10
  
   left outer join 
   (
    SELECT --TCTIPREA
     ----TRIM(CDTABLA) AS CDTABLA,
     trim(CDELEMEN) AS cve_elem,
     trim(DSELEMEN) as desc_elem
    FROM  BDDLCRU.ktctget
    WHERE TRIM(CDTABLA)='KSITCSS' AND CDIDIOMA ='ES'
   ) a03
   on (a03.cve_elem=ksim10.tccausin)
 
   left outer join 
   (
    SELECT --TCTIPREA
     ----TRIM(CDTABLA) AS CDTABLA,
     trim(CDELEMEN) AS cve_elem,
     trim(DSELEMEN) as desc_elem
    FROM  BDDLCRU.KTCTGET
    WHERE TRIM(CDTABLA)='KTCTTVG' AND CDIDIOMA ='ES'
   ) a04
   on (a04.cve_elem=ksim10.tctipvia)
   
   left outer join 
   (
    SELECT --TCTIPREA
     ----TRIM(CDTABLA) AS CDTABLA,
     trim(CDELEMEN) AS cve_elem,
     trim(DSELEMEN) as desc_elem
    FROM  BDDLCRU.KTCTGET
    WHERE TRIM(CDTABLA)='KSITECS' AND CDIDIOMA ='ES'
   ) a05
   on (a05.cve_elem=ksim10.tccodcat)
   ;