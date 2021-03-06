INSERT OVERWRITE TABLE BDDLALM.ALM_ASEGURADO_DATOS_ADICIONALES_RFC0049313 --quitar
SELECT num_poliza,
       vigencia_poliza,
       num_version_poliza,
       num_poliza_cobranza,
       cve_asegurado,
       cve_sistema_int,
       cve_sistema,
       cve_cambio_gpo_individual,
       des_cambio_gpo_individual,
       id_secuencia_asegurado,
       cve_zona_tarificacion,
       des_zona_tarificacion,
       codigo_postal_tarificacion,
       nombre_poblacion,
       nombre_estado,
       apellido_paterno_asegurado,
       apellido_materno_asegurado,
       nombre_asegurado,
       ban_riesgo_selecto,
       cve_periodo_riesgo_selecto,
       des_periodo_riesgo_selecto,
       fch_alta,
       fch_baja,
       fch_ini_movimiento,
       fch_fin_movimiento,
       fch_antiguedad,
       fch_antiguedad_nacional,
       fch_antiguedad_internacional,
       fch_antiguedad_nacional_otra_cia,
       fch_antiguedad_internal_otra_cia,
       fch_nacimiento,
       cve_sexo,
       des_sexo,
       cve_estatus_asegurado,
       des_estatus_asegurado,
       cve_ultima_situacion,
       des_ultima_situacion,
       cve_titular_dependiente,
       des_titular_dependiente,
       cve_parentesco,
       des_parentesco,
       cve_gratuidad_asegurado,
       des_gratuidad_asegurado,
       edad_equivalente,
       cve_estado_civil,
       des_estado_civil,
       pct_extraprima_aut_deporte,
       pct_extraprima_man_deporte,
       pct_extraprima_man_lab_dep,
       pct_extraprima_aut_laboral,
       pct_extraprima_man_laboral,
       pct_extraprima_aut_obesidad,
       pct_extraprima_aut_edad,
       cve_temporalidad_extraprima,
       des_temporalidad_extraprima,
       fch_ini_poliza_otra_cia,
       fch_fin_poliza_otra_cia,
       nombre_otra_cia,
       val_peso,
       val_talla,
       ban_indice_masa_corporal,
       pct_extraprima_automatica,
       pct_extraprima_manual,
       cve_tipo_dictamen,
       des_tipo_dictamen,
       cve_cobertura_contable,
       des_cobertura_contable,
       pct_descuento,
       ban_derecho_pol_manual,
       imp_derecho_pol_manual,
       rfc,
       ban_penalizacion_hosp,
       ban_deducible_anual,
       cve_deducible_anual,
       des_deducible_anual,
       entidad_federativa,
       municipio_delegacion,
       tot_actividades_deportivas,
       tot_actividades_laborales,
       indice_masa_corporal,
       cuenta_email,
       curp,
       cve_nacionalidad,
       des_nacionalidad,
       cve_nacionalidad_residencia,
       des_nacionalidad_residencia,
       num_exterior,
       num_interior,
       nombre_calle,
       tipo_asentamiento,
       nombre_colonia,
       num_telefono_1,
       num_telefono_2,
       codigo_postal,
       cve_pais,
       des_pais,
       ts_alta_hdfs,
       idmd5,
       idmd5completo
FROM   bddldes.stg_asegurado_datos_adicionales; 