INSERT OVERWRITE TABLE bddldes.stg_comision_emitida_poliza_endoso_cbza_estadistico
SELECT 
  stg.am_contable
, stg.fch_contable
, stg.num_poliza
, stg.vigencia_poliza
, stg.num_version_poliza
, stg.num_poliza_cobranza
, stg.num_endoso
, stg.cve_cobertura_contable
, stg.des_cobertura_contable
, stg.num_consecutivo_movimiento_contable
, stg.cve_sistema
, stg.cve_sistema_int
, stg.cve_forma_pago
, stg.des_forma_pago
, stg.cve_tipo_movimiento
, stg.des_tipo_movimiento
, stg.mto_comision_prima_neta
, stg.mto_comision_derecho_poliza
, stg.mto_comision_recargo_pago_fraccionado
, stg.mto_isr_comisiones
, stg.mto_importe_comision_total
, stg.cve_unidad_medida
, stg.des_unidad_medida
, stg.pct_comision
, stg.cve_tipo_intermediario
, stg.des_tipo_intermediario
, stg.fch_movimiento_comision
, stg.fch_emision
, VE.canal_venta_estadistico
, VE.segmento_venta_estadistico
, stg.cve_segmento
, stg.des_segmento
, stg.cve_negocio_multinacional
, stg.des_negocio_multinacional
, stg.cve_tipo_bono
, stg.des_tipo_bono
, stg.afecto_plan_incentivo
, stg.ban_poliza_contributoria
, stg.pct_contribucion
, stg.ban_poliza_prestacion
, stg.cve_motivo_exclusion_plan_incentivo
, stg.des_motivo_exclusion_plan_incentivo
FROM bddldes.stg_comision_emitida_poliza_endoso_cbza stg
LEFT JOIN 
    ( 
      SELECT 
          NUM_POLIZA
        , NUM_VERSION_POLIZA
        , VIGENCIA_POLIZA
        , NUM_POLIZA_COBRANZA
        , CANAL_VENTA_ESTADISTICO
        , SEGMENTO_VENTA_ESTADISTICO
        , ANIO_MES 
        , CVE_SISTEMA
        , CVE_SISTEMA_INT
    FROM BDDLTRN.STG_ESTADISTICO_I${tabla_final} 
    UNION ALL 
    SELECT
          NUM_POLIZA
        , NUM_VERSION_POLIZA
        , VIGENCIA_POLIZA
        , NUM_POLIZA_COBRANZA
        , CANAL_VENTA_ESTADISTICO
        , SEGMENTO_VENTA_ESTADISTICO 
        , ANIO_MES 
        , CVE_SISTEMA
        , CVE_SISTEMA_INT
    FROM BDDLTRN.STG_ESTADISTICO_C${tabla_final} 
    UNION ALL 
    SELECT
          NUM_POLIZA
        , NUM_VERSION_POLIZA
        , VIGENCIA_POLIZA
        , NUM_POLIZA_COBRANZA
        , CANAL_VENTA_ESTADISTICO
        , SEGMENTO_VENTA_ESTADISTICO
        , ANIO_MES  
        , CVE_SISTEMA
        , CVE_SISTEMA_INT
    FROM BDDLTRN.STG_ESTADISTICO_A${tabla_final} 
    ) AS VE
    ON stg.NUM_POLIZA = VE.NUM_POLIZA
    AND stg.NUM_VERSION_POLIZA = VE.NUM_VERSION_POLIZA
    AND stg.VIGENCIA_POLIZA = VE.VIGENCIA_POLIZA
    AND stg.NUM_POLIZA_COBRANZA = VE.NUM_POLIZA_COBRANZA
    AND stg.CVE_SISTEMA = VE.CVE_SISTEMA
    AND stg.CVE_SISTEMA_INT = VE.CVE_SISTEMA_INT
    AND stg.am_contable = VE.ANIO_MES 
;