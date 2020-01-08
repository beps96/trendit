INSERT OVERWRITE TABLE bddltrn.stg_gmm_con_padecimiento_declarado_delta_extraccion
SELECT DISTINCT
  ase.num_poliza,
  ase.vigencia_poliza,
  ase.num_version_poliza,
  CAST('' AS VARCHAR(8))                                        AS num_poliza_cobranza,
  NVL(ase.cve_asegurado, ''),
  ase.id_secuencia_asegurado,
  CAST('INFO' AS VARCHAR(4))                                    AS cve_sistema,
  CAST('INFO' AS VARCHAR(4))                                    AS cve_sistema_int,
  CAST(NVL(cat_pad.cve_homologada, '')  AS VARCHAR(10))         AS cve_grupo_pad_icd9,
  CAST(
    CASE 
      WHEN cat_pad.cve_homologada IS NULL
        THEN CONCAT('Campo no identificado para la clave <',TRIM(m04.tcgricd),'>')
      ELSE NVL(cat_pad.des_homologada, '')
      END
    AS VARCHAR(100))                                            AS des_grupo_pad_icd9,
  CAST(NVL(TRIM(cat_icd9.cve_icd9), '')       AS VARCHAR(10))   AS cve_pad_icd9,
  CAST(
    CASE
      WHEN cat_icd9.cve_icd9 IS NULL
        THEN CONCAT('Campo no identificado para la clave <',TRIM(m04.tccdicd),'>') 
      ELSE NVL(cat_icd9.padecimiento_icd9_esp, '')
      END
    AS VARCHAR(100))                                            AS des_pad_icd9,
  CAST(NVL(TRIM(m04.inexlinc), '')      AS VARCHAR(2))          AS ind_pad_inc_exc,
  CAST(NVL(TRIM(m04.inpreexi), '')      AS VARCHAR(2))          AS ban_preexistencia,
  CAST(m04.vaextrau AS DECIMAL(16,4))                           AS pct_extraprima_automatica,
  CAST(m04.vaextrma AS DECIMAL(16,4))                           AS pct_extraprima_manual,
  from_unixtime(unix_timestamp()),
  CAST('' AS VARCHAR(32)),
  CAST('' AS VARCHAR(32))
FROM (SELECT num_poliza, vigencia_poliza, num_version_poliza, cve_asegurado, id_secuencia_asegurado 
      FROM bddlalm.alm_asegurado_datos_adicionales WHERE cve_sistema = 'INFO') AS ase  -- Se cambia tabla pivote y se elimina Delta por definicion de Backlog Unico
JOIN bddlcru.kacm04t AS m04 ON
      m04.cdnumpol = ase.num_poliza
  AND m04.ctvrspol = ase.num_version_poliza
  AND m04.ctsecobj = ase.id_secuencia_asegurado
LEFT JOIN (
  SELECT TRIM(cve_atributo_org) cve_atributo_org, TRIM(cve_homologada) cve_homologada, TRIM(des_homologada) des_homologada 
  FROM bddlalm.cat_homologados
  WHERE nombre_catalogo = 'CAT_GRUPO_PADECIMIENTO'
    AND cve_ramo = 'SA'
    AND cve_sistema = 'INFO') cat_pad ON
  TRIM(m04.tcgricd) = cat_pad.cve_atributo_org
LEFT JOIN bddlcru.cat_padecimiento_icd9 cat_icd9 ON
  TRIM(m04.tccdicd) = TRIM(cat_icd9.cve_icd9);