
-- DBM1

 SELECT
	'bddlalm.aut_elemento_poliza_tp' tabla,
	CONCAT (CAST (format_number (COUNT (1),
	0) AS STRING),
	' Registros' ) registros,
	CONCAT ('Fecha Maxima: ',
	CAST (DATE_FORMAT (MAX (tsultmod),
	'YYYY-MM-d') AS STRING) ) fecha
FROM
	bddlalm.aut_elemento_poliza_tp
UNION ALL
SELECT
	'bddlalm.aut_elemento_objeto_tp' tabla,
	CONCAT (CAST (format_number (COUNT (1),
	0) AS STRING),
	' Registros' ) registros,
	CONCAT ('Fecha Maxima: ',
	CAST (DATE_FORMAT (MAX (tsultmod),
	'YYYY-MM-d') AS STRING) ) fecha
FROM
	bddlalm.aut_elemento_objeto_tp
UNION ALL
SELECT
	'bddlalm.aut_elemento_cobertura_tp' tabla,
	CONCAT (CAST (format_number (COUNT (1),
	0) AS STRING),
	' Registros' ) registros,
	CONCAT ('Fecha Maxima: ',
	CAST (DATE_FORMAT (MAX (tsultmod),
	'YYYY-MM-d') AS STRING) ) fecha
FROM
	bddlalm.aut_elemento_cobertura_tp;


-- DBM2

 SELECT
	'bddlalm.aut_poliza' tabla,
	CONCAT (CAST (format_number (COUNT (1),
	0) AS STRING),
	' Registros' ) registros,
	CONCAT ('Fecha Maxima: ',
	CAST (DATE_FORMAT (MAX (ts_ultima_modificacion),
	'YYYY-MM-d') AS STRING ) ) fecha
FROM
	bddlalm.aut_poliza;


-- WKF_EMISION_LIBII

 SELECT
	'bddlapr.aut_insumos_sb1_emision_poliza' tabla,
	CONCAT (CAST (format_number (COUNT (1),
	0) AS STRING),
	' Registros' ) registros,
	CONCAT ('Fecha Maxima: ',
	CAST (DATE_FORMAT (MAX (fch_emision),
	'YYYY-MM-d') AS STRING ) ) fecha
FROM
	bddlapr.aut_insumos_sb1_emision_poliza
UNION ALL
SELECT
	'bddlapr.aut_insumos_sb1_emision_cobertura' tabla,
	CONCAT (CAST (format_number (COUNT (1),
	0) AS STRING),
	' Registros' ) registros,
	CONCAT ('Fecha Maxima: ',
	CAST (DATE_FORMAT (MAX (fch_emision),
	'YYYY-MM-d') AS STRING ) ) fecha
FROM
	bddlapr.aut_insumos_sb1_emision_cobertura;


-- Siniestros

 SELECT
	'bddlapr.aut_siniestro_poliza' tabla,
	CONCAT (CAST (format_number (COUNT (1),
	0) AS STRING),
	' Registros' ) registros,
	CONCAT ('Fecha Maxima: ',
	CAST (DATE_FORMAT (MAX (fch_contable),
	'YYYY-MM-d') AS STRING ) ) fecha
FROM
	bddlapr.aut_siniestro_poliza
UNION ALL
SELECT
	'bddlapr.aut_siniestro_cob_afecta' tabla,
	CONCAT (CAST (format_number (COUNT (1),
	0) AS STRING),
	' Registros' ) registros,
	CONCAT ('Fecha Maxima: ',
	CAST (DATE_FORMAT (MAX (fch_contable),
	'YYYY-MM-d') AS STRING ) ) fecha
FROM
	bddlapr.aut_siniestro_cob_afecta;
	