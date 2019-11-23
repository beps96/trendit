-- PASO 1 Actualizar Elementos a nivel póliza, objeto y cobertura
SELECT 'bddlalm.aut_elemento_poliza_tp' tabla,
       CONCAT (CAST (format_number (COUNT (1), 0) AS STRING),
               ' Registros'
              ) cifras
  FROM bddlalm.aut_elemento_poliza_tp
UNION ALL
SELECT 'bddlalm.aut_elemento_poliza_tp' tabla,
       CONCAT ('Fecha Maxima: ',
               CAST (MAX (tsultmod) AS STRING)
              ) cifras
  FROM bddlalm.aut_elemento_poliza_tp
UNION ALL
SELECT 'bddlalm.aut_elemento_objeto_tp' tabla,
       CONCAT (CAST (format_number (COUNT (1), 0) AS STRING),
               ' Registros'
              ) cifras
  FROM bddlalm.aut_elemento_objeto_tp
UNION ALL
SELECT 'bddlalm.aut_elemento_objeto_tp' tabla,
       CONCAT ('Fecha Maxima: ',
               CAST (MAX (tsultmod) AS STRING)
              ) cifras
  FROM bddlalm.aut_elemento_objeto_tp
UNION ALL
SELECT 'bddlalm.aut_elemento_cobertura_tp' tabla,
       CONCAT (CAST (format_number (COUNT (1), 0) AS STRING),
               ' Registros'
              ) cifras
  FROM bddlalm.aut_elemento_cobertura_tp
UNION ALL
SELECT 'bddlalm.aut_elemento_cobertura_tp' tabla,
       CONCAT ('Fecha Maxima: ',
               CAST (MAX (tsultmod) AS STRING)
              ) cifras
  FROM bddlalm.aut_elemento_cobertura_tp;  

-- PASO 2 Actualizar la tabla Aut Poliza

SELECT 'bddlalm.aut_poliza' tabla,
       CONCAT (CAST (format_number (COUNT (1), 0) AS STRING),
               ' Registros'
              ) cifras
  FROM bddlalm.aut_poliza
UNION ALL
SELECT 'bddlalm.aut_poliza' tabla,
       CONCAT ('Fecha Maxima: ',
               CAST (MAX (ts_ultima_modificacion) AS STRING)
              ) cifras
  FROM bddlalm.aut_poliza;