drop table bddldes.aut_siniestro_cob_afecta_insumo_nvos_parquet;
CREATE table bddldes.aut_siniestro_cob_afecta_insumo_nvos_parquet    stored as  textfile as
select *
from bddldes.aut_siniestro_cob_afecta_insumo_nvos;

drop table bddldes.aut_mec_reserva_siniestro_parquet;
CREATE table bddldes.aut_mec_reserva_siniestro_parquet    stored as textfile  as
select *
from bddldes.aut_mec_reserva_siniestro;