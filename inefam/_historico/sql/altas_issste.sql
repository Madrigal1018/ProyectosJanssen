with issste_fuente1_dateformat as (
	select 
		*,
		case 
			when cast(t1.mes as integer) < 10	then (t1.annual||'-0'||t1.mes||'-01')
												else (t1.annual||'-'||t1.mes||'-01')
		end as date
	from jia2.fct_mexico_inefam_audits_altas_issste_unops t1 
),
issste_fuente1 as (
	select 
		  cast(t1.date as varchar) date
		, t3.max_date
		, 'FUENTE 1' fuente
		, '-' marca
		, t2.institucion  
		, sum(cast(t1.piezas as decimal)) piezas
		, sum(cast(t1.importe as decimal)) importe
		, sum(cast(t1.precio as decimal)) precio
	from issste_fuente1_dateformat t1
		left outer join jia2.dim_mexico_inefam_audits_institutions t2
		on t1.clave_clues = t2.clave_clues
		cross join (
			select max(date) max_date from issste_fuente1_dateformat
			) t3
	group by t1.date, max_date, fuente, marca, institucion
),
issste_fuente2 as (
	select 
		case 
			when cast(t1.mes as integer) < 10	then (t1.annual||'-0'||t1.mes||'-01')
												else (t1.annual||'-'||t1.mes||'-01')
		end as date
		, cast(max(t1.ref_frz_dt) as varchar) max_date
		, 'FUENTE 2' fuente
		, '-' marca
		, t2.institucion  
		, sum(cast(t1.piezas as decimal)) piezas
		, sum(cast(t1.importe as decimal)) importe
		, sum(cast(t1.precio as decimal)) precio
	from jia2.fct_mexico_inefam_audits_altas_issste_unops_frz t1
		left outer join (
						select clave_clues, institucion
						from jia2.dim_mexico_inefam_audits_institutions_frz
						where ref_frz_dt = (select max(ref_frz_dt) from jia2.dim_mexico_inefam_audits_institutions_frz)
						) t2
		on t1.clave_clues = t2.clave_clues
	where
		 ref_frz_dt = (select max(ref_frz_dt) from jia2.fct_mexico_inefam_audits_altas_issste_unops_frz)
	group by date, fuente, clave_cbycm, institucion
)
select *
from issste_fuente1
union all
select *
from issste_fuente2