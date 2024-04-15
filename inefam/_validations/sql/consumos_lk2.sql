with censida as(
select
	case
		when cast(t1.mes as integer) < 10 then (t1.annual||'-0'||t1.mes||'-01')
		else  (t1.annual||'-'||t1.mes||'-01')
	end as date,
	'CENSIDA' institucion,
	t1.clave_cbycm product_id,
	sum(cast(t1.inventario as decimal)) lk2_inventario,
	sum(cast(t1.consumo_autorizado as decimal)) lk2_var
from jia2.fct_mexico_inefam_audits_censida_consumptions t1 
where 
	cast(t1.annual as integer) >= (select cast(max(annual) as integer) - 4 from jia2.fct_mexico_inefam_audits_censida_consumptions)
group by date, product_id, institucion
),
imss as (
select
	case
		when cast(t1.mes as integer) < 10 then (t1.annual||'-0'||t1.mes||'-01')
		else  (t1.annual||'-'||t1.mes||'-01')
	end as date,
	'IMSS' institucion,
	t1.clave_cbycm product_id,
	sum(cast(t1.inventario as decimal)) lk2_inventario,
	sum(cast(t1.cpm_r as decimal)) lk2_var
from jia2.fct_mexico_inefam_audits_imss_consumptions t1
where 
	cast(t1.annual as integer) >= (select cast(max(annual) as integer) - 4 from jia2.fct_mexico_inefam_audits_imss_consumptions)
group by date, product_id, institucion
),
issste as (
select
	case
		when cast(t1.mes as integer) < 10 then (t1.annual||'-0'||t1.mes||'-01')
		else  (t1.annual||'-'||t1.mes||'-01')
	end as date,
	'ISSSTE' institucion,
	t1.clave_cbycm product_id,
	sum(cast(t1.inventario as decimal)) lk2_inventario,
	sum(cast(t1.dpn as decimal)) lk2_var
from jia2.fct_mexico_inefam_audits_issste_consumptions t1
where 
	cast(t1.annual as integer) >= (select cast(max(annual) as integer) - 4 from jia2.fct_mexico_inefam_audits_altas_issste_unops)
group by date, product_id, institucion
)
select *
from censida
union all 
select *
from imss
union all
select *
from issste