with normales as (
select 
	date,
	'Normales' Institucion,
	t1.product_code Product_ID,
	sum(cast(t1.sales_units as NUMERIC)) LK2_Units,
	sum(ROUND(cast(t1.sales_values as NUMERIC), 2)) LK2_MXN
from jia2.bv_mexico_inefam_audits_deliveries_frz t1
where  
	getyear(cast("date" as DATE)) >= 
		(
		select max(getyear(cast("date" as date))) - 4
		from jia2.bv_mexico_inefam_audits_deliveries_frz
		)
	and 
	ref_frz_dt = 
		(
		select max(ref_frz_dt) 
		from jia2.bv_mexico_inefam_audits_deliveries_frz
		)
group by date, institucion, product_id
),
imss as (
select
	case
		when mes < 10 then (annual||'-0'||mes||'-01')
		else  (annual||'-'||mes||'-01')
	end as date, 
	'IMSS' as institucion,
	clave_cbycm product_id,
	sum(piezas_alta_imss) lk2_units,
	sum(importe) lk2_mxn
from jia2.fct_mexico_inefam_audits_altas_imss_unops
where annual >= (select max(annual) - 2 from jia2.fct_mexico_inefam_audits_altas_imss_unops)
group by date, institucion, product_id
),
issste as (
select
	case
		when mes < 10 then (annual||'-0'||mes||'-01')
		else  (annual||'-'||mes||'-01')
	end as date,
	'ISSSTE' institucion,
	clave_cbycm product_id,
	sum(piezas) lk2_units,
	sum(importe) lk2_mxn
from jia2.fct_mexico_inefam_audits_altas_issste_unops 
where annual >= (select max(annual) - 2 from jia2.fct_mexico_inefam_audits_altas_issste_unops)
group by date, institucion, product_id
)
select *
from normales
union all
select *
from imss
union all 
select *
from issste