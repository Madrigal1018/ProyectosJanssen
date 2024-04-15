select 
	case 
		when t1.mes < 10 then (t1.annual||'-0'||t1.mes||'-01')
		else (t1.annual||'-'||t1.mes||'-01')
	end as date, 
	t2.institucion,
	t1.clave_cbycm product_id,
	sum(cast(t1.piezas as decimal)) lk2_units,
	sum(cast(t1.importe_max as decimal)) lk2_mxn
from jia2.fct_mexico_inefam_audits_contracts t1
	left join jia2.dim_mexico_inefam_audits_institutions t2
	on t1.clave_clues = t2.clave_clues
where cast(t1.annual as integer) >= (
									select max(cast (annual as integer))-4
									from jia2.fct_mexico_inefam_audits_contracts
									)
	and t2.institucion in ('IMSS', 'ISSSTE', 'CENSIDA')
group by date, product_id, institucion 