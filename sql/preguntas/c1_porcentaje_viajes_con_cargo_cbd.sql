-- c1) Porcentaje de viajes con cbd_congestion_fee mayor a 0 en el mes asignado.
select
    count(*) as viajes,
    count(*) filter (where f.cbd_congestion_fee > 0) as viajes_con_cargo,
    round(100.0 * count(*) filter (where f.cbd_congestion_fee > 0) / count(*), 2) as porcentaje
from fact_trips as f
join dim_date as d on d.fecha = f.fecha
where d.anio = $anio and d.mes = $mes
