-- c2) Las 5 zonas de origen con mayor proporción de viajes con cbd_congestion_fee mayor a 0.
--     Solo entran zonas con al menos $min_viajes_zona viajes en el mes: con pocos viajes la proporción no dice nada.
with por_zona as (
    select
        f.pu_zona_id,
        count(*) as viajes,
        count(*) filter (where f.cbd_congestion_fee > 0) as viajes_con_cargo
    from fact_trips as f
    join dim_date as d on d.fecha = f.fecha
    where d.anio = $anio and d.mes = $mes
    group by f.pu_zona_id
    having count(*) >= $min_viajes_zona
)
select
    z.zona,
    z.borough,
    p.viajes,
    p.viajes_con_cargo,
    round(100.0 * p.viajes_con_cargo / p.viajes, 2) as porcentaje
from por_zona as p
join dim_zone as z on z.zona_id = p.pu_zona_id
order by 1.0 * p.viajes_con_cargo / p.viajes desc, p.viajes desc, z.zona
limit 5
