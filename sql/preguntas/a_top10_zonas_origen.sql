-- a) Las 10 zonas de origen con más viajes en el mes asignado y su variación contra el mismo mes del año anterior.
with por_zona as (
    select
        a.pu_zona_id,
        coalesce(sum(a.viajes) filter (where d.anio = $anio), 0) as viajes_actual,
        coalesce(sum(a.viajes) filter (where d.anio = $anio - 1), 0) as viajes_anterior
    from agg_zone_hour as a
    join dim_date as d on d.fecha = a.fecha
    where d.mes = $mes and d.anio in ($anio, $anio - 1)
    group by a.pu_zona_id
)
select
    z.zona,
    z.borough,
    p.viajes_actual,
    p.viajes_anterior,
    p.viajes_actual - p.viajes_anterior as variacion,
    round(100.0 * (p.viajes_actual - p.viajes_anterior) / nullif(p.viajes_anterior, 0), 1) as variacion_pct
from por_zona as p
join dim_zone as z on z.zona_id = p.pu_zona_id
order by p.viajes_actual desc, z.zona
limit 10
