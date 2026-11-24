-- h1) Las 5 combinaciones de zona de origen y de destino donde más propina se dejó en monto (suma de tip_amount), en los dos meses.
with por_par as (
    select
        d.anio,
        f.pu_zona_id,
        f.do_zona_id,
        sum(f.tip_amount) as propina_total,
        count(*) filter (where f.tip_amount > 0) as viajes_con_propina,
        count(*) as viajes
    from fact_trips as f
    join dim_date as d on d.fecha = f.fecha
    where d.mes = $mes and d.anio in ($anio, $anio - 1)
    group by d.anio, f.pu_zona_id, f.do_zona_id
)

select
    p.anio,
    row_number() over (partition by p.anio order by p.propina_total desc, zo.zona, zd.zona) as puesto,
    zo.zona as zona_origen,
    zd.zona as zona_destino,
    round(p.propina_total, 2) as propina_total,
    p.viajes_con_propina,
    p.viajes,
    round(p.propina_total / nullif(p.viajes_con_propina, 0), 2) as propina_media_con_propina
from por_par as p
join dim_zone as zo on zo.zona_id = p.pu_zona_id
join dim_zone as zd on zd.zona_id = p.do_zona_id
qualify row_number() over (partition by p.anio order by p.propina_total desc, zo.zona, zd.zona) <= 5
order by p.anio, puesto
