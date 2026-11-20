-- b) La hora del día con mayor ingreso promedio por viaje (total_amount), en cada uno de los dos meses.
with por_hora as (
    select
        d.anio,
        f.hora,
        count(*) as viajes,
        avg(f.total_amount) as ingreso_promedio
    from fact_trips as f
    join dim_date as d on d.fecha = f.fecha
    where d.mes = $mes and d.anio in ($anio, $anio - 1)
    group by d.anio, f.hora
)
select
    anio,
    hora,
    viajes,
    round(ingreso_promedio, 2) as ingreso_promedio
from por_hora
qualify row_number() over (partition by anio order by ingreso_promedio desc) = 1
order by anio
