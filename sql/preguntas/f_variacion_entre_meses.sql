-- f) Variación de viajes totales y de tarifa media (fare_amount) entre el mes asignado y el mismo mes del año anterior.
--    Se agregan los fines de semana de cada mes, porque cambian la cantidad de viajes.
with por_anio as (
    select
        d.anio,
        count(*) as viajes,
        avg(f.fare_amount) as tarifa_media
    from fact_trips as f
    join dim_date as d on d.fecha = f.fecha
    where d.mes = $mes and d.anio in ($anio, $anio - 1)
    group by d.anio
),

fines_de_semana as (
    select anio, count(*) filter (where es_fin_de_semana) as dias
    from dim_date
    where mes = $mes and anio in ($anio, $anio - 1)
    group by anio
)

select
    ant.viajes as viajes_anterior,
    act.viajes as viajes_actual,
    round(100.0 * (act.viajes - ant.viajes) / ant.viajes, 2) as variacion_viajes_pct,
    round(ant.tarifa_media, 2) as tarifa_media_anterior,
    round(act.tarifa_media, 2) as tarifa_media_actual,
    round(100.0 * (act.tarifa_media - ant.tarifa_media) / ant.tarifa_media, 2) as variacion_tarifa_pct,
    fin_ant.dias as dias_fin_de_semana_anterior,
    fin_act.dias as dias_fin_de_semana_actual
from por_anio as ant
cross join por_anio as act
join fines_de_semana as fin_ant on fin_ant.anio = ant.anio
join fines_de_semana as fin_act on fin_act.anio = act.anio
where ant.anio = $anio - 1 and act.anio = $anio
