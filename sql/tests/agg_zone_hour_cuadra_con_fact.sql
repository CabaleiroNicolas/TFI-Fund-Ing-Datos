-- Falla si la agregada no suma los mismos viajes e ingreso que la tabla de hechos (tolerancia: 1 dólar).
select a.viajes as viajes_agg, f.viajes as viajes_fact, a.ingreso as ingreso_agg, f.ingreso as ingreso_fact
from (select sum(viajes) as viajes, sum(ingreso_total) as ingreso from {{ ref('agg_zone_hour') }}) as a
cross join (select count(*) as viajes, sum(total_amount) as ingreso from {{ ref('fact_trips') }}) as f
where a.viajes != f.viajes or abs(a.ingreso - f.ingreso) > 1
