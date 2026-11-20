-- e) Viajes y tarifa media (fare_amount) de los que empiezan o terminan en JFK o LaGuardia, por nombre de zona.
--    Se muestran los dos meses. La fila "sin repetir" cuenta una sola vez los viajes que van de un aeropuerto al otro.
with aeropuertos as (
    select zona_id, zona
    from dim_zone
    where zona like '%JFK%' or zona like '%LaGuardia%'
),

viajes as (
    select d.anio, f.pu_zona_id, f.do_zona_id, f.fare_amount
    from fact_trips as f
    join dim_date as d on d.fecha = f.fecha
    where d.mes = $mes and d.anio in ($anio, $anio - 1)
)

select
    v.anio,
    a.zona as aeropuerto,
    count(*) as viajes,
    round(avg(v.fare_amount), 2) as tarifa_media
from viajes as v
join aeropuertos as a on a.zona_id = v.pu_zona_id or a.zona_id = v.do_zona_id
group by v.anio, a.zona

union all

select
    v.anio,
    'JFK o LaGuardia (sin repetir)' as aeropuerto,
    count(*) as viajes,
    round(avg(v.fare_amount), 2) as tarifa_media
from viajes as v
where v.pu_zona_id in (select zona_id from aeropuertos)
   or v.do_zona_id in (select zona_id from aeropuertos)
group by v.anio

order by anio, aeropuerto
