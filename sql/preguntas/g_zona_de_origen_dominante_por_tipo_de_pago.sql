-- g) Por tipo de pago, la zona de origen con más viajes, en los dos meses.
with por_zona as (
    select
        d.anio,
        f.payment_type,
        f.pu_zona_id,
        count(*) as viajes
    from fact_trips as f
    join dim_date as d on d.fecha = f.fecha
    where d.mes = $mes and d.anio in ($anio, $anio - 1)
    group by d.anio, f.payment_type, f.pu_zona_id
),

con_total as (
    select
        *,
        sum(viajes) over (partition by anio, payment_type) as viajes_del_tipo
    from por_zona
)

select
    c.anio,
    c.payment_type,
    case c.payment_type
        when 0 then 'Flex Fare'
        when 1 then 'Tarjeta de crédito'
        when 2 then 'Efectivo'
        when 3 then 'Sin cargo'
        when 4 then 'Disputa'
        when 5 then 'Desconocido'
        when 6 then 'Viaje anulado'
    end as tipo_de_pago,
    z.zona,
    z.borough,
    c.viajes,
    c.viajes_del_tipo,
    round(100.0 * c.viajes / c.viajes_del_tipo, 1) as porcentaje_del_tipo
from con_total as c
join dim_zone as z on z.zona_id = c.pu_zona_id
qualify row_number() over (partition by c.anio, c.payment_type order by c.viajes desc, z.zona) = 1
order by c.anio, c.payment_type
