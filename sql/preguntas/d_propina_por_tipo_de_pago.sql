-- d) Propina promedio como porcentaje de la tarifa (fare_amount), por tipo de pago, en los dos meses.
select
    d.anio,
    f.payment_type,
    case f.payment_type
        when 0 then 'Flex Fare'
        when 1 then 'Tarjeta de crédito'
        when 2 then 'Efectivo'
        when 3 then 'Sin cargo'
        when 4 then 'Disputa'
        when 5 then 'Desconocido'
        when 6 then 'Viaje anulado'
    end as tipo_de_pago,
    count(*) as viajes,
    round(avg(100.0 * f.tip_amount / f.fare_amount), 2) as propina_pct_promedio,
    round(100.0 * sum(f.tip_amount) / sum(f.fare_amount), 2) as propina_pct_sobre_totales
from fact_trips as f
join dim_date as d on d.fecha = f.fecha
where d.mes = $mes and d.anio in ($anio, $anio - 1)
  and f.fare_amount > 0
  and f.tip_amount is not null
group by d.anio, f.payment_type
order by d.anio, f.payment_type
