# Resultados: marzo de 2025 contra marzo de 2024

Salen de `make all MES=2025-03`. Cada consulta está en `sql/preguntas/` y se ejecuta sobre el modelo estrella (`data/modelo.duckdb`) con `$anio = 2025`, `$mes = 3` y `$min_viajes_zona = 1000`; compara siempre contra el mismo mes de `$anio - 1`. Las cifras cuentan solo viajes aceptados por la validación de calidad. Cada corrida guarda además su informe completo en `data/resultados/AAAA-MM.md`.

## a) Las 10 zonas de origen con más viajes y su variación contra marzo de 2024

| zona | borough | viajes_actual | viajes_anterior | variacion | variacion_pct |
|---|---|---|---|---|---|
| Midtown Center | Manhattan | 175034 | 158988 | 16046 | 10.1 |
| Upper East Side South | Manhattan | 165094 | 152245 | 12849 | 8.4 |
| JFK Airport | Queens | 148224 | 149429 | -1205 | -0.8 |
| Upper East Side North | Manhattan | 147813 | 142716 | 5097 | 3.6 |
| Times Sq/Theatre District | Manhattan | 130690 | 118564 | 12126 | 10.2 |
| Penn Station/Madison Sq West | Manhattan | 127849 | 111780 | 16069 | 14.4 |
| Midtown East | Manhattan | 123952 | 120657 | 3295 | 2.7 |
| Lincoln Square East | Manhattan | 113249 | 111845 | 1404 | 1.3 |
| Union Sq | Manhattan | 110036 | 94612 | 15424 | 16.3 |
| East Village | Manhattan | 106631 | 89493 | 17138 | 19.2 |

Consulta (`sql/preguntas/a_top10_zonas_origen.sql`):

```sql
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
```

**Interpretación:** Las zonas con más viajes son del centro de Manhattan (Midtown Center lidera con 175.034) y el JFK es la única del top 10 que bajó (−0,8 %); las otras nueve crecieron entre 1,3 % y 19,2 %.

## b) La hora del día con mayor ingreso promedio por viaje

| anio | hora | viajes | ingreso_promedio |
|---|---|---|---|
| 2024 | 5 | 21431 | 35.58 |
| 2025 | 5 | 31995 | 33.56 |

Consulta (`sql/preguntas/b_hora_con_mayor_ingreso_por_viaje.sql`):

```sql
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
```

**Interpretación:** La hora de mayor ingreso por viaje es las 5 de la mañana en los dos meses ($35,58 en 2024 y $33,56 en 2025), probablemente por viajes largos en una franja de poca demanda: son apenas 21.431 y 31.995 viajes.

## c) Porcentaje de viajes con cbd_congestion_fee mayor a 0 (marzo de 2025)

| viajes | viajes_con_cargo | porcentaje |
|---|---|---|
| 3955294 | 2936837 | 74.25 |

Consulta (`sql/preguntas/c1_porcentaje_viajes_con_cargo_cbd.sql`):

```sql
-- c1) Porcentaje de viajes con cbd_congestion_fee mayor a 0 en el mes asignado.
select
    count(*) as viajes,
    count(*) filter (where f.cbd_congestion_fee > 0) as viajes_con_cargo,
    round(100.0 * count(*) filter (where f.cbd_congestion_fee > 0) / count(*), 2) as porcentaje
from fact_trips as f
join dim_date as d on d.fecha = f.fecha
where d.anio = $anio and d.mes = $mes
```

**Interpretación:** El 74,25 % de los viajes (2.936.837 de 3.955.294) pagó el cargo de congestión; el resto, en principio, son viajes que no tocan la zona de cobro (Manhattan al sur de la calle 60).

## c) Las 5 zonas de origen con mayor proporción de esos viajes

| zona | borough | viajes | viajes_con_cargo | porcentaje |
|---|---|---|---|---|
| SoHo | Manhattan | 34502 | 34444 | 99.83 |
| Greenwich Village North | Manhattan | 58177 | 58036 | 99.76 |
| West Village | Manhattan | 96003 | 95767 | 99.75 |
| Little Italy/NoLiTa | Manhattan | 45863 | 45749 | 99.75 |
| Greenwich Village South | Manhattan | 64056 | 63895 | 99.75 |

Consulta (`sql/preguntas/c2_top5_zonas_con_mayor_proporcion_cbd.sql`):

```sql
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
```

**Interpretación:** Las 5 zonas (99,75 % a 99,83 %) son barrios de Manhattan dentro de la zona de cobro (SoHo, Greenwich Village, West Village, NoLiTa): casi todo viaje que sale de ahí paga el cargo. Solo entran zonas con al menos 1.000 viajes.

## d) Propina promedio como porcentaje de la tarifa, por tipo de pago

| anio | payment_type | tipo_de_pago | viajes | propina_pct_promedio | propina_pct_sobre_totales |
|---|---|---|---|---|---|
| 2024 | 0 | Flex Fare | 364913 | 3.4 | 3.67 |
| 2024 | 1 | Tarjeta de crédito | 2575235 | 25.18 | 22.39 |
| 2024 | 2 | Efectivo | 457544 | 0.0 | 0.0 |
| 2024 | 3 | Sin cargo | 12578 | 0.01 | 0.01 |
| 2024 | 4 | Disputa | 28434 | 0.19 | 0.11 |
| 2025 | 0 | Flex Fare | 725959 | 1.97 | 2.03 |
| 2025 | 1 | Tarjeta de crédito | 2663759 | 26.1 | 22.65 |
| 2025 | 2 | Efectivo | 377606 | 0.0 | 0.0 |
| 2025 | 3 | Sin cargo | 14185 | 0.01 | 0.01 |
| 2025 | 4 | Disputa | 44707 | 0.05 | 0.05 |

Consulta (`sql/preguntas/d_propina_por_tipo_de_pago.sql`):

```sql
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
```

**Interpretación:** Con tarjeta la propina es de 26,1 % de la tarifa en 2025 (25,2 % en 2024). El efectivo da 0,0 % y no es interpretable: el taxímetro no registra las propinas en efectivo, así que ese 0 significa "no registrada", no "no hubo propina". Flex Fare (2 a 3 %), Sin cargo y Disputa tienen valores muy bajos.

## e) Viajes y tarifa media de los que empiezan o terminan en JFK o LaGuardia

| anio | aeropuerto | viajes | tarifa_media |
|---|---|---|---|
| 2024 | JFK Airport | 179253 | 65.13 |
| 2024 | JFK o LaGuardia (sin repetir) | 320262 | 55.7 |
| 2024 | LaGuardia Airport | 144271 | 43.82 |
| 2025 | JFK Airport | 177572 | 64.45 |
| 2025 | JFK o LaGuardia (sin repetir) | 318293 | 55.29 |
| 2025 | LaGuardia Airport | 143512 | 43.83 |

Consulta (`sql/preguntas/e_aeropuertos_jfk_laguardia.sql`):

```sql
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
```

**Interpretación:** En marzo de 2025 hubo 318.293 viajes con origen o destino en alguno de los dos aeropuertos (JFK: 177.572 a $64,45; LaGuardia: 143.512 a $43,83), 0,6 % menos que en 2024. La suma de las dos primeras filas supera a la tercera porque los viajes entre ambos aeropuertos se cuentan una sola vez.

## f) Variación de viajes totales y de tarifa media entre los dos meses

| viajes_anterior | viajes_actual | variacion_viajes_pct | tarifa_media_anterior | tarifa_media_actual | variacion_tarifa_pct | dias_fin_de_semana_anterior | dias_fin_de_semana_actual |
|---|---|---|---|---|---|---|---|
| 3453563 | 3955294 | 14.53 | 19.06 | 18.4 | -3.47 | 10 | 10 |

Consulta (`sql/preguntas/f_variacion_entre_meses.sql`):

```sql
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
```

**Interpretación e hipótesis:** Los viajes subieron 14,53 % y la tarifa media bajó 3,47 % ($19,06 a $18,40), con los mismos 10 días de fin de semana en ambos meses.

- No parece un cambio de composición: la distancia media casi no cambió (de 3,36 a 3,33 millas).
- La baja viene de los viajes Flex Fare (`payment_type = 0`) con `fare_amount` negativo: 14.187 en marzo de 2024 (0,4 % de los aceptados) y 128.456 en marzo de 2025 (3,2 %), con tarifa media de −$1,44 y −$4,27. Se aceptaron porque R05 solo mira `total_amount`, que es positivo.
- Sin esos viajes la tarifa media pasa de $19,15 a $19,16 (+0,08 %). No se excluyeron del cálculo y no se determinó a qué corresponde la tarifa negativa.

---

Las tres consultas siguientes no son del enunciado: se agregaron como análisis adicional sobre zonas y propinas. Salen del mismo `make all MES=2025-03`.

## g) Zona de origen con más viajes por tipo de pago

| anio | payment_type | tipo_de_pago | zona | borough | viajes | viajes_del_tipo | porcentaje_del_tipo |
|---|---|---|---|---|---|---|---|
| 2024 | 0 | Flex Fare | East Village | Manhattan | 16985 | 379418 | 4.5 |
| 2024 | 1 | Tarjeta de crédito | Midtown Center | Manhattan | 124870 | 2575266 | 4.8 |
| 2024 | 2 | Efectivo | JFK Airport | Queens | 33222 | 457678 | 7.3 |
| 2024 | 3 | Sin cargo | JFK Airport | Queens | 1173 | 12650 | 9.3 |
| 2024 | 4 | Disputa | JFK Airport | Queens | 3055 | 28551 | 10.7 |
| 2025 | 0 | Flex Fare | East Village | Manhattan | 33041 | 854427 | 3.9 |
| 2025 | 1 | Tarjeta de crédito | Midtown Center | Manhattan | 135396 | 2663793 | 5.1 |
| 2025 | 2 | Efectivo | JFK Airport | Queens | 29029 | 377925 | 7.7 |
| 2025 | 3 | Sin cargo | JFK Airport | Queens | 1356 | 14279 | 9.5 |
| 2025 | 4 | Disputa | JFK Airport | Queens | 6351 | 44870 | 14.2 |

Consulta (`sql/preguntas/g_zona_de_origen_dominante_por_tipo_de_pago.sql`):

```sql
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
```

**Interpretación:** En los dos meses predomina la misma zona en cada tipo de pago: Midtown Center con tarjeta (135.396 viajes en 2025, 5,1 % de los de ese tipo), East Village con Flex Fare (33.041) y el JFK en efectivo, sin cargo y disputa; en las disputas el JFK pesa más en 2025 (14,2 %) que en 2024 (10,7 %).

## h1) Combinaciones de zona de origen y destino con más propina en monto

| anio | puesto | zona_origen | zona_destino | propina_total | viajes_con_propina | viajes | propina_media_con_propina |
|---|---|---|---|---|---|---|---|
| 2024 | 1 | JFK Airport | Times Sq/Theatre District | 76205.23 | 4998 | 7275 | 15.25 |
| 2024 | 2 | LaGuardia Airport | Times Sq/Theatre District | 74209.25 | 5892 | 6892 | 12.59 |
| 2024 | 3 | JFK Airport | Outside of NYC | 68213.29 | 3537 | 5731 | 19.29 |
| 2024 | 4 | Times Sq/Theatre District | LaGuardia Airport | 49933.46 | 3970 | 4616 | 12.58 |
| 2024 | 5 | JFK Airport | Midtown South | 45908.44 | 3100 | 4054 | 14.81 |
| 2025 | 1 | LaGuardia Airport | Times Sq/Theatre District | 72450.62 | 5748 | 6624 | 12.6 |
| 2025 | 2 | JFK Airport | Times Sq/Theatre District | 71582.95 | 4673 | 6561 | 15.32 |
| 2025 | 3 | JFK Airport | Outside of NYC | 63880.76 | 3464 | 5636 | 18.44 |
| 2025 | 4 | Times Sq/Theatre District | LaGuardia Airport | 51530.77 | 4023 | 4712 | 12.81 |
| 2025 | 5 | Upper East Side South | Upper East Side North | 47893.31 | 19621 | 23066 | 2.44 |

Consulta (`sql/preguntas/h1_pares_de_zonas_con_mas_propina_en_monto.sql`):

```sql
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
```

**Interpretación:** Por monto dominan los viajes entre los aeropuertos y Times Sq/Theatre District (en 2025, LaGuardia a Times Sq: $72.451 y JFK a Times Sq: $71.583): son unos 6.600 viajes cada uno, con propina media alta ($12,60 y $15,32 por viaje con propina). La propina solo se registra con tarjeta (97 % de los viajes con propina), así que el ranking refleja a quienes pagan con tarjeta.

## h2) Combinaciones de zona de origen y destino con más viajes con propina

| anio | puesto | zona_origen | zona_destino | viajes_con_propina | viajes | porcentaje_con_propina | propina_total |
|---|---|---|---|---|---|---|---|
| 2024 | 1 | Upper East Side South | Upper East Side North | 17744 | 21177 | 83.8 | 43879.41 |
| 2024 | 2 | Upper East Side North | Upper East Side South | 15574 | 18834 | 82.7 | 39227.05 |
| 2024 | 3 | Upper East Side North | Upper East Side North | 11922 | 15012 | 79.4 | 25518.76 |
| 2024 | 4 | Upper East Side South | Upper East Side South | 10979 | 14227 | 77.2 | 24845.85 |
| 2024 | 5 | Midtown Center | Upper East Side South | 8512 | 10451 | 81.4 | 23128.55 |
| 2025 | 1 | Upper East Side South | Upper East Side North | 19621 | 23066 | 85.1 | 47893.31 |
| 2025 | 2 | Upper East Side North | Upper East Side South | 16544 | 19818 | 83.5 | 41302.78 |
| 2025 | 3 | Upper East Side North | Upper East Side North | 12380 | 15251 | 81.2 | 26352.48 |
| 2025 | 4 | Upper East Side South | Upper East Side South | 12068 | 15206 | 79.4 | 27512.74 |
| 2025 | 5 | Midtown Center | Upper East Side South | 9230 | 11062 | 83.4 | 26341.67 |

Consulta (`sql/preguntas/h2_pares_de_zonas_con_mas_viajes_con_propina.sql`):

```sql
-- h2) Las 5 combinaciones de zona de origen y de destino con más viajes con propina (tip_amount mayor a 0), en los dos meses.
with por_par as (
    select
        d.anio,
        f.pu_zona_id,
        f.do_zona_id,
        count(*) filter (where f.tip_amount > 0) as viajes_con_propina,
        count(*) as viajes,
        sum(f.tip_amount) as propina_total
    from fact_trips as f
    join dim_date as d on d.fecha = f.fecha
    where d.mes = $mes and d.anio in ($anio, $anio - 1)
    group by d.anio, f.pu_zona_id, f.do_zona_id
)

select
    p.anio,
    row_number() over (partition by p.anio order by p.viajes_con_propina desc, zo.zona, zd.zona) as puesto,
    zo.zona as zona_origen,
    zd.zona as zona_destino,
    p.viajes_con_propina,
    p.viajes,
    round(100.0 * p.viajes_con_propina / p.viajes, 1) as porcentaje_con_propina,
    round(p.propina_total, 2) as propina_total
from por_par as p
join dim_zone as zo on zo.zona_id = p.pu_zona_id
join dim_zone as zd on zd.zona_id = p.do_zona_id
qualify row_number() over (partition by p.anio order by p.viajes_con_propina desc, zo.zona, zd.zona) <= 5
order by p.anio, puesto
```

**Interpretación:** Por cantidad dominan los viajes cortos dentro del Upper East Side (de Upper East Side South a North: 19.621 viajes con propina, el 85,1 % de sus viajes en 2025): mucha frecuencia y propina chica (unos $2,44 por viaje con propina). Contrasta con h1: el monto lo ponen los viajes largos y caros y la cantidad los cortos y frecuentes.
