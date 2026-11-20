-- Dimensión de fechas: una fila por día calendario de cada mes que hay en la capa curada.
with meses as (
    select distinct make_date(year, month, 1) as primer_dia
    from {{ source('curado', 'viajes') }}
),

dias as (
    select unnest(generate_series(primer_dia, primer_dia + interval 1 month - interval 1 day, interval 1 day))::date as fecha
    from meses
)

select
    fecha,
    year(fecha) as anio,
    month(fecha) as mes,
    day(fecha) as dia,
    isodow(fecha) as dia_semana,
    isodow(fecha) >= 6 as es_fin_de_semana
from dias
