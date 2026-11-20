-- Falla (devuelve una fila) si fact_trips no tiene exactamente las filas de la capa curada.
select f.filas as filas_fact_trips, c.filas as filas_curado
from (select count(*) as filas from {{ ref('fact_trips') }}) as f
cross join (select count(*) as filas from {{ source('curado', 'viajes') }}) as c
where f.filas != c.filas
