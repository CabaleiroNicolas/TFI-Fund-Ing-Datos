-- Tabla agregada: una fila por zona de origen, fecha y hora de subida.
select
    pu_zona_id,
    fecha,
    hora,
    count(*) as viajes,
    sum(total_amount) as ingreso_total,
    avg(duracion_min) as duracion_media_min
from {{ ref('fact_trips') }}
group by pu_zona_id, fecha, hora
