-- Dimensión de zonas: una fila por zona de la TLC.
select
    LocationID as zona_id,
    Borough as borough,
    Zone as zona,
    service_zone
from {{ source('raw', 'zonas') }}
