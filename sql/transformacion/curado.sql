-- Capa curada de un mes: los viajes aceptados, más columnas derivadas y las zonas de origen y destino.
-- Parámetro: ? = archivo_origen del mes (por ejemplo 'yellow_tripdata_2025-03.parquet').
SELECT
    a.*,

    -- Derivadas. La duración se mide en segundos: date_diff('minute', ..) descarta los segundos
    -- de cada extremo antes de restar y se equivoca hasta casi un minuto.
    date_diff('second', a.tpep_pickup_datetime, a.tpep_dropoff_datetime) / 60.0 AS duracion_min,
    a.trip_distance / NULLIF(date_diff('second', a.tpep_pickup_datetime, a.tpep_dropoff_datetime) / 3600.0, 0) AS velocidad_mph,
    CAST(a.tpep_pickup_datetime AS DATE) AS fecha_subida,
    hour(a.tpep_pickup_datetime) AS hora_subida,
    isodow(a.tpep_pickup_datetime) AS dia_semana,          -- ISO 8601: 1 = lunes ... 7 = domingo
    -- Solo con tarjeta la propina se registra sola; en los demás medios un 0 no es interpretable.
    CASE WHEN a.payment_type = 1 AND a.fare_amount > 0
         THEN 100.0 * a.tip_amount / a.fare_amount END AS propina_pct,

    -- Zonas: barrio y nombre de la zona de origen (pu) y de destino (do).
    zo.borough AS pu_borough,
    zo.zona    AS pu_zona,
    zd.borough AS do_borough,
    zd.zona    AS do_zona
FROM aceptadas AS a
LEFT JOIN zonas AS zo ON a.pu_location_id = zo.zona_id
LEFT JOIN zonas AS zd ON a.do_location_id = zd.zona_id
WHERE a.archivo_origen = ?