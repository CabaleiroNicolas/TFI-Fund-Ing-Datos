# Pipeline NYC Taxi

Pipeline batch que ingiere, valida, limpia y modela los viajes de taxi amarillo de Nueva York (TLC) para un mes asignado y el mismo mes del año anterior, y responde preguntas de negocio sobre el modelo resultante.

## Cómo instalar y ejecutar

Requisitos: Python 3.10 o superior, make y git.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Ejecución completa para un mes (se procesa también el mismo mes del año anterior):

```bash
make all MES=2025-03
```

> PENDIENTE: el Makefile todavía no existe. Por ahora la ejecución completa es `python run.py --mes 2025-03`; el Makefile va a llamar a ese mismo comando.

Los datos no se incluyen en el repositorio (`data/` está en `.gitignore`). Se descargan en la primera corrida.

Tests:

```bash
python -m pytest tests -q
```

## Arquitectura

```
run.py                                punto de entrada: python run.py --mes AAAA-MM
src/                                  una etapa por módulo, más pipeline.py que las ordena
data/raw/yellow/year=AAAA/month=MM/   archivos originales, sin modificar
data/raw/zones/                       tabla de zonas
data/curado/year=AAAA/month=M/        capa curada (Parquet), un archivo por mes
data/pipeline.duckdb                  ingestion_log, vistas y tablas de calidad (y, más adelante, del modelo)
contracts/yellow_schema.yaml          contrato de esquema
contracts/reglas_calidad.yaml         reglas de calidad (son datos, no código)
sql/transformacion/curado.sql         consulta que arma la capa curada
tests/                                tests automatizados
```

Los datos pasan de una capa a la siguiente así:

```
parquet crudos -> vista viajes_unificados -> aceptadas / cuarentena -> Parquet curado -> modelo
   (ingesta)          (esquema)                   (calidad)             (transformación)
```

| Etapa | Módulo | Qué hace |
|---|---|---|
| 1. Ingesta | `src/e1_ingesta.py` | Descarga los archivos sin modificarlos, calcula el hash y registra todo en `ingestion_log` |
| 2. Esquema | `src/e2_esquema.py` | Compara cada archivo con el contrato, normaliza nombres y tipos y unifica los dos meses en la vista `viajes_unificados` |
| 3. Calidad | `src/e3_calidad.py` | Evalúa las reglas sobre `viajes_unificados`, manda las filas rechazadas a `cuarentena` y las demás a `aceptadas`, reporta y reconcilia los conteos |
| 4. Transformación | `src/e4_transformacion.py` | Escribe la capa curada (columnas derivadas y zonas) como Parquet particionado por año y mes |
| 5. Modelo | `src/e5_modelo.py` | PENDIENTE |

`src/pipeline.py` define el orden de las etapas y `run.py` las ejecuta para el mes pedido.

## Decisiones

### Ingesta

- **Carpetas estilo Hive.** Los archivos se guardan en `data/raw/yellow/year=AAAA/month=MM/`, como pide la consigna.
- **Idempotencia.** Antes de descargar se busca en `ingestion_log` el hash de la última descarga exitosa de ese archivo. Si el archivo existe y su hash actual coincide, no se descarga. Si falta o fue modificado, se vuelve a bajar. Solo cuenta como descargado lo que tiene una fila `ok` en el log, así que una corrida cortada a la mitad se recupera sola en la siguiente.
- **Escritura atómica.** La descarga va a un archivo `.part`, el hash se calcula mientras se descarga, y recién al terminar se renombra al nombre final. Un corte de red no deja un archivo incompleto con el nombre definitivo. Se procesa de a bloques de 1 MB, sin cargar el archivo entero en memoria.
- **Reintentos.** Hasta 3 reintentos después del primer intento (4 intentos en total), con esperas de 2, 4 y 8 segundos. Se reintentan errores de conexión, timeouts, cortes a mitad de la descarga y errores 5xx, porque pueden ser pasajeros. No se reintentan los errores 4xx: esperar no hace aparecer un archivo que no existe.
- **`ingestion_log`.** Tabla en DuckDB con archivo, URL, filas, tamaño en bytes, hash SHA-256, fecha y hora, estado (`ok` o `error`) y un mensaje. Es de solo agregado: cada corrida deja filas, incluso cuando no descarga nada (estado `ok`, mensaje "omitido"). Lo que no se duplica son los datos.
- **Falla rápido.** Ante el primer error la ingesta se corta. Códigos de salida: 0 si todo salió bien, 1 si falló una descarga y 2 si el parámetro `--mes` tiene mal el formato.

### Esquema

- **Formato del contrato.** El contrato está en YAML (`contracts/yellow_schema.yaml`), un archivo versionado y separado del código, como pide la consigna. Se eligió YAML porque se lee fácil, admite comentarios y en Git los cambios se ven columna por columna. JSON no admite comentarios, y un CSV no puede expresar las políticas junto con las columnas.
- **Contenido.** Cada columna tiene `nombre` (el nombre destino en snake_case), `origen` (cómo se llama en el archivo), `tipo`, `requerida` y `descripcion`. Las opcionales además tienen `default_si_falta`. El contrato también tiene un campo `version` y un bloque `politicas`. Las reglas de calidad quedan fuera del contrato: el contrato dice cómo es la estructura y las reglas dicen qué valores se aceptan.
- **Validación del contrato.** Antes de usarlo se revisa que esté bien armado (claves obligatorias, tipos y políticas válidos, columnas no repetidas). Un error de tipeo falla al arrancar y con un mensaje claro.
- **Políticas.**

  | Situación | Qué hace el pipeline |
  |---|---|
  | Falta una columna requerida | Falla |
  | Falta una columna opcional | Usa `default_si_falta` |
  | Aparece una columna no contratada | Avisa y la ignora |
  | Tipo distinto al del contrato | Convierte si la conversión no pierde información (por ejemplo, entero a entero más grande), sino falla |

- **Valor de `cbd_congestion_fee` en 2024: `NULL`.** El archivo de 2024 no tiene esa columna porque el cargo no existía. Un `0` afirmaría que el viaje no pagó el cargo, y eso es falso. En los archivos de 2025 la columna no tiene nulos y los viajes sin cargo traen `0`, de modo que con `NULL` en 2024 se distingue "el cargo existía y no se cobró" de "el cargo no existía". Además las agregaciones (promedios, conteos) ignoran los nulos y no mezclan ambos casos. La decisión queda escrita en el contrato (`default_si_falta: null`).
- **Si mañana aparece una columna que no está en el contrato.** El pipeline sigue, la ignora y deja una advertencia con su nombre. No se corta porque una columna nueva no vuelve inválidos los datos que sí conocemos, pero tampoco pasa en silencio. Si se prefiere que corte, se cambia la política a `error` en el contrato sin tocar el código.
- **Lectura de los archivos crudos sin detección Hive.** Como los archivos están en carpetas `year=.../month=...`, DuckDB agrega por defecto `year` y `month` como columnas, aunque no existen en el parquet. Se desactivó con `hive_partitioning = false` al leer los crudos. Si no, aparecerían como columnas nuevas falsas y después chocarían con las columnas de la partición de la capa curada.
- **Unificación como vista.** `viajes_unificados` es una vista que une los dos meses con `UNION ALL`, no una tabla. Así la etapa de esquema no copia ni carga en memoria los más de 7 millones de filas. Cada `SELECT` se genera a partir de la lista de columnas del contrato, así que quedan en el mismo orden. Se agrega una columna `archivo_origen` que no pertenece al contrato, para la trazabilidad de la cuarentena. La vista es la capa "con el esquema ya aplicado": la etapa de calidad lee de ella, y los datos unificados quedan guardados físicamente en las tablas `aceptadas` y `cuarentena`, que juntas contienen todas las filas de los dos meses.
- **La etapa de esquema también es una barrera.** Si un archivo no cumple el contrato, la etapa falla antes de que calidad o la capa curada toquen nada.

### Calidad

- **Reglas como datos.** Las 10 reglas están en `contracts/reglas_calidad.yaml`, no en el código. Cada una tiene `id`, `descripcion`, `severidad`, `umbral`, una `condicion` en SQL (verdadera cuando la fila **viola** la regla) y una `justificacion`. Para cambiar un umbral hay que editar el archivo, sin tocar Python. Se valida al cargarlo (ids no repetidos, marcadores como `{umbral}` que existan) y hay tests sobre eso.
- **Reglas, severidad y cuántas filas afectan.** Una fila puede violar varias reglas, por eso la suma de la columna es mayor que las filas en cuarentena.

  | Regla | Severidad | Qué detecta | Mar 2024 | Mar 2025 |
  |---|---|---|---:|---:|
  | R01 | rechazo | La fecha de subida cae fuera del mes del archivo | 23 | 33 |
  | R02 | rechazo | La bajada es anterior o igual a la subida | 1.128 | 22.280 |
  | R03 | rechazo | Distancia menor o igual a 0 | 87.168 | 103.722 |
  | R04 | rechazo | Distancia mayor a 100 millas | 122 | 214 |
  | R05 | rechazo | `total_amount` negativo | 44.311 | 68.686 |
  | R06 | advertencia | `passenger_count` nulo o 0 | 466.562 | 939.378 |
  | R07 | rechazo | Zona de origen o destino que no existe en la tabla de zonas (nulo cuenta como inexistente) | 0 | 0 |
  | R08 | rechazo | Duplicado exacto de otra fila (se conserva la primera copia) | 0 | 0 |
  | R09 | rechazo | Velocidad promedio mayor a 100 mph | 1.178 | 961 |
  | R10 | advertencia | Zona 264 o 265 (desconocida) | 35.374 | 28.415 |

  Resultado por mes:

  | | Mar 2024 | Mar 2025 |
  |---|---:|---:|
  | Filas leídas | 3.582.628 | 4.145.257 |
  | Aceptadas | 3.453.563 (96,4 %) | 3.955.294 (95,4 %) |
  | En cuarentena | 129.065 (3,6 %) | 189.963 (4,6 %) |
  | Con alguna advertencia | 498.809 (13,9 %) | 965.965 (23,3 %) |

- **Rechazo o advertencia.** Una regla de rechazo manda la fila a `cuarentena`, porque el dato es imposible o no se puede usar. Una de advertencia deja la fila en `aceptadas` y anota el id de la regla en la columna `flags` (una lista). R06 es advertencia porque descartar entre el 13 % y el 23 % de los viajes borraría una parte enorme de los datos. R10 es advertencia porque las zonas 264 y 265 son códigos válidos de la tabla y el viaje es utilizable.
- **Umbrales.** R04 usa 100 millas: la mediana es 1,8 millas y el percentil 99,9 ronda las 29, así que 100 queda muy fuera de lo posible para un taxi de la ciudad. R09 usa 100 mph: un taxi urbano no puede promediar eso. R09 y R10 son reglas propias, descubiertas al explorar los datos.
- **Cuarentena.** La tabla `cuarentena` tiene las columnas del contrato, `archivo_origen`, `reglas_rechazo` (la lista de todas las reglas de rechazo que violó la fila) y `reglas_advertencia`. Nada se borra: toda fila rechazada se puede ver con el motivo.
- **Tratamiento de nulos.** En SQL, comparar con `NULL` da `NULL`, y un `CASE WHEN` con condición `NULL` no se activa: una regla no marca una fila cuando la columna que evalúa es nula. Por eso las reglas que sí pueden encontrarse con nulos los tratan de forma explícita: en R06 un `passenger_count` nulo se marca igual que un 0, y en R07 una zona nula cuenta como inexistente. PENDIENTE: confirmar con una consulta que `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `trip_distance` y `total_amount` no traen nulos en los datos reales (R01 a R05 y R09 dependen de ellas).
- **Reconciliación.** Para cada mes se verifica que `filas leídas = aceptadas + cuarentena`. Las filas leídas se cuentan directamente del archivo original, aparte de la consulta que reparte las filas, para que el control sea independiente. Si no cierra, se lanza `ErrorReconciliacion` con el archivo y la diferencia.
- **Una transacción por mes.** Borrar las filas viejas del mes, insertar las nuevas, reconciliar y guardar el reporte ocurren dentro de una transacción. Si la reconciliación falla se hace `ROLLBACK` y las tablas quedan como estaban. Las tablas se crean antes de abrir la transacción: crearlas dentro de la misma donde se cargaban millones de filas dejó una tabla vacía sin ningún error (se encontró al probar con los datos reales).
- **Idempotencia.** Reprocesar un mes borra sus filas anteriores (por `archivo_origen`) y las vuelve a insertar, en la misma transacción. Los demás meses no se tocan, y los reportes (`reporte_resumen`, `reporte_reglas`) conservan una fila por archivo.
- **Reportes.** Se imprimen al final de la etapa y quedan guardados en `reporte_resumen` (leídas, aceptadas, cuarentena, con advertencia) y `reporte_reglas` (filas que viola cada regla).
- **Lo que mostraron los datos.** R02 pasa de unas 1.100 filas en 2024 a entre 22.000 y 35.000 en 2025 (marzo y abril). No se investigó la causa. R07 y R08 no encontraron nada en los datos reales; se comprobó que funcionan con datos sintéticos en los tests.

### Transformación

- **Capa curada.** Contiene los viajes aceptados de cada mes con columnas derivadas y la información de las zonas, en `data/curado/year=AAAA/month=M/curado.parquet`. Es un Parquet comprimido con zstd, un archivo por mes. Tiene 32 columnas: las 20 del contrato, `archivo_origen` y `flags`, las 6 derivadas y las 4 de zonas. La consulta está en `sql/transformacion/curado.sql` y lee de `aceptadas`, así que lo rechazado nunca llega a esta capa.
- **Columnas derivadas.**

  | Columna | Cómo se calcula | Decisión |
  |---|---|---|
  | `duracion_min` | segundos entre subida y bajada, dividido por 60 | Se mide en segundos: `date_diff('minute', ...)` descarta los segundos de cada extremo antes de restar y se equivoca hasta casi un minuto |
  | `velocidad_mph` | distancia / (segundos / 3600) | Con `NULLIF` para que una duración 0 dé `NULL` y no infinito. Las duraciones 0 ya van a cuarentena por R02 |
  | `fecha_subida`, `hora_subida` | de `tpep_pickup_datetime` | Se toma la subida, no la bajada: un viaje que empieza a las 23:59 pertenece a ese día |
  | `dia_semana` | `isodow` de la subida | ISO 8601: 1 = lunes ... 7 = domingo |
  | `propina_pct` | 100 × propina / tarifa | Solo si `payment_type = 1` (tarjeta) y `fare_amount > 0`; en los demás casos es `NULL` |

- **Por qué `propina_pct` solo con tarjeta.** El diccionario de la TLC indica que la propina se registra sola únicamente para pagos con tarjeta. En efectivo un `tip_amount` de 0 no significa "no dejó propina" sino "no se registró". El medio de pago 0 (Flex Fare) tiene 91,9 % de propinas en 0 y no es comparable. Con todos los medios mezclados el promedio daba cerca de 18 %, y solo con tarjeta 26,1 %. Las columnas `tip_amount` y `payment_type` se conservan, así que no se pierde información.
- **Zonas.** Dos `LEFT JOIN` a la tabla de zonas, uno por el origen (`pu_borough`, `pu_zona`) y otro por el destino (`do_borough`, `do_zona`). Se usa `LEFT JOIN` para que una zona sin datos no haga desaparecer el viaje. Si la tabla de zonas tuviera un id repetido, el `JOIN` duplicaría filas, y el chequeo de conteo lo detecta y corta (hay un test).
- **Un archivo por mes, escrito a mano.** Se probó `COPY ... PARTITION_BY` de DuckDB con `OVERWRITE`, y en la versión 2.0.0 borraba las particiones de los otros meses, que es justo lo que la consigna prohíbe. Por eso cada mes se escribe como su propio archivo dentro de su carpeta `year=/month=`, y DuckDB puede leerlas todas con `hive_partitioning`.
- **`month=3` sin cero.** En la capa curada la carpeta es `month=3`: con `month=03` DuckDB infiere el mes como texto y con `month=3` como número. Los archivos crudos conservan `month=03`, como se nombran los de la TLC.
- **Reemplazo atómico de una partición.** El mes se escribe en `curado.parquet.part` dentro de la misma carpeta, se cuentan sus filas y recién entonces se renombra a `curado.parquet` con `os.replace`. Si algo falla antes, se borra el `.part` y la partición anterior queda intacta. El sufijo `.part` evita que el archivo a medio escribir coincida con el patrón `curado.parquet` al leer.
- **Segunda reconciliación.** Las filas aceptadas del mes en la base tienen que ser iguales a las filas del Parquet escrito. Las del Parquet se cuentan desde su propio archivo, no desde la consulta que lo generó. Si no coinciden se lanza `ErrorReconciliacion` y no se reemplaza nada. Si un mes no tiene filas aceptadas, falla con un mensaje claro en vez de escribir una partición vacía.

#### Demostración: reprocesar un mes reemplaza solo su partición

Se corrió `python run.py --mes 2025-03`, después `python run.py --mes 2025-04` y después otra vez `python run.py --mes 2025-03`. Cada corrida procesa el mes pedido y el mismo mes de 2024. Después de las corridas se miró, para cada archivo de `data/curado/`, la hora de escritura, el hash y el conteo de filas.

| Partición | Tras correr marzo y abril | Tras volver a correr marzo |
|---|---|---|
| `year=2024/month=3` | 3.453.563 filas · 18:00:15 · sha256 `e8a0b7…` | 3.453.563 filas · **18:10:00 · sha256 `90d624…`** |
| `year=2025/month=3` | 3.955.294 filas · 18:00:16 · sha256 `aae1b1…` | 3.955.294 filas · **18:10:01 · sha256 `94b465…`** |
| `year=2024/month=4` | 3.427.577 filas · 18:07:23 · sha256 `832be0…` | 3.427.577 filas · 18:07:23 · sha256 `832be0…` |
| `year=2025/month=4` | 3.776.498 filas · 18:07:24 · sha256 `e2aed5…` | 3.776.498 filas · 18:07:24 · sha256 `e2aed5…` |

- Las particiones de marzo se reescribieron (cambia la hora y el hash) y mantienen el mismo conteo. Las de abril no se tocaron.
- El hash cambia aunque el contenido sea el mismo porque DuckDB escribe en paralelo y el orden de las filas dentro del archivo puede variar. La idempotencia es de contenido (mismos conteos, mismos reportes de calidad), no de bytes.
- Las tablas de calidad tampoco se duplicaron. Filas por archivo en `aceptadas` / `cuarentena`, iguales antes y después de volver a correr marzo:

  | Archivo | Aceptadas | Cuarentena |
  |---|---:|---:|
  | `yellow_tripdata_2024-03.parquet` | 3.453.563 | 129.065 |
  | `yellow_tripdata_2025-03.parquet` | 3.955.294 | 189.963 |
  | `yellow_tripdata_2024-04.parquet` | 3.427.577 | 86.712 |
  | `yellow_tripdata_2025-04.parquet` | 3.776.498 | 194.055 |

### Orquestación y reproducibilidad

- **Un comando.** `run.py --mes AAAA-MM` ejecuta las etapas en orden (ingesta, esquema, calidad, curado). `src/pipeline.py` tiene una función por etapa, todas con la misma firma `(con, anio, mes)`, y una lista `ETAPAS` que las recorre. Agregar una etapa es agregar una línea.
- **Una sola conexión.** Todas las etapas comparten la conexión a DuckDB, que se abre al empezar y se cierra siempre al terminar. Se fija `memory_limit = 4GB` (la mitad de los 8 GB del equipo objetivo): si una consulta necesita más, DuckDB escribe a disco en vez de agotar la memoria.
- **Qué meses se procesan.** `meses_a_procesar` es el único lugar que define el par (el mes pedido y el mismo mes del año anterior).
- **Falla rápido y dice dónde.** Si una etapa falla, el pipeline se corta ahí y el error indica cuál fue (`falló la etapa 'esquema': ...`). Códigos de salida: 0 todo bien, 1 falló una etapa, 2 `--mes` con mal formato.
- **La base no es la fuente de verdad.** Todo lo que hay en `data/pipeline.duckdb` se reconstruye a partir de los Parquet crudos y del código. Se puede borrar `data/` entera y el pipeline la recrea. Si se borra solo la base, la ingesta vuelve a descargar los archivos (el log vacío no tiene hashes con qué compararlos) y quedan solo los meses que se vuelvan a correr. Lo único que no se recupera es el historial de descargas de `ingestion_log`.
- **Acumulación de meses.** Cada corrida deja su par de meses y no toca los demás: correr varios meses deja una partición curada y una tanda de filas en `aceptadas` y `cuarentena` por cada uno.

### Modelo

> PENDIENTE: granularidad de cada tabla y decisiones del modelo estrella (Parte 5).

## Limitaciones conocidas

- El hash se compara contra lo que anotó el log. Si la TLC republica un archivo con el mismo nombre, la ingesta no lo detecta.
- El servidor responde 403 (no 404) cuando un archivo no existe, así que un 403 puede significar que el mes todavía no está publicado.
- La comparación de nombres de columna distingue mayúsculas. Si la TLC renombrara `Airport_fee` a `airport_fee`, el pipeline lo trataría como una columna requerida faltante y fallaría.
- El pipeline se probó con archivos de 2024 y 2025, que tienen el mismo esquema salvo `cbd_congestion_fee`. No se probó con años anteriores.
- El umbral de R09 (100 mph) es amplio: pasan viajes de hasta 99,3 mph, que siguen siendo dudosos. Es una cota, no una garantía.
- R05 solo mira `total_amount`. Hay viajes aceptados con `fare_amount` negativo; no afectan a `propina_pct` porque solo se calcula con `fare_amount > 0`. PENDIENTE: revisar cuántos son y qué son.
- El salto de R02 entre 2024 y 2025 no está explicado.
- `payment_type = 0` (Flex Fare) es más del 21 % de los viajes aceptados de marzo de 2025 y trae nulos en varias columnas. Las reglas no lo tratan aparte.
- La idempotencia de la capa curada es de contenido, no de bytes (el hash del archivo puede cambiar al reprocesar).
- El pico de RAM que se informa es el del proceso completo (`ru_maxrss`), no solo el de DuckDB.
- PENDIENTE: completar al terminar el proyecto.

## Tiempo de ejecución y memoria

> PENDIENTE: medir desde un clon limpio, con descarga incluida, con `/usr/bin/time -v make all MES=2025-03` en la máquina de entrega (pico de RAM y tiempo total).

Mediciones de `python run.py --mes 2025-03` en la máquina de desarrollo, con los archivos ya descargados (el pipeline imprime el tiempo y el pico de RAM al terminar):

| Corrida | Tiempo total | Calidad | Curado | Pico de RAM |
|---|---:|---:|---:|---:|
| `--mes 2025-03` | 13,9 s | 11,4 s | 2,0 s | 2.711 MB |
| `--mes 2025-04` | 11,7 s | 8,9 s | 2,2 s | 2.689 MB |
| `--mes 2025-03` otra vez | 16,0 s | 12,9 s | 2,4 s | 2.723 MB |

Medición parcial de la ingesta: primera corrida (descarga de unos 130 MB) alrededor de 5 s, y segunda corrida (solo verificación) alrededor de 0,5 s.

## Uso de IA

> PENDIENTE: revisar y ajustar este texto a lo que realmente se hizo.

Se usó Claude (Anthropic) como asistente durante el desarrollo: para explicar conceptos que no se vieron en la materia (hash, reintentos con espera creciente, formato Hive, DuckDB), para proponer la estructura del proyecto y borradores de código, y para interpretar mensajes de error. El código se ejecutó, se probó y se adaptó en la propia máquina, y todo lo entregado puede explicarse en la defensa.