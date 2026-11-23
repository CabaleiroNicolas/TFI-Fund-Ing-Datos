# Pipeline NYC Taxi

Pipeline batch que ingiere, valida, limpia y modela los viajes de taxi amarillo de Nueva York (TLC) para un mes asignado y el mismo mes del año anterior, y responde preguntas de negocio sobre el modelo resultante.

Herramientas: dlt (ingesta), DuckDB (esquema, calidad y transformación), dbt (modelo estrella), Dagster (orquestación) y Docker con Dozzle (ejecución y logs).

## Cómo instalar y ejecutar

Requisitos: Python 3.10 o superior, make y git. Docker es opcional: si está disponible se usa y, si no, el pipeline corre directo con Python.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Ejecución completa para un mes (se procesa también el mismo mes del año anterior):

```bash
make all MES=2025-03
```

`make all` elige solo cómo correr:

- **Con Docker** (si está instalado y responde): construye la imagen, levanta Dozzle y la interfaz de Dagster, y corre el pipeline en un contenedor, orquestado por Dagster. Los logs se ven en la terminal, en Dozzle (http://localhost:8080) y en Dagster (http://localhost:3000). Los contenedores quedan encendidos al terminar, para poder seguir consultando los logs; `make down` los apaga. La primera vez tarda unos minutos más porque construye la imagen.
- **Sin Docker**: corre directo con Python (`run.py`), sin Dozzle ni Dagster, y avisa por qué.

Otros comandos:

| Comando | Qué hace |
|---|---|
| `make all MES=AAAA-MM` | Pipeline completo: en Docker si se puede y, si no, con Python |
| `make all MES=AAAA-MM MODO=local` | Lo mismo, pero forzando Python: no usa Docker aunque esté instalado |
| `make local MES=AAAA-MM` | Solo con Python, sin Docker (equivale a `python run.py --mes AAAA-MM`) |
| `make docker MES=AAAA-MM` | Solo en Docker; falla si Docker no está disponible |
| `make test` | Tests automatizados con el Python local |
| `make test-docker` | Todos los tests dentro de la imagen de Docker |
| `make down` | Apaga los contenedores (pipeline, Dagster y Dozzle); `data/` no se toca |

Sin `make`, el mismo pipeline se lanza con `python3 lanzar.py --mes AAAA-MM` (con `--local`, o `-l`, para no usar Docker).

Dagster sin Docker es opcional: `pip install -r requirements-dagster.txt` y después `python run.py --mes 2025-03 --dagster`. Su interfaz se levanta con `python -m src.orquestacion web`.

Los datos no se incluyen en el repositorio (`data/` está en `.gitignore`). Se descargan en la primera corrida.

Tests:

```bash
python -m pytest tests -q
```

Los tests de Dagster se saltean si Dagster no está instalado. Dentro de la imagen de Docker (`make test-docker`) corren los de Dagster y se saltean solo los que necesitan `make`, que la imagen no trae.

## Arquitectura

```
run.py                                punto de entrada: python run.py --mes AAAA-MM [--dagster]
lanzar.py                             entrada de `make all`: elige Docker o Python y sigue el log
Makefile                              atajos (all, local, docker, test, test-docker, down)
Dockerfile, docker-compose.yml        imagen y servicios (pipeline, Dagster, Dozzle)
docker-entrypoint.sh                  corre el pipeline y deja el contenedor encendido
src/                                  una etapa por módulo (e1 a e6) y pipeline.py que las ordena;
                                      metricas.py (medición y logs) y orquestacion.py (Dagster)
contracts/yellow_schema.yaml          contrato de esquema
contracts/reglas_calidad.yaml         reglas de calidad (son datos, no código)
sql/transformacion/curado.sql         consulta que arma la capa curada
sql/modelo/                           modelos de dbt (modelo estrella) y su schema.yml
sql/tests/                            tests de dbt escritos en SQL
sql/preguntas/                        una consulta por pregunta de negocio
dbt_project.yml, profiles.yml         configuración de dbt
requirements.txt                      dependencias del pipeline, con versiones fijas
requirements-dagster.txt              Dagster, que se instala aparte
tests/                                tests automatizados
RESULTADOS.MD                         respuestas a las preguntas de marzo de 2025, con su interpretación

data/                                 no está en el repositorio
  raw/yellow/year=AAAA/month=MM/      archivos originales, sin modificar (los escribe dlt)
  raw/zones/                          tabla de zonas
  curado/year=AAAA/month=M/           capa curada (Parquet), un archivo por mes
  pipeline.duckdb                     ingestion_log, vistas y tablas de calidad
  modelo.duckdb                       modelo estrella (lo construye dbt)
  resultados/AAAA-MM.md               informe de las preguntas de cada corrida
  logs/AAAA-MM.jsonl                  log estructurado de la corrida
  dagster_home/                       historial de corridas y logs de Dagster
  dbt_logs/, dbt_target/              logs y resultados de dbt
  .tmp/, .dlt/                        archivos temporales de la ingesta
```

Los datos pasan de una capa a la siguiente así:

```
parquet crudos -> vista viajes_unificados -> aceptadas / cuarentena -> Parquet curado -> modelo estrella -> preguntas
   (ingesta)          (esquema)                   (calidad)             (transformación)     (modelo)        (preguntas)
```

| Etapa | Módulo | Qué hace |
|---|---|---|
| 1. Ingesta | `src/e1_ingesta.py` | Descarga los archivos, los carga con dlt sin modificarlos, calcula el hash y registra todo en `ingestion_log` |
| 2. Esquema | `src/e2_esquema.py` | Compara cada archivo con el contrato, normaliza nombres y tipos y unifica los dos meses en la vista `viajes_unificados` |
| 3. Calidad | `src/e3_calidad.py` | Evalúa las reglas sobre `viajes_unificados`, manda las filas rechazadas a `cuarentena` y las demás a `aceptadas`, reporta y reconcilia los conteos |
| 4. Transformación | `src/e4_transformacion.py` | Escribe la capa curada (columnas derivadas y zonas) como Parquet particionado por año y mes |
| 5. Modelo | `src/e5_modelo.py` | Construye con dbt el modelo estrella sobre la capa curada y corre sus tests |
| 6. Preguntas | `src/e6_preguntas.py` | Responde las preguntas de negocio sobre el modelo y guarda el informe del mes |

`src/pipeline.py` define el orden de las etapas y `run.py` las ejecuta para el mes pedido.

## Decisiones

### Ingesta

- **dlt como herramienta de ingesta.** Cada archivo se descarga a `data/.tmp` y se le entrega a dlt, que lo escribe en `data/raw/` con su destino `filesystem`: Parquet para los viajes y CSV para las zonas. Los viajes se leen de a lotes de 100.000 filas, sin cargar el archivo entero en memoria. DuckDB se usa para el resto del pipeline, no para ingestar.
- **Cómo se configuró dlt.**
  - Los nombres de columnas no se cambian (`SCHEMA__NAMING=direct`) y la telemetría está apagada.
  - Cada archivo tiene su propio pipeline de dlt y se borra su estado al terminar. dlt va acumulando el esquema de todo lo que carga en una tabla, y con un pipeline compartido el archivo de 2024 habría salido con una columna `cbd_congestion_fee` inventada, llena de nulos.
  - Los timestamps de la TLC no tienen zona horaria, y se le indica a dlt que no la agregue. Si no, DuckDB los corre varias horas al leerlos.
- **Carpetas estilo Hive.** Los archivos se guardan en `data/raw/yellow/year=AAAA/month=MM/`, como pide la consigna.
- **Idempotencia.** Antes de descargar se busca en `ingestion_log` el hash de la última descarga exitosa de ese archivo. Si el archivo existe y su hash actual coincide, no se descarga. Si falta o fue modificado, se vuelve a bajar. Solo cuenta como descargado lo que tiene una fila `ok` en el log, así que una corrida cortada a la mitad se recupera sola en la siguiente.
- **Escritura atómica y verificación.** La descarga va a un archivo `.part` en `data/.tmp` y recién al terminar se renombra. El hash se calcula mientras se descarga, de a bloques de 1 MB. Después de que dlt escribe el archivo final se cuentan sus filas y tienen que coincidir con las del archivo de origen; si no, la ingesta falla y deja el error en el log.
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

- **Un comando.** `run.py --mes AAAA-MM` ejecuta las seis etapas en orden. `src/pipeline.py` tiene una función por etapa, todas con la misma firma `(con, anio, mes)`, que devuelven un diccionario con lo que procesaron, y una lista `ETAPAS` que las recorre (nombre, función y si usa la base del pipeline). Agregar una etapa es agregar una línea.
- **Una sesión por corrida.** La clase `Sesion` abre la conexión a DuckDB y la medición al empezar, corre cada etapa midiéndola y lo cierra todo al terminar, aunque falle. Es la misma que usan las dos formas de correr el pipeline: el `for` de `ejecutar()` y Dagster. Se fija `memory_limit = 4GB` en DuckDB: si una consulta necesita más, escribe a disco en vez de agotar la memoria.
- **La conexión se cierra antes de dbt.** Después de calidad y curado, la conexión del pipeline retiene unos 2,4 GB de memoria aunque ya no se use. dbt corre en otro proceso y, con la conexión abierta, las dos memorias se suman. Por eso se cierra antes de la primera etapa que no usa la base (`modelo`). Medido en la misma máquina y con el mismo mes: el pico de la etapa `modelo` bajó de 4.534 MB a 2.132 MB.
- **Qué meses se procesan.** `meses_a_procesar` es el único lugar que define el par (el mes pedido y el mismo mes del año anterior).
- **Falla rápido y dice dónde.** Si una etapa falla, el pipeline se corta ahí y el error indica cuál fue (`falló la etapa 'esquema': ...`). Códigos de salida: 0 todo bien, 1 falló una etapa, 2 `--mes` con mal formato. La corrida deja sus métricas en el log aunque falle.
- **La base no es la fuente de verdad.** Todo lo que hay en `data/pipeline.duckdb` y en `data/modelo.duckdb` se reconstruye a partir de los Parquet crudos y del código. Se puede borrar `data/` entera y el pipeline la recrea. Si se borra solo la base, la ingesta vuelve a descargar los archivos (el log vacío no tiene hashes con qué compararlos) y quedan solo los meses que se vuelvan a correr. Lo único que no se recupera es el historial de descargas de `ingestion_log`.
- **Acumulación de meses.** Cada corrida deja su par de meses y no toca los demás: correr varios meses deja una partición curada y una tanda de filas en `aceptadas` y `cuarentena` por cada uno.

### Modelo

- **dbt.** Los modelos son los `SELECT` de `sql/modelo/`. `dbt build` los crea en orden de dependencias, como tablas en `data/modelo.duckdb`, y después corre los tests. Se ejecuta en un proceso aparte, así libera sus archivos y su memoria al terminar. Lee la capa curada (Parquet) y la tabla de zonas directamente, con `external_location` en `sql/modelo/fuentes.yml`, sin copiarlas a otra base.
- **Modelo estrella.** Una tabla de hechos con dos dimensiones y una tabla agregada:

  | Tabla | Una fila por... | Clave | Qué tiene |
  |---|---|---|---|
  | `fact_trips` | viaje aceptado de la capa curada | sin clave propia | `fecha` (→ `dim_date`), `pu_zona_id` y `do_zona_id` (→ `dim_zone`), hora, las medidas (distancia, duración, velocidad, tarifa, propina, cargos, total) y los códigos (`payment_type`, `ratecode_id`, `vendor_id`). Conserva `flags` y `archivo_origen` para la trazabilidad |
  | `dim_date` | día calendario de cada mes presente en la capa curada | `fecha` | año, mes, día, día de la semana (ISO) y si es fin de semana |
  | `dim_zone` | zona de la TLC | `zona_id` | borough, nombre de la zona y `service_zone` |
  | `agg_zone_hour` | zona de origen, fecha y hora de subida | (`pu_zona_id`, `fecha`, `hora`) | viajes, ingreso total y duración media. Se construye a partir de `fact_trips`; la usa la pregunta a |

- **Decisiones.**
  - Una sola dimensión de zonas para los dos extremos del viaje: `fact_trips` tiene dos claves hacia `dim_zone`, una para el origen y otra para el destino.
  - `fact_trips` no repite lo que ya sale de las dimensiones (nombres de zona y día de la semana). Los códigos de pago, tarifa y proveedor quedan como códigos en la tabla de hechos; la pregunta d traduce `payment_type` a texto con un `CASE`.
  - `dim_date` se arma con los meses que hay en la capa curada, no con un calendario fijo.
  - El modelo se construye con **todas** las particiones curadas que haya en `data/curado/`, no solo con el par del mes pedido. Las preguntas filtran por `$anio` y `$mes`.
- **Tests de dbt (15).** `not_null` y `unique` en las claves; `relationships` de `fact_trips` hacia `dim_date` y `dim_zone` (una por cada clave); `not_null` en la granularidad de `agg_zone_hour`; y dos tests escritos en SQL en `sql/tests/`: `fact_trips_cuadra_con_curado` (la tabla de hechos tiene exactamente las filas de la capa curada) y `agg_zone_hour_cuadra_con_fact` (la agregada suma los mismos viajes y el mismo ingreso que la de hechos, con tolerancia de 1 dólar). Si algún modelo o test falla, la etapa falla y el pipeline se corta.

### Preguntas de negocio

- **Una consulta por pregunta.** Están en `sql/preguntas/`: `a`, `b`, `c1`, `c2`, `d`, `e` y `f`. Se ejecutan sobre `data/modelo.duckdb`, que se abre solo para lectura.
- **Parámetros.** Cada consulta recibe solo los que usa:

  | Parámetro | Valor | Dónde se usa |
  |---|---|---|
  | `$anio`, `$mes` | los de `--mes` | todas; comparan contra el mismo mes de `$anio - 1` |
  | `$min_viajes_zona` | 1000 | c2: una zona con menos viajes en el mes no entra al ranking de proporciones, porque con pocos viajes la proporción no dice nada |

- **Una pregunta sin resultados es un error**, no una respuesta vacía.
- **Salida.** Se imprimen al final de la etapa y cada corrida guarda su informe completo (consulta, parámetros y resultado) en `data/resultados/AAAA-MM.md`. Las respuestas de marzo de 2025, con su interpretación, están en `RESULTADOS.MD`.

### Docker, Dozzle y Dagster

`docker-compose.yml` define tres servicios:

| Servicio | Contenedor | Qué hace |
|---|---|---|
| `pipeline` | `nyc-taxi-pipeline` | Corre `run.py --mes AAAA-MM --dagster` y después queda encendido |
| `dagster` | `nyc-taxi-dagster` | Interfaz web de Dagster (puerto 3000); lee el historial que escribe el pipeline |
| `dozzle` | | Pantalla web con los logs de todos los contenedores (puerto 8080) |

`pipeline` y `dagster` usan la misma imagen: `python:3.12-slim` con las dependencias de `requirements.txt` y de `requirements-dagster.txt`. Los dos montan la carpeta `./data` de la máquina.

`make all` ejecuta `lanzar.py`, que hace lo siguiente:

1. Valida el mes y consulta si Docker sirve (comando `docker`, servicio respondiendo y plugin `docker compose`).
2. Construye la imagen.
3. Levanta Dozzle.
4. Levanta la interfaz de Dagster y espera hasta 120 s a que esté sana, para que su carpeta de historial ya exista cuando el pipeline empiece a escribir en ella.
5. Levanta el contenedor del pipeline (antes borra el de la corrida anterior, si quedó).
6. Muestra su log hasta que aparece la línea `== PIPELINE TERMINÓ: código N ==` y termina con ese código.

Decisiones:

- **El contenedor no termina cuando termina el pipeline.** `docker-entrypoint.sh` corre `run.py`, imprime esa línea con el código de salida y se queda en `sleep infinity`, para poder seguir viendo los logs en Dozzle. `lanzar.py` lee la línea para saber cuándo y cómo terminó.
- **Docker nunca hace fallar el pipeline.** Si Docker no está instalado o no responde, falta `docker compose`, no se puede construir la imagen o el contenedor no arranca, se avisa y se corre directo con Python. Si lo que falla es Dozzle o la interfaz de Dagster, el pipeline corre igual en Docker y se avisa. Con `make docker` no hay respaldo: si Docker no sirve, falla.
- **Un fallo del pipeline sí se informa.** Si el pipeline falla dentro del contenedor, `make all` termina con el mismo código y no lo reintenta en local: no es un problema de infraestructura, y volver a correrlo escondería el error y repetiría el trabajo. Si el contenedor muere sin avisar, se usa su código de salida y, si lo cortó Docker por memoria, se dice.
- **Memoria.** El contenedor del pipeline tiene un tope de 6 GB y sin swap (`mem_limit` y `memswap_limit`), así que el límite es real. Adentro, DuckDB trabaja con `memory_limit = 4GB`.
- **Archivos.** `./data` se monta dentro del contenedor: los resultados quedan en la máquina y se comparten entre corridas locales y corridas en Docker. En Linux, el contenedor corre con el usuario que lanzó `make`, para que `data/` no quede a nombre de root.
- **Puertos.** 3000 y 8080 solo se abren en `127.0.0.1`: no son accesibles desde la red. Dozzle corre sin analíticas.
- **`.dockerignore`.** Deja afuera `data/`, `.venv/` y `.git/`. Sin eso el contexto de construcción pesaba 1,3 GB.

### Logs y métricas

Cada etapa deja una línea de log con lo que tardó, lo que usó y lo que procesó. Los mismos eventos se ven en tres lugares:

- **La terminal y Dozzle**, como líneas `clave=valor` (fáciles de filtrar: `etapa=calidad`, `evento=error`).
- **`data/logs/AAAA-MM.jsonl`**, un JSON por línea, con todos los eventos (también el inicio de cada etapa). Se sobrescribe en cada corrida del mismo mes.
- **Dagster**, que además guarda el historial de corridas (ver más abajo).

Una línea de ejemplo (calidad, de una corrida real en Docker):

```
ts=2026-11-22T04:59:47+00:00 level=INFO etapa=calidad evento=fin duracion_s=7.69 cpu_s=49.22 cpu_pct=640 ram_fin_mb=2479 pico_etapa_mb=2593 filas=7727885 aceptadas=7408857 cuarentena=319028 con_advertencia=1464774
```

| Campo | Qué significa |
|---|---|
| `duracion_s` | Segundos de reloj que tardó la etapa |
| `cpu_s` | Segundos de CPU sumando todos los núcleos, incluidos los procesos hijos que ya terminaron (dbt) |
| `cpu_pct` | `cpu_s` sobre `duracion_s`: 100 es un núcleo ocupado todo el tiempo, 640 son unos seis |
| `ram_fin_mb` | Memoria del proceso del pipeline al terminar la etapa |
| `pico_etapa_mb` | Pico de memoria durante la etapa: suma de la memoria del proceso y de sus hijos (dbt), muestreada cada 0,1 s |
| `filas` | Lo que procesó o produjo la etapa (no significa lo mismo en todas, ver abajo) |

Qué cuenta `filas` en cada etapa: **ingesta**, las filas de los archivos del par de meses más las de la tabla de zonas; **esquema**, no informa filas; **calidad**, las filas leídas de los dos archivos; **curado**, las filas escritas en la capa curada (las aceptadas); **modelo**, la suma de las filas de las cuatro tablas del modelo (incluye todos los meses curados); **preguntas**, las filas de las tablas de respuesta. Cada etapa agrega además sus propios datos (por ejemplo, `aceptadas` y `cuarentena` en calidad, o `tests_dbt` en modelo).

Eventos: `corrida_inicio` (mes, entorno, versión de Python, núcleos y memoria disponible), `inicio` de cada etapa (solo en el archivo), `fin` o `error` de cada etapa, y `corrida_fin` (estado, duración total, CPU y pico de RAM). Al terminar se imprime además una tabla resumen con una fila por etapa.

**Pico de memoria.** Se mide en un hilo aparte que cada 0,1 s suma la memoria del proceso y de sus hijos. El pico de la corrida es el mayor entre los de las etapas y el que informa el sistema operativo para el proceso. `ram_disponible_mb` es el tope del contenedor si lo hay (cgroup v2) o la memoria de la máquina.

### Dagster

- **Una etapa, un asset.** Cada etapa es un asset de Dagster (grupo `nyc_taxi`). Las dependencias entre ellos solo fijan el orden (ingesta, esquema, calidad, curado, modelo, preguntas): los datos siguen pasando por DuckDB y los Parquet, no por Dagster. El job `pipeline_nyc_taxi` materializa los seis.
- **El recurso `sesion`** abre la `Sesion` al empezar la corrida y la cierra al terminar, y los seis assets usan esa misma sesión: comparten la conexión y suman al mismo resumen. Las etapas son las mismas funciones que corre `ejecutar()`; Dagster reemplaza solo el `for` que las recorre.
- **Qué registra.** Cada asset escribe su línea `clave=valor` en el log de Dagster y devuelve lo medido como metadatos de la materialización (duración, filas, RAM).
- **Dónde guarda el historial.** En `data/dagster_home` (variable `DAGSTER_HOME`): corridas, eventos y los logs de cada paso en `storage/<id de la corrida>/compute_logs/`. El `.err` tiene los logs propios de Dagster y el `.out` lo que imprime el pipeline. Esa carpeta la comparten el contenedor que corre el pipeline y el que sirve la interfaz.
- **Ejecutor en proceso.** Todos los pasos corren en el mismo proceso, necesario para compartir la conexión a DuckDB. Las etapas son secuenciales de todos modos.
- **Sin daemon ni schedules.** El pipeline corre una vez por comando; no hay nada que programar. La telemetría de Dagster está desactivada.
- **Los meses no se modelan como particiones de Dagster.** El par de meses lo define `meses_a_procesar` y el modelo se construye con todas las particiones curadas.
- **Es opcional.** Dagster está en `requirements-dagster.txt`, no en `requirements.txt`. Si no está instalado o no puede abrir su carpeta, el pipeline corre sin él y lo avisa. Sin Docker, `make all` corre sin Dagster.

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
- El modelo se reconstruye con todas las particiones curadas, así que su tiempo crece con la cantidad de meses acumulados en `data/curado/`.
- El pico de RAM es una muestra (cada 0,1 s): puede perder un pico más corto que ese intervalo.
- Dozzle y la interfaz de Dagster se probaron en Linux. El historial de Dagster es una base SQLite compartida entre dos contenedores por un volumen montado; en otros sistemas (Docker Desktop en Mac o Windows) los bloqueos de archivo podrían comportarse distinto y no se probó.
- Los logs de Docker y de Dagster salen con la hora en UTC.
- Sin Docker no hay Dozzle ni Dagster: el seguimiento queda en la terminal y en `data/logs/`.

## Tiempo de ejecución y memoria

Medición de `make all MES=2025-03` en Docker (contenedor con 6.144 MB de memoria y 16 núcleos), tal como la imprime el pipeline. Los archivos ya estaban descargados (la ingesta los omitió) y en `data/curado/` había cuatro meses, así que `modelo` se construyó sobre 13.735.570 viajes:

| Etapa | Duración | CPU | RAM al final | Pico de la etapa | Filas |
|---|---:|---:|---:|---:|---:|
| ingesta | 0,2 s | 0,2 s | 227 MB | 250 MB | 7.728.150 |
| esquema | 0,0 s | 0,0 s | 228 MB | 252 MB | |
| calidad | 7,7 s | 49,2 s | 2.479 MB | 2.593 MB | 7.727.885 |
| curado | 2,4 s | 21,7 s | 2.155 MB | **2.882 MB** | 7.408.857 |
| modelo | 9,2 s | 27,9 s | 244 MB | 2.132 MB | 14.112.035 |
| preguntas | 0,3 s | 2,4 s | 246 MB | 578 MB | 35 |
| **total** | **20,7 s** | **102,9 s** | | **2.882 MB** | |

El pico de toda la corrida es de 2.882 MB, en la etapa `curado`, contra los 6.144 MB del contenedor y los 8 GB del equipo objetivo. Antes de cerrar la conexión de DuckDB antes de dbt, el pico llegaba a 4.534 MB en la etapa `modelo`.

> PENDIENTE: repetir desde un clon limpio, con la descarga y la construcción de la imagen incluidas, y anotar el tiempo total de `time make all MES=2025-03`. El pico de RAM sale del resumen que imprime el pipeline: `time` o `/usr/bin/time -v` sobre `make` no lo mide, porque en Docker el pipeline corre en otro contenedor.

## Uso de IA

> PENDIENTE: escribir esta sección con palabras propias antes de entregar: qué herramientas de IA se usaron, para qué partes del trabajo y qué se hizo para entender y verificar lo que se entregó.
