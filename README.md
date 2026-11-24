# Pipeline NYC Taxi

Integrantes:

- Cabaleiro Nicolas - 53136
- Argañaraz Vargas Angelica - 53492

Trabajo final de Fundamentos de la Ingeniería de Datos. Es un pipeline batch que baja los viajes de yellow taxi de Nueva York (TLC) de un mes y del mismo mes del año anterior, los valida, los limpia, arma un modelo estrella y responde las preguntas de negocio. Todo se corre con un solo comando: `make all MES=AAAA-MM`.

## Stack

| Herramienta | Para qué | Por qué esa |
|---|---|---|
| Python 3.10+ | Todo el pipeline | Es el lenguaje en el que están las otras herramientas, y para lo demás alcanza con la biblioteca estándar |
| dlt | Ingesta | Escribe los archivos de destino leyendo de a lotes, así que no carga un archivo entero en memoria. Además deja registrado su historial de cargas en sus propios archivos; el pipeline no los usa, pero sirven para trazabilidad |
| DuckDB | Esquema, calidad y capa curada | Lee Parquet directo con SQL y no necesita un servidor. Con un tope de memoria procesa millones de filas, y como los datos de los viajes vienen en Parquet (formato columnar), es una herramienta pensada justo para eso |
| dbt (con dbt-duckdb) | Modelo estrella | Los modelos son archivos con un `SELECT`, dbt los ordena según sus dependencias y corre tests sobre ellos |
| Dagster | Orquestación | Cada etapa es un asset. Guarda el historial de corridas y los logs de cada etapa, y los muestra en una interfaz web |
| Docker y Compose | Ejecución | El pipeline corre igual en cualquier máquina, y con un solo comando levanta también Dagster y Dozzle |
| Dozzle | Ver los logs | Muestra en el navegador los logs de todos los contenedores. Para un batch que dura segundos alcanza; Prometheus y Grafana eran mucho más de lo que hacía falta |
| YAML | Contrato de esquema y reglas de calidad | Se lee fácil, admite comentarios y en Git se ve qué cambió |
| pytest | Tests | Es el estándar de Python y alcanza para todo lo que hay que probar |
| make y `lanzar.py` | Un solo comando | `make all` llama a `lanzar.py`, que decide si usa Docker o Python. Está escrito en Python y no en el Makefile para que también se pueda correr sin make |

## Cómo instalar y ejecutar

Hace falta Python 3.10 o superior, make y git. Docker es opcional: si está y responde se usa, y si no, el pipeline corre directo con Python.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Para correr todo con un mes (también procesa el mismo mes del año anterior):

```bash
make all MES=2025-03
```

`make all` decide solo cómo correr:

- Con Docker: construye la imagen, levanta Dozzle y la interfaz de Dagster, y corre el pipeline en un contenedor orquestado por Dagster. Los logs se ven en la terminal, en Dozzle (http://localhost:8080) y en Dagster (http://localhost:3000). Los contenedores quedan encendidos al terminar para poder seguir mirando los logs; `make down` los apaga. La primera vez tarda unos minutos más porque construye la imagen.
- Sin Docker: corre directo con Python (`run.py`), sin Dozzle ni Dagster, y avisa por qué.

Antes de correr hay que cerrar cualquier programa conectado a `data/pipeline.duckdb` o a `data/modelo.duckdb` (por ejemplo DBeaver): DuckDB solo deja que un proceso escriba en la base a la vez.

Otros comandos:

| Comando | Qué hace |
|---|---|
| `make all MES=AAAA-MM` | Pipeline completo: en Docker si se puede y, si no, con Python |
| `make all MES=AAAA-MM MODO=local` | Lo mismo, pero forzando Python: no usa Docker aunque esté instalado |
| `make local MES=AAAA-MM` | Solo con Python, sin Docker (equivale a `python run.py --mes AAAA-MM`) |
| `make docker MES=AAAA-MM` | Solo en Docker; falla si Docker no está disponible |
| `make test` | Tests con el Python local |
| `make test-docker` | Todos los tests dentro de la imagen de Docker |
| `make down` | Apaga los contenedores (pipeline, Dagster y Dozzle); `data/` no se toca |

Sin `make`, el mismo pipeline se lanza con `python3 lanzar.py --mes AAAA-MM` (con `--local`, o `-l`, para no usar Docker).

Dagster sin Docker es opcional: `pip install -r requirements-dagster.txt` y después `python run.py --mes 2025-03 --dagster`. Su interfaz se levanta con `python -m src.orquestacion web`.

Los datos no están en el repositorio (`data/` está en `.gitignore`): se descargan en la primera corrida.

Para correr los tests: `python -m pytest tests -q`. Los de Dagster se saltean si Dagster no está instalado. Dentro de la imagen de Docker (`make test-docker`) corren esos y se saltean solo los que necesitan `make`, que la imagen no trae.

## Cómo está armado

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
RESULTADOS.md                         respuestas a las preguntas de marzo de 2025, con su interpretación

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

Los datos van pasando de una capa a la siguiente así:

```
parquet crudos -> vista viajes_unificados -> aceptadas / cuarentena -> Parquet curado -> modelo estrella -> preguntas
   (ingesta)          (esquema)                   (calidad)             (transformación)     (modelo)        (preguntas)
```

| Etapa | Módulo | Qué hace |
|---|---|---|
| 1. Ingesta | `src/e1_ingesta.py` | Descarga los archivos, los carga con dlt sin modificarlos, calcula el hash y registra todo en `ingestion_log` |
| 2. Esquema | `src/e2_esquema.py` | Compara cada archivo con el contrato, normaliza nombres y tipos y junta los dos meses en la vista `viajes_unificados` |
| 3. Calidad | `src/e3_calidad.py` | Evalúa las reglas sobre `viajes_unificados`, manda las filas rechazadas a `cuarentena` y el resto a `aceptadas`, y reporta y reconcilia los conteos |
| 4. Transformación | `src/e4_transformacion.py` | Escribe la capa curada (columnas derivadas y zonas) como Parquet particionado por año y mes |
| 5. Modelo | `src/e5_modelo.py` | Construye con dbt el modelo estrella sobre la capa curada y corre sus tests |
| 6. Preguntas | `src/e6_preguntas.py` | Responde las preguntas de negocio sobre el modelo y guarda el informe del mes |

Cada etapa es una función con la misma firma, `(con, anio, mes)`, y devuelve un diccionario con lo que procesó. `src/pipeline.py` las junta en la lista `ETAPAS` y `run.py` las recorre en orden: agregar una etapa es agregar una línea. Qué meses se procesan lo define un solo lugar, `meses_a_procesar`: el mes pedido y el mismo mes del año anterior.

Si una etapa falla, el pipeline se corta ahí y el error dice cuál fue (`falló la etapa 'esquema': ...`). Los códigos de salida son 0 si salió todo bien, 1 si falló una etapa y 2 si `--mes` tiene mal el formato. Las métricas de la corrida quedan en el log aunque falle.

### Qué se puede borrar

`data/` se puede borrar entera: todo lo que hay en `pipeline.duckdb` y `modelo.duckdb` sale de los Parquet crudos y del código, y el pipeline la vuelve a crear. Si se borra solo la base, la ingesta vuelve a bajar los archivos (el log vacío no tiene hashes para comparar) y las tablas de calidad quedan solo con los meses que se vuelvan a correr. Lo único que no se recupera es el historial de `ingestion_log`.

Cada corrida procesa su par de meses y no toca los demás. Si se corren varios meses quedan todos en la capa curada, y el modelo se arma con todos.

## Decisiones

Acá se explica por qué cada cosa se hizo como se hizo, etapa por etapa.

### Ingesta

- Cada archivo se baja a `data/.tmp` y se le pasa a dlt, que lo escribe en `data/raw/` con el destino `filesystem`: Parquet para los viajes y CSV para las zonas. Lee de a lotes de 100.000 filas, así que nunca carga un archivo entero en memoria.
- dlt se configuró con tres decisiones: no renombrar las columnas (`SCHEMA__NAMING=direct`) y sin telemetría; un pipeline por archivo, porque con uno compartido dlt acumula el esquema y el archivo de 2024 salía con una columna `cbd_congestion_fee` inventada, llena de nulos; y timestamps sin zona horaria, porque si no DuckDB los corre varias horas al leerlos.
- Los archivos quedan en carpetas estilo Hive (`year=AAAA/month=MM/`).
- No repite descargas. Antes de bajar un archivo busca en `ingestion_log` de `pipeline.duckdb` el hash de su última descarga exitosa y, si el archivo existe y el hash coincide, lo saltea. Una corrida cortada a la mitad se recupera sola, porque solo cuenta como descargado lo que tiene una fila `ok`.
- La descarga va a un `.part` que se renombra recién al terminar, y el hash se calcula mientras baja. Cuando dlt termina se cuentan las filas del archivo final y tienen que coincidir con las del origen; si no, falla.
- Reintenta hasta 3 veces (esperas de 2, 4 y 8 s) los errores de conexión, los timeouts, los cortes a mitad de la descarga y los 5xx, que pueden ser pasajeros. Los 4xx no: esperar no hace aparecer un archivo que no existe. Ante el primer archivo que falla, la ingesta se corta.
- `ingestion_log` es una tabla de DuckDB con el archivo, la URL, las filas, el tamaño, el hash SHA-256, la fecha, el estado (`ok` o `error`) y un mensaje. Solo se le agregan filas, incluso cuando no se descarga nada (mensaje "omitido"). Lo que no se duplica son los datos.

### Esquema

- El contrato es un YAML versionado, `contracts/yellow_schema.yaml`, separado del código, como pide la consigna. Se eligió porque se lee fácil, admite comentarios y en Git los cambios se ven columna por columna. JSON no admite comentarios y un CSV no puede poner las políticas junto a las columnas.
- Cada columna tiene `nombre` (en snake_case), `origen` (cómo se llama en el archivo), `tipo`, `requerida` y `descripcion`, y las opcionales también `default_si_falta`. Hay además un `version` y un bloque `politicas`. Las reglas de calidad no están ahí: el contrato dice cómo es la estructura y las reglas dicen qué valores se aceptan. Al cargarlo se revisa que esté bien armado, así que un error de tipeo falla al arrancar y con un mensaje claro.
- Qué hace el pipeline en cada caso:

  | Situación | Qué hace |
  |---|---|
  | Falta una columna requerida | Falla |
  | Falta una columna opcional | Usa `default_si_falta` |
  | Aparece una columna que no está en el contrato | Avisa y la ignora |
  | El tipo es distinto al del contrato | Convierte si no pierde información (por ejemplo, de entero a entero más grande); si no, falla |

- Una columna nueva no invalida los datos que ya se conocen, pero tampoco tiene que pasar en silencio: por eso se la ignora y se deja un aviso con su nombre. Si se prefiere que corte, se cambia la política a `error` en el contrato, sin tocar código.
- `cbd_congestion_fee` vale `NULL` en 2024. Ese año el cargo no existía y el archivo no trae la columna. Un `0` diría que el viaje no pagó el cargo, y es falso. Con `NULL` se distingue "no se cobró" (el `0` de 2025) de "no existía", y los promedios y conteos ignoran los nulos en vez de mezclar los dos casos. Está escrito en el contrato (`default_si_falta: null`).
- Los crudos se leen con `hive_partitioning = false`. Si no, DuckDB agrega `year` y `month` como columnas que no están en el Parquet y que después chocarían con las de la partición del curado.
- `viajes_unificados` es una vista (un `UNION ALL` de los dos meses) y no una tabla, para no copiar más de 7 millones de filas en esta etapa. Cada `SELECT` sale de las columnas del contrato, más `archivo_origen` para rastrear la cuarentena. Los datos unificados quedan guardados en `aceptadas` y `cuarentena`.
- Si un archivo no cumple el contrato, la etapa falla antes de que calidad o el curado toquen algo.

### Calidad

- Las 10 reglas están en `contracts/reglas_calidad.yaml`, no en el código. Cada una tiene `id`, `descripcion`, `severidad`, `umbral`, una `condicion` en SQL (verdadera cuando la fila **viola** la regla) y una `justificacion`. Para cambiar un umbral se edita el archivo, sin tocar Python. Se valida al cargarlo y hay tests de eso.
- Cuántas filas afecta cada regla. Una fila puede violar varias, por eso cada columna suma más que las filas en cuarentena:

  | Regla | Severidad | Qué detecta | Mar 2024 | Mar 2025 |
  |---|---|---|---:|---:|
  | R01 | rechazo | La fecha de subida cae fuera del mes del archivo | 23 | 33 |
  | R02 | rechazo | La bajada es anterior o igual a la subida | 1.128 | 22.280 |
  | R03 | rechazo | Distancia menor o igual a 0 | 87.168 | 103.722 |
  | R04 | rechazo | Distancia mayor a 100 millas | 122 | 214 |
  | R05 | rechazo | `total_amount` negativo | 44.311 | 68.686 |
  | R06 | advertencia | `passenger_count` nulo o 0 | 466.562 | 939.378 |
  | R07 | rechazo | Zona de origen o destino que no existe en la tabla de zonas (un nulo cuenta como inexistente) | 0 | 0 |
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

- Una regla de rechazo manda la fila a `cuarentena`, porque el dato es imposible o no se puede usar. Una de advertencia la deja en `aceptadas` y anota el id de la regla en `flags`. R06 es advertencia porque descartar entre el 13 % y el 23 % de los viajes borraría una parte enorme de los datos, y R10 porque las zonas 264 y 265 son códigos válidos de la tabla.
- R04 usa 100 millas porque la mediana es 1,8 y el percentil 99,9 ronda las 29. R09 usa 100 mph por lo mismo. R09 y R10 son reglas propias, que salieron de explorar los datos.
- `cuarentena` tiene las columnas del contrato, `archivo_origen`, `reglas_rechazo` (todas las reglas de rechazo que violó la fila) y `reglas_advertencia`. No se borra nada: toda fila rechazada se puede ver con su motivo.
- Nulos. En SQL, comparar con `NULL` da `NULL`, y entonces una regla no marca una fila cuando la columna que evalúa es nula. Por eso R06 cuenta un `passenger_count` nulo igual que un 0 y R07 trata una zona nula como inexistente. Para las demás se revisaron los datos reales (marzo y abril de 2024 y 2025): las fechas, `trip_distance`, `total_amount`, las zonas, `payment_type` y `fare_amount` no tienen ningún nulo, así que R01 a R05 y R09 no corren ese riesgo. Los nulos que hay están en `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge` y `Airport_fee`, siempre en las mismas filas, que son justo las de `payment_type = 0` (Flex Fare): 426.190 en marzo de 2024 y 916.663 en marzo de 2025.
- Cada mes se procesa en una transacción: se borran sus filas anteriores (por `archivo_origen`), se insertan las nuevas, se reconcilia y se guarda el reporte (`reporte_resumen` y `reporte_reglas`). La reconciliación comprueba que `filas leídas = aceptadas + cuarentena`, y cuenta las leídas directo del archivo original para que el control sea independiente. Si no cierra se lanza `ErrorReconciliacion` y se hace `ROLLBACK`. Por eso reprocesar un mes no duplica nada ni toca los otros. Las tablas se crean antes de abrir la transacción: crearlas adentro dejó una tabla vacía sin ningún error (se encontró al probar con datos reales).
- Lo que mostraron los datos:
  - R02 pasa de unas 1.100 filas en 2024 a entre 22.000 y 35.000 en 2025 (marzo y abril). Casi todo el aumento (96 a 97 %) viene del `VendorID` 7, que aparece recién en 2025: sus viajes tienen la bajada igual a la subida (duración 0) pero distancia (2,9 millas en promedio) y cobro (unos $25) normales. Parecen viajes reales con la hora de bajada mal registrada; se rechazan igual porque no se puede calcular su duración ni su velocidad.
  - R07 y R08 no encontraron nada en los datos reales. En los tests, con datos sintéticos, se comprobó que funcionan.
  - Hay viajes aceptados con `fare_amount` negativo, que R05 no ve porque solo mira `total_amount`. Son todos Flex Fare y su total es positivo: 14.187 en marzo de 2024 (0,4 % de las aceptadas) y 128.456 en marzo de 2025 (3,2 %). No se excluyeron, pero mueven la tarifa media de la pregunta f (ver las limitaciones).

### Transformación

- La capa curada tiene los viajes aceptados de cada mes con columnas derivadas y los datos de las zonas, en `data/curado/year=AAAA/month=M/curado.parquet`: un Parquet por mes, comprimido con zstd, con 32 columnas (las 20 del contrato, `archivo_origen`, `flags`, 6 derivadas y 4 de zonas). La consulta está en `sql/transformacion/curado.sql` y lee de `aceptadas`, así que lo rechazado nunca llega acá.
- Columnas derivadas:

  | Columna | Cómo se calcula | Por qué así |
  |---|---|---|
  | `duracion_min` | segundos entre subida y bajada, dividido por 60 | `date_diff('minute', ...)` descarta los segundos de cada extremo antes de restar y se equivoca hasta casi un minuto |
  | `velocidad_mph` | distancia / (segundos / 3600) | Con `NULLIF`, para que una duración 0 dé `NULL` y no infinito (esas filas ya van a cuarentena por R02) |
  | `fecha_subida`, `hora_subida` | de `tpep_pickup_datetime` | Un viaje que empieza a las 23:59 es de ese día, aunque termine en el siguiente |
  | `dia_semana` | `isodow` de la subida | ISO 8601: 1 es lunes y 7 es domingo |
  | `propina_pct` | 100 × propina / tarifa | Solo con `payment_type = 1` (tarjeta) y `fare_amount > 0`; en los demás casos es `NULL` |

- `propina_pct` es solo con tarjeta porque, según el diccionario de la TLC, la propina se registra únicamente en pagos con tarjeta: en efectivo un `tip_amount` de 0 significa "no se registró" y no "no dejó propina". Flex Fare (`payment_type = 0`) tiene 91,9 % de propinas en 0 y tampoco es comparable. Con todos los medios de pago mezclados el promedio daba cerca de 18 %, y solo con tarjeta 26,1 %. `tip_amount` y `payment_type` se conservan, así que no se pierde información.
- Para las zonas se hacen dos `LEFT JOIN` a la tabla de zonas, uno por el origen (`pu_borough`, `pu_zona`) y otro por el destino (`do_borough`, `do_zona`). Se usa `LEFT JOIN` para que una zona sin datos no haga desaparecer el viaje. Si la tabla de zonas tuviera un id repetido, el `JOIN` duplicaría filas y el chequeo de conteo lo detecta (hay un test).
- Cada mes se escribe como un archivo aparte, a mano. Se probó `COPY ... PARTITION_BY` de DuckDB con `OVERWRITE` y, en la versión 2.0.0, borraba las particiones de los otros meses, justo lo que la consigna prohíbe. Cada mes queda como su propio archivo en su carpeta `year=/month=`, y DuckDB las lee todas juntas con `hive_partitioning`.
- La carpeta del curado es `month=3` y no `month=03`: con `month=03` DuckDB infiere el mes como texto y con `month=3` como número. Los crudos conservan `month=03`, como los nombra la TLC.
- El reemplazo de una partición es atómico. El mes se escribe en `curado.parquet.part`, se cuentan sus filas y recién ahí se renombra a `curado.parquet` con `os.replace`. Si algo falla antes, se borra el `.part` y la partición anterior queda intacta.
- Hay una segunda reconciliación: las filas aceptadas del mes en la base tienen que ser iguales a las del Parquet escrito, contadas desde el propio archivo. Si no coinciden se lanza `ErrorReconciliacion` y no se reemplaza nada. Si un mes no tiene filas aceptadas, falla con un mensaje claro en vez de escribir una partición vacía.

#### Demostración: reprocesar un mes reemplaza solo su partición

Se corrió `python run.py --mes 2025-03`, después `--mes 2025-04` y después otra vez `--mes 2025-03`. Cada corrida procesa el mes pedido y el mismo mes de 2024. Al terminar se miró, en cada archivo de `data/curado/`, la hora de escritura, el hash y la cantidad de filas.

| Partición | Tras correr marzo y abril | Tras volver a correr marzo |
|---|---|---|
| `year=2024/month=3` | 3.453.563 filas · 18:00:15 · sha256 `e8a0b7…` | 3.453.563 filas · **18:10:00 · sha256 `90d624…`** |
| `year=2025/month=3` | 3.955.294 filas · 18:00:16 · sha256 `aae1b1…` | 3.955.294 filas · **18:10:01 · sha256 `94b465…`** |
| `year=2024/month=4` | 3.427.577 filas · 18:07:23 · sha256 `832be0…` | 3.427.577 filas · 18:07:23 · sha256 `832be0…` |
| `year=2025/month=4` | 3.776.498 filas · 18:07:24 · sha256 `e2aed5…` | 3.776.498 filas · 18:07:24 · sha256 `e2aed5…` |

- Las particiones de marzo se reescribieron (cambian la hora y el hash) con el mismo conteo, y las de abril no se tocaron.
- El hash cambia aunque el contenido sea el mismo porque DuckDB escribe en paralelo y el orden de las filas puede variar. La idempotencia es de contenido (mismos conteos, mismos reportes de calidad), no de bytes.
- Las tablas de calidad tampoco se duplicaron: las filas por archivo en `aceptadas` y `cuarentena` fueron las mismas antes y después (por ejemplo, `yellow_tripdata_2025-03.parquet` siguió con 3.955.294 aceptadas y 189.963 en cuarentena).

### Modelo

- Los modelos de dbt son los `SELECT` de `sql/modelo/`. `dbt build` los crea en orden de dependencias como tablas en `data/modelo.duckdb` y después corre los tests. Corre en un proceso aparte, así que libera sus archivos y su memoria al terminar. Lee la capa curada (Parquet) y la tabla de zonas directamente, con `external_location` en `sql/modelo/fuentes.yml`, sin copiarlas a otra base.
- El modelo es una estrella con una tabla de hechos, dos dimensiones y una tabla agregada:

  | Tabla | Una fila por... | Clave | Qué tiene |
  |---|---|---|---|
  | `fact_trips` | viaje aceptado de la capa curada | sin clave propia | `fecha` (→ `dim_date`), `pu_zona_id` y `do_zona_id` (→ `dim_zone`), hora, las medidas (distancia, duración, velocidad, tarifa, propina, cargos, total) y los códigos (`payment_type`, `ratecode_id`, `vendor_id`). Conserva `flags` y `archivo_origen` para rastrear |
  | `dim_date` | día calendario de cada mes presente en la capa curada | `fecha` | año, mes, día, día de la semana (ISO) y si es fin de semana |
  | `dim_zone` | zona de la TLC | `zona_id` | borough, nombre de la zona y `service_zone` |
  | `agg_zone_hour` | zona de origen, fecha y hora de subida | (`pu_zona_id`, `fecha`, `hora`) | viajes, ingreso total y duración media. Sale de `fact_trips` y la usa la pregunta a |

- Hay una sola dimensión de zonas para los dos extremos del viaje: `fact_trips` tiene dos claves hacia `dim_zone`, una para el origen y otra para el destino. No repite lo que ya sale de las dimensiones (nombres de zona y día de la semana), y los códigos de pago, tarifa y proveedor quedan como códigos (la pregunta d traduce `payment_type` a texto con un `CASE`).
- `dim_date` se arma con los meses que hay en la capa curada, no con un calendario fijo. Y el modelo se construye con todas las particiones curadas de `data/curado/`, no solo con el par del mes pedido: las preguntas filtran por `$anio` y `$mes`.
- Son 15 tests de dbt: `not_null` y `unique` en las claves, `relationships` de `fact_trips` hacia `dim_date` y `dim_zone`, `not_null` en la granularidad de `agg_zone_hour` y dos escritos en SQL en `sql/tests/`. Uno comprueba que `fact_trips` tiene exactamente las filas de la capa curada, y el otro que `agg_zone_hour` suma los mismos viajes y el mismo ingreso que `fact_trips` (con 1 dólar de tolerancia). Si falla un modelo o un test, falla la etapa y el pipeline se corta.

### Preguntas de negocio

- Hay una consulta por pregunta en sql/preguntas/: las del enunciado (a, b, c1, c2, d, e y f) y tres propias sobre zonas y propinas (g, h1 y h2). Se ejecutan sobre `data/modelo.duckdb`, que se abre solo para lectura.
- Cada consulta recibe solo los parámetros que usa:

  | Parámetro | Valor | Dónde se usa |
  |---|---|---|
  | `$anio`, `$mes` | los de `--mes` | todas; comparan contra el mismo mes de `$anio - 1` |
  | `$min_viajes_zona` | 1000 | c2: una zona con menos viajes en el mes no entra al ranking de proporciones, porque con pocos viajes la proporción no dice nada |

- Una pregunta sin resultados es un error, no una respuesta vacía.
- Las tres propias: g da la zona de origen con más viajes de cada tipo de pago (si dos zonas empatan, gana la que va primero por nombre). h1 y h2 ordenan los pares (zona de origen, zona de destino) y muestran los 5 primeros de cada mes: h1 por la suma de tip_amount y h2 por la cantidad de viajes con tip_amount > 0. La propina solo se registra con tarjeta (el 97 % de los viajes con propina), así que esos rankings reflejan a quienes pagan con tarjeta.
- Las respuestas se imprimen al final de la etapa y cada corrida guarda su informe completo (consulta, parámetros y resultado) en `data/resultados/AAAA-MM.md`. Las de marzo de 2025, con su interpretación, están en `RESULTADOS.md`.

## Docker y Dozzle

`docker-compose.yml` define tres servicios:

| Servicio | Contenedor | Qué hace |
|---|---|---|
| `pipeline` | `nyc-taxi-pipeline` | Corre `run.py --mes AAAA-MM --dagster` y después queda encendido |
| `dagster` | `nyc-taxi-dagster` | Interfaz web de Dagster (puerto 3000); lee el historial que escribe el pipeline |
| `dozzle` | | Pantalla web con los logs de todos los contenedores (puerto 8080) |

`pipeline` y `dagster` usan la misma imagen, `python:3.12-slim` con las dependencias de `requirements.txt` y de `requirements-dagster.txt`, y los dos montan la carpeta `./data` de la máquina.

`make all` ejecuta `lanzar.py`, que hace esto:

1. Valida el mes y mira si Docker sirve (comando `docker`, servicio respondiendo y plugin `docker compose`).
2. Construye la imagen.
3. Levanta Dozzle.
4. Levanta la interfaz de Dagster y espera hasta 120 s a que esté sana, para que su carpeta de historial ya exista cuando el pipeline empiece a escribir.
5. Levanta el contenedor del pipeline (antes borra el de la corrida anterior, si quedó).
6. Muestra su log hasta que aparece la línea `== PIPELINE TERMINÓ: código N ==` y termina con ese código.

Decisiones:

- El contenedor no termina cuando termina el pipeline. `docker-entrypoint.sh` corre `run.py`, imprime esa línea con el código de salida y se queda en `sleep infinity`, para que se puedan seguir viendo los logs en Dozzle. `lanzar.py` lee la línea para saber cuándo y cómo terminó.
- Docker nunca hace fallar el pipeline. Si Docker no está o no responde, falta `docker compose`, no se puede construir la imagen o el contenedor no arranca, se avisa y se corre directo con Python. Si lo que falla es Dozzle o la interfaz de Dagster, el pipeline corre igual en Docker y se avisa. Con `make docker` no hay respaldo: si Docker no sirve, falla.
- Si el pipeline falla dentro del contenedor, `make all` termina con el mismo código y no lo reintenta en local. No es un problema de infraestructura, y volver a correrlo escondería el error y repetiría el trabajo. Si el contenedor muere sin avisar se usa su código de salida y, si lo cortó Docker por memoria, se dice.
- El contenedor del pipeline tiene un tope de 6 GB y sin swap (`mem_limit` y `memswap_limit`), así que el límite es real. Adentro, DuckDB trabaja con `memory_limit = 4GB`.
- `./data` se monta dentro del contenedor: los resultados quedan en la máquina y se comparten entre corridas locales y de Docker. En Linux el contenedor corre con el usuario que lanzó `make`, para que `data/` no quede a nombre de root.
- Los puertos 3000 y 8080 solo se abren en `127.0.0.1`, no se ven desde la red. Dozzle corre sin analíticas.
- `.dockerignore` deja afuera `data/`, `.venv/` y `.git/`. Sin eso el contexto de construcción pesaba 1,3 GB.

## Dagster

- Cada etapa es un asset de Dagster (grupo `nyc_taxi`). Las dependencias entre ellos solo fijan el orden (ingesta, esquema, calidad, curado, modelo, preguntas): los datos siguen pasando por DuckDB y los Parquet, no por Dagster. El job `pipeline_nyc_taxi` materializa los seis.
- Las etapas son las mismas funciones que corre `ejecutar()`; Dagster reemplaza solo el `for` que las recorre.
- El recurso `sesion` abre al empezar la corrida la clase `Sesion` (la conexión a DuckDB y la medición) y la cierra al terminar, aunque haya fallas. Los seis assets usan esa misma sesión, así que comparten la conexión y suman al mismo resumen.
- Cada asset escribe su línea `clave=valor` en el log de Dagster y devuelve lo medido (duración, filas, RAM) como metadatos de la materialización.
- El historial queda en `data/dagster_home` (variable `DAGSTER_HOME`): corridas, eventos y los logs de cada paso en `storage/<id de la corrida>/compute_logs/`. El `.err` tiene los logs propios de Dagster y el `.out` lo que imprime el pipeline. Esa carpeta la comparten el contenedor que corre el pipeline y el que sirve la interfaz.
- Se usa el ejecutor en proceso: todos los pasos corren en el mismo proceso, que hace falta para compartir la conexión a DuckDB. Las etapas son secuenciales de todos modos.
- No hay daemon ni schedules, porque el pipeline corre una vez por comando. La telemetría está desactivada.
- Los meses no son particiones de Dagster: el par de meses lo define `meses_a_procesar` y el modelo se construye con todas las particiones curadas.
- Dagster es opcional. Está en `requirements-dagster.txt` y no en `requirements.txt`. Si no está instalado o no puede abrir su carpeta, el pipeline corre sin él y avisa. Sin Docker, `make all` corre sin Dagster.

## Logs y métricas

Cada etapa deja una línea de log con lo que tardó, lo que usó y lo que procesó. Los mismos eventos se ven en tres lugares:

- La terminal y Dozzle, como líneas `clave=valor`, fáciles de filtrar (`etapa=calidad`, `evento=error`).
- `data/logs/AAAA-MM.jsonl`, un JSON por línea con todos los eventos (también el inicio de cada etapa). Se sobrescribe en cada corrida del mismo mes.
- Dagster, que además guarda el historial de corridas.

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
| `pico_etapa_mb` | Pico de memoria durante la etapa: la suma de la memoria del proceso y la de sus hijos (dbt), muestreada cada 0,1 s |
| `filas` | Lo que procesó o produjo la etapa (no significa lo mismo en todas, ver abajo) |

Qué cuenta `filas` en cada etapa: en ingesta, las filas de los archivos del par de meses más las de la tabla de zonas; en esquema no informa filas; en calidad, las filas leídas de los dos archivos; en curado, las filas escritas en la capa curada (las aceptadas); en modelo, la suma de las filas de las cuatro tablas (incluye todos los meses curados); en preguntas, las filas de las tablas de respuesta. Además cada etapa agrega sus propios datos, como `aceptadas` y `cuarentena` en calidad o `tests_dbt` en modelo.

Los eventos son `corrida_inicio` (mes, entorno, versión de Python, núcleos y memoria disponible), `inicio` de cada etapa (solo en el archivo), `fin` o `error` de cada etapa y `corrida_fin` (estado, duración total, CPU y pico de RAM). Al terminar se imprime además una tabla resumen con una fila por etapa.

El pico de memoria lo mide un hilo aparte que cada 0,1 s suma la memoria del proceso y la de sus hijos. El pico de la corrida es el mayor entre los de las etapas y el que informa el sistema operativo para el proceso. `ram_disponible_mb` es el tope del contenedor si lo hay (cgroup v2) o la memoria de la máquina.

## Tiempo de ejecución y memoria

Medición de `make all MES=2025-03` en Docker (contenedor con 6.144 MB y 16 núcleos), tal como la imprime el pipeline. Los archivos ya estaban descargados (la ingesta los omitió) y en `data/curado/` había cuatro meses, así que `modelo` se construyó sobre 13.735.570 viajes:

| Etapa | Duración | CPU | RAM al final | Pico de la etapa | Filas |
|---|---:|---:|---:|---:|---:|
| ingesta | 0,2 s | 0,2 s | 227 MB | 250 MB | 7.728.150 |
| esquema | 0,0 s | 0,0 s | 228 MB | 252 MB | |
| calidad | 7,7 s | 49,2 s | 2.479 MB | 2.593 MB | 7.727.885 |
| curado | 2,4 s | 21,7 s | 2.155 MB | **2.882 MB** | 7.408.857 |
| modelo | 9,2 s | 27,9 s | 244 MB | 2.132 MB | 14.112.035 |
| preguntas | 0,3 s | 2,4 s | 246 MB | 578 MB | 35 |
| **total** | **20,7 s** | **102,9 s** | | **2.882 MB** | |

El pico de toda la corrida es de 2.882 MB, en la etapa `curado`, contra los 6.144 MB del contenedor y los 8 GB del equipo objetivo.

También se corrió `make all MES=2025-04 MODO=local` (sin Docker, con Python 3.14, 16 núcleos y descargando los dos archivos): tardó 51,4 s en total, de los cuales 31,6 s fueron de la ingesta por la descarga, y el pico fue de 3.498 MB, con seis meses en la capa curada. En otra computadora, corriendo con `lanzar.py`, el pico máximo fue de 3.600 MB.

Para que el pico no suba, el pipeline cierra su conexión a DuckDB antes de la primera etapa que no usa la base (`modelo`). Después de calidad y curado esa conexión retiene unos 2,4 GB aunque ya no se use, y dbt corre en otro proceso, así que con la conexión abierta las dos memorias se suman. Medido en la misma máquina y con el mismo mes, el pico de la etapa `modelo` bajó de 4.534 MB a 2.132 MB. Además `memory_limit = 4GB` en DuckDB hace que, si una consulta necesita más, escriba a disco en vez de agotar la memoria.

## Limitaciones conocidas

- El hash se compara contra lo que anotó el log. Si la TLC republica un archivo con el mismo nombre, la ingesta no lo detecta.
- El servidor responde 403 (no 404) cuando un archivo no existe, así que un 403 puede significar que el mes todavía no está publicado.
- La comparación de nombres de columna distingue mayúsculas. Si la TLC renombrara `Airport_fee` a `airport_fee`, el pipeline lo trataría como una columna requerida que falta y fallaría.
- Se probó con archivos de 2024 y 2025, que tienen el mismo esquema salvo `cbd_congestion_fee`. No se probó con años anteriores.
- El umbral de R09 (100 mph) es amplio: pasan viajes de hasta 99,3 mph, que siguen siendo dudosos. Es una cota, no una garantía.
- El salto de R02 en 2025 viene casi todo del `VendorID` 7 (nuevo ese año). Esos viajes, de 21.000 a 34.000 por mes, parecen válidos y se rechazan porque no se puede calcular su duración.
- Hay viajes aceptados con `fare_amount` negativo (R05 solo mira `total_amount`): 14.187 en marzo de 2024 y 128.456 en marzo de 2025. Todos son Flex Fare y su `total_amount` es positivo. No se determinó a qué corresponde esa tarifa negativa. No afectan a `propina_pct`, que solo se calcula con `fare_amount > 0`, pero sí a la tarifa media de la pregunta f. Con todos los viajes aceptados la tarifa media baja 3,47 % (de 19,06 a 18,40 dólares). Sin esos viajes pasa de 19,15 a 19,16 (+0,08 %). La distancia media casi no cambió (de 3,36 a 3,33 millas), así que la baja no sale de que los viajes sean más cortos. Las preguntas e y f usan `avg(fare_amount)`, por lo que esas medias incluyen las tarifas negativas.
- Flex Fare (`payment_type = 0`) es el 11,0 % de los viajes aceptados de marzo de 2024 y el 21,6 % de marzo de 2025, y trae nulos en cinco columnas. Las reglas no lo tratan aparte.
- La idempotencia de la capa curada es de contenido, no de bytes (el hash del archivo puede cambiar al reprocesar).
- El modelo se reconstruye con todas las particiones curadas, así que tarda más a medida que se acumulan meses en `data/curado/`.
- El pico de RAM es una muestra (cada 0,1 s) y puede perder un pico más corto que ese intervalo.
- Dozzle y la interfaz de Dagster se probaron en Linux. El historial de Dagster es una base SQLite compartida entre dos contenedores por un volumen montado, y en otros sistemas (Docker Desktop en Mac o Windows) los bloqueos de archivo podrían comportarse distinto. No se probó.
- Los logs de Docker y de Dagster salen con la hora en UTC.
- Sin Docker no hay Dozzle ni Dagster: el seguimiento queda en la terminal y en `data/logs/`.

## Uso de IA

Se usó Claude (de Anthropic) a lo largo de todo el trabajo, para tres cosas:

- Para entender. Se le pidió que explicara de a poco las herramientas que no se conocían (y cómo se conecta todo) antes de tocar código, y se siguió preguntando hasta poder explicar cada parte con palabras propias.
- Para escribir código, los tests, la configuración de Docker, Dagster y las métricas, y borradores de este README y de `RESULTADOS.md`.
- Para revisar los datos. Corrió consultas sobre los Parquet reales para comprobar cosas, por ejemplo cuántos nulos hay y cuántos viajes tienen tarifa negativa.

Las decisiones las tomó el grupo: el alcance (quedaron afuera Prometheus, Grafana y la nube para no agrandar el trabajo), los cambios (dlt, Dozzle, Docker, los logs de Dagster, que el contenedor no termine solo) y que `make all` no falle nunca por culpa de Docker.

Todo se verificó corriendo los comandos y los tests, probando `make all` en otra computadora y mirando las bases en DBeaver para comparar con lo que informa el pipeline.
