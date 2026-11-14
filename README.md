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

> PENDIENTE: el Makefile todavía no existe. Por ahora solo está la ingesta: `python -m src.ingesta --mes 2025-03`.

Los datos no se incluyen en el repositorio (`data/` está en `.gitignore`). Se descargan en la primera corrida.

## Arquitectura

```
data/raw/yellow/year=AAAA/month=MM/   archivos originales, sin modificar
data/raw/zones/                       tabla de zonas
data/pipeline.duckdb                  ingestion_log y tablas del modelo
contracts/yellow_schema.yaml          contrato de esquema
src/                                  una etapa por módulo
tests/                                tests automatizados
```

| Etapa | Módulo | Qué hace |
|---|---|---|
| 1. Ingesta | `src/ingesta.py` | Descarga los archivos sin modificarlos, calcula el hash y registra todo en `ingestion_log` |
| 2. Esquema | `src/esquema.py` | Compara cada archivo con el contrato, normaliza nombres y tipos y unifica los dos meses en la vista `viajes_unificados` |
| 3. Calidad | `src/calidad.py` | PENDIENTE |
| 4. Transformación | `src/transformacion.py` | PENDIENTE |
| 5. Modelo | `src/modelo.py` | PENDIENTE |

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
- **Unificación como vista.** `viajes_unificados` es una vista que une los dos meses con `UNION ALL`, no una tabla. Así no se copian ni se cargan en memoria los más de 7 millones de filas. Cada `SELECT` se genera a partir de la lista de columnas del contrato, así que quedan en el mismo orden. Se agrega una columna `archivo_origen` que no pertenece al contrato, para la trazabilidad de la cuarentena.

### Calidad, transformación y modelo

> PENDIENTE: reglas, severidades, umbrales y tratamiento de nulos (Parte 3). Granularidad de cada tabla y demostración del reprocesamiento con conteos antes y después (Partes 4 y 5).

## Limitaciones conocidas

- El hash se compara contra lo que anotó el log. Si la TLC republica un archivo con el mismo nombre, la ingesta no lo detecta.
- El servidor responde 403 (no 404) cuando un archivo no existe, así que un 403 puede significar que el mes todavía no está publicado.
- La comparación de nombres de columna distingue mayúsculas. Si la TLC renombrara `Airport_fee` a `airport_fee`, el pipeline lo trataría como una columna requerida faltante y fallaría.
- El pipeline se probó con archivos de 2024 y 2025, que tienen el mismo esquema salvo `cbd_congestion_fee`. No se probó con años anteriores.
- PENDIENTE: completar al terminar el proyecto.

## Tiempo de ejecución y memoria

> PENDIENTE: medir con `/usr/bin/time -v make all MES=2025-03` en la máquina de entrega (pico de RAM y tiempo total).
> Medición parcial de la ingesta: primera corrida (descarga de unos 130 MB) alrededor de 5 s, y segunda corrida (solo verificación) alrededor de 0,5 s.

## Uso de IA

> PENDIENTE: revisar y ajustar este texto a lo que realmente se hizo.

Se usó Claude (Anthropic) como asistente durante el desarrollo: para explicar conceptos que no se vieron en la materia (hash, reintentos con espera creciente, formato Hive, DuckDB), para proponer la estructura del proyecto y borradores de código, y para interpretar mensajes de error. El código se ejecutó, se probó y se adaptó en la propia máquina, y todo lo entregado puede explicarse en la defensa.