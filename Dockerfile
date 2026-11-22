# Imagen del pipeline: Python 3.12 y las dependencias fijadas en requirements.txt y requirements-dagster.txt.
# La usan los tres servicios de docker-compose.yml: el pipeline, la interfaz de Dagster y (con otra imagen) Dozzle.
FROM python:3.12-slim

# PYTHONUNBUFFERED: cada línea sale al log apenas se imprime, así Dozzle la muestra en vivo.
# RUNTIME__LOG_LEVEL: dlt informa todo lo que hace (en una corrida local solo avisa problemas).
# DAGSTER_HOME: dónde guarda Dagster sus corridas y logs, dentro de data/ (que se comparte con tu máquina).
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    RUNTIME__LOG_LEVEL=INFO \
    DAGSTER_HOME=/app/data/dagster_home \
    HOME=/tmp

WORKDIR /app

# Primero las dependencias: esta capa se reutiliza mientras los requirements no cambien.
COPY requirements.txt requirements-dagster.txt ./
RUN pip install -r requirements.txt -r requirements-dagster.txt

COPY . .
# El contenedor corre con tu usuario (para que data/ no quede a nombre de root): /app tiene que ser escribible.
RUN chmod -R a+rwX /app

# El mes llega como argumento (lo pasa docker-compose.yml). El script corre el pipeline y después deja el
# contenedor encendido, para que se puedan seguir viendo sus logs. El servicio de Dagster cambia este punto de entrada.
ENTRYPOINT ["sh", "/app/docker-entrypoint.sh"]
