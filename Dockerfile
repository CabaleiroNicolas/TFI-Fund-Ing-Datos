# Imagen del pipeline: Python 3.12 y las dependencias fijadas en requirements.txt.
FROM python:3.12-slim

# PYTHONUNBUFFERED: cada línea sale al log apenas se imprime, así Dozzle la muestra en vivo.
# RUNTIME__LOG_LEVEL: dlt informa todo lo que hace (en una corrida local solo avisa problemas).
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    RUNTIME__LOG_LEVEL=INFO \
    HOME=/tmp

WORKDIR /app

# Primero las dependencias: esta capa se reutiliza mientras requirements.txt no cambie.
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
# El contenedor corre con tu usuario (para que data/ no quede a nombre de root): /app tiene que ser escribible.
RUN chmod -R a+rwX /app

# El mes llega como argumento (lo pasa docker-compose.yml). El script corre el pipeline y después deja el
# contenedor encendido, para que se puedan seguir viendo sus logs.
ENTRYPOINT ["sh", "/app/docker-entrypoint.sh"]
