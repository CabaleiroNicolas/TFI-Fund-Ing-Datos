#!/bin/sh
# Corre el pipeline y, cuando termina (bien o mal), deja el contenedor encendido para poder seguir
# mirando sus logs en Dozzle. lanzar.py lee la línea "PIPELINE TERMINÓ" para saber el resultado.
python run.py "$@"
codigo=$?
echo "== PIPELINE TERMINÓ: código $codigo =="
echo "== El contenedor sigue encendido para consultar los logs. Para apagarlo: make down =="
exec sleep infinity
