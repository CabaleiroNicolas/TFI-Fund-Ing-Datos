# Atajos del proyecto.
#   make all MES=2025-03     pipeline completo: en Docker (con Dozzle) si se puede y, si no, directo con Python
#   make local MES=2025-03   solo con Python, sin Docker
#   make docker MES=2025-03  solo en Docker: falla si Docker no está disponible
#   make test                tests automatizados
#   make down                apaga Dozzle
PYTHON ?= python3

.PHONY: all local docker test down

# Pipeline completo para un mes (procesa también el mismo mes del año anterior).
all:
	@test -n "$(MES)" || { echo "Falta el mes. Uso: make all MES=2025-03"; exit 1; }
	$(PYTHON) lanzar.py --mes $(MES)

local:
	@test -n "$(MES)" || { echo "Falta el mes. Uso: make local MES=2025-03"; exit 1; }
	$(PYTHON) lanzar.py --mes $(MES) --modo local

docker:
	@test -n "$(MES)" || { echo "Falta el mes. Uso: make docker MES=2025-03"; exit 1; }
	$(PYTHON) lanzar.py --mes $(MES) --modo docker

# Tests automatizados.
test:
	$(PYTHON) -m pytest tests -q

# Apaga Dozzle (los datos de data/ no se tocan).
down:
	docker compose down
