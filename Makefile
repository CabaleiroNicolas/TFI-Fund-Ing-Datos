# Atajos del proyecto. Uso:  make all MES=2025-03   |   make test
PYTHON ?= python3

.PHONY: all test

# Pipeline completo para un mes (procesa también el mismo mes del año anterior).
all:
	@test -n "$(MES)" || { echo "Falta el mes. Uso: make all MES=2025-03"; exit 1; }
	$(PYTHON) run.py --mes $(MES)

# Tests automatizados.
test:
	$(PYTHON) -m pytest tests -q
