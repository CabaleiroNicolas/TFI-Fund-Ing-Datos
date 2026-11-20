# Etapa 5: construye el modelo estrella con dbt a partir de la capa curada y corre sus tests.
# Los modelos son los SELECT de sql/modelo/; los tests, schema.yml y sql/tests/.
import json
import subprocess
import sys
from pathlib import Path

import duckdb

from src.config import RAIZ
from src.e3_calidad import miles


def construir_modelo(raiz: Path = RAIZ) -> list:
    # Corre 'dbt build' (crea los modelos en orden de dependencias y después corre los tests) en un proceso
    # aparte: así dbt libera sus archivos y su memoria al terminar. Devuelve un resumen por nodo.
    # Si algún modelo o test falla, o dbt no llega a ejecutarse, levanta una excepción.
    target = raiz / "data" / "dbt_target"
    (target / "run_results.json").unlink(missing_ok=True)          # que no quede el resultado de otra corrida
    comando = [sys.executable, "-c", "from dbt.cli.main import cli; cli()",     # equivale al comando 'dbt'
               "build", "--project-dir", str(raiz), "--profiles-dir", str(raiz),
               "--target-path", str(target), "--log-path", str(raiz / "data" / "dbt_logs")]
    # cwd=raiz: las rutas de profiles.yml y de las fuentes son relativas ("data/...")
    proceso = subprocess.run(comando, cwd=raiz)

    nodos = leer_resultados(target / "run_results.json")
    fallados = [n["nombre"] for n in nodos if n["estado"] not in ("success", "pass")]
    if fallados:
        raise RuntimeError(f"fallaron {len(fallados)} nodos de dbt: {', '.join(fallados)}")
    if proceso.returncode != 0 or not nodos:
        raise RuntimeError(f"dbt terminó con código {proceso.returncode}; el detalle está en {raiz / 'data' / 'dbt_logs'}")

    # dbt-duckdb no informa cuántas filas creó cada modelo: se cuentan en la base ya construida.
    con = duckdb.connect(str(raiz / "data" / "modelo.duckdb"), read_only=True)
    try:
        for n in nodos:
            if n["tipo"] == "model":
                n["filas"] = con.execute(f'SELECT count(*) FROM "{n["nombre"]}"').fetchall()[0][0]
    finally:
        con.close()
    return nodos


def leer_resultados(ruta: Path) -> list:
    # run_results.json lo escribe dbt al terminar: un resultado por modelo y por test.
    if not ruta.exists():
        return []
    resultados = json.loads(ruta.read_text(encoding="utf-8"))["results"]
    nodos = []
    for r in resultados:
        tipo, _, nombre = r["unique_id"].split(".")[:3]            # p. ej. 'model.nyc_taxi.dim_date'
        nodos.append({"nombre": nombre, "tipo": tipo, "estado": r["status"], "segundos": r["execution_time"]})
    return nodos


def mostrar_resumen(nodos: list) -> None:
    for n in (n for n in nodos if n["tipo"] == "model"):
        print(f"   modelo {n['nombre']}: {miles(n['filas'])} filas, {n['segundos']:.1f} s")
    tests = [n for n in nodos if n["tipo"] == "test"]
    print(f"   tests de dbt: {sum(n['estado'] == 'pass' for n in tests)} de {len(tests)} aprobados")