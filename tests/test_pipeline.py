# Tests de punta a punta: corren 'python run.py' de verdad (dlt omitido, dbt real) sobre datos sintéticos, en una copia del
# proyecto, y revisan las métricas que deja el log.
import hashlib
import json
import os
from functools import partial
import shutil
import subprocess
import sys
from datetime import datetime

import duckdb
import pytest

from src import pipeline
from src.config import RAIZ
from src.metricas import Corrida

VIAJES = 6000          # por mes (c2 pide al menos 1000 viajes por zona)
ZONAS_USADAS = [132, 138, 161, 237]    # 132 = JFK, 138 = LaGuardia (la pregunta e las busca por nombre)


def escribir_mes_crudo(raiz, anio, mes, con_cbd):
    # Un mes de viajes con los nombres y tipos de la TLC. Cada 50 viajes uno dura 0 minutos (viola R02) y cada 40 uno no
    # tiene pasajeros (advertencia R06): así el reporte de calidad tiene algo que contar.
    carpeta = raiz / "data" / "raw" / "yellow" / f"year={anio}" / f"month={mes:02d}"
    carpeta.mkdir(parents=True)
    ruta = carpeta / f"yellow_tripdata_{anio}-{mes:02d}.parquet"
    cbd = ", 1.5::DOUBLE AS cbd_congestion_fee" if con_cbd else ""
    con = duckdb.connect()
    con.execute(f"""
        COPY (
            SELECT 1::INTEGER AS VendorID,
                   TIMESTAMP '{anio}-{mes:02d}-05 08:00:00' + to_minutes(i) AS tpep_pickup_datetime,
                   TIMESTAMP '{anio}-{mes:02d}-05 08:00:00' + to_minutes(i + CASE WHEN i % 50 = 0 THEN 0 ELSE 15 END) AS tpep_dropoff_datetime,
                   (CASE WHEN i % 40 = 0 THEN 0 ELSE 1 END)::BIGINT AS passenger_count,
                   2.5::DOUBLE AS trip_distance, 1::BIGINT AS RatecodeID, 'N' AS store_and_fwd_flag,
                   ([132, 138, 161, 237])[1 + i % 4]::INTEGER AS PULocationID, 237::INTEGER AS DOLocationID,
                   (1 + i % 2)::BIGINT AS payment_type,
                   12.0::DOUBLE AS fare_amount, 0.5::DOUBLE AS extra, 0.5::DOUBLE AS mta_tax,
                   (CASE WHEN i % 2 = 0 THEN 2.0 ELSE 0.0 END)::DOUBLE AS tip_amount, 0.0::DOUBLE AS tolls_amount,
                   1.0::DOUBLE AS improvement_surcharge, 16.0::DOUBLE AS total_amount,
                   2.5::DOUBLE AS congestion_surcharge, 0.0::DOUBLE AS Airport_fee {cbd}
            FROM range({VIAJES}) t(i)
        ) TO '{ruta}' (FORMAT parquet)
    """)
    con.close()
    return ruta


def registrar_como_ingestado(raiz, ruta):
    # Anota el archivo en ingestion_log como ya cargado, así la etapa de ingesta lo omite en vez de descargarlo.
    huella = hashlib.sha256(ruta.read_bytes()).hexdigest()
    con = duckdb.connect(str(raiz / "data" / "pipeline.duckdb"))
    con.execute("""CREATE TABLE IF NOT EXISTS ingestion_log (archivo VARCHAR, url VARCHAR, filas BIGINT,
                   tamano_bytes BIGINT, sha256 VARCHAR, fecha_hora TIMESTAMP, estado VARCHAR, mensaje VARCHAR)""")
    filas = con.execute("SELECT count(*) FROM read_parquet(?)", [str(ruta)]).fetchall()[0][0] \
        if ruta.suffix == ".parquet" else con.execute("SELECT count(*) FROM read_csv(?)", [str(ruta)]).fetchall()[0][0]
    con.execute("INSERT INTO ingestion_log VALUES (?, '', ?, ?, ?, ?, 'ok', 'preparado por el test')",
                [ruta.name, filas, ruta.stat().st_size, huella, datetime.now()])
    con.close()


@pytest.fixture
def proyecto(tmp_path):
    # Copia del proyecto con dos meses sintéticos y la tabla de zonas, lista para 'python run.py --mes 2025-03'.
    for carpeta in ["src", "contracts", "sql"]:
        shutil.copytree(RAIZ / carpeta, tmp_path / carpeta, ignore=shutil.ignore_patterns("__pycache__"))
    for archivo in ["run.py", "dbt_project.yml", "profiles.yml"]:
        shutil.copy(RAIZ / archivo, tmp_path / archivo)
    zonas = tmp_path / "data" / "raw" / "zones"
    zonas.mkdir(parents=True)
    nombres = {132: "JFK Airport", 138: "LaGuardia Airport"}
    lineas = ['"LocationID","Borough","Zone","service_zone"'] + [
        f'{i},"Manhattan","{nombres.get(i, f"Zona {i}")}","Yellow Zone"' for i in range(1, 266)]
    ruta_zonas = zonas / "taxi_zone_lookup.csv"
    ruta_zonas.write_text("\n".join(lineas) + "\n", encoding="utf-8")
    registrar_como_ingestado(tmp_path, ruta_zonas)
    registrar_como_ingestado(tmp_path, escribir_mes_crudo(tmp_path, 2020, 1, con_cbd=False))   # otro mes, de una corrida anterior
    registrar_como_ingestado(tmp_path, escribir_mes_crudo(tmp_path, 2024, 3, con_cbd=False))
    registrar_como_ingestado(tmp_path, escribir_mes_crudo(tmp_path, 2025, 3, con_cbd=True))
    return tmp_path


def correr(raiz, mes="2025-03"):
    return subprocess.run([sys.executable, "run.py", "--mes", mes], cwd=raiz, capture_output=True, text=True, timeout=300)


def eventos(raiz, mes="2025-03"):
    ruta = raiz / "data" / "logs" / f"{mes}.jsonl"
    return [json.loads(linea) for linea in ruta.read_text(encoding="utf-8").splitlines()]


def test_una_corrida_completa_deja_las_metricas_de_cada_etapa(proyecto):
    proceso = correr(proyecto)
    assert proceso.returncode == 0, proceso.stdout + proceso.stderr

    lista = eventos(proyecto)
    fines = {e["etapa"]: e for e in lista if e.get("evento") == "fin"}
    assert list(fines) == ["ingesta", "esquema", "calidad", "curado", "modelo", "preguntas"]
    assert [e["evento"] for e in lista if "etapa" not in e] == ["corrida_inicio", "corrida_fin"]
    assert lista[-1]["estado"] == "ok" and lista[-1]["etapas"] == 6
    # Cada etapa midió tiempo, CPU y memoria.
    for e in fines.values():
        assert e["duracion_s"] >= 0 and e["cpu_s"] >= 0
        assert e["ram_fin_mb"] > 0 and e["pico_etapa_mb"] > 0
    assert lista[-1]["pico_ram_mb"] >= max(e["pico_etapa_mb"] for e in fines.values())
    # Y lo que procesó: dos meses de VIAJES viajes cada uno, más las 265 zonas en la ingesta.
    assert fines["ingesta"]["omitidos"] == 3 and fines["ingesta"]["descargados"] == 0
    assert fines["ingesta"]["filas"] == 2 * VIAJES + 265
    assert fines["calidad"]["filas"] == 2 * VIAJES
    assert fines["calidad"]["aceptadas"] + fines["calidad"]["cuarentena"] == 2 * VIAJES
    assert fines["calidad"]["cuarentena"] == 2 * (VIAJES // 50)             # los viajes de 0 minutos
    assert fines["curado"]["filas"] == fines["calidad"]["aceptadas"]
    assert fines["modelo"]["modelos"] == 4 and fines["modelo"]["tests_dbt"] == 15
    consultas = len(list((RAIZ / "sql" / "preguntas").glob("*.sql")))                        # una por archivo
    assert fines["preguntas"]["consultas"] == consultas and fines["preguntas"]["filas"] >= consultas     # cada consulta devuelve algo


def test_la_pantalla_muestra_lineas_clave_valor_y_el_resumen(proyecto):
    salida = correr(proyecto).stdout
    assert "evento=corrida_inicio" in salida and "evento=corrida_fin estado=ok" in salida
    assert "level=INFO etapa=calidad evento=fin duracion_s=" in salida
    assert "evento=inicio" not in salida                       # el inicio de cada etapa va solo al archivo
    assert "== RESUMEN 2025-03 ==" in salida and "pico de RAM:" in salida
    # La última línea dice lo mismo que el resumen y que el evento final del log.
    pico = eventos(proyecto)[-1]["pico_ram_mb"]
    assert f"pico de RAM: {pico} MB de" in salida and f"Pico de RAM: {pico} MB" in salida.splitlines()[-1]


def test_si_una_etapa_falla_el_log_dice_cual_y_el_resumen_llega_hasta_ahi(proyecto):
    (proyecto / "contracts" / "reglas_calidad.yaml").unlink()          # rompe la etapa de calidad
    proceso = correr(proyecto)
    assert proceso.returncode == 1
    assert "falló la etapa 'calidad'" in proceso.stderr

    lista = eventos(proyecto)
    error = next(e for e in lista if e.get("evento") == "error")
    assert error["etapa"] == "calidad" and error["level"] == "ERROR" and "reglas_calidad" in error["error"]
    assert [e["etapa"] for e in lista if e.get("evento") == "fin"] == ["ingesta", "esquema"]
    assert lista[-1]["evento"] == "corrida_fin" and lista[-1]["estado"] == "error"
    assert "Listo en" not in proceso.stdout


ORIGINALES = list(pipeline.ETAPAS)


class BaseFalsa:
    # Una conexión que solo cuenta cuántas veces la cerraron.
    def __init__(self):
        self.cerradas = 0

    def execute(self, sql):
        pass

    def close(self):
        self.cerradas += 1


@pytest.fixture
def pipeline_falso(monkeypatch, tmp_path):
    base = BaseFalsa()
    monkeypatch.setattr(pipeline, "conectar", lambda: base)
    monkeypatch.setattr(pipeline, "Corrida", partial(Corrida, directorio_logs=tmp_path))
    return base


def test_la_base_se_cierra_antes_de_la_primera_etapa_que_no_la_usa(pipeline_falso):
    # Retiene gigas de memoria aunque no se use: tiene que liberarse antes de que arranque dbt.
    vistas = []

    def etapa(nombre):
        def correr(con, anio, mes):
            vistas.append((nombre, con is not None, pipeline_falso.cerradas))
            return {}
        return correr

    pipeline.ETAPAS[:] = [("a", etapa("a"), True), ("b", etapa("b"), True),
                          ("c", etapa("c"), False), ("d", etapa("d"), False)]
    try:
        pipeline.ejecutar(2025, 3)
    finally:
        pipeline.ETAPAS[:] = ORIGINALES
    assert vistas == [("a", True, 0), ("b", True, 0), ("c", False, 1), ("d", False, 1)]
    assert pipeline_falso.cerradas == 1                       # y no se cierra dos veces al final


def test_si_una_etapa_con_la_base_falla_la_base_se_cierra_una_sola_vez(pipeline_falso):
    def rota(con, anio, mes):
        raise ValueError("explotó")

    pipeline.ETAPAS[:] = [("a", rota, True), ("b", lambda con, anio, mes: {}, False)]
    try:
        with pytest.raises(RuntimeError, match="falló la etapa 'a': explotó"):
            pipeline.ejecutar(2025, 3)
    finally:
        pipeline.ETAPAS[:] = ORIGINALES
    assert pipeline_falso.cerradas == 1


def test_solo_dbt_y_las_preguntas_corren_sin_la_base():
    assert [(nombre, usa_base) for nombre, _, usa_base in ORIGINALES] == [
        ("ingesta", True), ("esquema", True), ("calidad", True), ("curado", True),
        ("modelo", False), ("preguntas", False)]


def test_un_ctrl_c_en_una_etapa_deja_la_corrida_como_fallida(pipeline_falso, tmp_path):
    def interrumpida(con, anio, mes):
        raise KeyboardInterrupt

    pipeline.ETAPAS[:] = [("a", interrumpida, True)]
    try:
        with pytest.raises(KeyboardInterrupt):
            pipeline.ejecutar(2025, 3)
    finally:
        pipeline.ETAPAS[:] = ORIGINALES
    final = [json.loads(linea) for linea in (tmp_path / "2025-03.jsonl").read_text(encoding="utf-8").splitlines()][-1]
    assert final["evento"] == "corrida_fin" and final["estado"] == "error"
    assert pipeline_falso.cerradas == 1


def test_pedir_dagster_sin_tenerlo_instalado_da_un_mensaje_claro_y_no_un_traceback(tmp_path):
    # Simula que 'import dagster' falla: un paquete falso que se adelanta en el camino de búsqueda.
    falso = tmp_path / "falso"
    (falso / "dagster").mkdir(parents=True)
    (falso / "dagster" / "__init__.py").write_text("raise ImportError('simulado: Dagster no está instalado')")
    proceso = subprocess.run([sys.executable, "run.py", "--mes", "2025-03", "--dagster"], cwd=RAIZ, capture_output=True,
                             text=True, timeout=120, env={**os.environ, "PYTHONPATH": str(falso)})
    assert proceso.returncode == 1
    assert "necesita Dagster" in proceso.stderr and "requirements-dagster.txt" in proceso.stderr
    assert "Traceback" not in proceso.stderr
