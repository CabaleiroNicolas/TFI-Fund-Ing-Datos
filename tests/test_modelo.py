# Tests de la Parte 5 (modelo): corren dbt de verdad sobre una capa curada sintética, en una copia del proyecto.
import shutil

import duckdb
import pytest

from src import e5_modelo as modelo
from src.config import RAIZ

VIAJES_POR_MES = 3


def escribir_curado(raiz, zona_origen=100):
    # Dos meses de la capa curada con VIAJES_POR_MES viajes cada uno, en carpetas estilo Hive.
    con = duckdb.connect()
    for anio, mes in [(2024, 3), (2025, 3)]:
        carpeta = raiz / "data" / "curado" / f"year={anio}" / f"month={mes}"
        carpeta.mkdir(parents=True)
        con.execute(f"""
            COPY (
                SELECT 1 AS vendor_id,
                       TIMESTAMP '{anio}-03-10 10:00:00' + to_minutes(i) AS tpep_pickup_datetime,
                       TIMESTAMP '{anio}-03-10 10:20:00' + to_minutes(i) AS tpep_dropoff_datetime,
                       1 AS passenger_count, 2.0 AS trip_distance, 1 AS ratecode_id, 'N' AS store_and_fwd_flag,
                       {zona_origen} AS pu_location_id, 200 AS do_location_id, 1 AS payment_type,
                       12.0 AS fare_amount, 0.5 AS extra, 0.5 AS mta_tax, 2.0 AS tip_amount, 0.0 AS tolls_amount,
                       1.0 AS improvement_surcharge, 16.0 AS total_amount, 2.5 AS congestion_surcharge,
                       0.0 AS airport_fee, 0.0 AS cbd_congestion_fee,
                       'yellow_tripdata_{anio}-03.parquet' AS archivo_origen, []::VARCHAR[] AS flags,
                       20.0 AS duracion_min, 6.0 AS velocidad_mph, DATE '{anio}-03-10' AS fecha_subida,
                       10 AS hora_subida, 1 AS dia_semana, 16.67 AS propina_pct
                FROM range({VIAJES_POR_MES}) t(i)
            ) TO '{carpeta / "curado.parquet"}' (FORMAT parquet)
        """)
    con.close()


@pytest.fixture
def proyecto(tmp_path):
    # Una copia mínima del proyecto: configuración de dbt, modelos, tests y la tabla de zonas (1 a 265).
    for archivo in ["dbt_project.yml", "profiles.yml"]:
        shutil.copy(RAIZ / archivo, tmp_path / archivo)
    shutil.copytree(RAIZ / "sql" / "modelo", tmp_path / "sql" / "modelo")
    shutil.copytree(RAIZ / "sql" / "tests", tmp_path / "sql" / "tests")
    zonas = tmp_path / "data" / "raw" / "zones"
    zonas.mkdir(parents=True)
    lineas = ['"LocationID","Borough","Zone","service_zone"'] + [f'{i},"Manhattan","Zona {i}","Yellow Zone"' for i in range(1, 266)]
    (zonas / "taxi_zone_lookup.csv").write_text("\n".join(lineas) + "\n", encoding="utf-8")
    return tmp_path


def conteos(raiz):
    con = duckdb.connect(str(raiz / "data" / "modelo.duckdb"), read_only=True)
    try:
        return {t: con.execute(f"SELECT count(*) FROM {t}").fetchall()[0][0]
                for t in ["fact_trips", "dim_zone", "dim_date", "agg_zone_hour"]}
    finally:
        con.close()


def test_el_modelo_se_construye_y_cuadra_con_la_capa_curada(proyecto):
    escribir_curado(proyecto)
    nodos = modelo.construir_modelo(proyecto)

    assert conteos(proyecto) == {"fact_trips": 2 * VIAJES_POR_MES, "dim_zone": 265, "dim_date": 62, "agg_zone_hour": 2}
    assert {n["nombre"] for n in nodos if n["tipo"] == "model"} == {"fact_trips", "dim_zone", "dim_date", "agg_zone_hour"}
    assert all(n["estado"] in ("success", "pass") for n in nodos)
    con = duckdb.connect(str(proyecto / "data" / "modelo.duckdb"), read_only=True)
    total_hechos = con.execute("SELECT sum(total_amount) FROM fact_trips").fetchall()[0][0]
    total_agg, viajes_agg = con.execute("SELECT sum(ingreso_total), sum(viajes) FROM agg_zone_hour").fetchall()[0]
    con.close()
    # todos los viajes de un mes comparten zona, fecha y hora: la agregada tiene una fila por mes, pero conserva todo
    assert viajes_agg == 2 * VIAJES_POR_MES
    assert total_hechos == total_agg == 2 * VIAJES_POR_MES * 16.0


def test_reconstruir_el_modelo_dos_veces_no_cambia_los_conteos(proyecto):
    escribir_curado(proyecto)
    modelo.construir_modelo(proyecto)
    antes = conteos(proyecto)
    modelo.construir_modelo(proyecto)
    assert conteos(proyecto) == antes


def test_si_un_viaje_apunta_a_una_zona_inexistente_la_etapa_falla(proyecto):
    escribir_curado(proyecto, zona_origen=999)                              # la tabla de zonas llega hasta 265
    with pytest.raises(RuntimeError, match="relationships_fact_trips_pu_zona_id"):
        modelo.construir_modelo(proyecto)