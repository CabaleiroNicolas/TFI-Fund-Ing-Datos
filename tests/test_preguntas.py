# Tests de las preguntas de negocio: un modelo mínimo armado a mano, con respuestas calculadas a mano.
import shutil

import duckdb
import pytest

from src import e6_preguntas as preguntas
from src.config import RAIZ

# (año, hora, zona origen, zona destino, tipo de pago, tarifa, propina, cargo cbd)
VIAJES = [
    # mes asignado (2025-03): 10 viajes (los dos últimos no entran en la pregunta d: tarifa 0 y propina nula)
    (2025, 8, 1, 2, 1, 10, 2, 0.75), (2025, 8, 1, 2, 1, 20, 2, 0.75), (2025, 9, 1, 3, 2, 10, 0, 0.75),
    (2025, 9, 2, 3, 1, 30, 3, 0), (2025, 17, 2, 1, 2, 10, 0, 0), (2025, 17, 132, 1, 1, 50, 5, 0),
    (2025, 18, 1, 138, 1, 40, 4, 0.75), (2025, 18, 132, 138, 1, 60, 6, 0), (2025, 10, 3, 3, 3, 0, 0, 0),
    (2025, 11, 3, 3, 0, 10, None, 0),
    # mismo mes de 2024: 4 viajes, sin cbd_congestion_fee
    (2024, 8, 1, 2, 1, 10, 1, None), (2024, 9, 2, 3, 2, 20, 0, None), (2024, 9, 2, 3, 1, 20, 4, None),
    (2024, 17, 132, 1, 1, 30, 3, None),
]


@pytest.fixture
def raiz(tmp_path):
    (tmp_path / "data").mkdir()
    shutil.copytree(RAIZ / "sql" / "preguntas", tmp_path / "sql" / "preguntas")      # las consultas reales
    con = duckdb.connect(str(tmp_path / "data" / "modelo.duckdb"))
    con.execute("CREATE TABLE dim_zone (zona_id INTEGER, borough VARCHAR, zona VARCHAR)")
    con.executemany("INSERT INTO dim_zone VALUES (?, ?, ?)", [
        (1, "Manhattan", "Zona Uno"), (2, "Bronx", "Zona Dos"), (3, "Queens", "Zona Tres"),
        (132, "Queens", "JFK Airport"), (138, "Queens", "LaGuardia Airport")])
    con.execute("""CREATE TABLE dim_date AS
        SELECT fecha, year(fecha) AS anio, month(fecha) AS mes, isodow(fecha) >= 6 AS es_fin_de_semana
        FROM (SELECT unnest(generate_series(DATE '2024-03-01', DATE '2024-03-31', INTERVAL 1 DAY))::date AS fecha
              UNION ALL SELECT unnest(generate_series(DATE '2025-03-01', DATE '2025-03-31', INTERVAL 1 DAY))::date)""")
    con.execute("""CREATE TABLE fact_trips (fecha DATE, hora BIGINT, pu_zona_id INTEGER, do_zona_id INTEGER,
        payment_type BIGINT, fare_amount DOUBLE, tip_amount DOUBLE, total_amount DOUBLE, cbd_congestion_fee DOUBLE)""")
    con.executemany("INSERT INTO fact_trips VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [(f"{a}-03-10", h, pu, do, pago, tarifa, propina, tarifa + (propina or 0), cbd)
                     for a, h, pu, do, pago, tarifa, propina, cbd in VIAJES])
    con.execute("""CREATE TABLE agg_zone_hour AS
        SELECT pu_zona_id, fecha, hora, count(*) AS viajes, sum(total_amount) AS ingreso_total
        FROM fact_trips GROUP BY ALL""")
    con.close()
    return tmp_path


@pytest.fixture
def respuestas(raiz):
    # Corre las consultas reales de sql/preguntas/ y devuelve {inicial de la pregunta: filas}.
    resultados = preguntas.correr_preguntas(2025, 3, raiz, min_viajes_zona=1)
    return {r["archivo"].split("_")[0]: r["filas"] for r in resultados}


def test_a_top_de_zonas_con_su_variacion_contra_el_anio_anterior(respuestas):
    assert respuestas["a"] == [
        ("Zona Uno", "Manhattan", 4, 1, 3, 300.0),
        ("JFK Airport", "Queens", 2, 1, 1, 100.0),
        ("Zona Dos", "Bronx", 2, 2, 0, 0.0),
        ("Zona Tres", "Queens", 2, 0, 2, None),                  # sin viajes el año anterior: no hay variación porcentual
    ]


def test_b_hora_con_mayor_ingreso_promedio_en_cada_mes(respuestas):
    assert respuestas["b"] == [(2024, 17, 1, 33.0), (2025, 18, 2, 55.0)]


def test_c_porcentaje_de_viajes_con_cargo_de_congestion_y_zonas_con_mayor_proporcion(respuestas):
    assert respuestas["c1"] == [(10, 4, 40.0)]
    assert respuestas["c2"][0] == ("Zona Uno", "Manhattan", 4, 4, 100.0)           # las 4 salidas de esa zona pagan el cargo


def test_c2_deja_afuera_las_zonas_con_menos_viajes_que_el_minimo(raiz):
    resultados = preguntas.correr_preguntas(2025, 3, raiz, min_viajes_zona=3)
    c2 = [r for r in resultados if r["archivo"].startswith("c2")][0]
    assert [fila[0] for fila in c2["filas"]] == ["Zona Uno"]                       # solo ella tiene 3 viajes o más


def test_d_la_propina_del_efectivo_es_cero_porque_no_se_registra(respuestas):
    esperado = [
        (2024, 1, "Tarjeta de crédito", 3, 13.33, 13.33),
        (2024, 2, "Efectivo", 1, 0.0, 0.0),
        (2025, 1, "Tarjeta de crédito", 6, 11.67, 10.48),
        (2025, 2, "Efectivo", 2, 0.0, 0.0),
    ]
    assert respuestas["d"] == esperado


def test_e_aeropuertos_cuenta_una_sola_vez_los_viajes_entre_ambos(respuestas):
    assert respuestas["e"] == [
        (2024, "JFK Airport", 1, 30.0),
        (2024, "JFK o LaGuardia (sin repetir)", 1, 30.0),
        (2025, "JFK Airport", 2, 55.0),
        (2025, "JFK o LaGuardia (sin repetir)", 3, 50.0),        # el viaje JFK -> LaGuardia cuenta una vez
        (2025, "LaGuardia Airport", 2, 50.0),
    ]


def test_f_variacion_de_viajes_y_de_tarifa_media(respuestas):
    assert respuestas["f"] == [(4, 10, 150.0, 20.0, 24.0, 20.0, 10, 10)]


def test_g_zona_de_origen_con_mas_viajes_por_tipo_de_pago(respuestas):
    assert respuestas["g"] == [
        (2024, 1, "Tarjeta de crédito", "JFK Airport", "Queens", 1, 3, 33.3),        # empate a 1 viaje: gana el nombre que va primero
        (2024, 2, "Efectivo", "Zona Dos", "Bronx", 1, 1, 100.0),
        (2025, 0, "Flex Fare", "Zona Tres", "Queens", 1, 1, 100.0),
        (2025, 1, "Tarjeta de crédito", "Zona Uno", "Manhattan", 3, 6, 50.0),
        (2025, 2, "Efectivo", "Zona Dos", "Bronx", 1, 2, 50.0),
        (2025, 3, "Sin cargo", "Zona Tres", "Queens", 1, 1, 100.0),
    ]


def test_h1_pares_de_zonas_con_mas_propina_en_monto(respuestas):
    assert respuestas["h1"] == [
        (2024, 1, "Zona Dos", "Zona Tres", 4.0, 1, 2, 4.0),
        (2024, 2, "JFK Airport", "Zona Uno", 3.0, 1, 1, 3.0),
        (2024, 3, "Zona Uno", "Zona Dos", 1.0, 1, 1, 1.0),
        (2025, 1, "JFK Airport", "LaGuardia Airport", 6.0, 1, 1, 6.0),
        (2025, 2, "JFK Airport", "Zona Uno", 5.0, 1, 1, 5.0),
        (2025, 3, "Zona Uno", "LaGuardia Airport", 4.0, 1, 1, 4.0),                   # empate en 4.0 con el puesto siguiente
        (2025, 4, "Zona Uno", "Zona Dos", 4.0, 2, 2, 2.0),
        (2025, 5, "Zona Dos", "Zona Tres", 3.0, 1, 1, 3.0),
    ]


def test_h2_pares_de_zonas_con_mas_viajes_con_propina(respuestas):
    assert respuestas["h2"] == [
        (2024, 1, "JFK Airport", "Zona Uno", 1, 1, 100.0, 3.0),
        (2024, 2, "Zona Dos", "Zona Tres", 1, 2, 50.0, 4.0),                          # un viaje de dos tuvo propina
        (2024, 3, "Zona Uno", "Zona Dos", 1, 1, 100.0, 1.0),
        (2025, 1, "Zona Uno", "Zona Dos", 2, 2, 100.0, 4.0),
        (2025, 2, "JFK Airport", "LaGuardia Airport", 1, 1, 100.0, 6.0),
        (2025, 3, "JFK Airport", "Zona Uno", 1, 1, 100.0, 5.0),
        (2025, 4, "Zona Dos", "Zona Tres", 1, 1, 100.0, 3.0),
        (2025, 5, "Zona Uno", "LaGuardia Airport", 1, 1, 100.0, 4.0),                 # el par sin propina (3 -> 3) no entra
    ]


def test_una_pregunta_sin_resultados_es_un_error(raiz):
    with pytest.raises(RuntimeError, match="no devolvió filas"):
        preguntas.correr_preguntas(2025, 4, raiz)                                  # abril no está en el modelo


def test_el_informe_guarda_cada_consulta_con_su_resultado(raiz):
    resultados = preguntas.correr_preguntas(2025, 3, raiz, min_viajes_zona=1)
    ruta = preguntas.guardar_informe(resultados, 2025, 3, raiz)
    assert ruta == raiz / "data" / "resultados" / "2025-03.md"
    texto = ruta.read_text(encoding="utf-8")
    assert "# Preguntas de negocio: 2025-03 contra 2024-03" in texto
    assert "`$anio` = 2025, `$mes` = 3, `$min_viajes_zona` = 1" in texto
    assert texto.count("```sql") == 10 and "| Zona Uno | Manhattan | 4 | 1 | 3 | 300.0 |" in texto
