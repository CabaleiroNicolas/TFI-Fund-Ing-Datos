# Tests de la Parte 3 (calidad): cada regla sobre datos sintéticos, duplicados,
# reconciliación y que una etapa que falla no deje nada a medias.
import duckdb
import pytest

from src import e3_calidad as calidad
from src.e2_esquema import cargar_contrato

CONTRATO = cargar_contrato()
COLUMNAS = [col["nombre"] for col in CONTRATO["columnas"]]
TIPOS = {col["nombre"]: col["tipo"] for col in CONTRATO["columnas"]}
REGLAS = calidad.cargar_reglas()
ARCHIVO = "yellow_tripdata_2025-03.parquet"

# Un viaje que no viola ninguna regla en marzo de 2025. Cada caso cambia solo lo que necesita.
VIAJE_VALIDO = dict(
    vendor_id=1, tpep_pickup_datetime="2025-03-10 10:00:00", tpep_dropoff_datetime="2025-03-10 10:20:00",
    passenger_count=1, trip_distance=2.0, ratecode_id=1, store_and_fwd_flag="N",
    pu_location_id=100, do_location_id=200, payment_type=1, fare_amount=12.0, extra=0.5,
    mta_tax=0.5, tip_amount=2.0, tolls_amount=0.0, improvement_surcharge=1.0, total_amount=16.0,
    congestion_surcharge=2.5, airport_fee=0.0, cbd_congestion_fee=0.0,
)


@pytest.fixture
def con(tmp_path):
    # Base en memoria con la vista de zonas (ids 1 a 265), como en el pipeline real.
    zonas = tmp_path / "zonas.csv"
    lineas = ['"LocationID","Borough","Zone","service_zone"']
    lineas += [f'{i},"Manhattan","Zona {i}","Yellow Zone"' for i in range(1, 266)]
    zonas.write_text("\n".join(lineas) + "\n", encoding="utf-8")

    conexion = duckdb.connect()
    calidad.crear_vista_zonas(conexion, zonas)
    yield conexion
    conexion.close()


def cargar_viajes(con, filas):
    # Crea la tabla 'viajes' con los tipos del contrato y le agrega 'archivo_origen'.
    definicion = ", ".join(f"{nombre} {TIPOS[nombre]}" for nombre in COLUMNAS)
    con.execute(f"CREATE OR REPLACE TABLE viajes ({definicion}, archivo_origen VARCHAR)")
    for fila in filas:
        datos = {**VIAJE_VALIDO, **fila}
        valores = [datos[nombre] for nombre in COLUMNAS] + [ARCHIVO]
        marcadores = ", ".join(["?"] * len(valores))
        con.execute(f"INSERT INTO viajes VALUES ({marcadores})", valores)


def evaluar_una_fila(con, **cambios):
    # Evalúa un solo viaje y devuelve (reglas_rechazo, reglas_advertencia).
    cargar_viajes(con, [cambios])
    calidad.evaluar(con, "SELECT * FROM viajes", CONTRATO, REGLAS, 2025, 3)
    return con.execute("SELECT reglas_rechazo, reglas_advertencia FROM evaluado").fetchall()[0]


# --- una regla por caso -------------------------------------------------------------------------

CASOS_RECHAZO = [
    ("R01", dict(tpep_pickup_datetime="2002-12-31 22:17:10", tpep_dropoff_datetime="2002-12-31 22:40:00")),
    ("R01", dict(tpep_pickup_datetime="2025-04-01 00:00:00", tpep_dropoff_datetime="2025-04-01 00:20:00")),
    ("R01", dict(tpep_pickup_datetime="2025-02-28 23:59:59", tpep_dropoff_datetime="2025-03-01 00:10:00")),
    ("R02", dict(tpep_dropoff_datetime="2025-03-10 10:00:00")),     # dura 0
    ("R02", dict(tpep_dropoff_datetime="2025-03-10 09:50:00")),     # termina antes de empezar
    ("R03", dict(trip_distance=0.0)),
    ("R03", dict(trip_distance=-1.0)),
    ("R04", dict(trip_distance=150.0, tpep_dropoff_datetime="2025-03-10 13:00:00")),
    ("R05", dict(total_amount=-5.0)),
    ("R07", dict(pu_location_id=999)),
    ("R07", dict(do_location_id=None)),                             # NULL cuenta como zona inexistente
    ("R09", dict(trip_distance=50.0, tpep_dropoff_datetime="2025-03-10 10:10:00")),   # 300 mph
]

CASOS_ADVERTENCIA = [
    ("R06", dict(passenger_count=0)),
    ("R06", dict(passenger_count=None)),
    ("R10", dict(pu_location_id=264)),
    ("R10", dict(do_location_id=265)),
]

CASOS_VALIDOS = [
    dict(),
    dict(trip_distance=100.0, tpep_dropoff_datetime="2025-03-10 13:00:00"),     # justo en el límite de R04
    dict(tpep_pickup_datetime="2025-03-01 00:00:00", tpep_dropoff_datetime="2025-03-01 00:15:00"),
    dict(tpep_pickup_datetime="2025-03-31 23:59:59", tpep_dropoff_datetime="2025-04-01 00:10:00"),
    dict(total_amount=0.0),
]


@pytest.mark.parametrize("regla, cambios", CASOS_RECHAZO)
def test_regla_de_rechazo_manda_la_fila_a_cuarentena(con, regla, cambios):
    rechazo, advertencia = evaluar_una_fila(con, **cambios)
    assert rechazo == [regla]
    assert advertencia == []


@pytest.mark.parametrize("regla, cambios", CASOS_ADVERTENCIA)
def test_regla_de_advertencia_marca_pero_no_rechaza(con, regla, cambios):
    rechazo, advertencia = evaluar_una_fila(con, **cambios)
    assert rechazo == []
    assert advertencia == [regla]


@pytest.mark.parametrize("cambios", CASOS_VALIDOS)
def test_viaje_valido_no_viola_ninguna_regla(con, cambios):
    assert evaluar_una_fila(con, **cambios) == ([], [])


def test_una_fila_puede_violar_varias_reglas(con):
    rechazo, _ = evaluar_una_fila(con, trip_distance=-1.0, total_amount=-3.0)
    assert rechazo == ["R03", "R05"]


def test_duplicado_solo_marca_las_copias_sobrantes(con):
    # Tres filas: dos idénticas y una distinta. Se queda una de las idénticas, la otra va a R08.
    cargar_viajes(con, [dict(tip_amount=2.0), dict(tip_amount=2.0), dict(tip_amount=3.0)])
    calidad.evaluar(con, "SELECT * FROM viajes", CONTRATO, REGLAS, 2025, 3)

    marcadas = con.execute("""
        SELECT tip_amount, count(*) FILTER (WHERE list_contains(reglas_rechazo, 'R08'))
        FROM evaluado GROUP BY tip_amount ORDER BY tip_amount
    """).fetchall()
    assert marcadas == [(2.0, 1), (3.0, 0)]


# --- las reglas como datos -------------------------------------------------------------------------

def test_el_archivo_de_reglas_real_es_valido():
    # cargar_reglas ya lo validó al importar; acá se comprueba que hay reglas y que los ids no se repiten.
    ids = [regla["id"] for regla in REGLAS]
    assert len(ids) > 0
    assert len(ids) == len(set(ids))


def test_validar_reglas_rechaza_un_id_repetido():
    with pytest.raises(ValueError, match="repetido"):
        calidad.validar_reglas({"reglas": [REGLAS[0], REGLAS[0]]})


def test_validar_reglas_rechaza_un_marcador_inexistente():
    mala = {**REGLAS[0], "condicion": "trip_distance > {no_existe}"}
    with pytest.raises(ValueError, match="marcador"):
        calidad.validar_reglas({"reglas": [mala]})


def test_limites_del_mes_en_diciembre_pasan_al_anio_siguiente():
    assert calidad.limites_del_mes(2025, 12) == ("TIMESTAMP '2025-12-01'", "TIMESTAMP '2026-01-01'")


# --- reconciliación ----------------------------------------------------------------------------------

def test_reconciliar_acepta_cuentas_que_cierran():
    calidad.reconciliar(10, 7, 3, ARCHIVO)


@pytest.mark.parametrize("leidas, aceptadas, cuarentena", [(10, 7, 2), (10, 8, 3), (10, 0, 0)])
def test_reconciliar_falla_si_se_pierde_o_se_inventa_una_fila(leidas, aceptadas, cuarentena):
    with pytest.raises(calidad.ErrorReconciliacion):
        calidad.reconciliar(leidas, aceptadas, cuarentena, ARCHIVO)


# --- etapa completa ----------------------------------------------------------------------------------

def procesar(con, leidas):
    calidad.procesar_calidad(con, CONTRATO, REGLAS, "SELECT * FROM viajes", leidas, ARCHIVO, 2025, 3)


def filas_de_prueba():
    # una válida, una válida con advertencia (R06) y una rechazada (R03)
    return [dict(), dict(passenger_count=0, tip_amount=1.0), dict(trip_distance=0.0, tip_amount=5.0)]


def test_la_etapa_separa_aceptadas_y_cuarentena(con):
    cargar_viajes(con, filas_de_prueba())
    procesar(con, leidas=3)

    assert calidad.contar_destino(con, ARCHIVO) == (2, 1)
    assert con.execute("SELECT reglas_rechazo FROM cuarentena").fetchall() == [(["R03"],)]
    assert con.execute("SELECT flags FROM aceptadas WHERE len(flags) > 0").fetchall() == [(["R06"],)]
    assert con.execute(
        "SELECT filas_leidas, filas_aceptadas, filas_cuarentena, filas_con_advertencia FROM reporte_resumen"
    ).fetchall() == [(3, 2, 1, 1)]
    por_regla = dict(con.execute("SELECT regla_id, filas FROM reporte_reglas").fetchall())
    assert por_regla["R03"] == 1 and por_regla["R06"] == 1 and por_regla["R07"] == 0
    assert len(por_regla) == len(REGLAS)


def test_reprocesar_el_mismo_mes_no_duplica_filas(con):
    cargar_viajes(con, filas_de_prueba())
    procesar(con, leidas=3)
    procesar(con, leidas=3)

    assert calidad.contar_destino(con, ARCHIVO) == (2, 1)
    assert con.execute("SELECT count(*) FROM reporte_resumen").fetchall()[0][0] == 1
    assert con.execute("SELECT count(*) FROM reporte_reglas").fetchall()[0][0] == len(REGLAS)


def test_si_la_reconciliacion_falla_no_queda_nada_a_medias(con):
    cargar_viajes(con, filas_de_prueba())
    procesar(con, leidas=3)

    # Cambian los datos del mes (5 viajes válidos) y se informa un conteo de origen equivocado.
    cargar_viajes(con, [dict(tip_amount=float(i)) for i in range(5)])
    with pytest.raises(calidad.ErrorReconciliacion):
        procesar(con, leidas=4)

    # Todo sigue como después de la primera corrida: ni filas nuevas ni reporte nuevo.
    assert calidad.contar_destino(con, ARCHIVO) == (2, 1)
    assert con.execute("SELECT filas_leidas FROM reporte_resumen").fetchall() == [(3,)]