# Tests de la Parte 4 (transformación): columnas derivadas, zonas, estructura Hive
# y que reprocesar un mes reemplace solo su partición.
from datetime import date

import duckdb
import pytest

from src import e3_calidad as calidad
from src import e4_transformacion as transformacion
from src.e1_ingesta import calcular_hash
from src.e2_esquema import cargar_contrato

CONTRATO = cargar_contrato()
COLUMNAS = [col["nombre"] for col in CONTRATO["columnas"]]
TIPOS = {col["nombre"]: col["tipo"] for col in CONTRATO["columnas"]}

VIAJE_VALIDO = dict(
    vendor_id=1, tpep_pickup_datetime="2025-03-10 10:00:00", tpep_dropoff_datetime="2025-03-10 10:20:00",
    passenger_count=1, trip_distance=2.0, ratecode_id=1, store_and_fwd_flag="N",
    pu_location_id=100, do_location_id=200, payment_type=1, fare_amount=12.0, extra=0.5,
    mta_tax=0.5, tip_amount=2.0, tolls_amount=0.0, improvement_surcharge=1.0, total_amount=16.0,
    congestion_surcharge=2.5, airport_fee=0.0, cbd_congestion_fee=0.0,
)


def escribir_zonas(ruta, repetir_100=False):
    # Zonas 1 a 265. La 100 es Midtown (Manhattan) y la 200 Riverdale (Bronx).
    lineas = ['"LocationID","Borough","Zone","service_zone"']
    for i in range(1, 266):
        borough, zona = {100: ("Manhattan", "Midtown"), 200: ("Bronx", "Riverdale")}.get(i, ("Manhattan", f"Zona {i}"))
        lineas.append(f'{i},"{borough}","{zona}","Yellow Zone"')
    if repetir_100:
        lineas.append('100,"Manhattan","Midtown","Yellow Zone"')
    ruta.write_text("\n".join(lineas) + "\n", encoding="utf-8")


@pytest.fixture
def con(tmp_path):
    zonas = tmp_path / "zonas.csv"
    escribir_zonas(zonas)
    conexion = duckdb.connect()
    calidad.crear_vista_zonas(conexion, zonas)
    definicion = ", ".join(f"{nombre} {TIPOS[nombre]}" for nombre in COLUMNAS)
    conexion.execute(f"CREATE TABLE aceptadas ({definicion}, archivo_origen VARCHAR, flags VARCHAR[])")
    yield conexion
    conexion.close()


@pytest.fixture
def raiz(tmp_path):
    # Carpeta de la capa curada, separada de los demás archivos del test.
    return tmp_path / "curado"


def nombre_archivo(anio, mes):
    return f"yellow_tripdata_{anio}-{mes:02d}.parquet"


def guardar_aceptadas(con, anio, mes, filas):
    # Deja en 'aceptadas' exactamente estas filas para ese mes (reemplaza las que hubiera).
    archivo = nombre_archivo(anio, mes)
    con.execute("DELETE FROM aceptadas WHERE archivo_origen = ?", [archivo])
    for fila in filas:
        datos = {**VIAJE_VALIDO, **fila}
        valores = [datos[nombre] for nombre in COLUMNAS] + [archivo, []]
        marcadores = ", ".join(["?"] * len(valores))
        con.execute(f"INSERT INTO aceptadas VALUES ({marcadores})", valores)


def leer_curado(con, anio, mes, raiz):
    # Filas de la partición como lista de diccionarios.
    cursor = con.execute("SELECT * FROM read_parquet(?, hive_partitioning = false)",
                         [str(transformacion.ruta_curado(anio, mes, raiz))])
    nombres = [campo[0] for campo in cursor.description]
    return [dict(zip(nombres, fila)) for fila in cursor.fetchall()]


def curar_una_fila(con, raiz, **cambios):
    guardar_aceptadas(con, 2025, 3, [cambios])
    transformacion.curar_mes(con, 2025, 3, raiz)
    return leer_curado(con, 2025, 3, raiz)[0]


# --- columnas derivadas -------------------------------------------------------------------------------

def test_columnas_derivadas_de_un_viaje_conocido(con, raiz):
    fila = curar_una_fila(
        con, raiz, tpep_pickup_datetime="2025-03-10 10:00:00", tpep_dropoff_datetime="2025-03-10 10:30:00",
        trip_distance=15.0, fare_amount=12.0, tip_amount=3.0)
    assert fila["duracion_min"] == 30.0
    assert fila["velocidad_mph"] == 30.0           # 15 millas en media hora
    assert fila["fecha_subida"] == date(2025, 3, 10)
    assert fila["hora_subida"] == 10
    assert fila["dia_semana"] == 1                 # el 10/03/2025 fue lunes
    assert fila["propina_pct"] == 25.0             # 3 de propina sobre 12 de tarifa


CASOS_DERIVADAS = [
    # (cambios, columna, esperado)
    (dict(tpep_pickup_datetime="2025-03-10 10:00:30", tpep_dropoff_datetime="2025-03-10 10:02:00"),
     "duracion_min", 1.5),                                           # 90 segundos, no 2 minutos
    (dict(tpep_pickup_datetime="2025-03-09 23:59:59", tpep_dropoff_datetime="2025-03-10 00:10:00"),
     "hora_subida", 23),                                             # la hora es la de la subida
    (dict(tpep_pickup_datetime="2025-03-09 23:59:59", tpep_dropoff_datetime="2025-03-10 00:10:00"),
     "fecha_subida", date(2025, 3, 9)),                              # la fecha también
    (dict(tpep_pickup_datetime="2025-03-09 12:00:00", tpep_dropoff_datetime="2025-03-09 12:10:00"),
     "dia_semana", 7),                                               # domingo
    (dict(fare_amount=0.0, tip_amount=0.0, total_amount=3.0), "propina_pct", None),   # sin tarifa no hay porcentaje
    (dict(fare_amount=10.0, tip_amount=0.0), "propina_pct", 0.0),
    (dict(payment_type=2, tip_amount=0.0), "propina_pct", None),     # efectivo: la propina no se registra
    (dict(payment_type=0, tip_amount=1.0), "propina_pct", None),     # Flex Fare: tampoco es interpretable
]


@pytest.mark.parametrize("cambios, columna, esperado", CASOS_DERIVADAS)
def test_casos_borde_de_las_columnas_derivadas(con, raiz, cambios, columna, esperado):
    assert curar_una_fila(con, raiz, **cambios)[columna] == esperado


def test_zonas_de_origen_y_destino(con, raiz):
    fila = curar_una_fila(con, raiz, pu_location_id=100, do_location_id=200)
    assert (fila["pu_borough"], fila["pu_zona"]) == ("Manhattan", "Midtown")
    assert (fila["do_borough"], fila["do_zona"]) == ("Bronx", "Riverdale")


def test_una_zona_sin_datos_no_hace_perder_la_fila(con, raiz):
    # Con R07 esto no debería llegar a 'aceptadas', pero el LEFT JOIN asegura que no se pierda la fila.
    fila = curar_una_fila(con, raiz, pu_location_id=999)
    assert fila["pu_borough"] is None and fila["pu_zona"] is None
    assert fila["do_zona"] == "Riverdale"


# --- estructura Hive -----------------------------------------------------------------------------------

def test_la_particion_queda_en_year_month_y_se_lee_con_hive(con, raiz):
    guardar_aceptadas(con, 2025, 3, [dict(tip_amount=1.0), dict(tip_amount=2.0)])
    escritas = transformacion.curar_mes(con, 2025, 3, raiz)

    assert escritas == 2
    assert (raiz / "year=2025" / "month=3" / "curado.parquet").exists()
    assert list(raiz.rglob("*.part")) == []                          # no queda el archivo temporal
    # year y month están en la ruta, no dentro del archivo
    columnas = {fila[0] for fila in con.execute(
        "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning = false)",
        [str(transformacion.ruta_curado(2025, 3, raiz))]).fetchall()}
    assert "year" not in columnas and "month" not in columnas
    assert transformacion.contar_particiones(con, raiz) == [(2025, 3, 2)]


# --- reemplazo de una sola partición --------------------------------------------------------------------

def test_reprocesar_un_mes_reemplaza_solo_su_particion(con, raiz):
    guardar_aceptadas(con, 2024, 3, [dict(tip_amount=float(i)) for i in range(3)])
    guardar_aceptadas(con, 2025, 3, [dict(tip_amount=float(i)) for i in range(2)])
    transformacion.curar_mes(con, 2024, 3, raiz)
    transformacion.curar_mes(con, 2025, 3, raiz)
    assert transformacion.contar_particiones(con, raiz) == [(2024, 3, 3), (2025, 3, 2)]
    hash_2024 = calcular_hash(transformacion.ruta_curado(2024, 3, raiz))

    # Cambia lo aceptado de marzo de 2025 (ahora son 4 filas) y se reprocesa solo ese mes.
    guardar_aceptadas(con, 2025, 3, [dict(tip_amount=float(i)) for i in range(4)])
    transformacion.curar_mes(con, 2025, 3, raiz)

    assert transformacion.contar_particiones(con, raiz) == [(2024, 3, 3), (2025, 3, 4)]
    assert calcular_hash(transformacion.ruta_curado(2024, 3, raiz)) == hash_2024     # la otra, intacta


def test_reprocesar_dos_veces_el_mismo_mes_da_lo_mismo(con, raiz):
    guardar_aceptadas(con, 2025, 3, [dict(tip_amount=1.0), dict(tip_amount=2.0)])
    transformacion.curar_mes(con, 2025, 3, raiz)
    transformacion.curar_mes(con, 2025, 3, raiz)
    assert transformacion.contar_particiones(con, raiz) == [(2025, 3, 2)]


# --- fallas: la partición anterior no se toca -------------------------------------------------------------

def test_si_el_conteo_no_cierra_falla_y_deja_la_particion_vieja(con, raiz, tmp_path):
    guardar_aceptadas(con, 2025, 3, [dict(tip_amount=1.0), dict(tip_amount=2.0)])
    transformacion.curar_mes(con, 2025, 3, raiz)
    destino = transformacion.ruta_curado(2025, 3, raiz)
    hash_antes = calcular_hash(destino)

    # Una zona repetida en la tabla de zonas duplica las filas en el JOIN: 2 aceptadas, 4 curadas.
    zonas_mal = tmp_path / "zonas_mal.csv"
    escribir_zonas(zonas_mal, repetir_100=True)
    calidad.crear_vista_zonas(con, zonas_mal)

    with pytest.raises(calidad.ErrorReconciliacion):
        transformacion.curar_mes(con, 2025, 3, raiz)

    assert calcular_hash(destino) == hash_antes
    assert list(raiz.rglob("*.part")) == []


def test_sin_filas_aceptadas_no_escribe_nada(con, raiz):
    with pytest.raises(ValueError, match="no hay filas aceptadas"):
        transformacion.curar_mes(con, 2025, 3, raiz)
    assert not raiz.exists()