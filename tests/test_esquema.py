# Tests de la Parte 2 (esquema): el contrato frente a archivos reales de distintos años.
# Los archivos de TLC cambiaron entre 2022 y 2024: tipos más angostos o más anchos y airport_fee en minúscula.
import duckdb
import pytest

from src import e2_esquema as esquema

CONTRATO = esquema.cargar_contrato()
TIPOS_DEL_CONTRATO = {col["nombre"]: col["tipo"] for col in CONTRATO["columnas"]}

# Una fila con el esquema que traen los archivos de 2024 y 2025 (el que describe el contrato).
FILA_2025 = {
    "VendorID": "1::INTEGER", "tpep_pickup_datetime": "TIMESTAMP '2025-03-10 10:00:00'",
    "tpep_dropoff_datetime": "TIMESTAMP '2025-03-10 10:20:00'", "passenger_count": "2::BIGINT",
    "trip_distance": "2.0::DOUBLE", "RatecodeID": "1::BIGINT", "store_and_fwd_flag": "'N'",
    "PULocationID": "100::INTEGER", "DOLocationID": "200::INTEGER", "payment_type": "1::BIGINT",
    "fare_amount": "12.0::DOUBLE", "extra": "0.5::DOUBLE", "mta_tax": "0.5::DOUBLE",
    "tip_amount": "2.0::DOUBLE", "tolls_amount": "0.0::DOUBLE", "improvement_surcharge": "1.0::DOUBLE",
    "total_amount": "16.0::DOUBLE", "congestion_surcharge": "2.5::DOUBLE",
    "Airport_fee": "1.25::DOUBLE", "cbd_congestion_fee": "0.75::DOUBLE",
}


def escribir_parquet(ruta, cambios=None, quitar=(), renombrar=None):
    # Escribe un Parquet de una fila. 'cambios' reemplaza la expresión de una columna (para cambiarle
    # el tipo o el valor), 'quitar' la saca y 'renombrar' le cambia el nombre.
    columnas = {**FILA_2025, **(cambios or {})}
    for nombre in quitar:
        columnas.pop(nombre)
    renombrar = renombrar or {}
    seleccion = ", ".join(f'{expresion} AS "{renombrar.get(nombre, nombre)}"' for nombre, expresion in columnas.items())
    duckdb.connect().execute(f"COPY (SELECT {seleccion}) TO '{ruta}' (FORMAT PARQUET)")
    return ruta


def esquema_2022():
    # Cambios que trae un archivo de 2022: ids y conteos con otro tipo, airport_fee en minúscula y sin cbd.
    return dict(
        cambios={"VendorID": "1::BIGINT", "passenger_count": "2::DOUBLE", "RatecodeID": "1::DOUBLE",
                 "PULocationID": "100::BIGINT", "DOLocationID": "200::BIGINT"},
        quitar=("cbd_congestion_fee",), renombrar={"Airport_fee": "airport_fee"})


@pytest.fixture
def con():
    conexion = duckdb.connect()
    yield conexion
    conexion.close()


# --- nombres ------------------------------------------------------------------------------------

def test_los_nombres_se_buscan_sin_distinguir_mayusculas():
    real = {"airport_fee": "DOUBLE", "VendorID": "INTEGER"}
    assert esquema.nombre_en_archivo(real, "Airport_fee") == "airport_fee"
    assert esquema.nombre_en_archivo(real, "VendorID") == "VendorID"
    assert esquema.nombre_en_archivo(real, "cbd_congestion_fee") is None


def test_si_estan_las_dos_escrituras_se_prefiere_el_nombre_exacto():
    real = {"Airport_fee": "DOUBLE", "airport_fee": "DOUBLE"}
    assert esquema.nombre_en_archivo(real, "Airport_fee") == "Airport_fee"


def test_un_nombre_ambiguo_es_un_error():
    with pytest.raises(ValueError, match="ambigua"):
        esquema.nombre_en_archivo({"AIRPORT_FEE": "DOUBLE", "airport_fee": "DOUBLE"}, "Airport_fee")


# --- comparación con el contrato ------------------------------------------------------------------

def esquema_del_contrato():
    return {col["origen"]: col["tipo"] for col in CONTRATO["columnas"]}


def test_un_archivo_igual_al_contrato_no_tiene_diferencias():
    reporte = esquema.comparar(CONTRATO, esquema_del_contrato())
    assert all(reporte[clave] == [] for clave in reporte)


def test_la_comparacion_separa_faltantes_nuevas_y_tipos():
    real = esquema_del_contrato()
    del real["Airport_fee"]                    # requerida
    del real["cbd_congestion_fee"]             # opcional
    real["columna_nueva"] = "DOUBLE"
    real["trip_distance"] = "FLOAT"
    reporte = esquema.comparar(CONTRATO, real)
    assert reporte["faltantes_requeridas"] == ["Airport_fee"]
    assert reporte["faltantes_opcionales"] == ["cbd_congestion_fee"]
    assert reporte["nuevas"] == ["columna_nueva"]
    assert reporte["tipos_distintos"] == [("trip_distance", "FLOAT", "DOUBLE")]


def test_falta_una_columna_requerida_es_un_error():
    real = esquema_del_contrato()
    del real["fare_amount"]
    with pytest.raises(ValueError, match="faltan columnas requeridas"):
        esquema.verificar_esquema(CONTRATO, real, "yellow_tripdata_2025-03.parquet")


def test_una_columna_de_otro_nombre_no_se_confunde_con_una_renombrada():
    real = esquema_del_contrato()
    real["airport_surcharge"] = real.pop("Airport_fee")           # un nombre distinto, no solo mayúsculas
    reporte = esquema.comparar(CONTRATO, real)
    assert reporte["faltantes_requeridas"] == ["Airport_fee"]
    assert reporte["nuevas"] == ["airport_surcharge"]


def test_una_conversion_ensanchadora_se_acepta_sin_mirar_los_datos():
    real = esquema_del_contrato()
    real["VendorID"] = "SMALLINT"                                  # SMALLINT -> INTEGER no pierde nada
    esquema.verificar_esquema(CONTRATO, real, "yellow_tripdata_2025-03.parquet")


def test_una_conversion_que_no_es_segura_hace_falta_comprobarla():
    real = esquema_del_contrato()
    real["VendorID"] = "BIGINT"                                    # BIGINT -> INTEGER puede perder datos
    with pytest.raises(ValueError, match="VendorID viene como BIGINT"):
        esquema.verificar_esquema(CONTRATO, real, "yellow_tripdata_2022-07.parquet")
    esquema.verificar_esquema(CONTRATO, real, "yellow_tripdata_2022-07.parquet", sin_perdida=lambda *_: True)
    with pytest.raises(ValueError, match="VendorID viene como BIGINT"):
        esquema.verificar_esquema(CONTRATO, real, "yellow_tripdata_2022-07.parquet", sin_perdida=lambda *_: False)


def test_con_la_politica_en_error_no_se_convierte_nada():
    estricto = {**CONTRATO, "politicas": {**CONTRATO["politicas"], "tipo_distinto": "error"}}
    real = esquema_del_contrato()
    real["VendorID"] = "SMALLINT"
    with pytest.raises(ValueError, match="VendorID viene como SMALLINT"):
        esquema.verificar_esquema(estricto, real, "yellow_tripdata_2025-03.parquet", sin_perdida=lambda *_: True)


# --- archivos reales (Parquet chicos armados para el test) ----------------------------------------

def test_un_archivo_de_2025_se_unifica_con_los_tipos_del_contrato(con, tmp_path):
    ruta = escribir_parquet(tmp_path / "yellow_tripdata_2025-03.parquet")
    esquema.crear_vista_unificada(con, CONTRATO, [ruta])
    tipos = dict(con.execute("SELECT column_name, column_type FROM (DESCRIBE viajes_unificados)").fetchall())
    assert {nombre: tipos[nombre] for nombre in TIPOS_DEL_CONTRATO} == TIPOS_DEL_CONTRATO
    assert tipos["archivo_origen"] == "VARCHAR"


def test_un_archivo_de_2022_con_otros_tipos_y_airport_fee_en_minuscula_se_acepta(con, tmp_path, capsys):
    ruta = escribir_parquet(tmp_path / "yellow_tripdata_2022-07.parquet", **esquema_2022())
    esquema.crear_vista_unificada(con, CONTRATO, [ruta])

    tipos = dict(con.execute("SELECT column_name, column_type FROM (DESCRIBE viajes_unificados)").fetchall())
    assert {nombre: tipos[nombre] for nombre in TIPOS_DEL_CONTRATO} == TIPOS_DEL_CONTRATO
    fila = con.execute("SELECT vendor_id, passenger_count, ratecode_id, pu_location_id, do_location_id, "
                       "airport_fee, cbd_congestion_fee FROM viajes_unificados").fetchone()
    assert fila == (1, 2, 1, 100, 200, 1.25, None)               # los valores no cambian; cbd queda en NULL
    salida = capsys.readouterr().out
    assert "ningún valor cambia" in salida and "airport_fee" in salida


def test_un_nulo_en_una_columna_convertida_sigue_siendo_nulo(con, tmp_path):
    cambios = {**esquema_2022()["cambios"], "passenger_count": "CAST(NULL AS DOUBLE)"}
    ruta = escribir_parquet(tmp_path / "yellow_tripdata_2022-07.parquet", **{**esquema_2022(), "cambios": cambios})
    esquema.crear_vista_unificada(con, CONTRATO, [ruta])
    assert con.execute("SELECT passenger_count FROM viajes_unificados").fetchone() == (None,)


@pytest.mark.parametrize("columna, valor, tipo", [
    ("VendorID", "5000000000", "BIGINT"),          # no entra en un INTEGER
    ("PULocationID", "3000000000", "BIGINT"),
    ("passenger_count", "1.5", "DOUBLE"),          # perdería el decimal al pasar a BIGINT
    ("RatecodeID", "CAST('NaN' AS DOUBLE)", "DOUBLE"),
])
def test_una_conversion_que_cambia_algun_valor_se_rechaza(con, tmp_path, columna, valor, tipo):
    ruta = escribir_parquet(tmp_path / "yellow_tripdata_2022-07.parquet", cambios={columna: valor if "CAST" in valor else f"{valor}::{tipo}"})
    with pytest.raises(ValueError, match=f"{columna} viene como {tipo}"):
        esquema.crear_vista_unificada(con, CONTRATO, [ruta])


def test_si_un_solo_valor_cambia_se_rechaza_toda_la_columna(con, tmp_path):
    ruta = tmp_path / "yellow_tripdata_2022-07.parquet"
    duckdb.connect().execute(f"""
        COPY (SELECT * FROM (VALUES (1::BIGINT), (2::BIGINT), (9000000000::BIGINT)) AS t(VendorID))
        TO '{ruta}' (FORMAT PARQUET)""")
    assert esquema.convierte_sin_perdida(con, ruta, "VendorID", "BIGINT", "INTEGER") is False
    ruta_ok = tmp_path / "ok.parquet"
    duckdb.connect().execute(f"""
        COPY (SELECT * FROM (VALUES (1::BIGINT), (2::BIGINT), (NULL)) AS t(VendorID)) TO '{ruta_ok}' (FORMAT PARQUET)""")
    assert esquema.convierte_sin_perdida(con, ruta_ok, "VendorID", "BIGINT", "INTEGER") is True


def test_una_columna_requerida_ausente_en_el_archivo_corta_la_vista(con, tmp_path):
    ruta = escribir_parquet(tmp_path / "yellow_tripdata_2025-03.parquet", quitar=("fare_amount",))
    with pytest.raises(ValueError, match="faltan columnas requeridas"):
        esquema.crear_vista_unificada(con, CONTRATO, [ruta])
