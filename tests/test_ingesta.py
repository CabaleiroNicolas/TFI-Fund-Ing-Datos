# Tests de la Parte 1 (ingesta): idempotencia, que dlt deje el archivo tal cual, reintentos y log.
# Las descargas se reemplazan por copias de archivos sintéticos; dlt sí corre de verdad.
import shutil
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from src import e1_ingesta as ingesta

URL_2025 = ingesta.armar_url(2025, 3)
URL_2024 = ingesta.armar_url(2024, 3)


def escribir_parquet(ruta, filas=5, con_cbd=True):
    # Un archivo con la forma de los de la TLC: nombres originales, enteros de 32 bits y fechas sin zona horaria.
    inicio = datetime(2025, 3, 1, 10, 0, 0)
    columnas = {
        "VendorID": pa.array([1] * filas, pa.int32()),
        "tpep_pickup_datetime": pa.array([inicio + timedelta(minutes=i) for i in range(filas)], pa.timestamp("us")),
        "tpep_dropoff_datetime": pa.array([inicio + timedelta(minutes=i + 20) for i in range(filas)], pa.timestamp("us")),
        "trip_distance": pa.array([1.5 + i for i in range(filas)], pa.float64()),
        "PULocationID": pa.array([100 + i for i in range(filas)], pa.int32()),
        "Airport_fee": pa.array([0.0] * filas, pa.float64()),
    }
    if con_cbd:
        columnas["cbd_congestion_fee"] = pa.array([0.75] * filas, pa.float64())
    ruta.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(columnas), ruta)


@pytest.fixture
def entorno(tmp_path, monkeypatch):
    # Todo lo que escribe la ingesta va a una carpeta temporal, y la "descarga" copia archivos de 'origen'.
    raw = tmp_path / "data" / "raw"
    monkeypatch.setattr(ingesta, "DATA_RAW", raw)
    monkeypatch.setattr(ingesta, "RUTA_ZONAS", raw / "zones" / "taxi_zone_lookup.csv")
    monkeypatch.setattr(ingesta, "DIR_TMP", tmp_path / "data" / ".tmp")
    monkeypatch.setattr(ingesta, "DIR_ESTADO_DLT", tmp_path / "data" / ".dlt")

    origen = tmp_path / "origen"
    escribir_parquet(origen / "2025.parquet", con_cbd=True)
    escribir_parquet(origen / "2024.parquet", con_cbd=False)
    (origen / "zonas.csv").write_text(
        '"LocationID","Borough","Zone","service_zone"\n'
        '1,"EWR","Newark Airport","EWR"\n'
        '264,"Unknown","N/A","N/A"\n'
        '265,"N/A","Outside of NYC","N/A"\n', encoding="utf-8")
    fuentes = {URL_2025: origen / "2025.parquet", URL_2024: origen / "2024.parquet",
               ingesta.URL_ZONAS: origen / "zonas.csv"}

    descargas = []

    def descarga_falsa(url, destino, reintentos=3, espera_base=2):
        descargas.append(url)
        destino.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(fuentes[url], destino)
        return destino.stat().st_size, ingesta.calcular_hash(destino)

    monkeypatch.setattr(ingesta, "descargar_con_reintentos", descarga_falsa)

    con = ingesta.conectar(tmp_path / "data" / "pipeline.duckdb")
    yield {"con": con, "descargas": descargas, "origen": origen, "raw": raw}
    con.close()


def cargar_mes(entorno, anio, mes=3):
    url = ingesta.armar_url(anio, mes)
    cargar = lambda u, d: ingesta.cargar_viajes(u, d, anio, mes)
    return ingesta.ingestar_archivo(entorno["con"], url, ingesta.armar_ruta(anio, mes), cargar)


def filas_del_log(entorno):
    return entorno["con"].execute(
        "SELECT archivo, filas, estado, mensaje FROM ingestion_log ORDER BY fecha_hora").fetchall()


# --- carga y registro ---------------------------------------------------------------------------------

def test_la_primera_carga_deja_el_archivo_en_su_carpeta_y_lo_registra(entorno):
    assert cargar_mes(entorno, 2025) == "descargado"

    destino = ingesta.armar_ruta(2025, 3)
    assert destino == entorno["raw"] / "yellow" / "year=2025" / "month=03" / "yellow_tripdata_2025-03.parquet"
    assert destino.exists()
    [(archivo, filas, estado, mensaje)] = filas_del_log(entorno)
    assert (archivo, filas, estado) == ("yellow_tripdata_2025-03.parquet", 5, "ok")
    assert mensaje.startswith("cargado con dlt")


def test_dlt_deja_los_datos_tal_como_venian(entorno):
    cargar_mes(entorno, 2025)
    original = pq.read_table(entorno["origen"] / "2025.parquet")
    cargado = pq.read_table(ingesta.armar_ruta(2025, 3))
    # mismas columnas con sus nombres originales, mismos tipos (fechas sin zona horaria) y mismos valores
    assert cargado.schema.equals(original.schema)
    assert cargado.equals(original)


def test_cada_archivo_conserva_su_propio_esquema(entorno):
    # dlt acumula el esquema de lo que carga: sin aislar los archivos, el de 2024 salía con una
    # columna cbd_congestion_fee inventada y llena de nulos.
    cargar_mes(entorno, 2025)
    cargar_mes(entorno, 2024)
    assert "cbd_congestion_fee" in pq.read_schema(ingesta.armar_ruta(2025, 3)).names
    assert "cbd_congestion_fee" not in pq.read_schema(ingesta.armar_ruta(2024, 3)).names


def test_la_tabla_de_zonas_conserva_los_valores_n_a(entorno):
    ingesta.ingestar_archivo(entorno["con"], ingesta.URL_ZONAS, ingesta.RUTA_ZONAS, ingesta.cargar_zonas)
    lineas = ingesta.RUTA_ZONAS.read_text(encoding="utf-8").splitlines()
    assert lineas[0] == '"LocationID","Borough","Zone","service_zone"'
    assert any(l.startswith("264,") and "N/A" in l for l in lineas)         # 'N/A' sigue siendo texto, no nulo
    assert ingesta.contar_filas(entorno["con"], ingesta.RUTA_ZONAS) == 3


# --- idempotencia ---------------------------------------------------------------------------------------

def test_correr_dos_veces_no_vuelve_a_cargar_ni_duplica(entorno):
    cargar_mes(entorno, 2025)
    destino = ingesta.armar_ruta(2025, 3)
    hash_antes = ingesta.calcular_hash(destino)

    assert cargar_mes(entorno, 2025) == "omitido"

    assert entorno["descargas"] == [URL_2025]                                # no se volvió a bajar
    assert ingesta.calcular_hash(destino) == hash_antes                      # el archivo no se tocó
    assert [p.name for p in destino.parent.iterdir()] == [destino.name]      # y no hay un segundo archivo
    assert ingesta.contar_filas(entorno["con"], destino) == 5                # mismos datos, sin duplicar
    mensajes = [fila[3] for fila in filas_del_log(entorno)]
    assert len(mensajes) == 2 and mensajes[1].startswith("omitido")          # el log registra las dos corridas


def test_si_el_archivo_local_cambio_se_vuelve_a_cargar(entorno):
    cargar_mes(entorno, 2025)
    destino = ingesta.armar_ruta(2025, 3)
    escribir_parquet(destino, filas=2)                                       # alguien lo modifica a mano

    assert cargar_mes(entorno, 2025) == "descargado"

    assert ingesta.contar_filas(entorno["con"], destino) == 5                # volvió a quedar como el original
    assert "reemplazó un archivo previo" in filas_del_log(entorno)[-1][3]


def test_un_archivo_sin_fila_ok_en_el_log_no_cuenta_como_cargado(entorno):
    # Una corrida cortada a la mitad puede dejar el archivo sin haber llegado a registrarlo.
    destino = ingesta.armar_ruta(2025, 3)
    escribir_parquet(destino, filas=2)
    assert cargar_mes(entorno, 2025) == "descargado"
    assert ingesta.contar_filas(entorno["con"], destino) == 5


# --- fallas -----------------------------------------------------------------------------------------------

def test_si_la_carga_falla_queda_un_error_en_el_log_y_se_propaga(entorno, monkeypatch):
    def descarga_rota(url, destino, reintentos=3, espera_base=2):
        raise requests.ConnectionError("sin red")

    monkeypatch.setattr(ingesta, "descargar_con_reintentos", descarga_rota)
    with pytest.raises(requests.ConnectionError):
        cargar_mes(entorno, 2025)

    [(archivo, filas, estado, mensaje)] = filas_del_log(entorno)
    assert (archivo, filas, estado) == ("yellow_tripdata_2025-03.parquet", None, "error")
    assert "sin red" in mensaje
    assert not ingesta.armar_ruta(2025, 3).exists()


def test_si_las_filas_cargadas_no_coinciden_con_las_de_origen_falla(entorno, monkeypatch):
    original = ingesta.cargar_viajes

    def cargar_y_perder_filas(url, destino, anio, mes):
        info = original(url, destino, anio, mes)
        info["filas_fuente"] += 1                                            # el origen "tenía" una fila más
        return info

    monkeypatch.setattr(ingesta, "cargar_viajes", cargar_y_perder_filas)
    with pytest.raises(ValueError, match="distintas de las cargadas"):
        cargar_mes(entorno, 2025)
    assert filas_del_log(entorno)[-1][2] == "error"


# --- reintentos y parámetros -------------------------------------------------------------------------------

def error_http(codigo):
    respuesta = requests.Response()
    respuesta.status_code = codigo
    return requests.HTTPError(response=respuesta)


def test_reintenta_los_errores_pasajeros_con_espera_creciente(monkeypatch, tmp_path):
    intentos, esperas = [], []

    def descargar_inestable(url, destino):
        intentos.append(1)
        if len(intentos) < 3:
            raise requests.ConnectionError("corte")
        return 10, "abc"

    monkeypatch.setattr(ingesta, "descargar", descargar_inestable)
    monkeypatch.setattr(ingesta.time, "sleep", esperas.append)

    assert ingesta.descargar_con_reintentos("http://x", tmp_path / "a") == (10, "abc")
    assert esperas == [2, 4]


def test_si_sigue_fallando_se_rinde_despues_de_cuatro_intentos(monkeypatch, tmp_path):
    intentos, esperas = [], []

    def siempre_falla(url, destino):
        intentos.append(1)
        raise error_http(503)

    monkeypatch.setattr(ingesta, "descargar", siempre_falla)
    monkeypatch.setattr(ingesta.time, "sleep", esperas.append)

    with pytest.raises(requests.HTTPError):
        ingesta.descargar_con_reintentos("http://x", tmp_path / "a")
    assert len(intentos) == 4 and esperas == [2, 4, 8]


def test_un_error_4xx_no_se_reintenta(monkeypatch, tmp_path):
    intentos = []

    def no_existe(url, destino):
        intentos.append(1)
        raise error_http(403)

    monkeypatch.setattr(ingesta, "descargar", no_existe)
    monkeypatch.setattr(ingesta.time, "sleep", lambda s: pytest.fail("no debería esperar"))

    with pytest.raises(requests.HTTPError):
        ingesta.descargar_con_reintentos("http://x", tmp_path / "a")
    assert len(intentos) == 1


@pytest.mark.parametrize("texto, esperado", [("2025-03", (2025, 3)), ("2024-12", (2024, 12))])
def test_parsear_mes_acepta_el_formato_correcto(texto, esperado):
    assert ingesta.parsear_mes(texto) == esperado


@pytest.mark.parametrize("texto", ["2025-13", "2025-00", "2025", "marzo", "2025/03", ""])
def test_parsear_mes_rechaza_formatos_invalidos(texto):
    with pytest.raises(Exception, match="mes inválido"):
        ingesta.parsear_mes(texto)