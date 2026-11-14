# Orquesta las etapas del pipeline para un mes: ingesta, esquema, calidad y curado.
import resource
import sys
import time

from src.e1_ingesta import armar_ruta, conectar, contar_filas, ingestar
from src.e2_esquema import cargar_contrato, crear_vista_unificada
from src.e3_calidad import cargar_reglas, consulta_del_mes, crear_vista_zonas, miles, procesar_calidad
from src.e4_transformacion import curar_mes

LIMITE_MEMORIA = "4GB"


def meses_a_procesar(anio: int, mes: int) -> list:
    # El mes pedido y el mismo mes del año anterior: [(2024, 3), (2025, 3)].
    return [(anio - 1, mes), (anio, mes)]


def etapa_ingesta(con, anio: int, mes: int) -> None:
    ingestar(con, anio, mes)


def etapa_esquema(con, anio: int, mes: int) -> None:
    # Verifica los archivos contra el contrato y deja creada la vista 'viajes_unificados'.
    rutas = [armar_ruta(a, m) for a, m in meses_a_procesar(anio, mes)]
    crear_vista_unificada(con, cargar_contrato(), rutas)


def etapa_calidad(con, anio: int, mes: int) -> None:
    # Lee de la vista 'viajes_unificados' y reparte cada mes en 'aceptadas' y 'cuarentena'.
    contrato, reglas = cargar_contrato(), cargar_reglas()
    crear_vista_zonas(con)
    for a, m in meses_a_procesar(anio, mes):
        ruta = armar_ruta(a, m)
        procesar_calidad(con, contrato, reglas, consulta_del_mes(ruta.name),
                         contar_filas(con, ruta), ruta.name, a, m)


def etapa_curado(con, anio: int, mes: int) -> None:
    # Escribe una partición Parquet por mes, a partir de las filas aceptadas.
    crear_vista_zonas(con)
    for a, m in meses_a_procesar(anio, mes):
        filas = curar_mes(con, a, m)
        print(f"curado {a}-{m:02d}: {miles(filas)} filas")


ETAPAS = [
    ("ingesta", etapa_ingesta),
    ("esquema", etapa_esquema),
    ("calidad", etapa_calidad),
    ("curado", etapa_curado),
]


def pico_ram_mb() -> float:
    pico = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return pico / (1024 * 1024) if sys.platform == "darwin" else pico / 1024   # macOS lo da en bytes, Linux en KB


def ejecutar(anio: int, mes: int) -> None:
    # Corre las etapas en orden. Si una falla, se corta ahí y el error dice cuál fue.
    inicio = time.time()
    con = conectar()
    con.execute(f"SET memory_limit = '{LIMITE_MEMORIA}'")
    try:
        for nombre, etapa in ETAPAS:
            print(f"== etapa: {nombre} ==")
            t0 = time.time()
            try:
                etapa(con, anio, mes)
            except Exception as error:
                raise RuntimeError(f"falló la etapa '{nombre}': {error}") from error
            print(f"   {nombre}: {time.time() - t0:.1f} s")
    finally:
        con.close()
    print(f"Listo en {time.time() - inicio:.1f} s. Pico de RAM: {pico_ram_mb():.0f} MB")