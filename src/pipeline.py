# Orquesta las etapas del pipeline para un mes: ingesta, esquema, calidad, curado, modelo y preguntas.
# Cada etapa devuelve un diccionario con lo que procesó; src/metricas.py lo suma a lo que mide (tiempo, CPU, memoria).
from src.e1_ingesta import armar_ruta, conectar, contar_filas, ingestar
from src.e2_esquema import cargar_contrato, crear_vista_unificada
from src.e3_calidad import cargar_reglas, consulta_del_mes, crear_vista_zonas, miles, procesar_calidad
from src.e4_transformacion import curar_mes, ruta_curado
from src.e5_modelo import construir_modelo, mostrar_resumen
from src.e6_preguntas import correr_preguntas, guardar_informe, mostrar_resultados
from src.metricas import Corrida, a_mb

LIMITE_MEMORIA = "4GB"


def meses_a_procesar(anio: int, mes: int) -> list:
    # El mes pedido y el mismo mes del año anterior: [(2024, 3), (2025, 3)].
    return [(anio - 1, mes), (anio, mes)]


def etapa_ingesta(con, anio: int, mes: int) -> dict:
    resultados = ingestar(con, anio, mes)
    archivos = [archivo for archivo, _ in resultados]
    # Filas y tamaño de cada archivo, según su último registro 'ok' en ingestion_log.
    filas, tamano = con.execute(f"""
        SELECT sum(filas), sum(tamano_bytes)
        FROM (SELECT arg_max(filas, fecha_hora) AS filas, arg_max(tamano_bytes, fecha_hora) AS tamano_bytes
              FROM ingestion_log
              WHERE estado = 'ok' AND archivo IN ({', '.join('?' * len(archivos))})
              GROUP BY archivo)
    """, archivos).fetchall()[0]
    return {"filas": filas or 0, "archivos": len(resultados),
            "descargados": sum(estado == "descargado" for _, estado in resultados),
            "omitidos": sum(estado == "omitido" for _, estado in resultados),
            "mb_en_disco": a_mb(tamano or 0)}


def etapa_esquema(con, anio: int, mes: int) -> dict:
    # Verifica los archivos contra el contrato y deja creada la vista 'viajes_unificados'.
    rutas = [armar_ruta(a, m) for a, m in meses_a_procesar(anio, mes)]
    crear_vista_unificada(con, cargar_contrato(), rutas)
    return {"archivos": len(rutas)}


def etapa_calidad(con, anio: int, mes: int) -> dict:
    # Lee de la vista 'viajes_unificados' y reparte cada mes en 'aceptadas' y 'cuarentena'.
    contrato, reglas = cargar_contrato(), cargar_reglas()
    crear_vista_zonas(con)
    archivos = []
    for a, m in meses_a_procesar(anio, mes):
        ruta = armar_ruta(a, m)
        procesar_calidad(con, contrato, reglas, consulta_del_mes(ruta.name),
                         contar_filas(con, ruta), ruta.name, a, m)
        archivos.append(ruta.name)
    leidas, aceptadas, cuarentena, advertencia = con.execute(
        "SELECT sum(filas_leidas), sum(filas_aceptadas), sum(filas_cuarentena), sum(filas_con_advertencia) "
        "FROM reporte_resumen WHERE archivo_origen IN (?, ?)", archivos).fetchall()[0]
    return {"filas": leidas, "aceptadas": aceptadas, "cuarentena": cuarentena, "con_advertencia": advertencia}


def etapa_curado(con, anio: int, mes: int) -> dict:
    # Escribe una partición Parquet por mes, a partir de las filas aceptadas.
    crear_vista_zonas(con)
    total, tamano = 0, 0
    for a, m in meses_a_procesar(anio, mes):
        filas = curar_mes(con, a, m)
        print(f"curado {a}-{m:02d}: {miles(filas)} filas")
        total += filas
        tamano += ruta_curado(a, m).stat().st_size
    return {"filas": total, "particiones": len(meses_a_procesar(anio, mes)), "mb_en_disco": a_mb(tamano)}


def etapa_modelo(con, anio: int, mes: int) -> dict:
    # Construye el modelo estrella con dbt sobre toda la capa curada (no depende del mes pedido).
    nodos = construir_modelo()
    mostrar_resumen(nodos)
    modelos = [n for n in nodos if n["tipo"] == "model"]
    tests = [n for n in nodos if n["tipo"] == "test"]
    # Si algún test de dbt falla, construir_modelo levanta una excepción: si llegamos acá, pasaron todos.
    return {"filas": sum(n["filas"] for n in modelos), "modelos": len(modelos), "tests_dbt": len(tests)}


def etapa_preguntas(con, anio: int, mes: int) -> dict:
    # Responde las preguntas de negocio sobre el modelo para el mes pedido y su mes de comparación.
    resultados = correr_preguntas(anio, mes)
    mostrar_resultados(resultados)
    print(f"\n   informe guardado en {guardar_informe(resultados, anio, mes)}")
    return {"filas": sum(len(r["filas"]) for r in resultados), "consultas": len(resultados)}


# (nombre, función, usa la base del pipeline). dbt y las preguntas abren sus propias bases.
ETAPAS = [
    ("ingesta", etapa_ingesta, True),
    ("esquema", etapa_esquema, True),
    ("calidad", etapa_calidad, True),
    ("curado", etapa_curado, True),
    ("modelo", etapa_modelo, False),
    ("preguntas", etapa_preguntas, False),
]


class Sesion:
    # Una corrida de un mes: la base del pipeline, la medición (src/metricas.py) y las reglas de cuándo se cierra cada cosa.
    # La usan las dos formas de correr el pipeline: ejecutar() (directo) y los assets de Dagster (src/orquestacion.py).
    def __init__(self, anio: int, mes: int):
        self.anio, self.mes = anio, mes
        self.corrida = Corrida(anio, mes)
        self.con = None
        self.error = None

    def abrir(self) -> None:
        self.corrida.iniciar()
        self.con = conectar()
        self.con.execute(f"SET memory_limit = '{LIMITE_MEMORIA}'")

    def correr_etapa(self, nombre: str, etapa, usa_base: bool) -> dict:
        # Corre una etapa midiéndola. Devuelve lo que midió y lo que la etapa procesó. Si falla, el error dice cuál fue.
        if self.con is not None and not usa_base:
            # Aunque ya no se use, la conexión retiene gigas de memoria (medido: unos 2,4 GB después de calidad y
            # curado). dbt arranca ahora, y si la conexión sigue abierta las dos memorias se suman.
            self.con.close()
            self.con = None
        print(f"== etapa: {nombre} ==")
        with self.corrida.etapa(nombre) as medidor:
            try:
                medidor.datos = etapa(self.con, self.anio, self.mes)
            except Exception as error:
                self.error = RuntimeError(f"falló la etapa '{nombre}': {error}")
                raise self.error from error
            except BaseException as error:       # Ctrl+C y similares: no se envuelven, pero la corrida cuenta como fallida
                self.error = error
                raise
        return medidor.resultado

    def cerrar(self) -> dict:
        # Cierra la base y deja el resumen y el evento final, con el error si alguna etapa falló.
        if self.con is not None:
            self.con.close()
            self.con = None
        return self.corrida.terminar(self.error)


def ejecutar(anio: int, mes: int) -> None:
    # Corre las etapas en orden. Si una falla, se corta ahí y el error dice cuál fue.
    # La corrida deja sus métricas en el log (ver src/metricas.py) aunque falle.
    sesion = Sesion(anio, mes)
    sesion.abrir()
    try:
        for nombre, etapa, usa_base in ETAPAS:
            sesion.correr_etapa(nombre, etapa, usa_base)
    finally:
        total = sesion.cerrar()
    print(f"Listo en {total['duracion_s']:.1f} s. Pico de RAM: {total['pico_ram_mb']} MB")
