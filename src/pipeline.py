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


def ejecutar(anio: int, mes: int) -> None:
    # Corre las etapas en orden. Si una falla, se corta ahí y el error dice cuál fue.
    # La corrida deja sus métricas en el log (ver src/metricas.py) aunque falle.
    corrida = Corrida(anio, mes)
    corrida.iniciar()
    con = conectar()
    con.execute(f"SET memory_limit = '{LIMITE_MEMORIA}'")
    error = None
    try:
        for nombre, etapa, usa_base in ETAPAS:
            if con is not None and not usa_base:
                # Aunque ya no se use, la conexión retiene gigas de memoria (medido: unos 2,4 GB después de calidad y
                # curado). dbt arranca ahora, y si la conexión sigue abierta las dos memorias se suman.
                con.close()
                con = None
            print(f"== etapa: {nombre} ==")
            with corrida.etapa(nombre) as medidor:
                try:
                    medidor.datos = etapa(con, anio, mes)
                except Exception as e:
                    raise RuntimeError(f"falló la etapa '{nombre}': {e}") from e
    except BaseException as e:
        error = e
        raise
    finally:
        if con is not None:
            con.close()
        total = corrida.terminar(error)
    print(f"Listo en {total['duracion_s']:.1f} s. Pico de RAM: {total['pico_ram_mb']} MB")
