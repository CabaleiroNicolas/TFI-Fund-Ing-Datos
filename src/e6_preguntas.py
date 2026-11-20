# Etapa 6: responde las preguntas de negocio con las consultas de sql/preguntas/ (una por archivo) sobre el modelo.
from pathlib import Path

import duckdb

from src.config import RAIZ

MIN_VIAJES_ZONA = 1000     # c2: una zona con menos viajes en el mes no entra al ranking de proporciones


def correr_preguntas(anio: int, mes: int, raiz: Path = RAIZ, min_viajes_zona: int = MIN_VIAJES_ZONA) -> list:
    # Cada consulta recibe el mes asignado ($anio, $mes) y compara contra el mismo mes de $anio - 1.
    # La base del modelo se abre solo para lectura. Una pregunta sin resultados es un error, no una respuesta vacía.
    parametros = {"anio": anio, "mes": mes, "min_viajes_zona": min_viajes_zona}
    con = duckdb.connect(str(raiz / "data" / "modelo.duckdb"), read_only=True)
    resultados = []
    try:
        archivos = sorted((raiz / "sql" / "preguntas").glob("*.sql"))
        if not archivos:
            raise RuntimeError(f"no hay consultas en {raiz / 'sql' / 'preguntas'}")
        for archivo in archivos:
            sql = archivo.read_text(encoding="utf-8")
            nombres = con.extract_statements(sql)[0].named_parameters         # DuckDB rechaza parámetros que sobran
            usados = {nombre: parametros[nombre] for nombre in nombres}
            cursor = con.execute(sql, usados)
            columnas = [columna[0] for columna in cursor.description]
            filas = cursor.fetchall()
            if not filas:
                raise RuntimeError(f"la pregunta {archivo.name} no devolvió filas para {anio}-{mes:02d}")
            resultados.append({"archivo": archivo.name, "titulo": sql.splitlines()[0].removeprefix("-- "),
                               "sql": sql, "parametros": usados, "columnas": columnas, "filas": filas})
    finally:
        con.close()
    return resultados


def tabla_markdown(columnas: list, filas: list) -> str:
    lineas = ["| " + " | ".join(columnas) + " |", "|" + "---|" * len(columnas)]
    lineas += ["| " + " | ".join(str(valor) for valor in fila) + " |" for fila in filas]
    return "\n".join(lineas)


def mostrar_resultados(resultados: list) -> None:
    for r in resultados:
        print(f"\n{r['titulo']}")
        print(tabla_markdown(r["columnas"], r["filas"]))


def guardar_informe(resultados: list, anio: int, mes: int, raiz: Path = RAIZ) -> Path:
    # Un informe por mes pedido, con cada consulta y su resultado. Es el material de RESULTADOS.md.
    ruta = raiz / "data" / "resultados" / f"{anio}-{mes:02d}.md"
    ruta.parent.mkdir(parents=True, exist_ok=True)
    parametros = {nombre: valor for r in resultados for nombre, valor in r["parametros"].items()}
    detalle = ", ".join(f"`${nombre}` = {valor}" for nombre, valor in sorted(parametros.items()))
    partes = [f"# Preguntas de negocio: {anio}-{mes:02d} contra {anio - 1}-{mes:02d}\n\nParámetros de las consultas: {detalle}.\n"]
    for r in resultados:
        partes.append(f"## {r['titulo']}\n\n```sql\n{r['sql'].rstrip()}\n```\n\n{tabla_markdown(r['columnas'], r['filas'])}\n")
    ruta.write_text("\n".join(partes), encoding="utf-8")
    return ruta
