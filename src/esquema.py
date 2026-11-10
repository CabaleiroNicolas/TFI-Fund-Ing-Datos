# Esquema: lee el contrato, lo valida y lo compara contra los archivos reales.
import yaml
from pathlib import Path

from src.config import RUTA_CONTRATO

TIPOS_VALIDOS = {"INTEGER", "BIGINT", "DOUBLE", "VARCHAR", "TIMESTAMP"}

POLITICAS_VALIDAS = {
    "columna_faltante_requerida": {"error"},
    "columna_faltante_opcional": {"usar_default"},
    "columna_nueva_no_contratada": {"ignorar", "advertir_e_ignorar", "error"},
    "tipo_distinto": {"castear_si_es_seguro_sino_error", "error"},
}

# Conversiones que no pierden información: (tipo del archivo, tipo del contrato).
CASTEOS_SEGUROS = {
    ("SMALLINT", "INTEGER"), ("SMALLINT", "BIGINT"), ("INTEGER", "BIGINT"),
    ("INTEGER", "DOUBLE"), ("FLOAT", "DOUBLE"),
}


def cargar_contrato(ruta=RUTA_CONTRATO) -> dict:
    with open(ruta, encoding="utf-8") as f:
        contrato = yaml.safe_load(f)
    validar_contrato(contrato)
    return contrato


def validar_contrato(contrato: dict) -> None:
    # Revisa que el contrato esté bien armado. Si no, ValueError con el motivo.
    if not isinstance(contrato, dict):
        raise ValueError("contrato: el archivo está vacío o mal formado")

    for clave in ("version", "politicas", "columnas"):
        if clave not in contrato:
            raise ValueError(f"contrato: falta la clave '{clave}'")

    for politica, permitidos in POLITICAS_VALIDAS.items():
        valor = contrato["politicas"].get(politica)
        if valor not in permitidos:
            raise ValueError(
                f"contrato: la política '{politica}' vale {valor!r}, "
                f"valores posibles: {sorted(permitidos)}"
            )

    nombres = set()
    for col in contrato["columnas"]:
        for campo in ("nombre", "origen", "tipo", "requerida"):
            if campo not in col:
                raise ValueError(f"contrato: a la columna {col.get('nombre', '?')} le falta '{campo}'")
        if col["nombre"] in nombres:
            raise ValueError(f"contrato: columna repetida '{col['nombre']}'")
        nombres.add(col["nombre"])
        if col["tipo"] not in TIPOS_VALIDOS:
            raise ValueError(
                f"contrato: '{col['nombre']}' tiene tipo '{col['tipo']}', "
                f"valores posibles: {sorted(TIPOS_VALIDOS)}"
            )
        if not col["requerida"] and "default_si_falta" not in col:
            raise ValueError(f"contrato: '{col['nombre']}' es opcional y no define default_si_falta")


def leer_esquema_real(con, ruta: Path) -> dict:
    # Devuelve {nombre_de_columna: tipo} tal como viene en el archivo.
    filas = con.execute(
        "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning = false)",
        [str(ruta)],
    ).fetchall()
    return {fila[0]: fila[1] for fila in filas}



def comparar(contrato: dict, real: dict) -> dict:
    # Compara contrato y archivo. Solo describe diferencias, no decide nada.
    origenes = {col["origen"] for col in contrato["columnas"]}
    reporte = {
        "faltantes_requeridas": [],
        "faltantes_opcionales": [],
        "nuevas": [nombre for nombre in real if nombre not in origenes],
        "tipos_distintos": [],     # (columna, tipo en el archivo, tipo en el contrato)
    }
    for col in contrato["columnas"]:
        origen = col["origen"]
        if origen not in real:
            clave = "faltantes_requeridas" if col["requerida"] else "faltantes_opcionales"
            reporte[clave].append(origen)
        elif real[origen] != col["tipo"]:
            reporte["tipos_distintos"].append((origen, real[origen], col["tipo"]))
    return reporte


def verificar_esquema(contrato: dict, real: dict, archivo: str) -> dict:
    # Compara, aplica las políticas del contrato y devuelve el reporte.
    # Lanza ValueError si el esquema no se puede aceptar.
    politicas = contrato["politicas"]
    reporte = comparar(contrato, real)
    errores = []

    print(f"[esquema] {archivo}: {len(real)} columnas | "
          f"faltantes requeridas: {len(reporte['faltantes_requeridas'])} | "
          f"faltantes opcionales: {len(reporte['faltantes_opcionales'])} | "
          f"nuevas: {len(reporte['nuevas'])} | "
          f"tipos distintos: {len(reporte['tipos_distintos'])}")

    if reporte["faltantes_requeridas"]:
        errores.append(f"faltan columnas requeridas: {reporte['faltantes_requeridas']}")

    if reporte["faltantes_opcionales"]:
        print(f"  se usa el valor por defecto del contrato para: {reporte['faltantes_opcionales']}")

    for origen, tipo_real, tipo_contrato in reporte["tipos_distintos"]:
        seguro = (tipo_real, tipo_contrato) in CASTEOS_SEGUROS
        if seguro and politicas["tipo_distinto"] != "error":
            print(f"  se convierte {origen}: {tipo_real} -> {tipo_contrato}")
        else:
            errores.append(f"{origen} viene como {tipo_real} y el contrato pide {tipo_contrato}")

    if reporte["nuevas"]:
        if politicas["columna_nueva_no_contratada"] == "error":
            errores.append(f"columnas nuevas no contratadas: {reporte['nuevas']}")
        elif politicas["columna_nueva_no_contratada"] == "advertir_e_ignorar":
            print(f"  ADVERTENCIA: columnas nuevas no contratadas, se ignoran: {reporte['nuevas']}")

    if errores:
        raise ValueError(f"el esquema de {archivo} no cumple el contrato: " + "; ".join(errores))
    return reporte



def literal_sql(valor) -> str:
    # Convierte un valor de Python en su forma escrita en SQL.
    if valor is None:
        return "NULL"
    if isinstance(valor, str):
        return "'" + valor.replace("'", "''") + "'"
    return str(valor)


def construir_select(contrato: dict, real: dict, ruta: Path) -> str:
    # Arma el SELECT que lee un parquet y lo deja con los nombres y tipos del contrato.
    columnas = []
    for col in contrato["columnas"]:
        if col["origen"] in real:
            expresion = f'CAST("{col["origen"]}" AS {col["tipo"]})'
        else:
            expresion = f'CAST({literal_sql(col.get("default_si_falta"))} AS {col["tipo"]})'
        columnas.append(f'    {expresion} AS {col["nombre"]}')
    # Columna de trazabilidad: no es del contrato, la usa la cuarentena en la Parte 3.
    columnas.append(f"    {literal_sql(ruta.name)} AS archivo_origen")

    ruta_sql = str(ruta).replace("'", "''")
    return ("SELECT\n" + ",\n".join(columnas) +
            f"\nFROM read_parquet('{ruta_sql}', hive_partitioning = false)")


def crear_vista_unificada(con, contrato: dict, rutas: list) -> None:
    # Verifica cada archivo contra el contrato y crea la vista viajes_unificados.
    selects = []
    for ruta in rutas:
        real = leer_esquema_real(con, ruta)
        verificar_esquema(contrato, real, ruta.name)
        selects.append(construir_select(contrato, real, ruta))
    sql = "CREATE OR REPLACE VIEW viajes_unificados AS\n" + "\nUNION ALL\n".join(selects)
    con.execute(sql)