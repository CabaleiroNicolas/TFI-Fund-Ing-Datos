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



def nombre_en_archivo(real: dict, origen: str):
    # Nombre con el que viene la columna en el archivo, o None si no está.
    # TLC cambió mayúsculas entre años (airport_fee en 2022, Airport_fee desde 2023): se busca
    # primero el nombre exacto y, si no está, uno que solo difiera en mayúsculas.
    if origen in real:
        return origen
    parecidos = [nombre for nombre in real if nombre.lower() == origen.lower()]
    if len(parecidos) > 1:
        raise ValueError(f"la columna {origen} es ambigua en el archivo: {parecidos}")
    return parecidos[0] if parecidos else None


def comparar(contrato: dict, real: dict) -> dict:
    # Compara contrato y archivo. Solo describe diferencias, no decide nada.
    reporte = {
        "faltantes_requeridas": [],
        "faltantes_opcionales": [],
        "nuevas": [],
        "tipos_distintos": [],     # (columna del archivo, tipo en el archivo, tipo en el contrato)
        "renombradas": [],         # (nombre en el contrato, nombre en el archivo): solo cambian las mayúsculas
    }
    usadas = set()
    for col in contrato["columnas"]:
        origen = col["origen"]
        nombre = nombre_en_archivo(real, origen)
        if nombre is None:
            clave = "faltantes_requeridas" if col["requerida"] else "faltantes_opcionales"
            reporte[clave].append(origen)
            continue
        usadas.add(nombre)
        if nombre != origen:
            reporte["renombradas"].append((origen, nombre))
        if real[nombre] != col["tipo"]:
            reporte["tipos_distintos"].append((nombre, real[nombre], col["tipo"]))
    reporte["nuevas"] = [nombre for nombre in real if nombre not in usadas]
    return reporte


def verificar_esquema(contrato: dict, real: dict, archivo: str, sin_perdida=None) -> dict:
    # Compara, aplica las políticas del contrato y devuelve el reporte.
    # Lanza ValueError si el esquema no se puede aceptar.
    # sin_perdida(columna, tipo_archivo, tipo_contrato) -> bool comprueba contra los valores del archivo
    # que una conversión que no está en CASTEOS_SEGUROS no altera ninguno (ej. BIGINT -> INTEGER).
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

    for origen, nombre in reporte["renombradas"]:
        print(f"  la columna {origen} del contrato viene como {nombre} en el archivo: se usa igual")

    for nombre, tipo_real, tipo_contrato in reporte["tipos_distintos"]:
        permitido = politicas["tipo_distinto"] != "error"
        if permitido and (tipo_real, tipo_contrato) in CASTEOS_SEGUROS:
            print(f"  se convierte {nombre}: {tipo_real} -> {tipo_contrato}")
        elif permitido and sin_perdida is not None and sin_perdida(nombre, tipo_real, tipo_contrato):
            print(f"  se convierte {nombre}: {tipo_real} -> {tipo_contrato} (comprobado: ningún valor cambia)")
        else:
            errores.append(f"{nombre} viene como {tipo_real} y el contrato pide {tipo_contrato}")

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
        nombre = nombre_en_archivo(real, col["origen"])
        if nombre is not None:
            expresion = f'CAST("{nombre}" AS {col["tipo"]})'
        else:
            expresion = f'CAST({literal_sql(col.get("default_si_falta"))} AS {col["tipo"]})'
        columnas.append(f'    {expresion} AS {col["nombre"]}')
    # Columna de trazabilidad: no es del contrato, la usa la cuarentena en la Parte 3.
    columnas.append(f"    {literal_sql(ruta.name)} AS archivo_origen")

    ruta_sql = str(ruta).replace("'", "''")
    return ("SELECT\n" + ",\n".join(columnas) +
            f"\nFROM read_parquet('{ruta_sql}', hive_partitioning = false)")


def convierte_sin_perdida(con, ruta: Path, columna: str, tipo_archivo: str, tipo_contrato: str) -> bool:
    # True si convertir la columna al tipo del contrato no deja ningún valor distinto: ni uno que no
    # entre en el tipo nuevo ni uno que cambie al convertirlo (por ejemplo 1.5 -> 1).
    # Los nulos no cuentan, porque siguen siendo nulos.
    ruta_sql = str(ruta).replace("'", "''")
    try:
        alterados = con.execute(f"""
            SELECT count(*)
            FROM read_parquet('{ruta_sql}', hive_partitioning = false)
            WHERE "{columna}" IS NOT NULL
              AND (TRY_CAST("{columna}" AS {tipo_contrato}) IS NULL
                   OR CAST(TRY_CAST("{columna}" AS {tipo_contrato}) AS {tipo_archivo}) <> "{columna}")
        """).fetchone()[0]
    except Exception:       # un tipo que DuckDB no sabe comparar o convertir no se acepta
        return False
    return alterados == 0


def crear_vista_unificada(con, contrato: dict, rutas: list) -> None:
    # Verifica cada archivo contra el contrato y crea la vista viajes_unificados.
    selects = []
    for ruta in rutas:
        real = leer_esquema_real(con, ruta)
        verificar_esquema(contrato, real, ruta.name,
                          lambda columna, tipo_real, tipo_contrato, ruta=ruta:
                          convierte_sin_perdida(con, ruta, columna, tipo_real, tipo_contrato))
        selects.append(construir_select(contrato, real, ruta))
    sql = "CREATE OR REPLACE VIEW viajes_unificados AS\n" + "\nUNION ALL\n".join(selects)
    con.execute(sql)