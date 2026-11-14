# Calidad: carga las reglas, evalúa cada fila y cuenta las violaciones por regla.
from datetime import datetime
import yaml

from src.config import RUTA_REGLAS, RUTA_ZONAS

SEVERIDADES = {"rechazo", "advertencia"}
CAMPOS_REGLA = ("id", "descripcion", "severidad", "condicion", "justificacion")


def cargar_reglas(ruta=RUTA_REGLAS) -> list:
    with open(ruta, encoding="utf-8") as f:
        datos = yaml.safe_load(f)
    validar_reglas(datos)
    return datos["reglas"]


def parametros(regla: dict, inicio: str, fin: str, columnas: str) -> dict:
    # Valores con los que se completan los marcadores {..} de la condición.
    umbral = regla.get("umbral")
    return {
        "inicio_mes": inicio,
        "fin_mes": fin,
        "columnas": columnas,
        "umbral": "NULL" if umbral is None else str(umbral),
    }


def validar_reglas(datos: dict) -> None:
    # Revisa que el archivo de reglas esté bien armado. Si no, ValueError con el motivo.
    if not isinstance(datos, dict) or not isinstance(datos.get("reglas"), list):
        raise ValueError("reglas: el archivo no tiene una lista 'reglas'")

    ids = set()
    for regla in datos["reglas"]:
        for campo in CAMPOS_REGLA:
            if campo not in regla:
                raise ValueError(f"reglas: a la regla {regla.get('id', '?')} le falta '{campo}'")
        if "umbral" not in regla:
            raise ValueError(f"reglas: a la regla {regla['id']} le falta 'umbral' (puede ser null)")
        if regla["id"] in ids:
            raise ValueError(f"reglas: id repetido '{regla['id']}'")
        ids.add(regla["id"])
        if regla["severidad"] not in SEVERIDADES:
            raise ValueError(
                f"reglas: {regla['id']} tiene severidad '{regla['severidad']}', "
                f"valores posibles: {sorted(SEVERIDADES)}"
            )
        try:
            regla["condicion"].format(**parametros(regla, "x", "x", "x"))
        except (KeyError, IndexError, ValueError) as error:
            raise ValueError(f"reglas: la condición de {regla['id']} usa un marcador inválido ({error!r})")


def limites_del_mes(anio: int, mes: int) -> tuple:
    # Devuelve (inicio, fin) del mes como literales SQL. El fin no se incluye.
    sig_anio, sig_mes = (anio + 1, 1) if mes == 12 else (anio, mes + 1)
    return (f"TIMESTAMP '{anio}-{mes:02d}-01'", f"TIMESTAMP '{sig_anio}-{sig_mes:02d}-01'")


def consulta_del_mes(archivo: str) -> str:
    # SELECT que toma de la vista 'viajes_unificados' solo las filas de un archivo.
    # La vista la deja creada la etapa de esquema; esta etapa solo la lee.
    archivo_sql = archivo.replace("'", "''")
    return f"SELECT * FROM viajes_unificados WHERE archivo_origen = '{archivo_sql}'"


def crear_vista_zonas(con, ruta=RUTA_ZONAS) -> None:
    # La tabla de zonas como vista, con nombres propios. La usan la regla R07 y el enriquecimiento.
    ruta_sql = str(ruta).replace("'", "''")
    con.execute(f"""
        CREATE OR REPLACE VIEW zonas AS
        SELECT LocationID AS zona_id, Borough AS borough, Zone AS zona, service_zone
        FROM read_csv('{ruta_sql}')
    """)


def lista_de_reglas(piezas: list) -> str:
    # Expresión SQL que arma la lista de ids violados, sin los NULL.
    if not piezas:
        return "[]::VARCHAR[]"
    return f"list_sort(list_distinct([{', '.join(piezas)}]))"


def evaluar(con, select_sql: str, contrato: dict, reglas: list, anio: int, mes: int) -> None:
    # Crea la tabla temporal 'evaluado': cada fila del mes más la lista de reglas que viola.
    inicio, fin = limites_del_mes(anio, mes)
    columnas = ", ".join(col["nombre"] for col in contrato["columnas"])

    rechazo, advertencia = [], []
    for regla in reglas:
        condicion = regla["condicion"].format(**parametros(regla, inicio, fin, columnas))
        pieza = f"CASE WHEN {condicion} THEN '{regla['id']}' END"
        (rechazo if regla["severidad"] == "rechazo" else advertencia).append(pieza)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE evaluado AS
        SELECT
            e.*,
            {lista_de_reglas(rechazo)} AS reglas_rechazo,
            {lista_de_reglas(advertencia)} AS reglas_advertencia
        FROM ({select_sql}) AS e
    """)


def resumen_general(con) -> tuple:
    # (filas leídas, filas con algún rechazo, filas con alguna advertencia)
    return con.execute("""
        SELECT count(*),
               count(*) FILTER (WHERE len(reglas_rechazo) > 0),
               count(*) FILTER (WHERE len(reglas_advertencia) > 0)
        FROM evaluado
    """).fetchall()[0]


def contar_por_regla(con) -> list:
    # Cantidad de filas que violan cada regla: [(id, cantidad), ...]
    return con.execute("""
        SELECT regla, count(*) AS filas
        FROM (SELECT unnest(list_concat(reglas_rechazo, reglas_advertencia)) AS regla FROM evaluado)
        GROUP BY regla
        ORDER BY regla
    """).fetchall()

class ErrorReconciliacion(Exception):
    # Las filas leídas no coinciden con aceptadas + cuarentena.
    pass


def crear_tablas(con, contrato: dict) -> None:
    # Crea, si no existen, las tablas donde se escribe el resultado. Va ANTES de abrir la
    # transacción: con millones de filas, crearlas dentro de la misma transacción donde se
    # cargan dejó la tabla vacía y sin ningún error.
    columnas = ", ".join(col["nombre"] for col in contrato["columnas"])
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS aceptadas AS
        SELECT {columnas}, archivo_origen, reglas_advertencia AS flags FROM evaluado WHERE false
    """)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS cuarentena AS
        SELECT {columnas}, archivo_origen, reglas_rechazo, reglas_advertencia FROM evaluado WHERE false
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS reporte_resumen (
            archivo_origen VARCHAR, filas_leidas BIGINT, filas_aceptadas BIGINT,
            filas_cuarentena BIGINT, filas_con_advertencia BIGINT, fecha_hora TIMESTAMP)
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS reporte_reglas (
            archivo_origen VARCHAR, regla_id VARCHAR, severidad VARCHAR,
            descripcion VARCHAR, filas BIGINT, fecha_hora TIMESTAMP)
    """)


def separar(con, contrato: dict, archivo: str) -> None:
    # Reparte 'evaluado' en 'aceptadas' y 'cuarentena', reemplazando lo que ya hubiera de ese archivo.
    columnas = ", ".join(col["nombre"] for col in contrato["columnas"])
    con.execute("DELETE FROM aceptadas WHERE archivo_origen = ?", [archivo])
    con.execute("DELETE FROM cuarentena WHERE archivo_origen = ?", [archivo])
    con.execute(f"""
        INSERT INTO aceptadas
        SELECT {columnas}, archivo_origen, reglas_advertencia FROM evaluado
        WHERE len(reglas_rechazo) = 0
    """)
    con.execute(f"""
        INSERT INTO cuarentena
        SELECT {columnas}, archivo_origen, reglas_rechazo, reglas_advertencia FROM evaluado
        WHERE len(reglas_rechazo) > 0
    """)


def contar_destino(con, archivo: str) -> tuple:
    # (filas aceptadas, filas en cuarentena) de ese archivo, contadas en las tablas de destino.
    return tuple(
        con.execute(f"SELECT count(*) FROM {tabla} WHERE archivo_origen = ?", [archivo]).fetchall()[0][0]
        for tabla in ("aceptadas", "cuarentena")
    )


def reconciliar(leidas: int, aceptadas: int, cuarentena: int, archivo: str) -> None:
    # Falla si se perdió o se inventó alguna fila en el camino.
    if leidas != aceptadas + cuarentena:
        raise ErrorReconciliacion(
            f"{archivo}: filas leídas {leidas} distinto de aceptadas {aceptadas} + "
            f"cuarentena {cuarentena} (diferencia {leidas - aceptadas - cuarentena})"
        )


def guardar_reporte(con, reglas: list, archivo: str, leidas: int, aceptadas: int,
                    cuarentena: int, con_advertencia: int) -> None:
    # Deja el reporte del mes en dos tablas: un resumen y el detalle por regla.
    con.execute("DELETE FROM reporte_resumen WHERE archivo_origen = ?", [archivo])
    con.execute("DELETE FROM reporte_reglas WHERE archivo_origen = ?", [archivo])

    ahora = datetime.now()
    con.execute("INSERT INTO reporte_resumen VALUES (?, ?, ?, ?, ?, ?)",
                [archivo, leidas, aceptadas, cuarentena, con_advertencia, ahora])
    por_regla = dict(contar_por_regla(con))
    for regla in reglas:    # se recorre la lista de reglas, así las que no dispararon salen con 0
        con.execute("INSERT INTO reporte_reglas VALUES (?, ?, ?, ?, ?, ?)",
                    [archivo, regla["id"], regla["severidad"], regla["descripcion"],
                     por_regla.get(regla["id"], 0), ahora])


def miles(n: int) -> str:
    return f"{n:,}".replace(",", ".")


def imprimir_reporte(con, archivo: str) -> None:
    leidas, aceptadas, cuarentena, con_adv = con.execute(
        "SELECT filas_leidas, filas_aceptadas, filas_cuarentena, filas_con_advertencia "
        "FROM reporte_resumen WHERE archivo_origen = ?", [archivo]).fetchall()[0]

    def porcentaje(n):
        return 100 * n / leidas if leidas else 0

    print(f"Reporte de calidad: {archivo}")
    print(f"  filas leídas:           {miles(leidas):>12}")
    print(f"  aceptadas:              {miles(aceptadas):>12}  ({porcentaje(aceptadas):.1f} %)")
    print(f"  en cuarentena:          {miles(cuarentena):>12}  ({porcentaje(cuarentena):.1f} %)")
    print(f"  con alguna advertencia: {miles(con_adv):>12}  ({porcentaje(con_adv):.1f} %)")
    print("  filas que violan cada regla (una fila puede violar varias):")
    for regla_id, severidad, filas in con.execute(
            "SELECT regla_id, severidad, filas FROM reporte_reglas "
            "WHERE archivo_origen = ? ORDER BY regla_id", [archivo]).fetchall():
        print(f"    {regla_id}  {severidad:<11} {miles(filas):>10}")


def procesar_calidad(con, contrato: dict, reglas: list, select_sql: str, leidas: int,
                     archivo: str, anio: int, mes: int) -> None:
    # Etapa completa para un mes: evalúa, separa, reconcilia y guarda el reporte.
    # 'leidas' es el conteo del archivo original, calculado aparte para poder reconciliar.
    # Todo lo que se escribe en las tablas del mes va en una transacción: o entra completo o no entra.
    evaluar(con, select_sql, contrato, reglas, anio, mes)
    _, _, con_advertencia = resumen_general(con)
    crear_tablas(con, contrato)

    con.execute("BEGIN")
    try:
        separar(con, contrato, archivo)
        aceptadas, cuarentena = contar_destino(con, archivo)
        reconciliar(leidas, aceptadas, cuarentena, archivo)
        guardar_reporte(con, reglas, archivo, leidas, aceptadas, cuarentena, con_advertencia)
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    imprimir_reporte(con, archivo)