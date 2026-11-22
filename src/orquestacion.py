# Orquestación con Dagster: cada etapa del pipeline es un asset, y Dagster guarda el historial de corridas, sus logs y
# las métricas de cada etapa, que se ven en su interfaz web.
#   python run.py --mes 2025-03 --dagster     corre el pipeline a través de Dagster
#   python -m src.orquestacion web            levanta la interfaz web (http://localhost:3000)
# Los dos usan la misma instancia de Dagster (la carpeta DAGSTER_HOME), que es donde quedan las corridas.
import os
import sys
from pathlib import Path

from dagster import (AssetExecutionContext, AssetSelection, DagsterInstance, Definitions, Field, MaterializeResult, MetadataValue,
                     asset, define_asset_job, in_process_executor, resource)

from src.config import DIR_DAGSTER
from src.e1_ingesta import parsear_mes
from src.metricas import clave_valor
from src.pipeline import ETAPAS, Sesion, ejecutar

NOMBRE_JOB = "pipeline_nyc_taxi"
GRUPO = "nyc_taxi"

DESCRIPCIONES = {
    "ingesta": "Descarga con dlt los Parquet de la TLC (mes pedido y mismo mes del año anterior) y la tabla de zonas.",
    "esquema": "Verifica los archivos contra el contrato de esquema y arma la vista unificada de viajes.",
    "calidad": "Aplica las reglas de calidad y reparte cada mes en aceptadas y cuarentena, con su reporte.",
    "curado": "Escribe la capa curada: un Parquet por mes con las filas aceptadas y las columnas derivadas.",
    "modelo": "Construye con dbt el modelo estrella (fact_trips, dim_zone, dim_date, agg_zone_hour) y corre sus tests.",
    "preguntas": "Responde las preguntas de negocio sobre el modelo y guarda el informe del mes.",
}


# --- el recurso: una sesión por corrida ----------------------------------------------------------------

@resource(config_schema={"mes": Field(str, is_required=False, description="mes a procesar, AAAA-MM (por defecto, la variable MES)")})
def sesion(context):
    # Abre la sesión del pipeline al empezar la corrida (la base y la medición) y la cierra al terminar, aunque haya fallas.
    # Todos los assets de la corrida usan esta misma sesión: comparten la conexión y suman al mismo resumen.
    texto = context.resource_config.get("mes") or os.environ.get("MES")
    if not texto:
        raise ValueError("falta el mes: configurá el recurso 'sesion' con mes: AAAA-MM o definí la variable MES")
    try:
        anio, mes = parsear_mes(texto)
    except Exception as error:
        raise ValueError(str(error)) from error
    una_sesion = Sesion(anio, mes)
    una_sesion.abrir()
    try:
        yield una_sesion
    finally:
        total = una_sesion.cerrar()
        context.log.info(f"corrida {anio}-{mes:02d}: {clave_valor(total)}")


# --- los assets: uno por etapa -------------------------------------------------------------------------

def metadatos(resultado: dict) -> dict:
    # Lo medido en la etapa, como metadatos del asset: Dagster los muestra en cada ejecución.
    tipos = {bool: MetadataValue.bool, int: MetadataValue.int, float: MetadataValue.float}
    return {clave: tipos.get(type(valor), lambda v: MetadataValue.text(str(v)))(valor)
            for clave, valor in resultado.items() if clave != "etapa"}


def crear_asset(nombre: str, etapa, usa_base: bool, previa: str):
    @asset(name=nombre, deps=[previa] if previa else [], required_resource_keys={"sesion"},
           group_name=GRUPO, description=DESCRIPCIONES.get(nombre, ""))
    def asset_de_la_etapa(context: AssetExecutionContext) -> MaterializeResult:
        resultado = context.resources.sesion.correr_etapa(nombre, etapa, usa_base)
        context.log.info(clave_valor(resultado))
        return MaterializeResult(metadata=metadatos(resultado))

    return asset_de_la_etapa


def crear_assets(etapas: list = None) -> list:
    # Una cadena lineal: cada etapa depende de la anterior, en el orden de ETAPAS.
    etapas = etapas or ETAPAS
    previas = [None] + [nombre for nombre, _, _ in etapas[:-1]]
    return [crear_asset(nombre, etapa, usa_base, previa) for (nombre, etapa, usa_base), previa in zip(etapas, previas)]


defs = Definitions(
    assets=crear_assets(),
    jobs=[define_asset_job(NOMBRE_JOB, selection=AssetSelection.all(), description="Las seis etapas del pipeline, en orden.")],
    resources={"sesion": sesion},
    executor=in_process_executor,      # todo en un solo proceso: las etapas comparten la conexión a la base
)


# --- uso desde afuera ----------------------------------------------------------------------------------

def preparar_instancia() -> Path:
    # La instancia de Dagster guarda las corridas, los eventos y los logs. Vive en data/dagster_home (o en DAGSTER_HOME),
    # una carpeta que ven tanto el contenedor que corre el pipeline como el de la interfaz web.
    carpeta = Path(os.environ.setdefault("DAGSTER_HOME", str(DIR_DAGSTER)))
    carpeta.mkdir(parents=True, exist_ok=True)
    configuracion = carpeta / "dagster.yaml"
    if not configuracion.exists():
        configuracion.write_text("telemetry:\n  enabled: false\n", encoding="utf-8")      # sin telemetría, como dlt y dbt
    return carpeta


def motivo(error) -> str:
    # Dagster envuelve el error de la etapa en el suyo; lo que interesa es el de adentro ("falló la etapa 'x': ...").
    causa = error.cause or error
    return causa.message.strip().removeprefix("RuntimeError: ")


def ejecutar_con_dagster(anio: int, mes: int) -> None:
    # Corre las seis etapas como una corrida de Dagster. Si alguna falla levanta una excepción con el motivo.
    # Si Dagster ni siquiera puede abrir su instancia, el pipeline no se cae por eso: corre sin Dagster.
    try:
        preparar_instancia()
        instancia = DagsterInstance.get()
    except Exception as error:
        print(f"[aviso] Dagster no pudo abrir su instancia ({error}). Se corre el pipeline sin Dagster.")
        ejecutar(anio, mes)
        return
    config = {"resources": {"sesion": {"config": {"mes": f"{anio}-{mes:02d}"}}}}
    with instancia:
        resultado = defs.resolve_job_def(NOMBRE_JOB).execute_in_process(
            run_config=config, instance=instancia, raise_on_error=False)
    if not resultado.success:
        motivos = [motivo(e.event_specific_data.error) for e in resultado.get_step_failure_events()]
        raise RuntimeError("; ".join(motivos) or "la corrida de Dagster falló (ver su log)")


if __name__ == "__main__":
    if sys.argv[1:] == ["web"]:
        preparar_instancia()
        os.execv(sys.executable, [sys.executable, "-m", "dagster_webserver", "-h", "0.0.0.0", "-p", "3000", "-m", "src.orquestacion"])
    sys.exit("uso: python -m src.orquestacion web")
