# Tests de la orquestación con Dagster. Se saltean si Dagster no está instalado (requirements-dagster.txt).
import os
import subprocess
import sys
from functools import partial

import pytest

dagster = pytest.importorskip("dagster")

from dagster import (AssetSelection, DagsterInstance, DagsterRunStatus, Definitions, define_asset_job,  # noqa: E402
                     in_process_executor, materialize)

from src import orquestacion, pipeline  # noqa: E402
from test_pipeline import pipeline_falso, proyecto  # noqa: E402,F401  (fixtures compartidas)

CONFIG = {"resources": {"sesion": {"config": {"mes": "2025-03"}}}}


def etapas_falsas(falla_en=None):
    def etapa(nombre, datos):
        def correr(con, anio, mes):
            if nombre == falla_en:
                raise ValueError("explotó")
            return datos
        return correr

    return [("a", etapa("a", {"filas": 10, "mb_en_disco": 1.5, "nota": "listo"}), True),
            ("b", etapa("b", {"filas": 20}), False),
            ("c", etapa("c", {"filas": 30}), False)]


def correr_assets(etapas, config=CONFIG, instancia=None):
    return materialize(orquestacion.crear_assets(etapas), resources={"sesion": orquestacion.sesion}, run_config=config,
                       instance=instancia or DagsterInstance.ephemeral(), raise_on_error=False)


def metadatos_de(resultado, asset):
    (evento,) = [e for e in resultado.get_asset_materialization_events()
                 if e.materialization.asset_key.to_user_string() == asset]
    return {clave: valor.value for clave, valor in evento.materialization.metadata.items()}


def test_hay_un_asset_por_etapa_encadenados_en_el_orden_del_pipeline():
    assets = orquestacion.crear_assets()
    assert [a.key.to_user_string() for a in assets] == [nombre for nombre, _, _ in pipeline.ETAPAS]
    previas = {a.key.to_user_string(): sorted(k.to_user_string() for k in a.asset_deps[a.key]) for a in assets}
    assert previas == {"ingesta": [], "esquema": ["ingesta"], "calidad": ["esquema"], "curado": ["calidad"],
                       "modelo": ["curado"], "preguntas": ["modelo"]}
    assert all(a.group_names_by_key[a.key] == orquestacion.GRUPO and a.descriptions_by_key[a.key] for a in assets)


def test_cada_asset_publica_lo_medido_en_su_etapa_como_metadatos(pipeline_falso):
    resultado = correr_assets(etapas_falsas())
    assert resultado.success
    meta = metadatos_de(resultado, "a")
    assert meta["filas"] == 10 and meta["mb_en_disco"] == 1.5 and meta["nota"] == "listo"
    assert {"duracion_s", "cpu_s", "cpu_pct", "ram_fin_mb", "pico_etapa_mb"} <= set(meta)
    assert "etapa" not in meta
    assert metadatos_de(resultado, "c")["filas"] == 30


def test_las_etapas_de_una_corrida_comparten_una_sola_sesion(pipeline_falso):
    correr_assets(etapas_falsas())
    assert pipeline_falso.cerradas == 1          # la base se abrió una vez y se cerró una vez (antes de la etapa b)


def test_si_una_etapa_falla_no_corren_las_siguientes_y_la_sesion_se_cierra(pipeline_falso, tmp_path):
    resultado = correr_assets(etapas_falsas(falla_en="b"))
    assert not resultado.success
    assert [e.materialization.asset_key.to_user_string() for e in resultado.get_asset_materialization_events()] == ["a"]
    (fallo,) = resultado.get_step_failure_events()
    assert orquestacion.motivo(fallo.event_specific_data.error) == "falló la etapa 'b': explotó"
    assert pipeline_falso.cerradas == 1
    ultimo = (tmp_path / "2025-03.jsonl").read_text(encoding="utf-8").splitlines()[-1]
    assert '"evento": "corrida_fin"' in ultimo and '"estado": "error"' in ultimo


def test_el_mes_se_toma_de_la_variable_MES_si_el_recurso_no_lo_configura(pipeline_falso, tmp_path, monkeypatch):
    monkeypatch.setenv("MES", "2025-03")
    assert correr_assets(etapas_falsas(), config={}).success
    assert (tmp_path / "2025-03.jsonl").exists()


def test_sin_mes_o_con_un_mes_mal_escrito_la_corrida_falla_con_un_mensaje_claro(pipeline_falso, monkeypatch):
    monkeypatch.delenv("MES", raising=False)
    sin_mes = correr_assets(etapas_falsas(), config={})
    assert not sin_mes.success
    mal = correr_assets(etapas_falsas(), config={"resources": {"sesion": {"config": {"mes": "2025-13"}}}})
    assert not mal.success
    assert pipeline_falso.cerradas == 0          # no llegó a abrirse ninguna base


def test_todo_corre_en_un_solo_proceso_tambien_cuando_se_lanza_desde_la_interfaz():
    # Las etapas comparten la conexión a la base: si Dagster las repartiera en procesos distintos, no podrían.
    defs = orquestacion.defs
    assert defs.resolve_job_def(orquestacion.NOMBRE_JOB).executor_def.name == "in_process"
    assert defs.resolve_implicit_global_asset_job_def().executor_def.name == "in_process"      # el botón "Materialize all"


def test_los_metadatos_conservan_el_tipo_de_cada_numero():
    meta = orquestacion.metadatos({"etapa": "x", "filas": 7, "mb": 1.5, "listo": True, "nota": "hola", "otro": None})
    assert set(meta) == {"filas", "mb", "listo", "nota", "otro"}
    assert (meta["filas"].value, meta["mb"].value, meta["listo"].value, meta["nota"].value) == (7, 1.5, True, "hola")


def test_preparar_la_instancia_crea_la_carpeta_sin_telemetria_y_no_pisa_lo_que_hay(tmp_path, monkeypatch):
    monkeypatch.setenv("DAGSTER_HOME", str(tmp_path / "dagster"))
    carpeta = orquestacion.preparar_instancia()
    configuracion = carpeta / "dagster.yaml"
    assert configuracion.read_text(encoding="utf-8") == "telemetry:\n  enabled: false\n"
    configuracion.write_text("telemetry:\n  enabled: true\n", encoding="utf-8")          # algo que el usuario cambió
    orquestacion.preparar_instancia()
    assert configuracion.read_text(encoding="utf-8") == "telemetry:\n  enabled: true\n"


def test_sin_DAGSTER_HOME_la_instancia_va_a_data_dagster_home(tmp_path, monkeypatch):
    monkeypatch.setattr(orquestacion.os, "environ", {})            # un entorno vacío, descartado al terminar el test
    monkeypatch.setattr(orquestacion, "DIR_DAGSTER", tmp_path / "data" / "dagster_home")
    carpeta = orquestacion.preparar_instancia()
    assert carpeta == tmp_path / "data" / "dagster_home" and carpeta.is_dir()
    assert orquestacion.os.environ["DAGSTER_HOME"] == str(carpeta)


@pytest.fixture
def defs_falsas(monkeypatch, tmp_path):
    # Reemplaza las definiciones reales por unas con etapas falsas, y la instancia por una carpeta temporal.
    monkeypatch.setenv("DAGSTER_HOME", str(tmp_path / "instancia"))

    def armar(falla_en=None):
        monkeypatch.setattr(orquestacion, "defs", Definitions(
            assets=orquestacion.crear_assets(etapas_falsas(falla_en)),
            jobs=[define_asset_job(orquestacion.NOMBRE_JOB, selection=AssetSelection.all())],
            resources={"sesion": orquestacion.sesion}, executor=in_process_executor))
    return armar


def corridas_guardadas():
    with DagsterInstance.get() as instancia:
        return [(r.job_name, r.status, r.run_config["resources"]["sesion"]["config"]["mes"]) for r in instancia.get_runs()]


def test_ejecutar_con_dagster_deja_la_corrida_guardada_en_la_instancia(pipeline_falso, defs_falsas):
    defs_falsas()
    orquestacion.ejecutar_con_dagster(2025, 3)
    assert corridas_guardadas() == [(orquestacion.NOMBRE_JOB, DagsterRunStatus.SUCCESS, "2025-03")]


def test_ejecutar_con_dagster_levanta_el_motivo_si_una_etapa_falla(pipeline_falso, defs_falsas):
    defs_falsas(falla_en="b")
    with pytest.raises(RuntimeError, match="^falló la etapa 'b': explotó$"):
        orquestacion.ejecutar_con_dagster(2025, 3)
    assert corridas_guardadas()[0][1] == DagsterRunStatus.FAILURE


def test_si_dagster_no_puede_abrir_su_instancia_el_pipeline_corre_sin_dagster(tmp_path, monkeypatch, capsys):
    ocupado = tmp_path / "archivo_comun"
    ocupado.write_text("no es una carpeta")
    monkeypatch.setenv("DAGSTER_HOME", str(ocupado))                      # no se puede crear una carpeta ahí
    corridas = []
    monkeypatch.setattr(orquestacion, "ejecutar", lambda anio, mes: corridas.append((anio, mes)))
    orquestacion.ejecutar_con_dagster(2025, 3)
    assert corridas == [(2025, 3)]
    assert "Dagster no pudo abrir su instancia" in capsys.readouterr().out


# --- de punta a punta: python run.py --dagster sobre datos sintéticos ------------------------------------

def correr_con_dagster(raiz):
    entorno = {**os.environ, "DAGSTER_HOME": str(raiz / "data" / "dagster_home")}
    return subprocess.run([sys.executable, "run.py", "--mes", "2025-03", "--dagster"], cwd=raiz, env=entorno,
                          capture_output=True, text=True, timeout=300)


def test_el_pipeline_completo_corre_por_dagster_y_queda_en_su_historial(proyecto, monkeypatch):
    proceso = correr_con_dagster(proyecto)
    assert proceso.returncode == 0, proceso.stdout + proceso.stderr
    assert "evento=corrida_fin estado=ok" in proceso.stdout and "RESUMEN 2025-03" in proceso.stdout

    monkeypatch.setenv("DAGSTER_HOME", str(proyecto / "data" / "dagster_home"))
    with DagsterInstance.get() as instancia:
        (corrida,) = instancia.get_runs()
        assert corrida.status == DagsterRunStatus.SUCCESS
        materializados = [r.asset_entry.asset_key.to_user_string() for r in instancia.get_asset_records()]
        assert sorted(materializados) == sorted(nombre for nombre, _, _ in pipeline.ETAPAS)
    assert any((proyecto / "data" / "dagster_home" / "storage").glob("*/compute_logs/*.out"))   # el log de la corrida


def test_si_el_pipeline_falla_por_dagster_el_error_es_el_de_la_etapa(proyecto, monkeypatch):
    (proyecto / "contracts" / "reglas_calidad.yaml").unlink()
    proceso = correr_con_dagster(proyecto)
    assert proceso.returncode == 1
    assert "ERROR: falló la etapa 'calidad'" in proceso.stderr and "reglas_calidad" in proceso.stderr
    monkeypatch.setenv("DAGSTER_HOME", str(proyecto / "data" / "dagster_home"))
    with DagsterInstance.get() as instancia:
        assert [r.status for r in instancia.get_runs()] == [DagsterRunStatus.FAILURE]
