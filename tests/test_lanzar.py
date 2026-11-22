# Tests del lanzador (`make all`): usa Docker si se puede y, si no, sigue directo con Python.
# Docker se reemplaza por un doble que registra los comandos y contesta lo que cada test le indica.
import os
import re
import stat
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

import lanzar

RAIZ_REAL = Path(__file__).resolve().parent.parent


class DockerFalso:
    def __init__(self, hay_docker=True, falla=(), codigo_pipeline=0, estado="exited 0 false", codigo_local=0):
        self.hay_docker, self.falla, self.codigo_pipeline = hay_docker, falla, codigo_pipeline
        self.estado, self.codigo_local = estado, codigo_local
        self.comandos = []                     # [(comando como texto, env)]

    def correr(self, comando, **opciones):
        texto = " ".join(str(parte) for parte in comando)
        self.comandos.append((texto, opciones.get("env")))
        if comando[:2] == [sys.executable, "run.py"]:
            return SimpleNamespace(returncode=self.codigo_local, stdout="")
        if any(fragmento in texto for fragmento in self.falla):
            return SimpleNamespace(returncode=1, stdout="")
        if comando[:2] == ["docker", "inspect"]:
            return SimpleNamespace(returncode=0, stdout=self.estado)
        return SimpleNamespace(returncode=0, stdout="")

    def textos(self):
        return [texto for texto, _ in self.comandos]

    def corrio(self, fragmento):
        return any(fragmento in texto for texto in self.textos())

    def corrio_en_local(self):
        return self.corrio(f"{sys.executable} run.py")

    def posicion(self, fragmento):
        return next(i for i, texto in enumerate(self.textos()) if fragmento in texto)


@pytest.fixture
def entorno(monkeypatch, tmp_path):
    def armar(**opciones):
        falso = DockerFalso(**opciones)
        monkeypatch.setattr(lanzar, "correr", falso.correr)
        monkeypatch.setattr(lanzar.shutil, "which", lambda nombre: "/usr/bin/docker" if falso.hay_docker else None)
        monkeypatch.setattr(lanzar, "faltan_dependencias", lambda: [])
        monkeypatch.setattr(lanzar, "seguir_logs", lambda: falso.codigo_pipeline)
        monkeypatch.setattr(lanzar, "RAIZ", tmp_path)
        return falso
    return armar


# --- con Docker -------------------------------------------------------------------------------------------

def test_con_docker_corre_todo_en_orden_y_el_pipeline_no_corre_en_local(entorno, capsys, tmp_path):
    falso = entorno()
    assert lanzar.main(["--mes", "2025-03"]) == 0
    orden = ["compose build pipeline", "up -d dozzle", "up -d --wait --wait-timeout 120 dagster", "rm -f nyc-taxi-pipeline",
             "up -d --no-deps pipeline"]
    posiciones = [falso.posicion(fragmento) for fragmento in orden]
    assert posiciones == sorted(posiciones) and len(set(posiciones)) == 5      # Dagster sano antes de empezar el pipeline
    assert not falso.corrio_en_local()
    salida = capsys.readouterr().out
    assert "http://localhost:8080" in salida and "http://localhost:3000" in salida and "make down" in salida
    assert (tmp_path / "data").is_dir()


def test_el_mes_viaja_a_docker_por_la_variable_de_entorno_MES(entorno):
    falso = entorno()
    lanzar.main(["--mes", "2025-03"])
    assert all(env["MES"] == "2025-03" for _, env in falso.comandos if env)


def test_si_el_pipeline_falla_dentro_de_docker_se_devuelve_su_codigo_sin_reintentar_en_local(entorno, capsys):
    falso = entorno(codigo_pipeline=2)
    assert lanzar.main(["--mes", "2025-03"]) == 2
    assert not falso.corrio_en_local()
    assert "falló dentro de Docker (código 2)" in capsys.readouterr().err


@pytest.mark.parametrize("estado, esperado", [("exited 3 false", 3), ("exited 0 false", 1), ("exited 137 true", 137)])
def test_si_el_contenedor_murio_sin_avisar_se_usa_su_codigo_de_salida(entorno, capsys, estado, esperado):
    falso = entorno(codigo_pipeline=None, estado=estado)
    assert lanzar.main(["--mes", "2025-03"]) == esperado
    assert not falso.corrio_en_local()
    assert ("límite de memoria" in capsys.readouterr().err) == estado.endswith("true")


@pytest.mark.parametrize("estado", ["running 0 false", "created 0 false"])
def test_si_el_contenedor_nunca_llego_a_ejecutarse_se_sigue_en_local(entorno, capsys, estado):
    falso = entorno(codigo_pipeline=None, estado=estado)
    assert lanzar.main(["--mes", "2025-03"]) == 0
    assert falso.corrio_en_local()
    assert "no llegó a ejecutarse" in capsys.readouterr().out


# --- respaldo: nada de Docker puede hacer fallar el pipeline -------------------------------------------------

@pytest.mark.parametrize("opciones, motivo", [
    ({"hay_docker": False}, "no está instalado el comando docker"),
    ({"falla": ["docker info"]}, "no responde"),
    ({"falla": ["docker compose version"]}, "falta el plugin docker compose"),
    ({"falla": ["compose build"]}, "no se pudo construir la imagen"),
    ({"falla": ["up -d --no-deps pipeline"]}, "no pudo arrancar"),
])
def test_si_docker_no_sirve_se_corre_directo_con_python(entorno, capsys, opciones, motivo):
    falso = entorno(**opciones)
    assert lanzar.main(["--mes", "2025-03"]) == 0
    assert falso.corrio_en_local()
    assert motivo in capsys.readouterr().out


def test_si_falla_dozzle_el_pipeline_sigue_en_docker(entorno, capsys):
    falso = entorno(falla=["up -d dozzle"])
    assert lanzar.main(["--mes", "2025-03"]) == 0
    assert not falso.corrio_en_local() and falso.corrio("up -d --no-deps pipeline")
    salida = capsys.readouterr().out
    assert "no se pudo levantar Dozzle" in salida and "localhost:8080" not in salida


def test_si_falla_la_interfaz_de_dagster_el_pipeline_sigue_en_docker(entorno, capsys):
    falso = entorno(falla=["--wait"])
    assert lanzar.main(["--mes", "2025-03"]) == 0
    assert not falso.corrio_en_local() and falso.corrio("up -d --no-deps pipeline")
    salida = capsys.readouterr().out
    assert "no se pudo levantar la interfaz de Dagster" in salida and "data/dagster_home" in salida
    assert "localhost:3000" not in salida


def test_el_codigo_de_un_pipeline_que_falla_no_se_confunde_con_un_problema_de_docker(entorno):
    falso = entorno(codigo_pipeline=1, codigo_local=0)
    assert lanzar.main(["--mes", "2025-03"]) == 1              # no se "arregla" corriendo otra vez en local
    assert not falso.corrio_en_local()


# --- modos y argumentos -----------------------------------------------------------------------------------

def test_en_modo_docker_sin_docker_falla_y_no_corre_nada_en_local(entorno, capsys):
    falso = entorno(hay_docker=False)
    assert lanzar.main(["--mes", "2025-03", "--modo", "docker"]) == 1
    assert not falso.corrio_en_local()
    assert "se pidió correr en Docker" in capsys.readouterr().err


def test_en_modo_local_no_se_toca_docker(entorno):
    falso = entorno()
    assert lanzar.main(["--mes", "2025-03", "--modo", "local"]) == 0
    assert falso.corrio_en_local() and not falso.corrio("docker")


def test_el_codigo_de_salida_de_la_corrida_local_se_devuelve(entorno):
    entorno(hay_docker=False, codigo_local=1)
    assert lanzar.main(["--mes", "2025-03"]) == 1


@pytest.mark.parametrize("mes", ["2025-13", "2025-3", "marzo", "2025-00", "25-03", ""])
def test_un_mes_mal_escrito_se_rechaza_antes_de_tocar_nada(entorno, mes):
    falso = entorno()
    with pytest.raises(SystemExit) as salida:
        lanzar.main(["--mes", mes])
    assert salida.value.code == 2 and falso.comandos == []


def test_faltando_dependencias_de_python_la_corrida_local_avisa_como_instalarlas(entorno, monkeypatch, capsys):
    falso = entorno(hay_docker=False)
    monkeypatch.setattr(lanzar, "faltan_dependencias", lambda: ["duckdb", "dlt"])
    assert lanzar.main(["--mes", "2025-03"]) == 1
    assert not falso.corrio_en_local()
    error = capsys.readouterr().err
    assert "duckdb, dlt" in error and "pip install -r requirements.txt" in error


@pytest.mark.parametrize("plataforma, uid, esperado", [("linux", 1000, True), ("linux", 0, False), ("darwin", 501, False)])
def test_en_linux_sin_ser_root_los_contenedores_corren_con_tu_usuario(entorno, monkeypatch, plataforma, uid, esperado):
    falso = entorno()
    monkeypatch.setattr(lanzar.sys, "platform", plataforma)
    monkeypatch.setattr(lanzar.os, "getuid", lambda: uid, raising=False)
    monkeypatch.setattr(lanzar.os, "getgid", lambda: uid + 1, raising=False)
    lanzar.main(["--mes", "2025-03"])
    entornos = [env for _, env in falso.comandos if env]
    assert all(("HOST_UID" in env) == esperado for env in entornos)
    if esperado:
        assert all((env["HOST_UID"], env["HOST_GID"]) == ("1000", "1001") for env in entornos)


# --- seguir el log del contenedor -------------------------------------------------------------------------

class PopenFalso:
    def __init__(self, lineas):
        self.stdout = iter(lineas)
        self.terminado = self.esperado = False

    def terminate(self):
        self.terminado = True

    def wait(self):
        self.esperado = True


@pytest.mark.parametrize("lineas, esperado", [
    (["etapa uno\n", "== PIPELINE TERMINÓ: código 0 ==\n", "no se lee\n"], 0),
    (["etapa uno\n", "== PIPELINE TERMINÓ: código 3 ==\n"], 3),
    (["etapa uno\n", "se cortó el log\n"], None),
])
def test_seguir_logs_devuelve_el_codigo_del_aviso_y_deja_vivo_al_contenedor(monkeypatch, capsys, lineas, esperado):
    proceso = PopenFalso(lineas)
    monkeypatch.setattr(lanzar.subprocess, "Popen", lambda *args, **kwargs: proceso)
    assert lanzar.seguir_logs() == esperado
    assert proceso.terminado and proceso.esperado           # se cierra el 'docker logs -f', no el contenedor
    salida = capsys.readouterr().out
    assert "etapa uno" in salida
    assert "no se lee" not in salida


# --- el script de entrada del contenedor ------------------------------------------------------------------

@pytest.mark.parametrize("codigo", [0, 3])
def test_el_script_del_contenedor_avisa_el_codigo_y_queda_encendido(tmp_path, codigo):
    # 'python' y 'sleep' falsos: el primero termina con el código pedido y el segundo solo anota que lo llamaron.
    carpeta = tmp_path / "bin"
    carpeta.mkdir()
    for nombre, contenido in {"python": f'echo "pipeline: $*"; exit {codigo}', "sleep": 'echo "sleep: $*"'}.items():
        ruta = carpeta / nombre
        ruta.write_text(f"#!/bin/sh\n{contenido}\n")
        ruta.chmod(ruta.stat().st_mode | stat.S_IEXEC)
    resultado = subprocess.run(["sh", str(RAIZ_REAL / "docker-entrypoint.sh"), "--mes", "2025-03", "--dagster"],
                               capture_output=True, text=True, env={**os.environ, "PATH": f"{carpeta}:{os.environ['PATH']}"})
    lineas = resultado.stdout.splitlines()
    assert "pipeline: run.py --mes 2025-03 --dagster" in lineas
    aviso = next(linea for linea in lineas if lanzar.AVISO_FIN.search(linea))          # lo que lanzar.py sabe leer
    assert int(lanzar.AVISO_FIN.search(aviso).group(1)) == codigo
    assert lineas[-1] == "sleep: infinity"


# --- los archivos de Docker -------------------------------------------------------------------------------

@pytest.fixture(scope="module")
def compose():
    return yaml.safe_load((RAIZ_REAL / "docker-compose.yml").read_text(encoding="utf-8"))["services"]


def test_el_compose_tiene_el_pipeline_dagster_y_dozzle(compose):
    assert set(compose) == {"pipeline", "dagster", "dozzle"}
    assert compose["pipeline"]["container_name"] == lanzar.CONTENEDOR


def test_el_pipeline_corre_orquestado_por_dagster_con_un_tope_de_memoria_real(compose):
    pipeline = compose["pipeline"]
    assert pipeline["command"][-1] == "--dagster"
    assert pipeline["mem_limit"] == pipeline["memswap_limit"] == "6g"         # sin swap: el límite es real


def test_dagster_y_el_pipeline_comparten_la_imagen_y_la_carpeta_donde_dagster_guarda_el_historial(compose):
    assert compose["dagster"]["image"] == compose["pipeline"]["image"]
    assert "./data:/app/data" in compose["dagster"]["volumes"] and "./data:/app/data" in compose["pipeline"]["volumes"]


def test_los_puertos_de_dagster_y_dozzle_solo_se_abren_en_esta_maquina(compose):
    assert compose["dagster"]["ports"] == ["127.0.0.1:3000:3000"]
    assert compose["dozzle"]["ports"] == ["127.0.0.1:8080:8080"]


def test_dagster_tiene_un_chequeo_de_salud_que_pregunta_por_su_puerto(compose):
    chequeo = compose["dagster"]["healthcheck"]
    assert "localhost:3000/server_info" in " ".join(chequeo["test"]) and chequeo["retries"] >= 20


def test_la_imagen_instala_las_dos_listas_de_dependencias_y_guarda_a_dagster_en_data():
    dockerfile = (RAIZ_REAL / "Dockerfile").read_text(encoding="utf-8")
    assert "pip install -r requirements.txt -r requirements-dagster.txt" in dockerfile
    assert "DAGSTER_HOME=/app/data/dagster_home" in dockerfile


def test_el_dockerignore_deja_afuera_los_datos_y_el_entorno_virtual():
    # Sin esto el build manda gigas a Docker (datos y .venv) y los mete en la imagen.
    lineas = (RAIZ_REAL / ".dockerignore").read_text(encoding="utf-8").split()
    assert {"data/", ".venv/", ".git/"} <= set(lineas)


def test_dagster_y_su_webserver_van_con_la_misma_version_fija():
    versiones = dict(linea.split("==") for linea in (RAIZ_REAL / "requirements-dagster.txt").read_text().split())
    assert set(versiones) == {"dagster", "dagster-webserver"} and len(set(versiones.values())) == 1
    assert re.fullmatch(r"\d+\.\d+\.\d+", versiones["dagster"])
