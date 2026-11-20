# Punto de entrada de `make all`: corre el pipeline en Docker (con Dozzle para ver los logs) y, si Docker
# no está disponible o no se puede armar, lo corre directo con Python como siempre.
# En Docker el contenedor queda encendido al terminar, para seguir mirando sus logs en Dozzle.
# Usa solo la biblioteca estándar, así arranca aunque todavía no estén instaladas las dependencias.
import argparse
import importlib.util
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent
CONTENEDOR = "nyc-taxi-pipeline"
AVISO_FIN = re.compile(r"== PIPELINE TERMINÓ: código (\d+) ==")     # lo imprime docker-entrypoint.sh
MODULOS = ["duckdb", "dlt", "dbt", "yaml", "requests", "pyarrow"]       # lo que importa el pipeline


def correr(comando, **opciones):
    return subprocess.run(comando, cwd=RAIZ, **opciones)


def motivo_sin_docker():
    # None si Docker sirve; si no, el motivo en una frase.
    if shutil.which("docker") is None:
        return "no está instalado el comando docker"
    try:
        if correr(["docker", "info"], capture_output=True, timeout=30).returncode != 0:
            return "el servicio de Docker no responde (¿está iniciado?)"
        if correr(["docker", "compose", "version"], capture_output=True, timeout=30).returncode != 0:
            return "falta el plugin docker compose"
    except (OSError, subprocess.TimeoutExpired) as error:
        return f"no se pudo consultar a Docker ({error})"
    return None


def correr_en_docker(mes: str):
    # Devuelve el código de salida del pipeline, o None si no se pudo armar el entorno de Docker.
    (RAIZ / "data").mkdir(exist_ok=True)           # la crea este usuario y no Docker (que la crearía como root)
    entorno = {**os.environ, "MES": mes}
    if sys.platform.startswith("linux") and os.getuid() != 0:
        entorno.update(HOST_UID=str(os.getuid()), HOST_GID=str(os.getgid()))   # los archivos de data/ quedan a tu nombre

    if correr(["docker", "compose", "build", "pipeline"], env=entorno).returncode != 0:
        print("[aviso] no se pudo construir la imagen de Docker.")
        return None
    if correr(["docker", "compose", "up", "-d", "dozzle"], env=entorno).returncode != 0:
        print("[aviso] no se pudo levantar Dozzle: el pipeline sigue, pero sin la pantalla de logs.")
    else:
        print("Logs en vivo en Dozzle: http://localhost:8080")

    correr(["docker", "rm", "-f", CONTENEDOR], capture_output=True)            # el de la corrida anterior, si quedó
    if correr(["docker", "compose", "up", "-d", "--no-deps", "pipeline"], env=entorno).returncode != 0:
        print("[aviso] el contenedor del pipeline no pudo arrancar.")
        return None

    # El contenedor queda encendido al terminar: se sigue su log hasta que el pipeline avisa que terminó.
    codigo = seguir_logs()
    if codigo is not None:
        print("El contenedor sigue encendido para consultar los logs (Dozzle). Para apagarlo: make down")
        return codigo
    # Se cortó el log sin ese aviso: el contenedor murió (por ejemplo, por memoria) o nunca llegó a ejecutarse.
    estado = correr(["docker", "inspect", "-f", "{{.State.Status}} {{.State.ExitCode}} {{.State.OOMKilled}}", CONTENEDOR],
                    capture_output=True, text=True)
    if estado.returncode != 0 or not estado.stdout.startswith("exited"):
        print("[aviso] el contenedor del pipeline no llegó a ejecutarse.")
        return None
    _, codigo, sin_memoria = estado.stdout.split()
    if sin_memoria == "true":
        print("ERROR: Docker cortó el pipeline por superar el límite de memoria del contenedor.", file=sys.stderr)
    return int(codigo) or 1


def seguir_logs():
    # Muestra el log del contenedor mientras se escribe y devuelve el código de salida del pipeline cuando
    # este avisa que terminó. Devuelve None si el log se corta antes (el contenedor se detuvo).
    proceso = subprocess.Popen(["docker", "logs", "-f", CONTENEDOR], cwd=RAIZ, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace")
    codigo = None
    try:
        for linea in proceso.stdout:
            print(linea, end="", flush=True)
            aviso = AVISO_FIN.search(linea)
            if aviso:
                codigo = int(aviso.group(1))
                break
    finally:
        proceso.terminate()                         # el contenedor sigue vivo: el log no termina solo
        proceso.wait()
    return codigo


def faltan_dependencias() -> list:
    return [modulo for modulo in MODULOS if importlib.util.find_spec(modulo) is None]


def correr_local(mes: str) -> int:
    faltan = faltan_dependencias()
    if faltan:
        print(f"ERROR: faltan dependencias de Python ({', '.join(faltan)}). Instalalas con: pip install -r requirements.txt",
              file=sys.stderr)
        return 1
    return correr([sys.executable, "run.py", "--mes", mes]).returncode


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Corre el pipeline en Docker si se puede y, si no, directo con Python.")
    parser.add_argument("--mes", required=True, help="mes asignado, formato AAAA-MM")
    parser.add_argument("--modo", choices=["auto", "docker", "local"], default="auto",
                        help="auto: Docker si está disponible y, si no, local (por defecto)")
    args = parser.parse_args(argv)
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", args.mes):
        parser.error(f"mes inválido: '{args.mes}'. Formato esperado: AAAA-MM (por ejemplo 2025-03)")

    if args.modo == "local":
        return correr_local(args.mes)

    motivo = motivo_sin_docker()
    if motivo is None:
        codigo = correr_en_docker(args.mes)
        if codigo is not None:
            if codigo != 0:
                print(f"ERROR: el pipeline falló dentro de Docker (código {codigo}). "
                      f"Sus logs: docker logs {CONTENEDOR}", file=sys.stderr)
            return codigo
        motivo = "no se pudo armar el entorno de Docker"

    if args.modo == "docker":
        print(f"ERROR: se pidió correr en Docker pero {motivo}.", file=sys.stderr)
        return 1
    print(f"[aviso] {motivo}. Se corre directo con Python, sin Docker ni Dozzle.")
    return correr_local(args.mes)


if __name__ == "__main__":
    sys.exit(main())
