# Métricas de la corrida: cada etapa deja una línea de log con su duración, CPU, memoria y filas.
# Las líneas son "clave=valor" (fáciles de buscar en Dozzle: etapa=calidad, evento=error, ...) y además
# se guardan como JSON, una línea por evento, en data/logs/AAAA-MM.jsonl.
import json
import logging
import os
import resource
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import psutil

from src.config import DIR_LOGS

MB = 1024 * 1024
LOGGER = logging.getLogger("pipeline")


# --- medición ------------------------------------------------------------------------------------------

def a_mb(bytes_: float) -> float:
    return round(bytes_ / MB, 1)


def pico_ram_mb() -> float:
    # Pico exacto que informa el sistema operativo desde que arrancó el proceso: el más grande entre este y
    # sus hijos (dbt corre aparte).
    pico = max(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss, resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)
    return pico / MB if sys.platform == "darwin" else pico / 1024   # macOS lo da en bytes, Linux en KB


def ram_proceso_mb() -> float:
    return psutil.Process().memory_info().rss / MB


def ram_disponible_mb(archivo_limite: Path = Path("/sys/fs/cgroup/memory.max")) -> float:
    # La memoria con la que cuenta la corrida: el tope del contenedor si lo hay (cgroup v2), o la RAM de la máquina.
    total = psutil.virtual_memory().total
    try:
        limite = archivo_limite.read_text().strip()
        if limite != "max":
            total = min(total, int(limite))
    except (OSError, ValueError):
        pass
    return total / MB


def cpu_usada_s() -> float:
    # Segundos de CPU gastados por este proceso y por los hijos que ya terminaron (dbt), sumando todos los núcleos.
    t = os.times()
    return t.user + t.system + t.children_user + t.children_system


def entorno(marca_docker: Path = Path("/.dockerenv")) -> str:
    # Docker deja este archivo en la raíz de todo contenedor.
    return "docker" if marca_docker.exists() else "local"


class PicoDeMemoria:
    # Mide el pico de memoria mientras dura un bloque 'with': suma la RAM de este proceso y de sus hijos (dbt), que
    # viven al mismo tiempo, y se queda con el máximo. Es una muestra: puede perder un pico más corto que el intervalo.
    # Buscar los hijos obliga a recorrer todos los procesos de la máquina, que es caro: se hace cada medio segundo.
    # Entre búsqueda y búsqueda se vuelve a leer la memoria de los que ya se conocen, que es barato.
    def __init__(self, intervalo: float = 0.1, buscar_hijos_cada: float = 0.5):
        self.intervalo = intervalo
        self.buscar_hijos_cada = buscar_hijos_cada
        self.pico_mb = 0.0
        self._proceso = psutil.Process()
        self._hijos = []
        self._ultima_busqueda = float("-inf")
        self._parar = threading.Event()
        self._hilo = threading.Thread(target=self._muestrear, daemon=True)

    def _leer_mb(self) -> float:
        ahora = time.monotonic()
        if ahora - self._ultima_busqueda >= self.buscar_hijos_cada:
            self._hijos = self._proceso.children(recursive=True)
            self._ultima_busqueda = ahora
        total = 0
        for p in [self._proceso, *self._hijos]:
            try:
                total += p.memory_info().rss
            except psutil.Error:      # un hijo que terminó justo ahora
                pass
        return total / MB

    def _muestrear(self) -> None:
        while not self._parar.is_set():
            self.pico_mb = max(self.pico_mb, self._leer_mb())
            self._parar.wait(self.intervalo)

    def __enter__(self):
        self.pico_mb = self._leer_mb()
        self._hilo.start()
        return self

    def __exit__(self, *_):
        self._parar.set()
        self._hilo.join()
        self._ultima_busqueda = float("-inf")           # una última búsqueda completa, para no perder un hijo recién nacido
        self.pico_mb = max(self.pico_mb, self._leer_mb())


# --- formato de las líneas -----------------------------------------------------------------------------

def _hora(registro: logging.LogRecord) -> str:
    return datetime.fromtimestamp(registro.created).astimezone().isoformat(timespec="seconds")


def _valor(valor) -> str:
    texto = str(valor)
    if texto == "" or any(c in texto for c in ' ="'):
        return json.dumps(texto, ensure_ascii=False)       # entre comillas, para que siga siendo una sola clave=valor
    return texto


class FormatoClaveValor(logging.Formatter):
    # ts=2026-11-22T00:24:12-03:00 level=INFO etapa=ingesta evento=fin duracion_s=9.6 ...
    def format(self, registro: logging.LogRecord) -> str:
        campos = {"ts": _hora(registro), "level": registro.levelname, **registro.campos}
        return " ".join(f"{clave}={_valor(valor)}" for clave, valor in campos.items())


class FormatoJson(logging.Formatter):
    def format(self, registro: logging.LogRecord) -> str:
        return json.dumps({"ts": _hora(registro), "level": registro.levelname, **registro.campos},
                          ensure_ascii=False, default=str)


class Pantalla(logging.StreamHandler):
    # Escribe en el sys.stdout de cada momento, no en el que había al crearlo: si algo lo redirige, el log lo sigue.
    def __init__(self, nivel: int = logging.NOTSET):
        logging.Handler.__init__(self, nivel)

    @property
    def stream(self):
        return sys.stdout


def configurar_logs(anio: int, mes: int, directorio: Path = DIR_LOGS) -> Path:
    # Pantalla: eventos importantes (INFO en adelante). Archivo: todo, también el inicio de cada etapa.
    cerrar_logs()
    directorio.mkdir(parents=True, exist_ok=True)
    ruta = directorio / f"{anio}-{mes:02d}.jsonl"
    pantalla = Pantalla(logging.INFO)
    pantalla.setFormatter(FormatoClaveValor())
    archivo = logging.FileHandler(ruta, mode="w", encoding="utf-8")
    archivo.setFormatter(FormatoJson())
    LOGGER.setLevel(logging.DEBUG)
    LOGGER.propagate = False
    LOGGER.addHandler(pantalla)
    LOGGER.addHandler(archivo)
    return ruta


def cerrar_logs() -> None:
    for handler in list(LOGGER.handlers):
        LOGGER.removeHandler(handler)
        handler.close()


def registrar(nivel: int, evento: str, **campos) -> None:
    # Una línea de log: primero la etapa (si es de una etapa), después el evento y después los números.
    ordenados = {"etapa": campos.pop("etapa")} if "etapa" in campos else {}
    ordenados["evento"] = evento
    ordenados.update(campos)
    LOGGER.log(nivel, evento, extra={"campos": ordenados})


# --- una etapa -----------------------------------------------------------------------------------------

class MedidorEtapa:
    # Uso:  with MedidorEtapa("calidad") as m:  m.datos = {...}
    # Al salir deja el evento 'fin' (o 'error' si hubo una excepción) con lo medido y con lo que la etapa anotó en 'datos'.
    def __init__(self, nombre: str, acumular: list = None):
        self.nombre = nombre
        self.acumular = acumular      # lista donde dejar el resultado, para el resumen final
        self.datos = {}
        self.resultado = {}

    def __enter__(self):
        self._t0 = time.perf_counter()
        self._cpu0 = cpu_usada_s()
        self._pico = PicoDeMemoria().__enter__()
        registrar(logging.DEBUG, "inicio", etapa=self.nombre, ram_inicio_mb=round(ram_proceso_mb()))
        return self

    def __exit__(self, tipo, error, _traza):
        self._pico.__exit__()
        duracion = time.perf_counter() - self._t0
        cpu = cpu_usada_s() - self._cpu0
        self.resultado = {
            "etapa": self.nombre,
            "duracion_s": round(duracion, 2),
            "cpu_s": round(cpu, 2),
            "cpu_pct": round(100 * cpu / duracion) if duracion > 0 else 0,   # 100 = un núcleo ocupado todo el tiempo
            "ram_fin_mb": round(ram_proceso_mb()),
            "pico_etapa_mb": round(self._pico.pico_mb),
            **self.datos,
        }
        if error is None:
            registrar(logging.INFO, "fin", **self.resultado)
        else:
            registrar(logging.ERROR, "error", error=f"{tipo.__name__}: {error}", **self.resultado)
        if self.acumular is not None:
            self.acumular.append(self.resultado)       # también las que fallan: el resumen muestra hasta dónde llegó
        return False      # la excepción sigue su camino


# --- una corrida entera --------------------------------------------------------------------------------

def miles(n) -> str:
    return f"{n:,}".replace(",", ".")


def tabla_resumen(etapas: list, total: dict) -> str:
    # Una fila por etapa y una de total. 'filas' es lo que procesó o produjo cada etapa (ver README).
    encabezado = f"{'etapa':<10} {'duración':>9} {'%':>5} {'CPU':>8} {'RAM fin':>8} {'pico etapa':>11} {'filas':>12}"
    lineas = [encabezado, "-" * len(encabezado)]
    for e in etapas:
        porcentaje = 100 * e["duracion_s"] / total["duracion_s"] if total["duracion_s"] else 0
        filas = miles(e["filas"]) if e.get("filas") is not None else "-"
        lineas.append(f"{e['etapa']:<10} {e['duracion_s']:>7.1f} s {porcentaje:>4.0f}% {e['cpu_s']:>6.1f} s "
                      f"{e['ram_fin_mb']:>5} MB {e['pico_etapa_mb']:>8} MB {filas:>12}")
    lineas.append("-" * len(encabezado))
    lineas.append(f"{'total':<10} {total['duracion_s']:>7.1f} s {'':>5} {total['cpu_s']:>6.1f} s")
    lineas.append(f"pico de RAM: {total['pico_ram_mb']} MB de {total['ram_disponible_mb']} MB disponibles "
                  f"({total['entorno']})")
    return "\n".join(lineas)


class Corrida:
    # Lleva la cuenta de una corrida: cuándo empezó, qué etapas terminaron y con qué números.
    def __init__(self, anio: int, mes: int, directorio_logs: Path = DIR_LOGS):
        self.anio, self.mes = anio, mes
        self.directorio_logs = directorio_logs
        self.etapas = []

    def iniciar(self) -> Path:
        self._t0 = time.perf_counter()
        self._cpu0 = cpu_usada_s()
        ruta = configurar_logs(self.anio, self.mes, self.directorio_logs)
        registrar(logging.INFO, "corrida_inicio", mes=f"{self.anio}-{self.mes:02d}", entorno=entorno(),
                  python=sys.version.split()[0], nucleos=os.cpu_count(), ram_disponible_mb=round(ram_disponible_mb()),
                  pid=os.getpid(), log=str(ruta))
        return ruta

    def etapa(self, nombre: str) -> MedidorEtapa:
        return MedidorEtapa(nombre, acumular=self.etapas)

    def terminar(self, error=None) -> dict:
        total = {
            "duracion_s": round(time.perf_counter() - self._t0, 2),
            "cpu_s": round(cpu_usada_s() - self._cpu0, 2),
            "pico_ram_mb": max([round(pico_ram_mb())] + [e["pico_etapa_mb"] for e in self.etapas]),
            "ram_disponible_mb": round(ram_disponible_mb()),
            "entorno": entorno(),
        }
        print(f"\n== RESUMEN {self.anio}-{self.mes:02d} ==")
        print(tabla_resumen(self.etapas, total))
        if error is None:
            registrar(logging.INFO, "corrida_fin", estado="ok", etapas=len(self.etapas), **total)
        else:
            registrar(logging.ERROR, "corrida_fin", estado="error", etapas=len(self.etapas),
                      error=str(error), **total)
        cerrar_logs()
        return total
