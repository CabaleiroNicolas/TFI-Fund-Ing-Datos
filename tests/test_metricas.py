# Tests de src/metricas.py: el formato de las líneas, lo que mide cada etapa y el resumen final.
import json
import logging
import time

import psutil
import pytest

from src import metricas


@pytest.fixture
def logs(tmp_path):
    # Configura el log en una carpeta temporal y lo cierra al terminar el test.
    ruta = metricas.configurar_logs(2025, 3, tmp_path)
    yield ruta
    metricas.cerrar_logs()


def lineas_json(ruta):
    return [json.loads(linea) for linea in ruta.read_text(encoding="utf-8").splitlines()]


def test_los_valores_con_espacios_comillas_o_signo_igual_van_entre_comillas():
    assert metricas._valor("ok") == "ok"
    assert metricas._valor(9.6) == "9.6"
    assert metricas._valor("dos palabras") == '"dos palabras"'
    assert metricas._valor('dijo "hola"') == '"dijo \\"hola\\""'
    assert metricas._valor("a=b") == '"a=b"'
    assert metricas._valor("") == '""'


def test_una_linea_sale_por_pantalla_como_clave_valor_y_al_archivo_como_json(capsys, logs):
    metricas.registrar(logging.INFO, "fin", etapa="calidad", duracion_s=1.5, error="se rompió todo")
    linea = capsys.readouterr().out.strip()
    assert linea.startswith("ts=") and " level=INFO etapa=calidad evento=fin duracion_s=1.5 " in linea
    assert linea.endswith('error="se rompió todo"')
    (evento,) = lineas_json(logs)
    assert (evento["level"], evento["etapa"], evento["evento"], evento["duracion_s"]) == ("INFO", "calidad", "fin", 1.5)
    assert evento["error"] == "se rompió todo"
    assert list(evento)[:4] == ["ts", "level", "etapa", "evento"]


def test_los_eventos_de_nivel_debug_van_solo_al_archivo(capsys, logs):
    metricas.registrar(logging.DEBUG, "inicio", etapa="curado")
    assert capsys.readouterr().out == ""
    assert lineas_json(logs)[0]["evento"] == "inicio"


def test_configurar_dos_veces_no_duplica_las_lineas(tmp_path, capsys):
    metricas.configurar_logs(2025, 3, tmp_path)
    ruta = metricas.configurar_logs(2025, 3, tmp_path)
    metricas.registrar(logging.INFO, "fin", etapa="x")
    metricas.cerrar_logs()
    assert capsys.readouterr().out.count("evento=fin") == 1
    assert len(lineas_json(ruta)) == 1
    assert ruta.name == "2025-03.jsonl"


def test_una_etapa_que_termina_bien_mide_tiempo_y_deja_lo_que_anoto(capsys, logs):
    acumulado = []
    with metricas.MedidorEtapa("calidad", acumular=acumulado) as medidor:
        time.sleep(0.2)
        medidor.datos = {"filas": 1234, "aceptadas": 1200}
    resultado = acumulado[0]
    assert resultado["etapa"] == "calidad" and 0.2 <= resultado["duracion_s"] < 1
    assert resultado["filas"] == 1234 and resultado["aceptadas"] == 1200
    assert resultado["ram_fin_mb"] > 0 and resultado["pico_etapa_mb"] > 0
    eventos = lineas_json(logs)
    assert [(e["level"], e["evento"]) for e in eventos] == [("DEBUG", "inicio"), ("INFO", "fin")]
    assert "etapa=calidad evento=fin" in capsys.readouterr().out


def test_dormir_casi_no_gasta_cpu_y_calcular_si(logs):
    with metricas.MedidorEtapa("dormir") as dormir:
        time.sleep(0.3)
    with metricas.MedidorEtapa("calcular") as calcular:
        fin = time.perf_counter() + 0.3
        while time.perf_counter() < fin:
            pass
    # Dormir no es cero (el muestreador de memoria también gasta un poco), pero tiene que quedar muy por debajo de calcular.
    assert calcular.resultado["cpu_pct"] >= 80 and calcular.resultado["cpu_s"] >= 0.2
    assert dormir.resultado["cpu_pct"] < calcular.resultado["cpu_pct"] / 2


def test_una_etapa_que_falla_deja_un_evento_de_error_y_la_excepcion_sigue(logs):
    acumulado = []
    with pytest.raises(ValueError, match="se rompió"):
        with metricas.MedidorEtapa("modelo", acumular=acumulado) as medidor:
            medidor.datos = {"filas": 5}
            raise ValueError("se rompió")
    error = lineas_json(logs)[-1]
    assert (error["level"], error["evento"], error["etapa"]) == ("ERROR", "error", "modelo")
    assert error["error"] == "ValueError: se rompió" and error["filas"] == 5
    assert acumulado and acumulado[0]["etapa"] == "modelo"          # el resumen final también la incluye


def test_el_pico_de_memoria_ve_lo_que_se_reserva_durante_el_bloque():
    base = metricas.ram_proceso_mb()
    with metricas.PicoDeMemoria(intervalo=0.01) as pico:
        reserva = bytearray(200 * 1024 * 1024)
        reserva[::4096] = b"x" * len(reserva[::4096])           # tocar las páginas para que cuenten
        time.sleep(0.2)
        del reserva
    assert pico.pico_mb >= base + 150


def test_el_pico_suma_la_memoria_de_los_procesos_hijos():
    import subprocess
    import sys
    hijo = ("import time; b = bytearray(150 * 1024 * 1024); b[::4096] = b'x' * len(b[::4096]); "
            "print('listo', flush=True); time.sleep(1)")
    base = metricas.ram_proceso_mb()
    with metricas.PicoDeMemoria(intervalo=0.01, buscar_hijos_cada=0.01) as pico:
        proceso = subprocess.Popen([sys.executable, "-c", hijo], stdout=subprocess.PIPE, text=True)
        proceso.stdout.readline()                      # el hijo ya reservó
        time.sleep(0.2)
        proceso.wait()
    assert pico.pico_mb >= base + 120


def test_la_memoria_disponible_es_el_tope_del_contenedor_si_existe(tmp_path):
    tope = tmp_path / "memory.max"
    tope.write_text("1073741824\n")
    assert metricas.ram_disponible_mb(tope) == 1024
    tope.write_text("max\n")                                   # sin tope: vale la RAM de la máquina
    assert metricas.ram_disponible_mb(tope) == psutil.virtual_memory().total / metricas.MB
    assert metricas.ram_disponible_mb(tmp_path / "no_existe") == psutil.virtual_memory().total / metricas.MB


def test_un_tope_de_contenedor_mayor_que_la_ram_no_la_agranda(tmp_path):
    tope = tmp_path / "memory.max"
    tope.write_text(str(psutil.virtual_memory().total * 10))
    assert metricas.ram_disponible_mb(tope) == psutil.virtual_memory().total / metricas.MB


def test_el_entorno_se_reconoce_por_el_archivo_que_deja_docker(tmp_path):
    marca = tmp_path / ".dockerenv"
    assert metricas.entorno(marca) == "local"
    marca.touch()
    assert metricas.entorno(marca) == "docker"


ETAPAS = [
    {"etapa": "ingesta", "duracion_s": 10.0, "cpu_s": 4.0, "ram_fin_mb": 800, "pico_etapa_mb": 900, "filas": 6585069},
    {"etapa": "esquema", "duracion_s": 0.0, "cpu_s": 0.0, "ram_fin_mb": 801, "pico_etapa_mb": 801},
    {"etapa": "modelo", "duracion_s": 30.0, "cpu_s": 31.5, "ram_fin_mb": 700, "pico_etapa_mb": 3100, "filas": 13735570},
]
TOTAL = {"duracion_s": 40.0, "cpu_s": 35.5, "pico_ram_mb": 3100, "ram_disponible_mb": 6144, "entorno": "docker"}


def test_la_tabla_resumen_tiene_una_fila_por_etapa_y_el_total():
    lineas = metricas.tabla_resumen(ETAPAS, TOTAL).splitlines()
    assert lineas[0].split() == ["etapa", "duración", "%", "CPU", "RAM", "fin", "pico", "etapa", "filas"]
    assert lineas[2].split() == ["ingesta", "10.0", "s", "25%", "4.0", "s", "800", "MB", "900", "MB", "6.585.069"]
    assert lineas[3].split()[-1] == "MB" or lineas[3].split()[-1] == "-"
    assert lineas[3].split()[0] == "esquema" and lineas[3].split()[-1] == "-"       # sin filas: un guion
    assert lineas[4].split()[:3] == ["modelo", "30.0", "s"] and lineas[4].split()[3] == "75%"
    assert lineas[6].split() == ["total", "40.0", "s", "35.5", "s"]
    assert lineas[7] == "pico de RAM: 3100 MB de 6144 MB disponibles (docker)"


def test_el_pico_de_la_corrida_incluye_lo_medido_en_las_etapas(tmp_path, capsys):
    corrida = metricas.Corrida(2025, 3, directorio_logs=tmp_path)
    corrida.iniciar()
    corrida.etapas.extend(ETAPAS)
    total = corrida.terminar()
    assert total["pico_ram_mb"] >= 3100
    salida = capsys.readouterr().out
    assert "== RESUMEN 2025-03 ==" in salida and "evento=corrida_fin estado=ok etapas=3" in salida
    assert lineas_json(tmp_path / "2025-03.jsonl")[-1]["evento"] == "corrida_fin"


def test_si_la_corrida_falla_el_evento_final_lleva_el_error(tmp_path, capsys):
    corrida = metricas.Corrida(2025, 3, directorio_logs=tmp_path)
    corrida.iniciar()
    corrida.terminar(RuntimeError("falló la etapa 'calidad': x"))
    final = lineas_json(tmp_path / "2025-03.jsonl")[-1]
    assert (final["level"], final["estado"], final["error"]) == ("ERROR", "error", "falló la etapa 'calidad': x")
    assert "estado=error" in capsys.readouterr().out


def test_una_etapa_con_cero_filas_muestra_un_cero_y_no_un_guion():
    etapa = {"etapa": "curado", "duracion_s": 1.0, "cpu_s": 1.0, "ram_fin_mb": 10, "pico_etapa_mb": 10, "filas": 0}
    fila = metricas.tabla_resumen([etapa], TOTAL).splitlines()[2]
    assert fila.split()[-1] == "0"


def test_las_lineas_del_pipeline_no_se_repiten_en_el_logger_raiz(tmp_path):
    # Si otra librería (dlt, por ejemplo) configura el logger raíz, nuestras líneas no deben salir dos veces.
    recibidos = []

    class Espia(logging.Handler):
        def emit(self, registro):
            recibidos.append(registro)

    espia = Espia()
    logging.getLogger().addHandler(espia)
    try:
        metricas.configurar_logs(2025, 3, tmp_path)
        metricas.registrar(logging.INFO, "fin", etapa="x")
    finally:
        metricas.cerrar_logs()
        logging.getLogger().removeHandler(espia)
    assert recibidos == []
