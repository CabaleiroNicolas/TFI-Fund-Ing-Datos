# Punto de entrada del pipeline: python run.py --mes 2025-03
import argparse
import sys

from src.e1_ingesta import parsear_mes
from src.pipeline import ejecutar


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Pipeline de viajes de taxi amarillo (TLC).")
    parser.add_argument("--mes", required=True, type=parsear_mes, help="mes asignado, formato AAAA-MM")
    parser.add_argument("--dagster", action="store_true",
                        help="correr las etapas a través de Dagster (su historial queda en data/dagster_home)")
    args = parser.parse_args(argv)
    anio, mes = args.mes
    try:
        if args.dagster:
            try:
                from src.orquestacion import ejecutar_con_dagster      # Dagster es opcional: solo se importa si se pide
            except ImportError:
                print("ERROR: --dagster necesita Dagster. Instalalo con: pip install -r requirements-dagster.txt",
                      file=sys.stderr)
                return 1
            ejecutar_con_dagster(anio, mes)
        else:
            ejecutar(anio, mes)
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())