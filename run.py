# Punto de entrada del pipeline: python run.py --mes 2025-03
import argparse
import sys

from src.e1_ingesta import parsear_mes
from src.pipeline import ejecutar


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Pipeline de viajes de taxi amarillo (TLC).")
    parser.add_argument("--mes", required=True, type=parsear_mes, help="mes asignado, formato AAAA-MM")
    args = parser.parse_args(argv)
    anio, mes = args.mes
    try:
        ejecutar(anio, mes)
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())