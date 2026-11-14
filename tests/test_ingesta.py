import hashlib

from src import e1_ingesta

CSV = b"a,b\n1,2\n3,4\n"


def test_idempotencia_no_vuelve_a_descargar(tmp_path, monkeypatch):
    llamadas = []

    def descarga_falsa(url, destino):
        llamadas.append(url)
        destino.parent.mkdir(parents=True, exist_ok=True)
        destino.write_bytes(CSV)
        return len(CSV), hashlib.sha256(CSV).hexdigest()

    monkeypatch.setattr(e1_ingesta, "descargar_con_reintentos", descarga_falsa)
    con = e1_ingesta.conectar(tmp_path / "test.duckdb")
    destino = tmp_path / "datos.csv"

    primera = e1_ingesta.ingestar_archivo(con, "http://falsa/datos.csv", destino)
    segunda = e1_ingesta.ingestar_archivo(con, "http://falsa/datos.csv", destino)

    assert primera == "descargado"
    assert segunda == "omitido"
    assert len(llamadas) == 1
    assert destino.read_bytes() == CSV