"""Acquire the public, reproducible Bronze inputs."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import shutil

IBGE = "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Dados_percentuais_das_Caracteristicas_urbanisticas_do_entorno_dos_domicilios/"
DICTIONARY = "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios_Caracteristicas_urbanisticas_do_entorno_dos_domicilios/dicionarios_de_dados_entorno.zip"
WFS = "https://wfs.geosampa.prefeitura.sp.gov.br/geoserver/geoportal/wfs"
LAYERS = ("estacao_metro", "estacao_trem", "linha_metro", "linha_trem", "distrito_municipal")
SOURCES = {
    "entorno.csv": IBGE + "br_setores_entorno_cd2022(in).csv",
    "entorno.gpkg": IBGE + "br_setores_entorno_cd2022.gpkg",
    "dicionario.zip": DICTIONARY,
    **{
        layer + ".geojson": WFS + "?" + urlencode({
            "service": "WFS", "version": "2.0.0", "request": "GetFeature",
            "typeNames": "geoportal:" + layer, "outputFormat": "application/json",
            "srsName": "EPSG:4326",
        }) for layer in LAYERS
    },
}


def download(directory, name, url):
    target = directory / name
    temporary = target.with_suffix(target.suffix + ".download")
    request = Request(url, headers={"User-Agent": "Humberto-Remaster/1.0"})
    try:
        with urlopen(request, timeout=90) as response, temporary.open("wb") as output:
            shutil.copyfileobj(response, output)
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)
    print(f"{target.name}: {target.stat().st_size:,} bytes")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("data/bronze"))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    with ThreadPoolExecutor(max_workers=4) as pool:
        downloads = [pool.submit(download, args.output, name, url) for name, url in SOURCES.items()]
        for job in downloads:
            job.result()


if __name__ == "__main__":
    main()
