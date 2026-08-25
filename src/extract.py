"""
extract.py
Baixa a tabela de preços de medicamentos da ANVISA (CMED)
Fonte: https://www.gov.br/anvisa/pt-br/assuntos/medicamentos/cmed/precos
"""

import os
import requests
import pandas as pd
import logging
from io import BytesIO

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CMED_URL = (
    "https://www.gov.br/anvisa/pt-br/assuntos/medicamentos/cmed/precos"
    "/arquivos/xls_conformidade_site_20260811_192510234.xlsx/@@download/file"
)

OUTPUT_PATH = "data/anvisa_cmed.csv"


def find_header_row(content: bytes) -> int:
    """Detecta a linha real do cabeçalho buscando 'PRODUTO' ou 'LABORATÓRIO'."""
    df_raw = pd.read_excel(BytesIO(content), header=None, engine="openpyxl", nrows=20)
    for i, row in df_raw.iterrows():
        valores = row.astype(str).str.upper().tolist()
        if any("PRODUTO" in v or "LABORAT" in v or "SUBSTÂNCIA" in v for v in valores):
            logger.info(f"Cabeçalho encontrado na linha {i}")
            return i
    logger.warning("Cabeçalho não encontrado nas primeiras 20 linhas — usando linha 0")
    return 0


def download_cmed(url: str, output: str) -> None:
    logger.info(f"Baixando tabela CMED de {url}")

    response = requests.get(url, timeout=120, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()
    content = response.content

    header_row = find_header_row(content)

    df = pd.read_excel(
        BytesIO(content),
        skiprows=header_row,
        engine="openpyxl"
    )

    # Remove colunas e linhas totalmente vazias
    df = df.dropna(how="all", axis=1).dropna(how="all", axis=0)

    os.makedirs("data", exist_ok=True)
    df.to_csv(output, index=False, encoding="utf-8", sep=";")

    logger.info(f"Dados salvos em {output} — {len(df):,} registros, {len(df.columns)} colunas")
    logger.info(f"Colunas: {list(df.columns[:10])}")


if __name__ == "__main__":
    download_cmed(CMED_URL, OUTPUT_PATH)
