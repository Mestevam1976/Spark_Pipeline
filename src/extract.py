"""
extract.py
Baixa a tabela de preços CMED da ANVISA e salva em CSV.
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
KEYWORDS    = ["PRODUTO", "LABORAT", "SUBSTÂN", "CNPJ", "REGISTRO"]


def find_header_row(content: bytes) -> int:
    """
    Encontra a linha do header real: precisa ter keyword E pelo menos
    5 colunas nomeadas (elimina linhas de título com célula mesclada).
    """
    for skip in range(25):
        try:
            df_test = pd.read_excel(
                BytesIO(content), skiprows=skip,
                engine="openpyxl", nrows=2
            )
            cols     = [str(c) for c in df_test.columns]
            named    = [c for c in cols if not c.startswith("Unnamed") and c.strip()]
            cols_str = " ".join(c.upper() for c in named)

            has_keyword = any(kw in cols_str for kw in KEYWORDS)
            has_enough  = len(named) >= 5

            logger.info(f"skiprows={skip}: {len(named)} colunas nomeadas | keyword={has_keyword}")

            if has_keyword and has_enough:
                logger.info(f"Header encontrado em skiprows={skip} → {named[:6]}")
                return skip
        except Exception as e:
            logger.warning(f"skiprows={skip} erro: {e}")

    logger.warning("Header não encontrado — usando skiprows=0")
    return 0


def download_cmed(url: str, output: str) -> None:
    logger.info(f"Baixando CMED de {url}")
    response = requests.get(url, timeout=120, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()
    content = response.content

    skip = find_header_row(content)

    df = pd.read_excel(BytesIO(content), skiprows=skip, engine="openpyxl")
    df = df.dropna(how="all", axis=1).dropna(how="all", axis=0)
    df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]

    os.makedirs("data", exist_ok=True)
    df.to_csv(output, index=False, encoding="utf-8", sep=";")

    logger.info(f"Salvo: {len(df):,} registros, {len(df.columns)} colunas")
    logger.info(f"Colunas: {list(df.columns[:8])}")


if __name__ == "__main__":
    download_cmed(CMED_URL, OUTPUT_PATH)
