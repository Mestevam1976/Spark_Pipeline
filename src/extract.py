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
    Testa skiprows de 0 a 15 e retorna o primeiro que produz
    colunas com nomes reais (não 'Unnamed').
    """
    for skip in range(16):
        try:
            df_test = pd.read_excel(
                BytesIO(content),
                skiprows=skip,
                engine="openpyxl",
                nrows=2,
            )
            cols_str = " ".join(str(c).upper() for c in df_test.columns)
            if any(kw in cols_str for kw in KEYWORDS):
                logger.info(f"Header encontrado em skiprows={skip} | primeiras colunas: {list(df_test.columns[:5])}")
                return skip
        except Exception as e:
            logger.warning(f"skiprows={skip} falhou: {e}")
    logger.warning("Header não encontrado — usando skiprows=0")
    return 0


def download_cmed(url: str, output: str) -> None:
    logger.info(f"Baixando tabela CMED de {url}")
    response = requests.get(url, timeout=120, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()
    content = response.content

    skip = find_header_row(content)

    df = pd.read_excel(
        BytesIO(content),
        skiprows=skip,
        engine="openpyxl",
    )

    # Remove colunas e linhas totalmente vazias
    df = df.dropna(how="all", axis=1).dropna(how="all", axis=0)
    # Remove colunas Unnamed residuais
    df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]

    os.makedirs("data", exist_ok=True)
    df.to_csv(output, index=False, encoding="utf-8", sep=";")

    logger.info(f"Salvo em {output} — {len(df):,} registros, {len(df.columns)} colunas")
    logger.info(f"Colunas: {list(df.columns[:8])}")


if __name__ == "__main__":
    download_cmed(CMED_URL, OUTPUT_PATH)
