"""
extract.py
Baixa a tabela de preços de medicamentos da ANVISA (CMED)
Fonte: https://www.gov.br/anvisa/pt-br/assuntos/medicamentos/cmed/precos
"""

import os
import requests
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# URL da tabela CMED — verifique a versão mais recente em:
# https://www.gov.br/anvisa/pt-br/assuntos/medicamentos/cmed/precos
CMED_URL = (
    "https://www.gov.br/anvisa/pt-br/assuntos/medicamentos/cmed/precos"
    "/@@download/file/Conformidades_e_DCB_28_06_2024.xlsx"
)

OUTPUT_PATH = "data/anvisa_cmed.csv"


def download_cmed(url: str, output: str) -> None:
    logger.info(f"Baixando tabela CMED de {url}")

    response = requests.get(url, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()

    # O arquivo da ANVISA vem em .xlsx — converte para CSV
    from io import BytesIO
    df = pd.read_excel(
        BytesIO(response.content),
        skiprows=0,       # ajuste se houver cabeçalho extra
        engine="openpyxl"
    )

    os.makedirs("data", exist_ok=True)
    df.to_csv(output, index=False, encoding="utf-8-sig", sep=";")

    logger.info(f"Dados salvos em {output} — {len(df):,} registros, {len(df.columns)} colunas")
    logger.info(f"Colunas disponíveis: {list(df.columns)}")


if __name__ == "__main__":
    download_cmed(CMED_URL, OUTPUT_PATH)
