import os, requests, pandas as pd, logging
from io import BytesIO

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CMED_URL    = "https://www.gov.br/anvisa/pt-br/assuntos/medicamentos/cmed/precos/arquivos/xls_conformidade_site_20260811_192510234.xlsx/@@download/file"
OUTPUT_PATH = "data/anvisa_cmed.csv"

def download_cmed(url, output):
    logger.info(f"Baixando CMED de {url}")
    r = requests.get(url, timeout=120, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()

    df_raw = pd.read_excel(BytesIO(r.content), header=None, engine="openpyxl")
    logger.info(f"Shape bruto: {df_raw.shape}")

    for i in range(min(25, len(df_raw))):
        vals = [str(v) for v in df_raw.iloc[i].tolist() if str(v) != 'nan']
        logger.info(f"Linha {i:02d}: {vals[:8]}")

    os.makedirs("data", exist_ok=True)
    df_raw.to_csv(output, index=False, header=False, encoding="utf-8", sep=";")
    logger.info("Diagnóstico salvo.")

if __name__ == "__main__":
    download_cmed(CMED_URL, OUTPUT_PATH)
