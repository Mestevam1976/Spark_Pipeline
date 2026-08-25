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
    content = r.content

    # Escolhe o skiprows com mais colunas nomeadas
    best_skip, best_count = 0, 0
    for skip in range(25):
        try:
            df_t = pd.read_excel(BytesIO(content), skiprows=skip, engine="openpyxl", nrows=1)
            named = sum(1 for c in df_t.columns if not str(c).startswith("Unnamed"))
            logger.info(f"skiprows={skip} → {named} colunas nomeadas")
            if named > best_count:
                best_count, best_skip = named, skip
            if named >= 10:
                break
        except Exception as e:
            logger.warning(f"skiprows={skip} erro: {e}")

    logger.info(f"Usando skiprows={best_skip} ({best_count} colunas nomeadas)")
    df = pd.read_excel(BytesIO(content), skiprows=best_skip, engine="openpyxl")

    # Remove apenas linhas completamente vazias — mantém TODAS as colunas
    df = df.dropna(how="all", axis=0)

    # Renomeia colunas Unnamed usando valor da primeira linha não-nula como fallback
    new_cols = []
    for i, col in enumerate(df.columns):
        if str(col).startswith("Unnamed"):
            new_cols.append(f"COL_{i:03d}")
        else:
            new_cols.append(str(col).strip())
    df.columns = new_cols

    os.makedirs("data", exist_ok=True)
    df.to_csv(output, index=False, encoding="utf-8", sep=";")
    logger.info(f"Salvo: {len(df):,} registros | {len(df.columns)} colunas")
    logger.info(f"Primeiras colunas: {list(df.columns[:10])}")

if __name__ == "__main__":
    download_cmed(CMED_URL, OUTPUT_PATH)
