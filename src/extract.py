import os, requests, pandas as pd, logging
from io import BytesIO
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CMED_PAGE   = "https://www.gov.br/anvisa/pt-br/assuntos/medicamentos/cmed/precos"
OUTPUT_PATH = "data/anvisa_cmed.csv"
SKIPROWS    = 41


def get_cmed_url() -> str:
    """Descobre automaticamente a URL do XLS mais recente na página da ANVISA."""
    logger.info(f"Buscando URL do CMED em {CMED_PAGE}")
    r = requests.get(CMED_PAGE, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.content, "html.parser")
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "xls_conformidade_site" in href and "@@download" in href:
            url = href if href.startswith("http") else "https://www.gov.br" + href
            logger.info(f"URL encontrada: {url}")
            return url

    raise ValueError("URL do CMED não encontrada na página da ANVISA — verifique o layout da página")


def download_cmed(url: str, output: str) -> None:
    logger.info(f"Baixando CMED de {url}")
    r = requests.get(url, timeout=120, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()

    df = pd.read_excel(BytesIO(r.content), skiprows=SKIPROWS, engine="openpyxl")
    df = df.dropna(how="all", axis=0)

    os.makedirs("data", exist_ok=True)
    df.to_csv(output, index=False, encoding="utf-8", sep=";")

    logger.info(f"Salvo: {len(df):,} registros | {len(df.columns)} colunas")
    logger.info(f"Colunas: {list(df.columns[:8])}")


if __name__ == "__main__":
    url = get_cmed_url()
    download_cmed(url, OUTPUT_PATH)
