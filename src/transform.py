import json, logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

INPUT_PATH  = "data/anvisa_cmed.csv"
OUTPUT_PATH = "data/metrics.json"


def build_spark():
    return (SparkSession.builder
        .appName("ANVISA-CMED")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate())


def find_col(cols, keywords):
    """Retorna a primeira coluna cujo nome contenha alguma keyword."""
    for kw in keywords:
        for c in cols:
            if kw.upper() in str(c).upper():
                return c
    return None


def safe_metrics(df, col_produto, col_lab, col_preco):
    """Calcula métricas com tratamento de erros."""
    df = df.withColumn(
        "preco_pf",
        F.regexp_replace(F.col(col_preco), ",", ".").cast(DoubleType())
    ).filter(F.col("preco_pf").isNotNull() & (F.col("preco_pf") > 0))

    total = df.count()
    logger.info(f"Registros com preço válido: {total:,}")

    if total == 0:
        return {"totais": {"total_medicamentos": 0, "total_laboratorios": 0,
                           "preco_pf_medio": None, "preco_pf_maximo": None,
                           "preco_pf_minimo": None},
                "top10_mais_caros": [], "top10_laboratorios": [],
                "distribuicao_categoria": []}

    totais = df.agg(
        F.count("*").alias("total_medicamentos"),
        F.round(F.avg("preco_pf"), 2).alias("preco_pf_medio"),
        F.round(F.max("preco_pf"), 2).alias("preco_pf_maximo"),
        F.round(F.min("preco_pf"), 2).alias("preco_pf_minimo"),
        F.countDistinct(col_lab).alias("total_laboratorios"),
    ).collect()[0].asDict()

    top10_caros = (df.select(col_produto, col_lab, "preco_pf")
        .orderBy(F.desc("preco_pf")).limit(10)
        .toPandas().rename(columns={col_produto: "produto", col_lab: "laboratorio"})
        .to_dict(orient="records"))

    top10_labs = (df.groupBy(col_lab)
        .agg(F.count("*").alias("qtd_produtos"),
             F.round(F.avg("preco_pf"), 2).alias("preco_medio"))
        .orderBy(F.desc("qtd_produtos")).limit(10)
        .toPandas().rename(columns={col_lab: "laboratorio"})
        .to_dict(orient="records"))

    return {"totais": totais, "top10_mais_caros": top10_caros,
            "top10_laboratorios": top10_labs, "distribuicao_categoria": []}


if __name__ == "__main__":
    spark = build_spark()
    try:
        df = (spark.read.option("header", "true").option("sep", ";")
              .option("encoding", "UTF-8").option("multiLine", "true")
              .csv(INPUT_PATH))

        cols = df.columns
        logger.info(f"Total colunas: {len(cols)}")
        logger.info(f"Todas as colunas: {cols}")

        col_produto  = find_col(cols, ["PRODUTO"]) or cols[0]
        col_lab      = find_col(cols, ["LABORATÓRIO", "LABORATORIO", "LABORAT"]) or cols[1]
        col_preco    = find_col(cols, ["PF SEM", "PF 0%", "PF Sem", "PRECO", "PREÇO"]) or cols[2]

        logger.info(f"Mapeamento → produto={col_produto} | lab={col_lab} | preço={col_preco}")

        metrics = safe_metrics(df, col_produto, col_lab, col_preco)

        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(metrics, f, ensure_ascii=False, indent=2, default=str)

        logger.info(f"Métricas: {metrics['totais']}")
    finally:
        spark.stop()
