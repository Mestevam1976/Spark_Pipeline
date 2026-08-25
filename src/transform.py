"""
transform.py
Processa a tabela CMED com PySpark e gera métricas agregadas.
Resultado salvo em data/metrics.json para o notify.py consumir.
"""

import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

INPUT_PATH  = "data/anvisa_cmed.csv"
OUTPUT_PATH = "data/metrics.json"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ANVISA-CMED-Pipeline")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def load_data(spark: SparkSession):
    logger.info(f"Carregando {INPUT_PATH}")
    df = (
        spark.read
        .option("header", "true")
        .option("sep", ";")
        .option("encoding", "UTF-8")          # PySpark não aceita utf-8-sig
        .option("multiLine", "true")
        .csv(INPUT_PATH)
    )
    logger.info(f"Colunas: {df.columns[:10]}")
    return df


def find_price_col(columns: list, keywords: list) -> str:
    """Encontra coluna de preço por palavras-chave parciais."""
    cols_upper = {c: c.upper() for c in columns}
    for kw in keywords:
        for original, upper in cols_upper.items():
            if kw.upper() in upper:
                return original
    return None


def clean(df, col_preco_pf: str):
    """Converte coluna de preço e remove nulos."""
    df = df.withColumn(
        "preco_pf",
        F.regexp_replace(F.col(col_preco_pf), ",", ".").cast(DoubleType())
    )
    before = df.count()
    df = df.filter(F.col("preco_pf").isNotNull() & (F.col("preco_pf") > 0))
    after = df.count()
    logger.info(f"Registros após limpeza: {after:,} (removidos {before - after:,})")
    return df


def compute_metrics(df, col_produto: str, col_lab: str, col_categoria: str) -> dict:
    logger.info("Calculando métricas…")

    totais = df.agg(
        F.count("*").alias("total_medicamentos"),
        F.round(F.avg("preco_pf"), 2).alias("preco_pf_medio"),
        F.round(F.max("preco_pf"), 2).alias("preco_pf_maximo"),
        F.round(F.min("preco_pf"), 2).alias("preco_pf_minimo"),
        F.countDistinct(col_lab).alias("total_laboratorios"),
    ).collect()[0].asDict()

    top10_caros = (
        df.select(col_produto, col_lab, "preco_pf")
        .orderBy(F.desc("preco_pf"))
        .limit(10)
        .toPandas()
        .rename(columns={col_produto: "produto", col_lab: "laboratorio"})
        .to_dict(orient="records")
    )

    top10_labs = (
        df.groupBy(col_lab)
        .agg(
            F.count("*").alias("qtd_produtos"),
            F.round(F.avg("preco_pf"), 2).alias("preco_medio")
        )
        .orderBy(F.desc("qtd_produtos"))
        .limit(10)
        .toPandas()
        .rename(columns={col_lab: "laboratorio"})
        .to_dict(orient="records")
    )

    dist_categoria = []
    if col_categoria:
        dist_categoria = (
            df.groupBy(col_categoria)
            .agg(F.count("*").alias("qtd"))
            .orderBy(F.desc("qtd"))
            .toPandas()
            .rename(columns={col_categoria: "categoria"})
            .to_dict(orient="records")
        )

    return {
        "totais": totais,
        "top10_mais_caros": top10_caros,
        "top10_laboratorios": top10_labs,
        "distribuicao_categoria": dist_categoria,
    }


if __name__ == "__main__":
    spark = build_spark()
    try:
        df = load_data(spark)
        cols = df.columns

        # Detecta colunas por palavras-chave — resiliente a mudanças de layout
        col_produto   = find_price_col(cols, ["PRODUTO"]) or cols[3]
        col_lab       = find_price_col(cols, ["LABORATÓRIO", "LABORATORIO"]) or cols[4]
        col_preco_pf  = find_price_col(cols, ["PF SEM", "PF 0%", "PF Sem"]) or cols[10]
        col_categoria = find_price_col(cols, ["TIPO", "CATEGORIA", "STATUS"])

        logger.info(f"Colunas mapeadas → produto: {col_produto} | lab: {col_lab} | preço: {col_preco_pf}")

        df = clean(df, col_preco_pf)
        metrics = compute_metrics(df, col_produto, col_lab, col_categoria)

        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(metrics, f, ensure_ascii=False, indent=2, default=str)

        logger.info(f"Métricas salvas → {metrics['totais']}")
    finally:
        spark.stop()
