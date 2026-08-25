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

# Mapeamento de colunas do CMED (ajuste se a ANVISA alterar o layout)
COL_PRODUTO    = "PRODUTO"
COL_LABORATORIO = "LABORATÓRIO"
COL_CATEGORIA  = "TIPO DE PRODUTO (STATUS DO PRODUTO)"
COL_PRECO_PF   = "PF Sem Impostos"    # Preço de Fábrica sem impostos
COL_PRECO_PMVG = "PMVG Sem Impostos"  # Preço Máximo Governo


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ANVISA-CMED-Pipeline")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")   # reduz overhead no runner
        .getOrCreate()
    )


def load_data(spark: SparkSession):
    logger.info(f"Carregando {INPUT_PATH}")
    df = (
        spark.read
        .option("header", "true")
        .option("sep", ";")
        .option("encoding", "utf-8-sig")
        .csv(INPUT_PATH)
    )
    logger.info(f"Schema: {df.dtypes}")
    return df


def clean(df):
    """Converte colunas de preço e remove nulos críticos."""
    df = df.withColumn(
        "preco_pf",
        F.regexp_replace(F.col(COL_PRECO_PF), ",", ".").cast(DoubleType())
    ).withColumn(
        "preco_pmvg",
        F.regexp_replace(F.col(COL_PRECO_PMVG), ",", ".").cast(DoubleType())
    )
    before = df.count()
    df = df.filter(F.col("preco_pf").isNotNull())
    after = df.count()
    logger.info(f"Registros após limpeza: {after:,} (removidos {before - after:,} sem preço PF)")
    return df


def compute_metrics(df) -> dict:
    logger.info("Calculando métricas…")

    # 1 — Totais gerais
    totais = df.agg(
        F.count("*").alias("total_medicamentos"),
        F.round(F.avg("preco_pf"), 2).alias("preco_pf_medio"),
        F.round(F.max("preco_pf"), 2).alias("preco_pf_maximo"),
        F.round(F.min("preco_pf"), 2).alias("preco_pf_minimo"),
        F.countDistinct(COL_LABORATORIO).alias("total_laboratorios"),
    ).collect()[0].asDict()

    # 2 — Top 10 medicamentos mais caros
    top10_caros = (
        df.select(COL_PRODUTO, COL_LABORATORIO, "preco_pf")
        .orderBy(F.desc("preco_pf"))
        .limit(10)
        .toPandas()
        .to_dict(orient="records")
    )

    # 3 — Top 10 laboratórios por volume de produtos
    top10_labs = (
        df.groupBy(COL_LABORATORIO)
        .agg(
            F.count("*").alias("qtd_produtos"),
            F.round(F.avg("preco_pf"), 2).alias("preco_medio")
        )
        .orderBy(F.desc("qtd_produtos"))
        .limit(10)
        .toPandas()
        .to_dict(orient="records")
    )

    # 4 — Distribuição por categoria de produto
    dist_categoria = (
        df.groupBy(COL_CATEGORIA)
        .agg(F.count("*").alias("qtd"))
        .orderBy(F.desc("qtd"))
        .toPandas()
        .to_dict(orient="records")
    )

    # 5 — Diferença PF vs PMVG (margem governo)
    margem = df.filter(F.col("preco_pmvg").isNotNull()).withColumn(
        "diferenca_pf_pmvg", F.round(F.col("preco_pf") - F.col("preco_pmvg"), 2)
    )
    margem_media = margem.agg(
        F.round(F.avg("diferenca_pf_pmvg"), 2).alias("margem_media_pf_vs_pmvg")
    ).collect()[0]["margem_media_pf_vs_pmvg"]

    return {
        "totais": totais,
        "top10_mais_caros": top10_caros,
        "top10_laboratorios": top10_labs,
        "distribuicao_categoria": dist_categoria,
        "margem_media_pf_vs_pmvg": margem_media,
    }


if __name__ == "__main__":
    spark = build_spark()
    try:
        df = load_data(spark)
        df = clean(df)
        metrics = compute_metrics(df)

        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(metrics, f, ensure_ascii=False, indent=2, default=str)

        logger.info(f"Métricas salvas em {OUTPUT_PATH}")
        logger.info(f"Resumo → {metrics['totais']}")
    finally:
        spark.stop()
