import json, logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

INPUT_PATH  = "data/anvisa_cmed.csv"
OUTPUT_PATH = "data/metrics.json"

COL_PRODUTO   = "PRODUTO"
COL_LAB       = "LABORATÓRIO"
COL_PRECO_PF  = "PF Sem Impostos"
COL_PRECO_PMC = "PMC Sem Impostos"
COL_CATEGORIA = "TIPO DE PRODUTO (STATUS DO PRODUTO)"
COL_CLASSE    = "CLASSE TERAPÊUTICA"


def build_spark():
    return (SparkSession.builder
        .appName("ANVISA-CMED")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate())


def to_double(df, col_name):
    return df.withColumn(
        col_name + "_num",
        F.regexp_replace(F.col(col_name), ",", ".").cast(DoubleType())
    )


if __name__ == "__main__":
    spark = build_spark()
    try:
        df = (spark.read
              .option("header", "true")
              .option("sep", ";")
              .option("encoding", "UTF-8")
              .option("multiLine", "true")
              .csv(INPUT_PATH))

        logger.info(f"Total colunas: {len(df.columns)}")

        # Converte colunas de preço
        df = to_double(df, COL_PRECO_PF)
        df = to_double(df, COL_PRECO_PMC)

        # Filtra só registros com PF válido
        df_pf = df.filter(
            F.col(COL_PRECO_PF + "_num").isNotNull() &
            (F.col(COL_PRECO_PF + "_num") > 0)
        )
        total = df_pf.count()
        logger.info(f"Registros com PF válido: {total:,}")

        # 1 — Totais gerais
        totais = df_pf.agg(
            F.count("*").alias("total_medicamentos"),
            F.round(F.avg(COL_PRECO_PF + "_num"), 2).alias("preco_pf_medio"),
            F.round(F.max(COL_PRECO_PF + "_num"), 2).alias("preco_pf_maximo"),
            F.round(F.min(COL_PRECO_PF + "_num"), 2).alias("preco_pf_minimo"),
            F.countDistinct(COL_LAB).alias("total_laboratorios"),
        ).collect()[0].asDict()

        # 2 — Top 10 medicamentos mais caros (produto único)
        top10_caros = (
            df_pf.groupBy(COL_PRODUTO, COL_LAB)
            .agg(F.round(F.max(COL_PRECO_PF + "_num"), 2).alias("preco_pf"))
            .orderBy(F.desc("preco_pf"))
            .limit(10)
            .toPandas()
            .rename(columns={COL_PRODUTO: "produto", COL_LAB: "laboratorio"})
            .to_dict(orient="records")
        )

        # 3 — Top 10 laboratórios por volume
        top10_labs = (
            df_pf.groupBy(COL_LAB)
            .agg(
                F.count("*").alias("qtd_produtos"),
                F.round(F.avg(COL_PRECO_PF + "_num"), 2).alias("preco_medio")
            )
            .orderBy(F.desc("qtd_produtos"))
            .limit(10)
            .toPandas()
            .rename(columns={COL_LAB: "laboratorio"})
            .to_dict(orient="records")
        )

        # 4 — Distribuição por tipo de produto
        dist_categoria = (
            df_pf.groupBy(COL_CATEGORIA)
            .agg(F.count("*").alias("qtd"))
            .orderBy(F.desc("qtd"))
            .toPandas()
            .rename(columns={COL_CATEGORIA: "categoria"})
            .to_dict(orient="records")
        )

        # 5 — Distribuição por classe terapêutica (top 10)
        dist_classe = (
            df_pf.groupBy(COL_CLASSE)
            .agg(F.count("*").alias("qtd"))
            .orderBy(F.desc("qtd"))
            .limit(10)
            .toPandas()
            .rename(columns={COL_CLASSE: "classe"})
            .to_dict(orient="records")
        )

        # 6 — Margem PF vs PMC (diferença média)
        df_margem = df_pf.filter(
            F.col(COL_PRECO_PMC + "_num").isNotNull() &
            (F.col(COL_PRECO_PMC + "_num") > 0)
        ).withColumn(
            "margem_pf_pmc",
            F.round(
                (F.col(COL_PRECO_PMC + "_num") - F.col(COL_PRECO_PF + "_num")) /
                F.col(COL_PRECO_PF + "_num") * 100, 2
            )
        )
        margem = df_margem.agg(
            F.round(F.avg("margem_pf_pmc"), 2).alias("margem_media_pf_pmc_pct"),
            F.round(F.avg(COL_PRECO_PMC + "_num") - F.avg(COL_PRECO_PF + "_num"), 2)
              .alias("diferenca_media_reais")
        ).collect()[0].asDict()

        metrics = {
            "totais":               totais,
            "top10_mais_caros":     top10_caros,
            "top10_laboratorios":   top10_labs,
            "distribuicao_categoria": dist_categoria,
            "distribuicao_classe":  dist_classe,
            "margem_pf_vs_pmc":     margem,
        }

        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(metrics, f, ensure_ascii=False, indent=2, default=str)

        logger.info(f"Totais: {totais}")
        logger.info(f"Margem PF vs PMC: {margem}")
        logger.info("Métricas salvas com sucesso.")
    finally:
        spark.stop()
