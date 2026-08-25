"""
notify.py
Lê data/metrics.json e dispara webhook para o n8n.
O n8n cuida de distribuir para Telegram + Gmail em paralelo.
"""

import json
import os
import sys
import requests
import logging
from datetime import datetime, timezone, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

METRICS_PATH = "data/metrics.json"
WEBHOOK_URL  = os.environ.get("N8N_WEBHOOK_URL", "")
BRT          = timezone(timedelta(hours=-3))


def load_metrics(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def build_payload(metrics: dict) -> dict:
    agora     = datetime.now(BRT).strftime("%d/%m/%Y %H:%M")
    totais    = metrics["totais"]
    top3      = metrics["top10_mais_caros"][:3]
    top3_labs = metrics["top10_laboratorios"][:3]

    # Mensagem resumida para Telegram
    telegram_msg = (
        f"📊 *ANVISA CMED — Relatório Diário*\n"
        f"🗓 {agora} (BRT)\n\n"
        f"*Totais*\n"
        f"• Medicamentos: {totais['total_medicamentos']:,}\n"
        f"• Laboratórios: {totais['total_laboratorios']:,}\n"
        f"• Preço PF médio: R$ {totais['preco_pf_medio']:.2f}\n"
        f"• Preço PF máximo: R$ {totais['preco_pf_maximo']:.2f}\n\n"
        f"*Top 3 mais caros*\n"
        + "\n".join(
            f"{i+1}. {r[list(r.keys())[0]]} — R$ {r['preco_pf']:.2f}"
            for i, r in enumerate(top3)
        )
        + f"\n\n*Top 3 labs por volume*\n"
        + "\n".join(
            f"{i+1}. {r[list(r.keys())[0]]} ({r['qtd_produtos']} produtos)"
            for i, r in enumerate(top3_labs)
        )
    )

    return {
        "timestamp":        agora,
        "fonte":            "ANVISA CMED",
        "pipeline":         "GitHub Actions → PySpark",
        "telegram_message": telegram_msg,
        "metrics":          metrics,     # payload completo para o Gmail
    }


def send_webhook(url: str, payload: dict) -> None:
    if not url:
        logger.error("N8N_WEBHOOK_URL não definido. Configure o secret no GitHub.")
        sys.exit(1)

    logger.info(f"Disparando webhook → {url}")
    resp = requests.post(
        url,
        json=payload,
        timeout=30,
        headers={"Content-Type": "application/json"},
    )
    resp.raise_for_status()
    logger.info(f"Webhook aceito — status {resp.status_code}: {resp.text[:200]}")


if __name__ == "__main__":
    metrics = load_metrics(METRICS_PATH)
    payload = build_payload(metrics)
    send_webhook(WEBHOOK_URL, payload)
