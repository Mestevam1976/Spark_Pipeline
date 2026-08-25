"""
notify.py
Lê data/metrics.json e dispara webhook para o n8n.
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


def safe(value, decimals=None, prefix="") -> str:
    """Converte qualquer valor para string com segurança."""
    if value is None:
        return "-"
    try:
        v = float(value)
        if decimals is not None:
            return f"{prefix}{v:,.{decimals}f}"
        return f"{prefix}{int(v):,}"
    except (TypeError, ValueError):
        return str(value) if value else "-"


def load_metrics(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def build_payload(metrics: dict) -> dict:
    agora     = datetime.now(BRT).strftime("%d/%m/%Y %H:%M")
    totais    = metrics.get("totais") or {}
    top3      = (metrics.get("top10_mais_caros") or [])[:3]
    top3_labs = (metrics.get("top10_laboratorios") or [])[:3]

    total_meds = safe(totais.get("total_medicamentos"))
    total_labs = safe(totais.get("total_laboratorios"))
    preco_med  = safe(totais.get("preco_pf_medio"), decimals=2, prefix="R$ ")
    preco_max  = safe(totais.get("preco_pf_maximo"), decimals=2, prefix="R$ ")

    linhas = [
        "📊 *ANVISA CMED — Relatório Diário*",
        "🗓 " + agora + " (BRT)",
        "",
        "*Totais*",
        "• Medicamentos: " + total_meds,
        "• Laboratórios: " + total_labs,
        "• Preço PF médio: " + preco_med,
        "• Preço PF máximo: " + preco_max,
    ]

    if top3:
        linhas += ["", "*Top 3 mais caros*"]
        for i, r in enumerate(top3):
            nome  = str(r.get("produto") or list(r.values())[0] or "-")
            preco = safe(r.get("preco_pf"), decimals=2, prefix="R$ ")
            linhas.append(str(i + 1) + ". " + nome + " — " + preco)

    if top3_labs:
        linhas += ["", "*Top 3 labs por volume*"]
        for i, r in enumerate(top3_labs):
            lab = str(r.get("laboratorio") or list(r.values())[0] or "-")
            qtd = safe(r.get("qtd_produtos"))
            linhas.append(str(i + 1) + ". " + lab + " (" + qtd + " produtos)")

    telegram_msg = "\n".join(linhas)

    return {
        "timestamp":        agora,
        "fonte":            "ANVISA CMED",
        "pipeline":         "GitHub Actions → PySpark",
        "telegram_chatid":  os.environ.get("TELEGRAM_CHAT_ID", "6383505618"),
        "telegram_message": telegram_msg,
        "metrics":          metrics,
    }


def send_webhook(url: str, payload: dict) -> None:
    if not url:
        logger.error("N8N_WEBHOOK_URL não definido.")
        sys.exit(1)
    logger.info("Disparando webhook → " + url)
    resp = requests.post(
        url,
        json=payload,
        timeout=30,
        headers={"Content-Type": "application/json"},
    )
    resp.raise_for_status()
    logger.info("Webhook aceito — status " + str(resp.status_code))


if __name__ == "__main__":
    metrics = load_metrics(METRICS_PATH)
    logger.info("Totais: " + str(metrics.get("totais")))
    payload = build_payload(metrics)
    send_webhook(WEBHOOK_URL, payload)
