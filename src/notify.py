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


def fmt(value, decimals=2, prefix="") -> str:
    """Formata número com segurança — retorna '-' se None."""
    if value is None:
        return "-"
    try:
        return f"{prefix}{float(value):,.{decimals}f}"
    except (TypeError, ValueError):
        return str(value)


def build_payload(metrics: dict) -> dict:
    agora   = datetime.now(BRT).strftime("%d/%m/%Y %H:%M")
    totais  = metrics.get("totais", {})
    top3    = metrics.get("top10_mais_caros", [])[:3]
    top3_labs = metrics.get("top10_laboratorios", [])[:3]

    telegram_msg = (
        f"📊 *ANVISA CMED — Relatório Diário*\n"
        f"🗓 {agora} (BRT)\n\n"
        f"*Totais*\n"
        f"• Medicamentos: {totais.get('total_medicamentos', '-'):,}\n"
        f"• Laboratórios: {totais.get('total_laboratorios', '-')}\n"
        f"• Preço PF médio: R$ {fmt(totais.get('preco_pf_medio'))}\n"
        f"• Preço PF máximo: R$ {fmt(totais.get('preco_pf_maximo'))}\n"
    )

    if top3:
        telegram_msg += "\n*Top 3 mais caros*\n"
        for i, r in enumerate(top3):
            nome  = r.get("produto") or list(r.values())[0]
            preco = fmt(r.get("preco_pf"))
            telegram_msg += f"{i+1}. {nome} — R$ {preco}\n"

    if top3_labs:
        telegram_msg += "\n*Top 3 labs por volume*\n"
        for i, r in enumerate(top3_labs):
            lab = r.get("laboratorio") or list(r.values())[0]
            qtd = r.get("qtd_produtos", "-")
            telegram_msg += f"{i+1}. {lab} ({qtd} produtos)\n"

    return {
        "timestamp":          agora,
        "fonte":              "ANVISA CMED",
        "pipeline":           "GitHub Actions → PySpark",
        "telegram_chatid":    os.environ.get("TELEGRAM_CHAT_ID", "6383505618"),
        "telegram_message":   telegram_msg,
        "metrics":            metrics,
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
    logger.info(f"Métricas carregadas — totais: {metrics.get('totais')}")
    payload = build_payload(metrics)
    send_webhook(WEBHOOK_URL, payload)
