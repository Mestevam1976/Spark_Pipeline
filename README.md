# 📊 ANVISA CMED — Automated Spark Pipeline

![ANVISA Spark Pipeline](https://github.com/Mestevam1976/Spark_Pipeline/actions/workflows/pipeline.yml/badge.svg)

Pipeline automatizado de engenharia de dados que processa diariamente a tabela de preços de medicamentos da ANVISA (CMED), calcula métricas agregadas com PySpark e entrega notificações via Telegram e e-mail — sem intervenção manual.

---

## 🏗️ Arquitetura

```
GitHub Actions (cron 07:00 BRT)
  └─ extract.py   → Baixa tabela CMED da ANVISA (xlsx → csv)
  └─ transform.py → Processa com PySpark (métricas agregadas)
  └─ notify.py    → Payload JSON → Webhook n8n
                        ├─ Telegram (resumo diário)
                        └─ Gmail   (relatório completo)
```

**Fluxo técnico:**
`ANVISA (fonte pública)` → `Python/requests` → `PySpark` → `JSON payload` → `Webhook` → `n8n` → `Telegram + Gmail`

---

## ⚙️ Stack

| Camada | Tecnologia |
|---|---|
| Agendamento | GitHub Actions (cron) |
| Extração | Python + requests + pandas |
| Processamento | Apache PySpark 3.5 |
| Orquestração | n8n (self-hosted) |
| Notificação | Telegram Bot + Gmail |

---

## 📈 Métricas calculadas

- Total de medicamentos e laboratórios registrados
- Preço de Fábrica (PF): média, mínimo e máximo
- Top 10 medicamentos mais caros
- Top 10 laboratórios por volume de produtos
- Distribuição por categoria/tipo de produto
- Diferença média PF vs PMVG (margem governo)

---

## 🚀 Como executar localmente

```bash
# Clone
git clone https://github.com/Mestevam1976/portfolio-spark-pipeline.git
cd portfolio-spark-pipeline

# Dependências (requer Java 11+)
pip install -r requirements.txt

# Configure o webhook
cp .env.example .env
# edite .env com sua URL do n8n

# Execute o pipeline
python src/extract.py
python src/transform.py
python src/notify.py
```

---

## 🔐 Secrets (GitHub Actions)

Configure em `Settings → Secrets and variables → Actions`:

| Secret | Descrição |
|---|---|
| `N8N_WEBHOOK_URL` | URL do webhook no n8n self-hosted |

---

## 📂 Estrutura

```
portfolio-spark-pipeline/
├── .github/workflows/pipeline.yml   # Agendamento e steps
├── src/
│   ├── extract.py                   # Download ANVISA CMED
│   ├── transform.py                 # PySpark transformations
│   └── notify.py                    # Webhook → n8n
├── data/                            # Gerado em runtime (gitignored)
├── requirements.txt
├── .env.example
└── .gitignore
```

---

## 📋 Aprendizados

- ✅ Processamento distribuído com Apache PySpark
- ✅ Automação de pipelines com GitHub Actions
- ✅ Integração de APIs REST com Python (requests)
- ✅ Orquestração de workflows com n8n (self-hosted)
- ✅ Boas práticas de segurança (.env, .gitignore, GitHub Secrets)
- ✅ Tratamento de dados públicos brasileiros (ANVISA/CMED)

---

## 🔗 Sobre o autor

**Márcio Estevam** — [data-skywalker.com](https://data-skywalker.com) | [GitHub](https://github.com/Mestevam1976) | [LinkedIn](https://linkedin.com/in/marcioestevam)

*Analista de BI Pleno na RaiaDrogasil (RD) — Rio Claro, SP*
