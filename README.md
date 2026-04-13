# Parkinson BR — Infraestrutura de Neuroimagem

> **Dashboard ao vivo →** [leonardochalhoub.github.io/Parkinson-BR-Stats](https://leonardochalhoub.github.io/Parkinson-BR-Stats/)

Distribuição de equipamentos de Ressonância Magnética (RM) no Brasil por UF, setor (SUS vs. Privado) e ano — de janeiro de 2005 a dezembro de 2025 — com base nos microdados mensais do DATASUS/CNES.

O dashboard é uma aplicação estática de página única (zero backend) publicada no GitHub Pages. Os dados são produzidos por um pipeline local em PySpark com arquitetura lakehouse e exportados como um único arquivo JSON.

---

## Visão geral

O Brasil apresenta uma das distribuições mais desiguais de infraestrutura de neuroimagem do mundo. Este projeto quantifica essa disparidade: quantos equipamentos de RM existem por estado, quantos estão disponíveis pelo sistema público (SUS) e como essa diferença evoluiu ao longo de 20 anos.

A análise apoia pesquisas em andamento sobre as barreiras ao diagnóstico precoce da Doença de Parkinson no Brasil.

**Destaques (2025 — média mensal de equipamentos):**

| Indicador | Valor |
|---|---|
| Total de RMs no Brasil | ~10.080 |
| São Paulo | ~3.054 (30%) |
| Top 3 estados (SP + RJ + MG) | ~56% do total nacional |
| Roraima (menor valor) | ~12 |

---

## Arquitetura

```
FTP DATASUS                   API IBGE SIDRA
    │                               │
    ▼                               ▼
┌─────────────────────────────────────────┐
│             Bronze  (Delta Lake)        │
│   lakehouse/bronze/cnes_eq              │
│   lakehouse/bronze/ibge/sidra_...       │
└──────────────────┬──────────────────────┘
                   │  PySpark
                   ▼
┌─────────────────────────────────────────┐
│             Silver  (Delta Lake)        │
│   lakehouse/silver/mri_br               │
│   lakehouse/silver/population           │
└──────────────────┬──────────────────────┘
                   │  exporta JSON
                   ▼
        docs/data/mri_br_state_year.json
                   │
                   ▼
        GitHub Pages  (SPA estática)
        app/web/  →  dashboard Plotly.js
```

---

## Estrutura do repositório

```
Parkinson-BR-Stats/
├── app/
│   ├── src/                        # Pipeline PySpark
│   │   ├── bronze/
│   │   │   ├── cnes_eq.py                           # DBC → Delta (CNES EQ)
│   │   │   └── cnes_eq_download_all_from_source.py  # Download FTP
│   │   └── silver/
│   │       ├── common.py                            # SparkSession + caminhos
│   │       ├── mri_br.py                            # Indicadores de RM por UF × ano
│   │       └── population.py                        # Painel populacional IBGE
│   └── web/                        # Fonte do dashboard (SPA estática)
│       ├── index.html
│       ├── app.js
│       ├── styles.css
│       ├── brazil-states.geojson
│       ├── Dockerfile              # Servidor nginx para self-hosting
│       ├── counters/               # Cloudflare Worker (contador de visitas)
│       └── data/                   # gitignored — gerado pelo pipeline
├── docs/                           # GitHub Pages (espelho de app/web/)
│   └── data/
│       └── mri_br_state_year.json  # commitado — servido ao navegador
├── reference/                      # Dicionários de dados (XLSX)
│   ├── CNES_EQ_Column_Dictionary.xlsx
│   └── CNES_EQ_Data_Dictionary_Complete.xlsx
└── lakehouse/                      # gitignored — tabelas Delta locais
```

---

## Pré-requisitos

| Ferramenta | Versão |
|---|---|
| Python | ≥ 3.10 |
| Java | ≥ 11 (exigido pelo Spark) |
| Apache Spark | 3.x (instalado automaticamente via `delta-spark`) |
| pysus | mais recente (`pip install pysus`) |
| delta-spark | ≥ 3.3 |

```bash
python -m venv .venv && source .venv/bin/activate
pip install pyspark delta-spark pysus pandas dbfread
```

---

## Executando o pipeline

### 1 — Download dos arquivos brutos do FTP DATASUS

Baixa todos os arquivos `EQ??AAMM.dbc` (~6.600 arquivos, 27 estados × 246 meses). Idempotente — ignora arquivos já existentes em disco.

```bash
python app/src/bronze/cnes_eq_download_all_from_source.py
# saída: data/cnes_eq/*.dbc
```

### 2 — Bronze: ingestão dos arquivos DBC no Delta Lake

```bash
PYTHONPATH=. python app/src/bronze/cnes_eq.py
# saída: lakehouse/bronze/cnes_eq/  (particionado por estado → ano → mes)
```

### 3 — Silver: painel populacional (IBGE SIDRA)

```bash
PYTHONPATH=. python app/src/silver/population.py
# saída: lakehouse/silver/population/
```

### 4 — Silver: indicadores de RM e exportação do JSON

```bash
PYTHONPATH=. python app/src/silver/mri_br.py
# saída: lakehouse/silver/mri_br/
#        app/web/data/mri_br_state_year.json  (desenvolvimento local)
#        docs/data/mri_br_state_year.json     (GitHub Pages)
```

### 5 — Servir o dashboard localmente

Qualquer servidor de arquivos estáticos funciona:

```bash
cd app/web
python -m http.server 8000
# acesse http://localhost:8000
```

Ou com Docker:

```bash
# Execute o passo 4 antes para gerar app/web/data/mri_br_state_year.json
docker build -t parkinson-br app/web
docker run -p 8080:80 parkinson-br
# acesse http://localhost:8080
```

---

## Funcionalidades do dashboard

- **Mapa coroplético** — densidade de RMs por UF com escalas de cores intercambiáveis
- **Ranking por estado** — gráfico de barras horizontal, filtrável por total / SUS / privado
- **Evolução Nacional** — barras anuais empilhadas (SUS + privado) ou série única
- **Variação Anual (Δ)** — delta ano a ano com rótulos inteiros acima das barras
- **Evolução por Região** — séries temporais por macrorregião
- **Cards KPI** — totais nacionais, per capita, crescimento acumulado e YoY
- Filtros: métrica (total vs. por 1M hab), setor (todos / SUS / privado), ano
- Exportação: planilha Excel (todos os dados + aba regional) e ZIP de PNGs
- Alternância de tema claro/escuro

---

## Fontes de dados

| Fonte | Conteúdo | URL |
|---|---|---|
| DATASUS — CNES/EQ | Contagem mensal de equipamentos de RM por estabelecimento, UF e indicador SUS | [datasus.saude.gov.br](https://datasus.saude.gov.br/transferencia-de-arquivos/) |
| IBGE SIDRA — Tabela 6579 | Estimativas populacionais por UF (variável 9324) | [sidra.ibge.gov.br/tabela/6579](https://sidra.ibge.gov.br/tabela/6579) |

**Filtro de equipamento:** `CODEQUIP = '42'` (Ressonância Magnética)  
**Indicador de setor:** `IND_SUS = 1` → disponível para pacientes SUS; `IND_SUS = 0` → somente privado  
**Período:** janeiro de 2005 – dezembro de 2025 (mensal)

### Nota metodológica

`total_mri_avg` é a média anual das contagens mensais de equipamentos por estabelecimento (`QT_EXIST`), somada para todos os estabelecimentos do estado. Estabelecimentos que alteram o valor de `IND_SUS` ao longo do ano são tratados corretamente: ambas as médias setoriais compartilham o mesmo denominador mensal, garantindo que `sus + privado = total` de forma exata.

---

## Consulta direta aos dados Bronze

```python
from app.src.silver.common import build_delta_spark

spark = build_delta_spark("explore")
df = spark.read.format("delta").load("lakehouse/bronze/cnes_eq")

# Contagem de RMs por estado em 2025
(df.filter("CODEQUIP = '42' AND ano = 2025")
   .groupBy("estado")
   .agg({"QT_EXIST": "sum"})
   .orderBy("sum(QT_EXIST)", ascending=False)
   .show())
```

---

## Licença

MIT — veja [LICENSE](LICENSE).

---

**Autor**

Leonardo Chalhoub  
[linkedin.com/in/leonardochalhoub](https://www.linkedin.com/in/leonardochalhoub/)  
[leochalhoub@hotmail.com](mailto:leochalhoub@hotmail.com)
