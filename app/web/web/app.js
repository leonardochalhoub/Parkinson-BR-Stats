/* global Plotly */

const COUNTERS_API_BASE = (() => {
  // Optional override:
  //   ?counters=https://your-worker.your-subdomain.workers.dev
  // Otherwise, set window.COUNTERS_API_BASE in index.html if desired.
  try {
    const u = new URL(window.location.href);
    const p = u.searchParams.get("counters");
    if (p) return p.replace(/\/+$/, "");
  } catch (_) {
    // ignore
  }
  if (typeof window !== "undefined" && window.COUNTERS_API_BASE) {
    return String(window.COUNTERS_API_BASE).replace(/\/+$/, "");
  }
  return ""; // empty => counters disabled (graceful)
})();

async function countersFetch(path, { method = "GET" } = {}) {
  if (!COUNTERS_API_BASE) return null;
  const url = `${COUNTERS_API_BASE}${path}`;
  const resp = await fetch(url, { method, headers: { "content-type": "application/json" } });
  if (!resp.ok) throw new Error(`Counters API error ${resp.status} for ${path}`);
  return await resp.json();
}

async function initCounters() {
  const visitEl = document.getElementById("visitCount");
  const dlEl = document.getElementById("downloadCount");
  if (!visitEl && !dlEl) return null;

  const render = (data) => {
    const v = data && typeof data.visits === "number" ? data.visits : 0;
    const d = data && typeof data.downloads === "number" ? data.downloads : 0;
    if (visitEl) visitEl.textContent = formatInt(v);
    if (dlEl) dlEl.textContent = formatInt(d);
  };

  // If not configured, show placeholder and bail.
  if (!COUNTERS_API_BASE) {
    if (visitEl) visitEl.textContent = "—";
    if (dlEl) dlEl.textContent = "—";
    return null;
  }

  // Load initial counts
  try {
    const data = await countersFetch("/api/counters", { method: "GET" });
    if (data) render(data);
  } catch (_) {
    // ignore
  }

  // Increment visits once per page load
  try {
    const data = await countersFetch("/api/visit", { method: "POST" });
    if (data) render(data);
  } catch (_) {
    // ignore
  }

  // Poll every 10s to reflect other visitors' changes
  const poll = async () => {
    try {
      const data = await countersFetch("/api/counters", { method: "GET" });
      if (data) render(data);
    } catch (_) {
      // ignore
    }
  };
  const interval = setInterval(poll, 10_000);

  async function incrementDownloads(by = 1) {
    // API increments by 1; call multiple times if needed.
    const n = Math.max(1, Math.trunc(by));
    for (let i = 0; i < n; i += 1) {
      try {
        const data = await countersFetch("/api/download", { method: "POST" });
        if (data) render(data);
      } catch (_) {
        // ignore
      }
    }
  }

  return {
    incrementDownloads,
    stop: () => clearInterval(interval),
  };
}

const DATA_URL = "./data/mri_br_state_year.json";

const GEOJSON_URL = ["./brazil-states.geojson"];

// Allow a base override via: ?base=/getPBFData/app/web/
const BASE = (() => {
  try {
    const u = new URL(window.location.href);
    const b = u.searchParams.get("base");
    return b ? b.replace(/\/+$/, "") : "";
  } catch (_) {
    return "";
  }
})();
const withBase = (p) => (BASE ? `${BASE}/${String(p).replace(/^\.?\//, "")}` : p);

function formatNumber(v, { decimals = 2 } = {}) {
  if (v === null || v === undefined || Number.isNaN(v)) return "null";
  if (Math.abs(v) >= 1e9) return v.toExponential(3);

  // Force a dot-free, pt-BR style formatting with a fixed number of decimals.
  // decimals=0 is used for counts like population/beneficiaries.
  return new Intl.NumberFormat("pt-BR", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  }).format(v);
}

function metricLabel(metric) {
  switch (metric) {
    case "mri_per_capita_scaled":
      return "MRI por 1M hab";
    case "total_mri_avg":
      return "Total MRI/ano (UF)";
    default:
      return metric;
  }
}

function metricColorbarTitle(metric) {
  switch (metric) {
    case "mri_per_capita_scaled":
      return "MRI/1M hab";
    case "total_mri_avg":
      return "MRI";
    default:
      return metric;
  }
}

function sumByYear(rows, metric) {
  const sums = new Map();
  for (const r of rows) {
    const y = r.ano;
    const v = r[metric];

    // Keep all years visible even if values are null.
    if (!sums.has(y)) sums.set(y, { sum: 0, n: 0 });

    if (v !== null && v !== undefined && !Number.isNaN(v)) {
      const cur = sums.get(y);
      cur.sum += v;
      cur.n += 1;
    }
  }

  const out = new Map();
  for (const [y, obj] of sums.entries()) out.set(y, obj.n === 0 ? null : obj.sum);
  return out;
}

function brazilByYear(rows, metric) {
  // For MRI dataset, all metrics are already computed in the JSON.
  // We plot a simple sum across UFs for each year (as in sumByYear).
  // Note: for per-capita metrics, this is NOT a population-weighted national metric;
  // it is only used for a quick bar visualization.
  return sumByYear(rows, metric);
}

async function loadJson(urlOrUrls) {
  const urls = Array.isArray(urlOrUrls) ? urlOrUrls : [urlOrUrls];

  let lastErr = null;
  for (const url of urls) {
    try {
      const resp = await fetch(url, { cache: "no-store" });
      if (!resp.ok) throw new Error(`Failed to load ${url}: ${resp.status}`);
      return await resp.json();
    } catch (e) {
      lastErr = e;
    }
  }
  throw lastErr || new Error(`Failed to load: ${urls.join(", ")}`);
}

function normalizeGeoJsonIds(geojson) {
  for (const f of geojson.features) {
    if (f.id) continue;
    const p = f.properties || {};
    f.id = p.sigla || p.SIGLA || p.uf || p.UF || p.abbrev || null;
  }
  return geojson;
}

function getReverseScale(colorscale) {
  // Ensure low=bright and high=dark for all scales.
  // Plotly palettes differ: some are dark→bright by default and must be reversed.
  return [
    "Viridis",
    "Cividis",
    "Blues",
    "Greens",
    "Greys",
    "YlGnBu",
    "YlOrRd",
  ].includes(colorscale);
}

function renderBar({ rows, years, metric }) {
  const isDark = document.body.classList.contains("dark");

  const barColor = isDark ? "#60a5fa" : "#2b6cb0";
  const textColor = isDark ? "rgba(232, 238, 249, 0.92)" : "#111";
  const gridColor = isDark ? "rgba(255,255,255,0.12)" : "rgba(0,0,0,0.08)";
  const bg = isDark ? "#0f172a" : "#ffffff";

  const sums = brazilByYear(rows, metric);
  const x = years;
  const y = years.map((yy) => (sums.get(yy) === undefined ? null : sums.get(yy)));

  // Text labels above each bar
  const text = y.map((v) =>
    v === null || v === undefined || Number.isNaN(v) ? "" : formatNumber(v, { decimals: 2 })
  );

  Plotly.newPlot(
    "barWrap",
    [
      {
        type: "bar",
        x,
        y,
        marker: { color: barColor },
        text,
        textposition: "outside",
        cliponaxis: false,
        hovertemplate: "Ano=%{x}<br>Valor=%{text}<extra></extra>",
      },
    ],
    {
      paper_bgcolor: bg,
      plot_bgcolor: bg,
      margin: { t: 10, r: 10, b: 70, l: 70 },
      yaxis: {
        title: metricLabel(metric),
        automargin: true,
        gridcolor: gridColor,
        zerolinecolor: gridColor,
        tickfont: { color: textColor },
        titlefont: { color: textColor },
      },
      xaxis: {
        title: "Ano",
        type: "category",
        tickmode: "array",
        tickvals: x,
        ticktext: x.map(String),
        tickangle: -45,
        automargin: true,
        gridcolor: "rgba(0,0,0,0)",

        tickfont: { color: textColor },
        titlefont: { color: textColor },
      },
      font: { color: textColor },
      annotations: [],
    },
    { displayModeBar: false, responsive: true }
  );
}

const UF_TO_REGIAO = {
  AC: "Norte",
  AL: "Nordeste",
  AM: "Norte",
  AP: "Norte",
  BA: "Nordeste",
  CE: "Nordeste",
  DF: "Centro-Oeste",
  ES: "Sudeste",
  GO: "Centro-Oeste",
  MA: "Nordeste",
  MG: "Sudeste",
  MS: "Centro-Oeste",
  MT: "Centro-Oeste",
  PA: "Norte",
  PB: "Nordeste",
  PE: "Nordeste",
  PI: "Nordeste",
  PR: "Sul",
  RJ: "Sudeste",
  RN: "Nordeste",
  RO: "Norte",
  RR: "Norte",
  RS: "Sul",
  SC: "Sul",
  SE: "Nordeste",
  SP: "Sudeste",
  TO: "Norte",
};

const REGIOES_ORDER = ["Norte", "Nordeste", "Centro-Oeste", "Sudeste", "Sul"];

function renderBeneficiaries() {
  // Not used in this MRI page; keep an empty placeholder so the layout doesn't break.
  const el = document.getElementById("benefWrap");
  if (el) el.innerHTML = "";
}

function renderMap({ rows, metric, colorscale, yearValue, years, geojson }) {
  const isDark = document.body.classList.contains("dark");

  const textColor = isDark ? "rgba(232, 238, 249, 0.92)" : "#111";
  const bg = isDark ? "#0f172a" : "#ffffff";
  const borderColor = isDark ? "rgba(255,255,255,0.45)" : "#ffffff";

  const isAgg = yearValue === "AGG";
  const year = isAgg ? null : parseInt(yearValue, 10);

  const reverseScale = getReverseScale(colorscale);

  const rowsNumericYears = rows.filter((r) => typeof r.ano === "number");
  const rowsYear = isAgg ? rowsNumericYears : rowsNumericYears.filter((r) => r.ano === year);

  // Aggregate per UF when isAgg=true.
  // For valor_* we can sum. For per-capita/per-benef in AGG, the denominator should be the
  // simple average across YEARS (one observation per year), not a sum across years.
  const byUf = new Map();
  for (const r of rowsYear) {
    const uf = r.estado;
    if (!byUf.has(uf)) {
      byUf.set(uf, {
        uf,
        num2021: 0, // sum(valor_2021 * 1e9) across years
        denom: null, // computed at the end as avg(pop) or avg(n_benef) across years
        metricSum: 0, // for additive metrics
        metricN: 0,
        _popByYear: new Map(),
        _benefByYear: new Map(),
        popLast: null,
        popLastYear: -Infinity,
        _mri_per_capita_scaled: null,
        _total_mri_avg: null,
      });
    }
    const acc = byUf.get(uf);

    {
      const v = r[metric];
      if (v !== null && v !== undefined && !Number.isNaN(v)) {
        acc.metricSum += v;
        acc.metricN += 1;
      }
    }

    // For hover "extras" in MRI dataset (show the *other* metric)
    if (
      r.mri_per_capita_scaled !== null &&
      r.mri_per_capita_scaled !== undefined &&
      !Number.isNaN(r.mri_per_capita_scaled)
    ) {
      acc._mri_per_capita_scaled = r.mri_per_capita_scaled;
    }
    if (r.total_mri_avg !== null && r.total_mri_avg !== undefined && !Number.isNaN(r.total_mri_avg)) {
      acc._total_mri_avg = r.total_mri_avg;
    }

    // For hover: show population from the selected year (if present), otherwise keep latest available.
    if (r.populacao !== null && r.populacao !== undefined && !Number.isNaN(r.populacao)) {
      if (r.ano >= acc.popLastYear) {
        acc.popLastYear = r.ano;
        acc.popLast = r.populacao;
      }
    }
  }

  // No special denom logic needed for MRI dataset.

  const rowsUf = Array.from(byUf.values()).sort((a, b) => a.uf.localeCompare(b.uf));
  const locations = rowsUf.map((r) => r.uf);

  const z = rowsUf.map((r) => {
    if (r.metricN === 0) return null;
    // MRI metrics: always mean across years when AGG
    return r.metricSum / r.metricN;
  });

  const hover = rowsUf.map((r) => {
    const label = metricLabel(metric);
    const val = r.metricN === 0 ? null : r.metricSum / r.metricN;
    const pop = r.popLast;
    const popSuffix = r.popLastYear > -Infinity ? ` (ano ${r.popLastYear})` : "";

    const title = isAgg ? `${label} (média anual)` : label;
    const extra =
      metric === "mri_per_capita_scaled"
        ? `<br>Total MRI/ano (UF): ${formatNumber(r._total_mri_avg, { decimals: 2 })}`
        : metric === "total_mri_avg"
          ? `<br>MRI por 1M hab: ${formatNumber(r._mri_per_capita_scaled, { decimals: 2 })}`
          : "";
    const decimals = metric === "mri_per_capita_scaled" ? 2 : 2;
    return `${r.uf}<br>${title}: ${formatNumber(val, { decimals })}${extra}<br>Pop${popSuffix}: ${formatNumber(pop, { decimals: 0 })}`;
  });

  // Shorten + split the AGG title into 2 lines and give it more top margin.
  // Plotly annotations don't auto-wrap reliably, so we insert a <br>.
  const mapTitle = isAgg
    ? `Distribuição por UF<br>Média anual (${years[0]}–${years[years.length - 1]})`
    : `Distribuição por UF — ${year}`;

  const titleFontSize = isAgg ? 16 : 20;
  const titleY = isAgg ? 1.06 : 1.02;

  Plotly.newPlot(
    "mapWrap",
    [
      {
        type: "choropleth",
        geojson,
        featureidkey: "id",
        locations,
        z,
        text: hover,
        hovertemplate: "%{text}<extra></extra>",
        colorscale,
        reversescale: reverseScale,
        colorbar: {
          title: { text: metricColorbarTitle(metric), font: { color: textColor } },
          tickfont: { color: textColor },
          outlinecolor: isDark ? "rgba(255,255,255,0.18)" : "rgba(0,0,0,0.15)",
        },
        marker: { line: { color: borderColor, width: 0.6 } },
      },
    ],
    {
      paper_bgcolor: bg,
      plot_bgcolor: bg,
      // More top margin so the 2-line AGG title doesn't get clipped.
      margin: { t: isAgg ? 96 : 55, r: 10, b: 10, l: 10 },
      geo: {
        fitbounds: "locations",
        visible: false,
        projection: { type: "mercator", scale: 1.55 },
        center: { lat: -14, lon: -52 },
        bgcolor: bg,
      },
      annotations: [
        {
          xref: "paper",
          yref: "paper",
          x: 0.5,
          y: titleY,
          xanchor: "center",
          yanchor: "bottom",
          showarrow: false,
          text: mapTitle,
          align: "center",
          bgcolor: "rgba(0,0,0,0)",
          borderwidth: 0,
          borderpad: 0,
          font: { size: titleFontSize, color: textColor },
        },
      ],
      font: { color: textColor },
    },
    { displayModeBar: false, responsive: true }
  );
}

function initBuildDate() {
  const el = document.getElementById("buildDate");
  if (el) el.textContent = new Date().toLocaleDateString("pt-BR");
}

function formatInt(v) {
  const n = typeof v === "number" ? v : Number(v);
  if (!Number.isFinite(n)) return "—";
  return new Intl.NumberFormat("pt-BR", { maximumFractionDigits: 0 }).format(Math.trunc(n));
}

function setLoading(isLoading) {
  const el = document.getElementById("loading");
  if (!el) return;
  el.style.display = isLoading ? "block" : "none";
}

function setZipProgress({ visible, pct = 0, label = "" }) {
  const wrap = document.getElementById("zipProgress");
  const bar = document.getElementById("zipProgressBar");
  const txt = document.getElementById("zipProgressText");

  // Also reflect progress on the button itself (more visible than the panel).
  const btn = document.getElementById("downloadAllPng");

  const p = Math.max(0, Math.min(100, Number.isFinite(pct) ? pct : 0));
  const pctText = `${Math.round(p)}%`;

  if (btn) {
    if (!btn.dataset.originalText) btn.dataset.originalText = btn.textContent || "Baixar imagens (PNG)";

    if (visible) {
      btn.disabled = true;
      btn.textContent = label ? `${label} ${pctText}` : `Gerando ZIP… ${pctText}`;
    } else {
      btn.disabled = false;
      btn.textContent = btn.dataset.originalText;
    }
  }

  // Keep the panel progress if it exists; but do not require it.
  if (!wrap || !bar || !txt) return;
  wrap.style.display = visible ? "block" : "none";
  bar.value = p;
  txt.textContent = label || pctText;
}

function setError(msg) {
  const el = document.getElementById("error");
  if (!el) return;
  el.textContent = msg || "";
  el.style.display = msg ? "block" : "none";
}

function safeFileToken(s) {
  return String(s).replace(/[^a-zA-Z0-9._-]+/g, "_");
}

function computeMapRows({ rowsYear, years, metric, yearValue }) {
  // Aggregate per UF for export.
  // MRI dashboard: we only export MRI metrics, and for AGG we show the mean across years.
  const byUf = new Map();

  for (const r of rowsYear) {
    const uf = r.estado;
    if (!byUf.has(uf)) {
      byUf.set(uf, {
        uf,
        metricSum: 0,
        metricN: 0,
        num2021: 0,
        benefSum: 0,
        benefN: 0,
        popSum: 0,
        popN: 0,
        popLast: null,
        popLastYear: -Infinity,
      });
    }
    const acc = byUf.get(uf);

    const v = r[metric];
    if (v !== null && v !== undefined && !Number.isNaN(v)) {
      acc.metricSum += v;
      acc.metricN += 1;
    }

    // Track latest population for context/hover.
    if (r.populacao !== null && r.populacao !== undefined && !Number.isNaN(r.populacao)) {
      if (r.ano >= acc.popLastYear) {
        acc.popLastYear = r.ano;
        acc.popLast = r.populacao;
      }
    }
  }

  const label = metricLabel(metric);

  return Array.from(byUf.values())
    .sort((a, b) => a.uf.localeCompare(b.uf))
    .map((r) => {
      let val = null;

      if (r.metricN !== 0) {
        if (metric === "valor_2021" || metric === "valor_nominal") {
          val = r.metricSum;
        } else {
          // ratio metrics: average of yearly per-capita / per-beneficiary
          val = r.metricN === 0 ? null : r.metricSum / r.metricN;
        }
      }

      const base = {
        Ano: yearValue === "AGG" ? `AGG_${years[0]}_${years[years.length - 1]}` : Number(yearValue),
        UF: r.uf || r.estado,
        Metrica: metric,
        MetricaLabel: label,
        Valor: val,
      };

      // Context columns
      base.Populacao = r.popLast;

      return base;
    });
}
function sheetNameFor(metric, kind) {
  // Excel sheet name limit: 31 chars
  const base = {
    mri_per_capita_scaled: "MRI_por_1Mhab",
    total_mri_avg: "Total_MRI_UF",
  }[metric] || metric;

  const suffix = kind === "bar" ? "Barras" : "Mapa";
  return `${base}_${suffix}`.slice(0, 31);
}

function buildPopulationTab({ wb, rows, years }) {
  // Population series used in the metrics (UF x Ano), plus documentation.
  // Source: IBGE "População residente estimada" (SIDRA Agregados API, agregado 6579, variável 9324, localidades N3).
  // In the pipeline, silver layer fills missing years using linear interpolation/extrapolation when necessary.
  const intro = [
    {
      Campo: "Fonte principal",
      Valor:
        "IBGE - SIDRA (API de Agregados) - População residente estimada (agregado 6579, variável 9324, localidades N3=UF).",
    },
    {
      Campo: "Como foi estimada quando necessário",
      Valor:
        "Quando a série não está disponível para algum ano, a tabela silver/populacao_uf_ano aplica preenchimento linear (interpolação/extrapolação) para completar UF×Ano no intervalo de anos do dataset.",
    },
    {
      Campo: "Observação",
      Valor:
        "Para o 'Acumulado' (AGG) em métricas per capita/per beneficiário, o denominador não deve ser somado ao longo dos anos. Neste Excel usamos a MÉDIA anual do denominador no período (média da população ou média de beneficiários por UF).",
    },
  ];

  // Build UF x Ano population table from the rows.
  const popByUfYear = new Map(); // key `${uf}|${ano}` -> pop
  for (const r of rows) {
    if (r.ano === null || r.ano === undefined || typeof r.ano !== "number") continue;
    const uf = r.estado;
    const ano = r.ano;
    const pop = r.populacao;
    if (!uf || pop === null || pop === undefined || Number.isNaN(pop)) continue;
    popByUfYear.set(`${uf}|${ano}`, pop);
  }

  const ufs = Array.from(new Set(rows.map((r) => r.estado))).sort((a, b) => a.localeCompare(b));

  const table = [];
  for (const uf of ufs) {
    for (const ano of years) {
      const pop = popByUfYear.get(`${uf}|${ano}`) ?? null;
      table.push({ UF: uf, Ano: ano, Populacao: pop });
    }
  }

  const sheet = XLSX.utils.book_new();
  // We'll append as 2 sections: intro (key-value) then table below.
  const wsIntro = XLSX.utils.json_to_sheet(intro, { skipHeader: false });
  XLSX.utils.sheet_add_aoa(wsIntro, [[""], [""], ["UF", "Ano", "Populacao"]], { origin: -1 });
  XLSX.utils.sheet_add_json(wsIntro, table, { origin: -1, skipHeader: true });

  XLSX.utils.book_append_sheet(wb, wsIntro, "Populacao");
}

function buildInflationFactorsTab({ wb, rows, years }) {
  // Inflation factors to 2021 price level.
  // Source: BCB/SGS IPCA monthly variation series (code 433).
  // Method: build monthly cumulative index, take December index by year, normalize 2021-12 = 1.0,
  // and compute deflator_to_2021 = index_2021_12 / index_year_12 (annual approximation).
  const intro = [
    { Campo: "Fonte", Valor: "Banco Central do Brasil (SGS) - IPCA variação mensal (%), série 433." },
    {
      Campo: "Método (resumo)",
      Valor:
        "Constrói índice mensal cumulativo a partir de (1 + IPCA/100). Usa o índice de dezembro de cada ano; normaliza para que dez/2021=1.0. Fator anual = índice_dez/2021 ÷ índice_dez/ano.",
    },
    {
      Campo: "Uso no dataset",
      Valor: "valor_2021 = valor_nominal × deflator_to_2021 (em R$ bi). Métricas per capita/per beneficiário usam valor_2021 convertido para R$.",
    },
  ];

  // We don't have deflator in the JSON, so we embed a reproducible reference:
  // the pipeline computes it in app/src/gold/pbf_estados_df_geo.py via _build_year_december_deflators_to_2021().
  const refs = [
    { Campo: "Código (pipeline)", Valor: "app/src/gold/pbf_estados_df_geo.py::_build_year_december_deflators_to_2021" },
    {
      Campo: "Endpoint",
      Valor: "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados?formato=json",
    },
  ];

  const ws = XLSX.utils.json_to_sheet([...intro, ...refs]);
  XLSX.utils.book_append_sheet(wb, ws, "Inflacao_Fatores");
}

function buildExcelWorkbookAll({ rows, years }) {
  const wb = XLSX.utils.book_new();

  // Add documentation tabs first for easy access.
  buildPopulationTab({ wb, rows, years });
  buildInflationFactorsTab({ wb, rows, years });

  const metrics = ["mri_per_capita_scaled", "total_mri_avg"];

  for (const metric of metrics) {
    // Bars sheet: Brazil totals per year
    const sums = brazilByYear(rows, metric);
    const barRows = years.map((y) => ({
      Ano: y,
      Metrica: metric,
      MetricaLabel: metricLabel(metric),
      TotalBrasil: sums.get(y) === undefined ? null : sums.get(y),
    }));
    XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(barRows), sheetNameFor(metric, "bar"));

    // Map sheet: All years + AGG
    const mapAllRows = [];

    // Only numeric-year rows (exclude backend "Agregado ..." rows)
    const rowsNumericYears = rows.filter((r) => typeof r.ano === "number");

    for (const y of years) {
      const rowsYear = rowsNumericYears.filter((r) => r.ano === y);
      mapAllRows.push(...computeMapRows({ rowsYear, years, metric, yearValue: String(y) }));
    }

    const rowsAllYears = years.flatMap((yy) => rowsNumericYears.filter((r) => r.ano === yy));
    mapAllRows.push(...computeMapRows({ rowsYear: rowsAllYears, years, metric, yearValue: "AGG" }));

    XLSX.utils.book_append_sheet(
      wb,
      XLSX.utils.json_to_sheet(mapAllRows),
      sheetNameFor(metric, "map")
    );
  }

  return wb;
}

function downloadExcelComplete({ rows, years }) {
  const wb = buildExcelWorkbookAll({ rows, years });

  const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "");
  const filename = `mri_br_completo_${stamp}.xlsx`;

  XLSX.writeFile(wb, filename, { compression: true });
}

function dataUrlToU8(dataUrl) {
  const base64 = dataUrl.split(",")[1];
  const binStr = atob(base64);
  const len = binStr.length;
  const bytes = new Uint8Array(len);
  for (let i = 0; i < len; i += 1) bytes[i] = binStr.charCodeAt(i);
  return bytes;
}

function triggerDownloadBlob(blob, filename) {
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(a.href), 10_000);
}

async function toPngBytes(divId, { width, height, scale = 2 }) {
  const dataUrl = await Plotly.toImage(divId, {
    format: "png",
    width,
    height,
    scale,
  });
  return dataUrlToU8(dataUrl);
}

async function downloadAllMapsZip({
  rows,
  years,
  metric,
  colorscale,
  geojson,
  // Bigger PNGs (Brazil was too small)
  mapSize = { width: 1800, height: 1300 },
  barSize = { width: 1600, height: 900 },
  benefSize = { width: 1600, height: 900 },
}) {
  // Create hidden offscreen containers so the user doesn't see plots changing.
  const scratch = document.createElement("div");
  scratch.style.position = "fixed";
  scratch.style.left = "-10000px";
  scratch.style.top = "0";
  scratch.style.width = "10px";
  scratch.style.height = "10px";
  scratch.style.overflow = "hidden";
  scratch.style.pointerEvents = "none";
  scratch.style.opacity = "0";
  document.body.appendChild(scratch);

  const barDiv = document.createElement("div");
  barDiv.id = "barExport";
  const benefDiv = document.createElement("div");
  benefDiv.id = "benefExport";
  const mapDiv = document.createElement("div");
  mapDiv.id = "mapExport";
  scratch.appendChild(barDiv);
  scratch.appendChild(benefDiv);
  scratch.appendChild(mapDiv);

  try {
    // Prepare ZIP
    const zip = new JSZip();
    const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "");
    const folder = zip.folder(`mri_br_${safeFileToken(colorscale)}_${stamp}`);

    const metrics = ["mri_per_capita_scaled", "total_mri_avg"];

    async function toPngBytesFromDiv(divEl, { width, height, scale = 2 }) {
      // Make sure Plotly has computed final layout before exporting
      // (offscreen divs can otherwise export blank images).
      try {
        await Plotly.Plots.resize(divEl);
      } catch (_) {
        // ignore
      }
      await new Promise((r) => setTimeout(r, 0));
      const dataUrl = await Plotly.toImage(divEl, { format: "png", width, height, scale });
      return dataUrlToU8(dataUrl);
    }

    // Render + capture without touching the visible charts
    async function renderBarAndCapture(metricKey) {
      const isDark = document.body.classList.contains("dark");

      const barColor = isDark ? "#60a5fa" : "#2b6cb0";
      const textColor = isDark ? "rgba(232, 238, 249, 0.92)" : "#111";
      const gridColor = isDark ? "rgba(255,255,255,0.12)" : "rgba(0,0,0,0.08)";
      const bg = isDark ? "#0f172a" : "#ffffff";

      const sums = brazilByYear(rows, metricKey);
      const x = years;
      const y = years.map((yy) => (sums.get(yy) === undefined ? null : sums.get(yy)));
      const text = y.map((v) => (v === null || v === undefined || Number.isNaN(v) ? "" : formatNumber(v)));

      await Plotly.newPlot(
        barDiv,
        [
          {
            type: "bar",
            x,
            y,
            marker: { color: barColor },
            text,
            textposition: "outside",
            cliponaxis: false,
          },
        ],
        {
          paper_bgcolor: bg,
          plot_bgcolor: bg,
          margin: { t: 10, r: 10, b: 70, l: 70 },
          yaxis: {
            title: metricLabel(metricKey),
            automargin: true,
            gridcolor: gridColor,
            zerolinecolor: gridColor,
            tickfont: { color: textColor },
            titlefont: { color: textColor },
          },
          xaxis: {
            title: "Ano",
            type: "category",
            tickmode: "array",
            tickvals: x,
            ticktext: x.map(String),
            tickangle: -45,
            automargin: true,
            gridcolor: "rgba(0,0,0,0)",
            tickfont: { color: textColor },
            titlefont: { color: textColor },
          },
          font: { color: textColor },
        },
        { displayModeBar: false, responsive: true }
      );

      return toPngBytesFromDiv(barDiv, barSize);
    }

    async function renderBeneficiariesAndCapture() {
      const isDark = document.body.classList.contains("dark");

      const textColor = isDark ? "rgba(232, 238, 249, 0.92)" : "#111";
      const gridColor = isDark ? "rgba(255,255,255,0.12)" : "rgba(0,0,0,0.08)";
      const bg = isDark ? "#0f172a" : "#ffffff";

      const byReg = beneficiariesByRegionYear(rows);

      // Fixed palette (stable across renders)
      const colors = {
        Norte: isDark ? "#60a5fa" : "#2563eb",
        Nordeste: isDark ? "#34d399" : "#059669",
        "Centro-Oeste": isDark ? "#fbbf24" : "#d97706",
        Sudeste: isDark ? "#f472b6" : "#db2777",
        Sul: isDark ? "#a78bfa" : "#7c3aed",
      };

      const traces = REGIOES_ORDER.filter((r) => byReg.has(r)).map((reg) => {
        const m = byReg.get(reg);
        const y = years.map((yy) => (m.has(yy) ? m.get(yy) : null));
        const text = y.map((v) => (v === null ? "" : formatNumber(v, { decimals: 0 })));
        return {
          type: "scatter",
          mode: "lines+markers",
          name: reg,
          x: years,
          y,
          line: { color: colors[reg] || "#888", width: 3 },
          marker: { color: colors[reg] || "#888", size: 6 },
          text,
        };
      });

      await Plotly.newPlot(
        benefDiv,
        traces,
        {
          paper_bgcolor: bg,
          plot_bgcolor: bg,
          margin: { t: 10, r: 10, b: 70, l: 90 },
          legend: {
            orientation: "h",
            x: 0,
            y: -0.25,
            xanchor: "left",
            yanchor: "top",
            font: { color: textColor },
          },
          yaxis: {
            title: "Beneficiários (Região)",
            rangemode: "tozero",
            automargin: true,
            gridcolor: gridColor,
            zerolinecolor: gridColor,
            tickfont: { color: textColor },
            titlefont: { color: textColor },
          },
          xaxis: {
            title: "Ano",
            type: "category",
            tickmode: "array",
            tickvals: years,
            ticktext: years.map(String),
            tickangle: -45,
            automargin: true,
            gridcolor: "rgba(0,0,0,0)",
            tickfont: { color: textColor },
            titlefont: { color: textColor },
          },
          font: { color: textColor },
        },
        { displayModeBar: false, responsive: true }
      );

      return toPngBytesFromDiv(benefDiv, benefSize);
    }

    async function renderMapAndCapture(metricKey, yearValue) {
      const isDark = document.body.classList.contains("dark");

      const textColor = isDark ? "rgba(232, 238, 249, 0.92)" : "#111";
      const bg = isDark ? "#0f172a" : "#ffffff";
      const borderColor = isDark ? "rgba(255,255,255,0.45)" : "#ffffff";

      const isAgg = yearValue === "AGG";
      const year = isAgg ? null : parseInt(yearValue, 10);
      const reverseScale = getReverseScale(colorscale);

      const rowsYear = isAgg ? rows : rows.filter((r) => r.ano === year);

      const byUf = new Map();
      for (const r of rowsYear) {
        const uf = r.estado;
        if (!byUf.has(uf)) byUf.set(uf, { uf, metricSum: 0, metricN: 0, num2021: 0, denom: 0 });
        const acc = byUf.get(uf);

        if (metricKey === "valor_2021" || metricKey === "valor_nominal") {
          const v = r[metricKey];
          if (v !== null && v !== undefined && !Number.isNaN(v)) {
            acc.metricSum += v;
            acc.metricN += 1;
          }
        } else if (metricKey === "pbfPerCapita" || metricKey === "pbfPerBenef") {
          const v2021 = r.valor_2021;
          if (v2021 === null || v2021 === undefined || Number.isNaN(v2021)) continue;

          const denomVal = metricKey === "pbfPerCapita" ? r.populacao : r.n_benef;
          if (denomVal === null || denomVal === undefined || Number.isNaN(denomVal) || denomVal <= 0) continue;

          acc.num2021 += v2021 * 1e9;
          acc.denom += denomVal;
          acc.metricN += 1;
        }
      }

  const rowsUf = Array.from(byUf.values()).sort((a, b) => a.uf.localeCompare(b.uf));
  const locations = rowsUf.map((r) => r.uf);
      const z = rowsUf.map((r) => {
        if (r.metricN === 0) return null;
        if (metricKey === "valor_2021" || metricKey === "valor_nominal") return r.metricSum;
        return r.denom === 0 ? null : r.num2021 / r.denom;
      });

      const mapTitle = isAgg
        ? `Distribuição por UF — acumulado ${years[0]}–${years[years.length - 1]}`
        : `Distribuição por UF — ${year}`;

      await Plotly.newPlot(
        mapDiv,
        [
          {
            type: "choropleth",
            geojson,
            featureidkey: "id",
            locations,
            z,
            colorscale,
            reversescale: reverseScale,
            colorbar: {
              title: { text: metricColorbarTitle(metricKey), font: { color: textColor } },
              tickfont: { color: textColor },
              outlinecolor: isDark ? "rgba(255,255,255,0.18)" : "rgba(0,0,0,0.15)",
            },
            marker: { line: { color: borderColor, width: 0.6 } },
          },
        ],
        {
          paper_bgcolor: bg,
          plot_bgcolor: bg,
          margin: { t: 55, r: 10, b: 10, l: 10 },
          geo: {
            fitbounds: "locations",
            visible: false,
            projection: { type: "mercator", scale: 1.55 },
            center: { lat: -14, lon: -52 },
            bgcolor: bg,
          },
          annotations: [
            {
              xref: "paper",
              yref: "paper",
              x: 0.5,
              y: 1.02,
              xanchor: "center",
              yanchor: "bottom",
              showarrow: false,
              text: mapTitle,
              font: { size: 20, color: textColor },
            },
          ],
          font: { color: textColor },
        },
        { displayModeBar: false, responsive: true }
      );

      return toPngBytesFromDiv(mapDiv, mapSize);
    }

    const totalSteps = 1 + metrics.length * (1 + 1 + years.length);
    let step = 0;
    const tick = (label) => {
      step += 1;
      setZipProgress({ visible: true, pct: (step / totalSteps) * 100, label });
    };

    // No "beneficiaries" chart in the MRI dashboard.

    for (const metricKey of metrics) {
      setZipProgress({ visible: true, pct: (step / totalSteps) * 100, label: `Métrica: ${metricKey}` });
      const metricFolder = folder.folder(safeFileToken(metricKey));

      // Bar chart
      {
        const bytes = await renderBarAndCapture(metricKey);
        metricFolder.file(`barras_total_brasil_${safeFileToken(metricKey)}.png`, bytes);
        tick(`${metricKey}: barras`);
      }

      // Map AGG
      {
        const bytes = await renderMapAndCapture(metricKey, "AGG");
        metricFolder.file(`mapa_AGG_${years[0]}-${years[years.length - 1]}_${safeFileToken(metricKey)}.png`, bytes);
        tick(`${metricKey}: AGG`);
      }

      // Maps each year
      for (const y of years) {
        const bytes = await renderMapAndCapture(metricKey, String(y));
        metricFolder.file(`mapa_${y}_${safeFileToken(metricKey)}.png`, bytes);
        tick(`${metricKey}: ${y}`);
      }
    }

    setZipProgress({ visible: true, pct: 99, label: "Compactando…" });
    const blob = await zip.generateAsync({ type: "blob", compression: "DEFLATE" });
    setZipProgress({ visible: true, pct: 100, label: "Iniciando download…" });

    const zipName = `mri_br_mapas_${safeFileToken(colorscale)}_${stamp}.zip`;
    triggerDownloadBlob(blob, zipName);
  } finally {
    setZipProgress({ visible: false, pct: 0, label: "" });
    scratch.remove();
  }
}

function applyTheme(theme) {
  const isDark = theme === "dark";
  document.body.classList.toggle("dark", isDark);
  try {
    localStorage.setItem("theme", isDark ? "dark" : "light");
  } catch (_) {
    // ignore
  }

  const toggle = document.getElementById("toggleDark");
  if (toggle && toggle.type === "checkbox") {
    toggle.checked = isDark;
  }
}

function initTheme() {
  let saved = null;
  try {
    saved = localStorage.getItem("theme");
  } catch (_) {
    saved = null;
  }

  if (saved === "dark" || saved === "light") {
    applyTheme(saved);
    return;
  }

  const prefersDark = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
  applyTheme(prefersDark ? "dark" : "light");
}

async function main() {
  initBuildDate();
  initTheme();
  setLoading(true);
  setError("");

  const counters = await initCounters();

  const metricSel = document.getElementById("metric");
  const colorscaleSel = document.getElementById("colorscale");
  const yearSel = document.getElementById("year");

  try {
    const [rows, geojsonRaw] = await Promise.all([
      loadJson(withBase(DATA_URL)),
      loadJson(GEOJSON_URL.map(withBase)),
    ]);
    const geojson = normalizeGeoJsonIds(geojsonRaw);

    // Exclude the "Agregado ..." rows coming from the backend gold table
    const years = Array.from(new Set(rows.filter((r) => typeof r.ano === "number").map((r) => r.ano))).sort(
      (a, b) => a - b
    );

    // Build year dropdown dynamically + add an aggregate option.
    yearSel.innerHTML =
      `<option value="AGG">Média anual (${years[0]}–${years[years.length - 1]})</option>` +
      years.map((y) => `<option value="${y}">${y}</option>`).join("");
    yearSel.value = years[years.length - 1];

    const downloadBtn = document.getElementById("downloadXlsx");
    const downloadAllPngBtn = document.getElementById("downloadAllPng");
    const toggleDarkBtn = document.getElementById("toggleDark");

    function render() {
      const metric = metricSel.value;
      const colorscale = colorscaleSel.value;
      const yearValue = yearSel.value;

      renderBar({ rows, years, metric });
      renderBeneficiaries({ rows, years });
      renderMap({ rows, metric, colorscale, yearValue, years, geojson });
    }

    async function onDownload() {
      downloadExcelComplete({ rows, years });
      if (counters && counters.incrementDownloads) await counters.incrementDownloads(1);
    }

    async function onDownloadAllPng() {
      const metric = metricSel.value;
      const colorscale = colorscaleSel.value;

      // Basic guardrails (this can take a while)
      setLoading(true);
      setError("");
      try {
        await downloadAllMapsZip({ rows, years, metric, colorscale, geojson });
        if (counters && counters.incrementDownloads) await counters.incrementDownloads(1);
      } catch (e) {
        setError(e && e.stack ? e.stack : String(e));
      } finally {
        setLoading(false);
      }
    }

    metricSel.addEventListener("change", render);
    colorscaleSel.addEventListener("change", render);
    yearSel.addEventListener("change", render);
    if (downloadBtn) downloadBtn.addEventListener("click", onDownload);
    if (downloadAllPngBtn) downloadAllPngBtn.addEventListener("click", onDownloadAllPng);
    if (toggleDarkBtn) {
      // Checkbox toggle
      toggleDarkBtn.addEventListener("change", () => {
        applyTheme(toggleDarkBtn.checked ? "dark" : "light");
        render(); // re-render charts so Plotly picks up dark palette
      });
    }

    render();
  } catch (e) {
    setError(e && e.stack ? e.stack : String(e));
  } finally {
    setLoading(false);
  }
}

main();
