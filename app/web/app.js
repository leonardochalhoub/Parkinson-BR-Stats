/* global Plotly */

// ─── Counters ────────────────────────────────────────────────────────────────

const COUNTERS_API_BASE = (() => {
  try {
    const u = new URL(window.location.href);
    const p = u.searchParams.get("counters");
    if (p) return p.replace(/\/+$/, "");
  } catch (_) {}
  if (typeof window !== "undefined" && window.COUNTERS_API_BASE) {
    return String(window.COUNTERS_API_BASE).replace(/\/+$/, "");
  }
  return "";
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

  if (!COUNTERS_API_BASE) {
    if (visitEl) visitEl.textContent = "—";
    if (dlEl) dlEl.textContent = "—";
    return null;
  }

  try {
    const data = await countersFetch("/api/counters", { method: "GET" });
    if (data) render(data);
  } catch (_) {}

  try {
    const data = await countersFetch("/api/visit", { method: "POST" });
    if (data) render(data);
  } catch (_) {}

  const poll = async () => {
    try {
      const data = await countersFetch("/api/counters", { method: "GET" });
      if (data) render(data);
    } catch (_) {}
  };
  const interval = setInterval(poll, 10_000);

  async function incrementDownloads(by = 1) {
    const n = Math.max(1, Math.trunc(by));
    for (let i = 0; i < n; i += 1) {
      try {
        const data = await countersFetch("/api/download", { method: "POST" });
        if (data) render(data);
      } catch (_) {}
    }
  }

  return { incrementDownloads, stop: () => clearInterval(interval) };
}

// ─── Data paths ──────────────────────────────────────────────────────────────

const DATA_URL = "./data/mri_br_state_year.json";
const GEOJSON_URL = ["./brazil-states.geojson"];

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

// ─── Setor / metric helpers ──────────────────────────────────────────────────

/**
 * Map (metric, setor) → the actual JSON field name.
 *
 * metric  : "mri_per_capita_scaled" | "total_mri_avg"
 * setor   : "todos" | "sus" | "privado"
 *
 * JSON schema (per row):
 *   todos  → mri_per_capita_scaled, total_mri_avg
 *   sus    → sus_mri_per_capita_scaled, sus_total_mri_avg
 *   privado→ priv_mri_per_capita_scaled, priv_total_mri_avg
 */
function resolveField(metric, setor) {
  if (setor === "sus") return `sus_${metric}`;
  if (setor === "privado") return `priv_${metric}`;
  return metric;
}

function setorLabel(setor) {
  if (setor === "sus") return "SUS";
  if (setor === "privado") return "Privado";
  return "Todos";
}

function metricBaseLabel(metric) {
  switch (metric) {
    case "mri_per_capita_scaled": return "MRI por 1M hab";
    case "total_mri_avg":         return "Total MRI/ano (UF)";
    default:                      return metric;
  }
}

function metricLabel(metric, setor = "todos") {
  const base = metricBaseLabel(metric);
  return setor === "todos" ? base : `${base} — ${setorLabel(setor)}`;
}

function metricColorbarTitle(metric) {
  switch (metric) {
    case "mri_per_capita_scaled": return "MRI/1M hab";
    case "total_mri_avg":         return "MRI";
    default:                      return metric;
  }
}

// ─── Formatters ──────────────────────────────────────────────────────────────

function formatNumber(v, { decimals = 2 } = {}) {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  if (Math.abs(v) >= 1e9) return v.toExponential(3);
  return new Intl.NumberFormat("pt-BR", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  }).format(v);
}

function formatInt(v) {
  const n = typeof v === "number" ? v : Number(v);
  if (!Number.isFinite(n)) return "—";
  return new Intl.NumberFormat("pt-BR", { maximumFractionDigits: 0 }).format(Math.trunc(n));
}

// ─── Aggregation helpers ─────────────────────────────────────────────────────

function sumByYear(rows, field) {
  const sums = new Map();
  for (const r of rows) {
    const y = r.ano;
    if (!sums.has(y)) sums.set(y, { sum: 0, n: 0 });
    const v = r[field];
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

// ─── Geography helpers ───────────────────────────────────────────────────────

const UF_TO_REGIAO = {
  AC: "Norte",    AL: "Nordeste",   AM: "Norte",     AP: "Norte",
  BA: "Nordeste", CE: "Nordeste",   DF: "Centro-Oeste", ES: "Sudeste",
  GO: "Centro-Oeste", MA: "Nordeste", MG: "Sudeste",  MS: "Centro-Oeste",
  MT: "Centro-Oeste", PA: "Norte",  PB: "Nordeste",  PE: "Nordeste",
  PI: "Nordeste", PR: "Sul",        RJ: "Sudeste",   RN: "Nordeste",
  RO: "Norte",    RR: "Norte",      RS: "Sul",       SC: "Sul",
  SE: "Nordeste", SP: "Sudeste",    TO: "Norte",
};

const REGIOES_ORDER = ["Norte", "Nordeste", "Centro-Oeste", "Sudeste", "Sul"];

function regionColors(isDark) {
  return isDark
    ? { Norte: "#60a5fa", Nordeste: "#34d399", "Centro-Oeste": "#fbbf24", Sudeste: "#f472b6", Sul: "#a78bfa" }
    : { Norte: "#2563eb", Nordeste: "#059669", "Centro-Oeste": "#d97706", Sudeste: "#db2777", Sul: "#7c3aed" };
}

// ─── KPI cards ────────────────────────────────────────────────────────────────

function renderKPIs({ rows, yearValue, years }) {
  const el = document.getElementById("kpiRow");
  if (!el) return;

  const isDark = document.body.classList.contains("dark");
  const isAgg = yearValue === "AGG";
  const latestYear = years[years.length - 1];
  const displayYear = isAgg ? latestYear : parseInt(yearValue, 10);
  const prevYear = displayYear - 1;

  const sumRows = (arr, field) => arr.reduce((s, r) => s + (r[field] || 0), 0);

  // AGG: average the Brazil-wide annual sum across all years.
  // Specific year: that year's values directly.
  function kpiSum(field) {
    if (!isAgg) return sumRows(rows.filter((r) => r.ano === displayYear), field);
    const byYear = sumByYear(rows, field);
    const vals = years.map((y) => byYear.get(y) ?? 0);
    return vals.reduce((s, v) => s + v, 0) / vals.length;
  }

  const total = kpiSum("total_mri_avg");
  const sus   = kpiSum("sus_total_mri_avg");
  const priv  = kpiSum("priv_total_mri_avg");
  const cnes  = kpiSum("cnes_count");

  const rows05   = rows.filter((r) => r.ano === 2005);
  const rowsPrev = rows.filter((r) => r.ano === prevYear);
  const sus05    = sumRows(rows05, "sus_total_mri_avg");
  const priv05   = sumRows(rows05, "priv_total_mri_avg");

  // Three distinct cases — no mixing of YoY and long-term in the same render.
  let growthSusVal, growthPrivVal, growthSub, growthLabel, growthIsBase;

  if (isAgg) {
    // AGG: cumulative growth from 2005 → latest year
    const susLatest  = sumRows(rows.filter((r) => r.ano === latestYear), "sus_total_mri_avg");
    const privLatest = sumRows(rows.filter((r) => r.ano === latestYear), "priv_total_mri_avg");
    growthSusVal  = sus05  > 0 ? ((susLatest  - sus05)  / sus05)  * 100 : null;
    growthPrivVal = priv05 > 0 ? ((privLatest - priv05) / priv05) * 100 : null;
    growthSub   = `2005 → ${latestYear}`;
    growthLabel = "Cresc. acum.";
    growthIsBase = false;
  } else if (displayYear === years[0]) {
    // First year of the series — no prior year to compare
    growthSusVal  = null;
    growthPrivVal = null;
    growthSub   = "ano base";
    growthLabel = "Cresc. YoY";
    growthIsBase = true;
  } else {
    // Specific year: pure YoY vs previous year
    const susPrev  = sumRows(rowsPrev, "sus_total_mri_avg");
    const privPrev = sumRows(rowsPrev, "priv_total_mri_avg");
    growthSusVal  = susPrev  > 0 ? ((sus  - susPrev)  / susPrev)  * 100 : null;
    growthPrivVal = privPrev > 0 ? ((priv - privPrev) / privPrev) * 100 : null;
    growthSub   = `${prevYear} → ${displayYear}`;
    growthLabel = "Cresc. YoY";
    growthIsBase = false;
  }

  const countSub = isAgg ? `média ${years[0]}–${latestYear}` : `em ${displayYear}`;
  const susShare  = total > 0 ? (sus  / total) * 100 : 0;
  const privShare = total > 0 ? (priv / total) * 100 : 0;

  const isUpSus  = growthSusVal  !== null && growthSusVal  >= 0;
  const isUpPriv = growthPrivVal !== null && growthPrivVal >= 0;

  const susColor   = isDark ? "var(--sus)"  : "#1e40af";
  const privColor  = isDark ? "var(--priv)" : "#9d174d";
  const greenColor = isDark ? "#4ade80" : "#16a34a";
  const redColor   = isDark ? "#f87171" : "#dc2626";
  const tealColor  = isDark ? "#34d399" : "#0d7858";

  const cards = [
    {
      label: "Total RM no Brasil",
      value: formatInt(Math.round(total)),
      sub: `equipamentos · ${countSub}`,
      borderColor: isDark ? "#60a5fa" : "#2563eb",
      color: isDark ? "#93c5fd" : "#1e3a8a",
    },
    {
      label: "Equipamentos SUS",
      value: formatInt(Math.round(sus)),
      sub: `${formatNumber(susShare, { decimals: 1 })}% do total · ${countSub}`,
      borderColor: "var(--sus)",
      color: susColor,
    },
    {
      label: "Equipamentos Privados",
      value: formatInt(Math.round(priv)),
      sub: `${formatNumber(privShare, { decimals: 1 })}% do total · ${countSub}`,
      borderColor: "var(--priv)",
      color: privColor,
    },
    {
      label: "Estabelecimentos CNES",
      value: formatInt(cnes),
      sub: `com RM · ${countSub}`,
      borderColor: isDark ? "#34d399" : "#059669",
      color: tealColor,
    },
    growthIsBase
      ? { label: `${growthLabel} Público`,  value: "ano base", sub: growthSub, borderColor: "#94a3b8", color: "#94a3b8" }
      : growthSusVal !== null
        ? {
            label: `${growthLabel} Público`,
            value: `${isUpSus ? "▲" : "▼"} ${isUpSus ? "+" : ""}${formatNumber(growthSusVal, { decimals: 1 })}%`,
            sub: growthSub,
            borderColor: isUpSus ? (isDark ? "#4ade80" : "#16a34a") : redColor,
            color: isUpSus ? greenColor : redColor,
          }
        : { label: `${growthLabel} Público`,  value: "—", sub: growthSub, borderColor: "#94a3b8", color: "#94a3b8" },
    growthIsBase
      ? { label: `${growthLabel} Privado`, value: "ano base", sub: growthSub, borderColor: "#94a3b8", color: "#94a3b8" }
      : growthPrivVal !== null
        ? {
            label: `${growthLabel} Privado`,
            value: `${isUpPriv ? "▲" : "▼"} ${isUpPriv ? "+" : ""}${formatNumber(growthPrivVal, { decimals: 1 })}%`,
            sub: growthSub,
            borderColor: isUpPriv ? (isDark ? "#f472b6" : "#be185d") : redColor,
            color: isUpPriv ? privColor : redColor,
          }
        : { label: `${growthLabel} Privado`, value: "—", sub: growthSub, borderColor: "#94a3b8", color: "#94a3b8" },
  ];

  el.innerHTML = cards
    .map(
      (c) => `
    <div class="kpiCard" style="--card-color:${c.borderColor}; border-top-color:${c.borderColor}">
      <div class="kpiLabel">${c.label}</div>
      <div class="kpiValue" style="color:${c.color}">${c.value}</div>
      <div class="kpiSub">${c.sub}</div>
    </div>`
    )
    .join("");
}

// ─── Charts ───────────────────────────────────────────────────────────────────

function renderBar({ rows, years, metric, barView }) {
  const isDark     = document.body.classList.contains("dark");
  const textColor  = isDark ? "rgba(232,238,249,0.9)" : "#0f172a";
  const gridColor  = isDark ? "rgba(255,255,255,0.07)" : "rgba(15,23,42,0.06)";
  const susColor   = isDark ? "#60a5fa" : "#2563eb";
  const privColor  = isDark ? "#f472b6" : "#be185d";
  const totalColor = isDark ? "#60a5fa" : "#2b6cb0";

  const isPerCapita = metric === "mri_per_capita_scaled";

  // barView: "stacked" | "todos" | "sus" | "privado"
  // "stacked" always uses count data (sus_total_mri_avg + priv_total_mri_avg),
  // ignoring the metric selector — stacking per-capita is not meaningful.
  // The other three options use whatever metric is selected.
  const showStacked = barView === "stacked";

  let traces;
  let barmode = "relative";
  let yLabel;

  if (showStacked) {
    // Use the metric-appropriate SUS and private fields.
    // sus + priv per-capita add up correctly (same population denominator).
    const susField  = resolveField(metric, "sus");
    const privField = resolveField(metric, "privado");
    const susSums   = sumByYear(rows, susField);
    const privSums  = sumByYear(rows, privField);
    const totals    = years.map((y) => (susSums.get(y) ?? 0) + (privSums.get(y) ?? 0));

    const fmt = (v) => v === null ? "" : (isPerCapita ? formatNumber(v, { decimals: 2 }) : formatInt(Math.round(v)));

    traces = [
      {
        type: "bar", name: "SUS",
        x: years, y: years.map((y) => susSums.get(y) ?? null),
        marker: { color: susColor },
        hovertemplate: `SUS %{x}: <b>%{y:${isPerCapita ? ".2f" : ".0f"}}</b><extra></extra>`,
      },
      {
        type: "bar", name: "Privado",
        x: years, y: years.map((y) => privSums.get(y) ?? null),
        marker: { color: privColor },
        text: totals.map(fmt),
        textposition: "outside",
        cliponaxis: false,
        textfont: { size: 10, color: textColor },
        hovertemplate: `Privado %{x}: <b>%{y:${isPerCapita ? ".2f" : ".0f"}}</b><extra></extra>`,
      },
    ];
    yLabel = isPerCapita ? "MRI por 1M hab (Brasil)" : "Total MRI/ano (Brasil)";
  } else {
    // "todos" | "sus" | "privado" — single bar, respects metric selector
    const effectiveSetor = barView === "todos" ? "todos"
                         : barView === "sus"   ? "sus"
                         : "privado";
    const field = resolveField(metric, effectiveSetor);
    const color = effectiveSetor === "sus" ? susColor
                : effectiveSetor === "privado" ? privColor
                : totalColor;
    const sums = sumByYear(rows, field);
    const vals = years.map((y) => sums.get(y) ?? null);

    traces = [{
      type: "bar", x: years, y: vals,
      marker: { color },
      text: vals.map((v) =>
        v === null ? "" : (isPerCapita ? formatNumber(v, { decimals: 2 }) : formatInt(Math.round(v)))
      ),
      textposition: "outside",
      cliponaxis: false,
      textfont: { size: 10, color: textColor },
      hovertemplate: "%{x}: <b>%{text}</b><extra></extra>",
    }];
    barmode = "group";
    yLabel = metricLabel(metric, effectiveSetor);
  }

  const subtitleMap = {
    stacked:  `SUS + Privado empilhado · ${metricBaseLabel(metric)}`,
    todos:    metricLabel(metric, "todos"),
    sus:      metricLabel(metric, "sus"),
    privado:  metricLabel(metric, "privado"),
  };
  const sub = document.getElementById("barSubtitle");
  if (sub) sub.textContent = subtitleMap[barView] ?? "";

  Plotly.newPlot(
    "barWrap", traces,
    {
      paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)",
      barmode,
      margin: { t: 28, r: 16, b: 58, l: 68 },
      showlegend: showStacked,
      legend: { orientation: "h", x: 0, y: -0.22, xanchor: "left", font: { color: textColor, size: 12 } },
      yaxis: {
        title: yLabel, automargin: true, gridcolor: gridColor, zerolinecolor: gridColor,
        tickfont: { color: textColor, size: 11 }, titlefont: { color: textColor, size: 12 },
        // Add 15% headroom so the outside labels don't clip
        rangemode: "tozero",
      },
      xaxis: {
        type: "category", tickmode: "array", tickvals: years, ticktext: years.map(String),
        tickangle: -45, automargin: true, gridcolor: "rgba(0,0,0,0)",
        tickfont: { color: textColor, size: 11 },
      },
      font: { color: textColor },
    },
    { displayModeBar: false, responsive: true }
  );
}

function renderRanking({ rows, years, metric, setor, yearValue }) {
  const field = resolveField(metric, setor);
  const label = metricLabel(metric, setor);
  const isDark = document.body.classList.contains("dark");

  const colors = regionColors(isDark);
  const textColor  = isDark ? "rgba(232,238,249,0.9)" : "#0f172a";
  const gridColor  = isDark ? "rgba(255,255,255,0.07)" : "rgba(15,23,42,0.06)";

  const isAgg = yearValue === "AGG";
  const year = isAgg ? null : parseInt(yearValue, 10);
  const filtered = isAgg
    ? rows.filter((r) => typeof r.ano === "number" && r.ano >= 2005)
    : rows.filter((r) => r.ano === year);

  const byUf = new Map();
  for (const r of filtered) {
    const v = r[field];
    if (v === null || v === undefined || Number.isNaN(Number(v))) continue;
    if (!byUf.has(r.estado)) byUf.set(r.estado, { sum: 0, n: 0 });
    const acc = byUf.get(r.estado);
    acc.sum += Number(v);
    acc.n += 1;
  }

  const states = Array.from(byUf.entries())
    .map(([uf, acc]) => ({ uf, val: acc.n > 0 ? acc.sum / acc.n : 0 }))
    .sort((a, b) => a.val - b.val);

  const x = states.map((r) => r.val);
  const y = states.map((r) => r.uf);
  const text = states.map((r) => formatNumber(r.val, { decimals: 2 }));
  const markerColors = y.map((uf) => colors[UF_TO_REGIAO[uf]] || "#888");

  // Region legend as annotation-style custom legend (plotly shapes)
  const regionLegendTraces = REGIOES_ORDER.map((reg) => ({
    type: "scatter", mode: "markers", name: reg,
    x: [null], y: [null],
    marker: { color: colors[reg], size: 10, symbol: "square" },
    showlegend: true,
  }));

  const rankingSub = document.getElementById("rankingSubtitle");
  if (rankingSub) {
    rankingSub.textContent = isAgg
      ? `Média ${years[0]}–${years[years.length - 1]} · ${metricLabel(metric, setor)}`
      : `${year} · ${metricLabel(metric, setor)}`;
  }

  Plotly.newPlot(
    "rankingWrap",
    [
      {
        type: "bar", orientation: "h", x, y, text, textposition: "outside",
        cliponaxis: false, marker: { color: markerColors },
        hovertemplate: "<b>%{y}</b>: %{text}<extra></extra>",
        showlegend: false,
      },
      ...regionLegendTraces,
    ],
    {
      paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)",
      margin: { t: 8, r: 80, b: 52, l: 44 },
      showlegend: true,
      legend: { orientation: "h", x: 0, y: -0.1, xanchor: "left", font: { color: textColor, size: 11 } },
      xaxis: { title: label, automargin: true, gridcolor: gridColor, zerolinecolor: gridColor,
               tickfont: { color: textColor, size: 11 }, titlefont: { color: textColor, size: 12 } },
      yaxis: { automargin: true, tickfont: { color: textColor, size: 11 }, type: "category" },
      font: { color: textColor },
    },
    { displayModeBar: false, responsive: true }
  );
}

function renderRegion({ rows, years, metric, setor }) {
  // Shows metric aggregated per region per year, respecting both metric and setor.
  //
  // For "total_mri_avg"       → sum(total_mri field) across states in region
  // For "mri_per_capita_scaled" → sum(total_mri) / sum(populacao) × 10^6 per region
  //   (proper population-weighted per-capita, not an average of state per-capitas)
  const totalField = resolveField("total_mri_avg", setor);
  const isPerCapita = metric === "mri_per_capita_scaled";
  const isDark = document.body.classList.contains("dark");

  const colors = regionColors(isDark);
  const textColor = isDark ? "rgba(232,238,249,0.9)" : "#0f172a";
  const gridColor = isDark ? "rgba(255,255,255,0.07)" : "rgba(15,23,42,0.06)";

  // Accumulate total MRI machines + population per region per year
  const byRegYear = new Map(); // region → Map(year → { total, pop })
  for (const r of rows) {
    if (typeof r.ano !== "number" || r.ano < 2005) continue;
    const reg = UF_TO_REGIAO[r.estado];
    if (!reg) continue;
    if (!byRegYear.has(reg)) byRegYear.set(reg, new Map());
    const m = byRegYear.get(reg);
    if (!m.has(r.ano)) m.set(r.ano, { total: 0, pop: 0 });
    const acc = m.get(r.ano);

    const vTotal = r[totalField];
    if (vTotal != null && !Number.isNaN(vTotal)) acc.total += vTotal;

    const vPop = r.populacao;
    if (vPop != null && !Number.isNaN(vPop)) acc.pop += vPop;
  }

  const traces = REGIOES_ORDER.filter((reg) => byRegYear.has(reg)).map((reg) => {
    const m = byRegYear.get(reg);
    const y = years.map((yr) => {
      const acc = m.get(yr);
      if (!acc) return null;
      if (isPerCapita) return acc.pop > 0 ? (acc.total / acc.pop) * 1e6 : 0;
      return acc.total;
    });
    const text = y.map((v) => (v === null ? "" : formatNumber(v, { decimals: isPerCapita ? 2 : 1 })));
    return {
      type: "scatter", mode: "lines+markers", name: reg,
      x: years, y, text,
      line: { color: colors[reg] || "#888", width: 2.5 },
      marker: { color: colors[reg] || "#888", size: 6 },
      hovertemplate: `<b>${reg}</b> %{x}: %{text}<extra></extra>`,
    };
  });

  const yLabel = metricLabel(metric, setor);

  const regionSub = document.getElementById("regionSubtitle");
  if (regionSub) regionSub.textContent = metricLabel(metric, setor) + " · por região · 2005–2025";

  Plotly.newPlot(
    "regionWrap", traces,
    {
      paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)",
      margin: { t: 4, r: 16, b: 60, l: 68 },
      legend: { orientation: "h", x: 0, y: -0.22, xanchor: "left", yanchor: "top", font: { color: textColor, size: 12 } },
      yaxis: { title: yLabel, rangemode: "tozero", automargin: true,
               gridcolor: gridColor, zerolinecolor: gridColor,
               tickfont: { color: textColor, size: 11 }, titlefont: { color: textColor, size: 12 } },
      xaxis: { type: "category", tickmode: "array",
               tickvals: years, ticktext: years.map(String), tickangle: -45, automargin: true,
               gridcolor: "rgba(0,0,0,0)", tickfont: { color: textColor, size: 11 } },
      font: { color: textColor },
    },
    { displayModeBar: false, responsive: true }
  );
}

function renderDelta({ rows, years, deltaView }) {
  // deltaView: "stacked" | "todos" | "sus" | "privado"
  const isDark    = document.body.classList.contains("dark");
  const textColor = isDark ? "rgba(232,238,249,0.9)" : "#0f172a";
  const gridColor = isDark ? "rgba(255,255,255,0.07)" : "rgba(15,23,42,0.06)";
  const susColor  = isDark ? "#60a5fa" : "#2563eb";
  const privColor = isDark ? "#f472b6" : "#be185d";
  const totalColor = isDark ? "#60a5fa" : "#2b6cb0";

  const susSums  = sumByYear(rows, "sus_total_mri_avg");
  const privSums = sumByYear(rows, "priv_total_mri_avg");

  const deltaYears = years.slice(1);
  const susDelta   = deltaYears.map((y, i) => (susSums.get(y) ?? 0) - (susSums.get(years[i]) ?? 0));
  const privDelta  = deltaYears.map((y, i) => (privSums.get(y) ?? 0) - (privSums.get(years[i]) ?? 0));
  const totDelta   = deltaYears.map((_, i) => susDelta[i] + privDelta[i]);

  let traces;
  let showLegend = false;

  // Integer labels shown above each bar; hover uses same text
  const fmtInt  = (v) => formatInt(Math.round(v));
  const susLbl  = susDelta.map(fmtInt);
  const privLbl = privDelta.map(fmtInt);
  const totLbl  = totDelta.map(fmtInt);

  const labelProps = {
    textposition: "outside",
    cliponaxis: false,
    textfont: { size: 10, color: textColor },
  };

  if (deltaView === "stacked") {
    showLegend = true;
    // For the stacked view show the grand total above the full stack (on the
    // Privado / top trace). The SUS trace carries empty text so only one label
    // appears per column.
    traces = [
      {
        type: "bar", name: "SUS",
        x: deltaYears, y: susDelta,
        text: susLbl.map(() => ""),   // no per-segment label; total is on top trace
        marker: { color: susColor },
        hovertemplate: "SUS Δ%{x}: <b>%{customdata}</b><extra></extra>",
        customdata: susLbl,
        ...labelProps,
      },
      {
        type: "bar", name: "Privado",
        x: deltaYears, y: privDelta,
        text: totLbl,                 // grand total above full stack
        marker: { color: privColor },
        hovertemplate: "Privado Δ%{x}: <b>%{customdata}</b><extra></extra>",
        customdata: privLbl,
        ...labelProps,
      },
    ];
  } else if (deltaView === "sus") {
    traces = [{
      type: "bar", name: "SUS",
      x: deltaYears, y: susDelta, text: susLbl, customdata: susLbl,
      marker: { color: susColor },
      hovertemplate: "SUS Δ%{x}: <b>%{customdata}</b><extra></extra>",
      ...labelProps,
    }];
  } else if (deltaView === "privado") {
    traces = [{
      type: "bar", name: "Privado",
      x: deltaYears, y: privDelta, text: privLbl, customdata: privLbl,
      marker: { color: privColor },
      hovertemplate: "Privado Δ%{x}: <b>%{customdata}</b><extra></extra>",
      ...labelProps,
    }];
  } else {
    // "todos" — combined total delta as single bar
    traces = [{
      type: "bar", name: "Total",
      x: deltaYears, y: totDelta, text: totLbl, customdata: totLbl,
      marker: { color: totalColor },
      hovertemplate: "Total Δ%{x}: <b>%{customdata}</b><extra></extra>",
      ...labelProps,
    }];
  }

  const subtitleMap = {
    stacked: "SUS + Privado empilhado",
    todos:   "Total combinado",
    sus:     "Público (SUS)",
    privado: "Privado",
  };
  const deltaSub = document.getElementById("deltaSubtitle");
  if (deltaSub) deltaSub.textContent = subtitleMap[deltaView] ?? "";

  Plotly.newPlot(
    "deltaWrap", traces,
    {
      paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)",
      barmode: "relative",
      margin: { t: 28, r: 16, b: 58, l: 68 },
      showlegend: showLegend,
      legend: { orientation: "h", x: 0, y: -0.22, xanchor: "left", font: { color: textColor, size: 12 } },
      shapes: [{
        type: "line",
        xref: "paper", x0: 0, x1: 1,
        yref: "y",     y0: 0, y1: 0,
        line: { color: textColor, width: 1, dash: "dot" },
      }],
      separators: ",.",
      yaxis: {
        title: "Δ equipamentos/ano", automargin: true,
        tickformat: ".2f",
        gridcolor: gridColor, zerolinecolor: gridColor,
        tickfont: { color: textColor, size: 11 }, titlefont: { color: textColor, size: 12 },
      },
      xaxis: {
        type: "category", tickmode: "array", tickvals: deltaYears, ticktext: deltaYears.map(String),
        tickangle: -45, automargin: true, gridcolor: "rgba(0,0,0,0)",
        tickfont: { color: textColor, size: 11 },
      },
      font: { color: textColor },
    },
    { displayModeBar: false, responsive: true }
  );
}

function getReverseScale(colorscale) {
  return ["Viridis", "Cividis", "Blues", "Greens", "Greys", "YlGnBu", "YlOrRd"].includes(colorscale);
}

function renderMap({ rows, metric, setor, colorscale, yearValue, years, geojson }) {
  const field = resolveField(metric, setor);
  const isDark = document.body.classList.contains("dark");

  const textColor   = isDark ? "rgba(232,238,249,0.9)" : "#0f172a";
  const bg          = isDark ? "#161b22" : "#ffffff";
  const borderColor = isDark ? "rgba(255,255,255,0.4)" : "#ffffff";

  const isAgg = yearValue === "AGG";
  const year = isAgg ? null : parseInt(yearValue, 10);
  const reverseScale = getReverseScale(colorscale);

  const rowsNum = rows.filter((r) => typeof r.ano === "number");
  const rowsYear = isAgg ? rowsNum : rowsNum.filter((r) => r.ano === year);

  // Fields for the cross-sector hover breakdown
  const fieldTodos = resolveField(metric, "todos");
  const fieldSus   = resolveField(metric, "sus");
  const fieldPriv  = resolveField(metric, "privado");

  const byUf = new Map();
  for (const r of rowsYear) {
    const uf = r.estado;
    if (!byUf.has(uf)) {
      byUf.set(uf, {
        uf,
        metricSum: 0, metricN: 0,
        todosSum: 0,  todosN: 0,
        susSum: 0,    susN: 0,
        privSum: 0,   privN: 0,
        popLast: null, popLastYear: -Infinity,
      });
    }
    const acc = byUf.get(uf);

    const v = r[field];
    if (v !== null && v !== undefined && !Number.isNaN(v)) { acc.metricSum += v; acc.metricN += 1; }

    const vT = r[fieldTodos];
    if (vT !== null && vT !== undefined && !Number.isNaN(vT)) { acc.todosSum += vT; acc.todosN += 1; }

    const vS = r[fieldSus];
    if (vS !== null && vS !== undefined && !Number.isNaN(vS)) { acc.susSum += vS; acc.susN += 1; }

    const vP = r[fieldPriv];
    if (vP !== null && vP !== undefined && !Number.isNaN(vP)) { acc.privSum += vP; acc.privN += 1; }

    if (r.populacao !== null && r.populacao !== undefined && !Number.isNaN(r.populacao)) {
      if (r.ano >= acc.popLastYear) { acc.popLastYear = r.ano; acc.popLast = r.populacao; }
    }
  }

  const rowsUf = Array.from(byUf.values()).sort((a, b) => a.uf.localeCompare(b.uf));
  const locations = rowsUf.map((r) => r.uf);

  const z = rowsUf.map((r) => r.metricN === 0 ? null : r.metricSum / r.metricN);

  const hover = rowsUf.map((r) => {
    const val   = r.metricN  === 0 ? null : r.metricSum  / r.metricN;
    const todos = r.todosN   === 0 ? null : r.todosSum   / r.todosN;
    const sus   = r.susN     === 0 ? null : r.susSum     / r.susN;
    const priv  = r.privN    === 0 ? null : r.privSum    / r.privN;
    const pop   = r.popLast;
    const popYr = r.popLastYear > -Infinity ? ` (${r.popLastYear})` : "";

    const base = metricBaseLabel(metric);
    const aggNote = isAgg ? " (média anual)" : "";

    return (
      `<b>${r.uf}</b><br>` +
      `${base}${aggNote}: <b>${formatNumber(val)}</b><br>` +
      `── Todos: ${formatNumber(todos)} · SUS: ${formatNumber(sus)} · Privado: ${formatNumber(priv)}<br>` +
      `Pop${popYr}: ${formatNumber(pop, { decimals: 0 })}`
    );
  });

  const mapTitle = isAgg
    ? `Distribuição por UF<br>Média anual (${years[0]}–${years[years.length - 1]})`
    : `Distribuição por UF — ${year}`;

  const titleFontSize = isAgg ? 16 : 20;
  const titleY = isAgg ? 1.06 : 1.02;

  Plotly.newPlot(
    "mapWrap",
    [{
      type: "choropleth", geojson, featureidkey: "id", locations, z,
      text: hover, hovertemplate: "%{text}<extra></extra>",
      colorscale, reversescale: reverseScale,
      colorbar: {
        title: { text: metricColorbarTitle(metric), font: { color: textColor } },
        tickfont: { color: textColor },
        outlinecolor: isDark ? "rgba(255,255,255,0.18)" : "rgba(0,0,0,0.15)",
      },
      marker: { line: { color: borderColor, width: 0.6 } },
    }],
    {
      paper_bgcolor: bg, plot_bgcolor: bg,
      margin: { t: isAgg ? 96 : 55, r: 10, b: 10, l: 10 },
      geo: { fitbounds: "locations", visible: false,
             projection: { type: "mercator", scale: 1.55 },
             center: { lat: -14, lon: -52 }, bgcolor: bg },
      annotations: [{
        xref: "paper", yref: "paper", x: 0.5, y: titleY,
        xanchor: "center", yanchor: "bottom", showarrow: false,
        text: mapTitle, align: "center",
        bgcolor: "rgba(0,0,0,0)", borderwidth: 0, borderpad: 0,
        font: { size: titleFontSize, color: textColor },
      }],
      font: { color: textColor },
    },
    { displayModeBar: false, responsive: true }
  );
}

// ─── Theme ────────────────────────────────────────────────────────────────────

function applyTheme(theme) {
  const isDark = theme === "dark";
  document.body.classList.toggle("dark", isDark);
  try { localStorage.setItem("theme", isDark ? "dark" : "light"); } catch (_) {}
  const toggle = document.getElementById("toggleDark");
  if (toggle && toggle.type === "checkbox") toggle.checked = isDark;
}

function initTheme() {
  let saved = null;
  try { saved = localStorage.getItem("theme"); } catch (_) {}
  if (saved === "dark" || saved === "light") { applyTheme(saved); return; }
  applyTheme(window.matchMedia?.("(prefers-color-scheme: dark)").matches ? "dark" : "light");
}

function initBuildDate() {
  const el = document.getElementById("buildDate");
  if (el) el.textContent = new Date().toLocaleDateString("pt-BR");
}

// ─── Loading / error UI ───────────────────────────────────────────────────────

function setLoading(isLoading) {
  const el = document.getElementById("loading");
  if (el) el.style.display = isLoading ? "block" : "none";
}

function setError(msg) {
  const el = document.getElementById("error");
  if (!el) return;
  el.textContent = msg || "";
  el.style.display = msg ? "block" : "none";
}

function setZipProgress({ visible, pct = 0, label = "" }) {
  const wrap = document.getElementById("zipProgress");
  const bar  = document.getElementById("zipProgressBar");
  const txt  = document.getElementById("zipProgressText");
  const btn  = document.getElementById("downloadAllPng");

  const p = Math.max(0, Math.min(100, Number.isFinite(pct) ? pct : 0));
  if (btn) {
    if (!btn.dataset.originalText) btn.dataset.originalText = btn.textContent;
    btn.disabled = visible;
    btn.textContent = visible
      ? (label ? `${label} ${Math.round(p)}%` : `Gerando ZIP… ${Math.round(p)}%`)
      : btn.dataset.originalText;
  }
  if (!wrap || !bar || !txt) return;
  wrap.style.display = visible ? "block" : "none";
  bar.value = p;
  txt.textContent = label || `${Math.round(p)}%`;
}

// ─── GeoJSON helpers ──────────────────────────────────────────────────────────

function normalizeGeoJsonIds(geojson) {
  for (const f of geojson.features) {
    if (f.id) continue;
    const p = f.properties || {};
    f.id = p.sigla || p.SIGLA || p.uf || p.UF || p.abbrev || null;
  }
  return geojson;
}

async function loadJson(urlOrUrls) {
  const urls = Array.isArray(urlOrUrls) ? urlOrUrls : [urlOrUrls];
  let lastErr = null;
  for (const url of urls) {
    try {
      const resp = await fetch(url, { cache: "no-store" });
      if (!resp.ok) throw new Error(`Failed to load ${url}: ${resp.status}`);
      return await resp.json();
    } catch (e) { lastErr = e; }
  }
  throw lastErr || new Error(`Failed to load: ${urls.join(", ")}`);
}

// ─── Excel export ─────────────────────────────────────────────────────────────

function safeFileToken(s) {
  return String(s).replace(/[^a-zA-Z0-9._-]+/g, "_");
}

function sheetNameFor(metric, setor, kind) {
  const base = { mri_per_capita_scaled: "MRI_1Mhab", total_mri_avg: "Total_MRI" }[metric] || metric;
  const s = setor === "todos" ? "" : `_${setor.toUpperCase()}`;
  return `${base}${s}_${kind === "bar" ? "Barras" : "Mapa"}`.slice(0, 31);
}

function buildPopulationTab({ wb, rows, years }) {
  const intro = [{
    Campo: "Fonte",
    Valor: "IBGE - SIDRA (API de Agregados) - População residente estimada (agregado 6579, variável 9324, N3=UF).",
  }];
  const popByUfYear = new Map();
  for (const r of rows) {
    if (typeof r.ano !== "number") continue;
    if (r.populacao !== null && r.populacao !== undefined && !Number.isNaN(r.populacao))
      popByUfYear.set(`${r.estado}|${r.ano}`, r.populacao);
  }
  const ufs = Array.from(new Set(rows.map((r) => r.estado))).sort();
  const table = [];
  for (const uf of ufs)
    for (const ano of years)
      table.push({ UF: uf, Ano: ano, Populacao: popByUfYear.get(`${uf}|${ano}`) ?? null });

  const ws = XLSX.utils.json_to_sheet(intro);
  XLSX.utils.sheet_add_aoa(ws, [[""], ["UF", "Ano", "Populacao"]], { origin: -1 });
  XLSX.utils.sheet_add_json(ws, table, { origin: -1, skipHeader: true });
  XLSX.utils.book_append_sheet(wb, ws, "Populacao");
}

function buildRegiaoTab({ wb, rows, years }) {
  const regiaoData = [];
  const setores = ["todos", "sus", "privado"];

  for (const setor of setores) {
    const totalField = resolveField("total_mri_avg", setor);

    const byRegYear = new Map();
    for (const r of rows) {
      if (typeof r.ano !== "number" || r.ano < 2005) continue;
      const reg = UF_TO_REGIAO[r.estado];
      if (!reg) continue;
      if (!byRegYear.has(reg)) byRegYear.set(reg, new Map());
      const m = byRegYear.get(reg);
      if (!m.has(r.ano)) m.set(r.ano, { total: 0, pop: 0 });
      const acc = m.get(r.ano);
      const vTotal = r[totalField];
      if (vTotal != null && !Number.isNaN(vTotal)) acc.total += vTotal;
      const vPop = r.populacao;
      if (vPop != null && !Number.isNaN(vPop)) acc.pop += vPop;
    }

    for (const reg of REGIOES_ORDER) {
      if (!byRegYear.has(reg)) continue;
      const m = byRegYear.get(reg);
      for (const year of years) {
        const acc = m.get(year);
        if (!acc) continue;
        regiaoData.push({
          Regiao: reg,
          Ano: year,
          Setor: setorLabel(setor),
          Total_MRI: Math.round(acc.total * 100) / 100,
          MRI_por_1M_hab: acc.pop > 0 ? Math.round((acc.total / acc.pop) * 1e6 * 1000) / 1000 : 0,
          Populacao: acc.pop,
        });
      }
    }
  }

  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(regiaoData), "Regioes");
}

function buildExcelWorkbookAll({ rows, years }) {
  const wb = XLSX.utils.book_new();
  buildPopulationTab({ wb, rows, years });
  buildRegiaoTab({ wb, rows, years });

  const metrics = ["mri_per_capita_scaled", "total_mri_avg"];
  const setores = ["todos", "sus", "privado"];

  for (const metric of metrics) {
    for (const setor of setores) {
      const field = resolveField(metric, setor);
      const label = metricLabel(metric, setor);

      // Bars sheet (Brazil total by year)
      const sums = sumByYear(rows, field);
      const barRows = years.map((y) => ({
        Ano: y, Setor: setorLabel(setor), Metrica: field, MetricaLabel: label,
        TotalBrasil: sums.get(y) ?? null,
      }));
      XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(barRows), sheetNameFor(metric, setor, "bar"));

      // Map sheet (all UFs, all years + AGG)
      const rowsNum = rows.filter((r) => typeof r.ano === "number");
      const mapAllRows = [];
      for (const y of years) {
        const rowsYear = rowsNum.filter((r) => r.ano === y);
        for (const r of rowsYear) {
          const v = r[field];
          mapAllRows.push({
            Ano: y, UF: r.estado, Setor: setorLabel(setor), Metrica: field,
            MetricaLabel: label, Valor: v ?? null, Populacao: r.populacao ?? null,
          });
        }
      }
      XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(mapAllRows), sheetNameFor(metric, setor, "map"));
    }
  }
  return wb;
}

function downloadExcelComplete({ rows, years }) {
  const wb = buildExcelWorkbookAll({ rows, years });
  const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "");
  XLSX.writeFile(wb, `mri_br_completo_${stamp}.xlsx`, { compression: true });
}

// ─── PNG / ZIP export ─────────────────────────────────────────────────────────

function dataUrlToU8(dataUrl) {
  const base64 = dataUrl.split(",")[1];
  const binStr = atob(base64);
  const bytes = new Uint8Array(binStr.length);
  for (let i = 0; i < binStr.length; i += 1) bytes[i] = binStr.charCodeAt(i);
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

async function downloadAllMapsZip({
  rows, years, colorscale, geojson,
  mapSize    = { width: 1800, height: 1300 },
  barSize    = { width: 1600, height: 900  },
  regionSize = { width: 1600, height: 900  },
}) {
  const scratch = document.createElement("div");
  scratch.style.cssText = "position:fixed;left:-10000px;top:0;width:10px;height:10px;overflow:hidden;pointer-events:none;opacity:0";
  document.body.appendChild(scratch);

  const barDiv    = document.createElement("div"); barDiv.id    = "barExport";
  const mapDiv    = document.createElement("div"); mapDiv.id    = "mapExport";
  const regionDiv = document.createElement("div"); regionDiv.id = "regionExport";
  scratch.appendChild(barDiv);
  scratch.appendChild(mapDiv);
  scratch.appendChild(regionDiv);

  async function toPngBytesFromDiv(divEl, size) {
    try { await Plotly.Plots.resize(divEl); } catch (_) {}
    await new Promise((r) => setTimeout(r, 0));
    const dataUrl = await Plotly.toImage(divEl, { format: "png", ...size, scale: 2 });
    return dataUrlToU8(dataUrl);
  }

  try {
    const zip = new JSZip();
    const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "");
    const folder = zip.folder(`mri_br_${safeFileToken(colorscale)}_${stamp}`);

    const metrics = ["mri_per_capita_scaled", "total_mri_avg"];
    const setores = ["todos", "sus", "privado"];
    // steps: (bar + AGG map + per-year maps) × metrics × setores  +  region chart × setores
    const totalSteps = metrics.length * setores.length * (2 + years.length) + setores.length;
    let step = 0;
    const tick = (label) => { step += 1; setZipProgress({ visible: true, pct: (step / totalSteps) * 100, label }); };

    for (const m of metrics) {
      for (const s of setores) {
        const field = resolveField(m, s);
        const subfolder = folder.folder(`${safeFileToken(m)}_${s}`);

        // Bar chart
        {
          const isDark = document.body.classList.contains("dark");
          const sums = sumByYear(rows, field);
          const x = years;
          const y = years.map((yy) => sums.get(yy) ?? null);
          const text = y.map((v) => v === null ? "" : formatNumber(v));
          await Plotly.newPlot(barDiv, [{
            type: "bar", x, y, marker: { color: isDark ? "#60a5fa" : "#2b6cb0" },
            text, textposition: "outside", cliponaxis: false,
          }], {
            paper_bgcolor: isDark ? "#0f172a" : "#ffffff",
            plot_bgcolor: isDark ? "#0f172a" : "#ffffff",
            margin: { t: 10, r: 10, b: 70, l: 70 },
            yaxis: { title: metricLabel(m, s) },
            xaxis: { title: "Ano", type: "category", tickmode: "array",
                     tickvals: x, ticktext: x.map(String), tickangle: -45, automargin: true },
          }, { displayModeBar: false, responsive: true });
          subfolder.file(`barras_${safeFileToken(m)}_${s}.png`, await toPngBytesFromDiv(barDiv, barSize));
          tick(`${m}/${s}: barras`);
        }

        // Map AGG
        {
          await renderMapIntoDiv(mapDiv, { rows, metric: m, setor: s, colorscale, yearValue: "AGG", years, geojson });
          subfolder.file(`mapa_AGG_${safeFileToken(m)}_${s}.png`, await toPngBytesFromDiv(mapDiv, mapSize));
          tick(`${m}/${s}: AGG`);
        }

        // Maps per year
        for (const y of years) {
          await renderMapIntoDiv(mapDiv, { rows, metric: m, setor: s, colorscale, yearValue: String(y), years, geojson });
          subfolder.file(`mapa_${y}_${safeFileToken(m)}_${s}.png`, await toPngBytesFromDiv(mapDiv, mapSize));
          tick(`${m}/${s}: ${y}`);
        }
      }
    }

    // Region charts (one per setor, always total_mri_avg variant)
    const regionFolder = folder.folder("regioes");
    for (const s of setores) {
      await renderRegionIntoDiv(regionDiv, { rows, years, setor: s });
      regionFolder.file(`regioes_${s}.png`, await toPngBytesFromDiv(regionDiv, regionSize));
      tick(`regioes: ${s}`);
    }

    setZipProgress({ visible: true, pct: 99, label: "Compactando…" });
    const blob = await zip.generateAsync({ type: "blob", compression: "DEFLATE" });
    setZipProgress({ visible: true, pct: 100, label: "Iniciando download…" });
    triggerDownloadBlob(blob, `mri_br_mapas_${safeFileToken(colorscale)}_${stamp}.zip`);
  } finally {
    setZipProgress({ visible: false, pct: 0, label: "" });
    scratch.remove();
  }
}

/** Render a choropleth into an arbitrary div element (used for ZIP export). */
function renderMapIntoDiv(divEl, { rows, metric, setor, colorscale, yearValue, years, geojson }) {
  const field = resolveField(metric, setor);
  const isDark = document.body.classList.contains("dark");
  const bg = isDark ? "#0f172a" : "#ffffff";
  const textColor = isDark ? "rgba(232,238,249,0.92)" : "#111";
  const borderColor = isDark ? "rgba(255,255,255,0.45)" : "#ffffff";
  const reverseScale = getReverseScale(colorscale);

  const isAgg = yearValue === "AGG";
  const year = isAgg ? null : parseInt(yearValue, 10);
  const rowsNum = rows.filter((r) => typeof r.ano === "number");
  const rowsYear = isAgg ? rowsNum : rowsNum.filter((r) => r.ano === year);

  const byUf = new Map();
  for (const r of rowsYear) {
    const uf = r.estado;
    if (!byUf.has(uf)) byUf.set(uf, { uf, metricSum: 0, metricN: 0 });
    const v = r[field];
    if (v !== null && v !== undefined && !Number.isNaN(v)) {
      const acc = byUf.get(uf);
      acc.metricSum += v; acc.metricN += 1;
    }
  }

  const rowsUf = Array.from(byUf.values()).sort((a, b) => a.uf.localeCompare(b.uf));
  const locations = rowsUf.map((r) => r.uf);
  const z = rowsUf.map((r) => r.metricN === 0 ? null : r.metricSum / r.metricN);

  const mapTitle = isAgg
    ? `${metricLabel(metric, setor)} — Média ${years[0]}–${years[years.length - 1]}`
    : `${metricLabel(metric, setor)} — ${year}`;

  return Plotly.newPlot(divEl, [{
    type: "choropleth", geojson, featureidkey: "id", locations, z,
    colorscale, reversescale: reverseScale,
    colorbar: { title: { text: metricColorbarTitle(metric), font: { color: textColor } }, tickfont: { color: textColor } },
    marker: { line: { color: borderColor, width: 0.6 } },
  }], {
    paper_bgcolor: bg, plot_bgcolor: bg,
    margin: { t: 55, r: 10, b: 10, l: 10 },
    geo: { fitbounds: "locations", visible: false,
           projection: { type: "mercator", scale: 1.55 }, center: { lat: -14, lon: -52 }, bgcolor: bg },
    annotations: [{
      xref: "paper", yref: "paper", x: 0.5, y: 1.02,
      xanchor: "center", yanchor: "bottom", showarrow: false,
      text: mapTitle, font: { size: 16, color: textColor },
    }],
    font: { color: textColor },
  }, { displayModeBar: false, responsive: true });
}

/** Render the region line chart into an arbitrary div (used for ZIP export). */
function renderRegionIntoDiv(divEl, { rows, years, setor }) {
  const field = resolveField("total_mri_avg", setor);
  const isDark = document.body.classList.contains("dark");
  const colors = regionColors(isDark);
  const bg = isDark ? "#0f172a" : "#ffffff";
  const textColor = isDark ? "rgba(232,238,249,0.92)" : "#111";
  const gridColor = isDark ? "rgba(255,255,255,0.12)" : "rgba(0,0,0,0.08)";

  const byRegYear = new Map();
  for (const r of rows) {
    if (typeof r.ano !== "number" || r.ano < 2005) continue;
    const reg = UF_TO_REGIAO[r.estado];
    if (!reg) continue;
    const v = r[field];
    if (v === null || v === undefined || Number.isNaN(v)) continue;
    if (!byRegYear.has(reg)) byRegYear.set(reg, new Map());
    const m = byRegYear.get(reg);
    m.set(r.ano, (m.get(r.ano) ?? 0) + v);
  }

  const traces = REGIOES_ORDER.filter((reg) => byRegYear.has(reg)).map((reg) => {
    const m = byRegYear.get(reg);
    return {
      type: "scatter", mode: "lines+markers", name: reg,
      x: years, y: years.map((yr) => m.get(yr) ?? null),
      line: { color: colors[reg] || "#888", width: 2.5 },
      marker: { color: colors[reg] || "#888", size: 5 },
    };
  });

  return Plotly.newPlot(divEl, traces, {
    paper_bgcolor: bg, plot_bgcolor: bg,
    margin: { t: 10, r: 10, b: 90, l: 70 },
    legend: { orientation: "h", x: 0, y: -0.3, xanchor: "left", font: { color: textColor } },
    yaxis: { title: `Total RM — ${setorLabel(setor)}`, rangemode: "tozero", automargin: true,
             gridcolor: gridColor, zerolinecolor: gridColor,
             tickfont: { color: textColor }, titlefont: { color: textColor } },
    xaxis: { title: "Ano", type: "category", tickmode: "array", tickvals: years,
             ticktext: years.map(String), tickangle: -45, automargin: true,
             gridcolor: "rgba(0,0,0,0)", tickfont: { color: textColor }, titlefont: { color: textColor } },
    font: { color: textColor },
  }, { displayModeBar: false, responsive: true });
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  initBuildDate();
  initTheme();
  setLoading(true);
  setError("");

  const counters = await initCounters();

  const metricSel     = document.getElementById("metric");
  const setorSel      = document.getElementById("setor");
  const barViewSel    = document.getElementById("barView");
  const deltaViewSel  = document.getElementById("deltaView");
  const colorscaleSel = document.getElementById("colorscale");
  const yearSel       = document.getElementById("year");

  try {
    const [rows, geojsonRaw] = await Promise.all([
      loadJson(withBase(DATA_URL)),
      loadJson(GEOJSON_URL.map(withBase)),
    ]);
    const geojson = normalizeGeoJsonIds(geojsonRaw);

    // Years 2001-2004 exist in the population series but have no MRI data
    // (DATASUS CNES equipment records start from 2005).
    const years = Array.from(
      new Set(
        rows
          .filter((r) => typeof r.ano === "number" && r.ano >= 2005)
          .map((r) => r.ano)
      )
    ).sort((a, b) => a - b);

    yearSel.innerHTML =
      `<option value="AGG">Média ${years[0]}–${years[years.length - 1]}</option>` +
      years.map((y) => `<option value="${y}">${y}</option>`).join("");
    yearSel.value = years[years.length - 1];

    function render() {
      const metric     = metricSel.value;
      const setor      = setorSel.value;
      const barView    = barViewSel    ? barViewSel.value    : "stacked";
      const deltaView  = deltaViewSel  ? deltaViewSel.value  : "stacked";
      const colorscale = colorscaleSel.value;
      const yearValue  = yearSel.value;

      renderKPIs({ rows, yearValue, years });
      renderBar({ rows, years, metric, barView });
      renderRanking({ rows, years, metric, setor, yearValue });
      renderRegion({ rows, years, metric, setor });
      renderDelta({ rows, years, deltaView });
      renderMap({ rows, metric, setor, colorscale, yearValue, years, geojson });

      // Update map subtitle
      const mapSub = document.getElementById("mapSubtitle");
      if (mapSub) {
        mapSub.textContent = yearValue === "AGG"
          ? `Média ${years[0]}–${years[years.length - 1]} · ${metricLabel(metric, setor)}`
          : `${yearValue} · ${metricLabel(metric, setor)}`;
      }
    }

    const downloadBtn      = document.getElementById("downloadXlsx");
    const downloadAllPngBtn = document.getElementById("downloadAllPng");
    const toggleDarkBtn    = document.getElementById("toggleDark");

    if (downloadBtn) downloadBtn.addEventListener("click", async () => {
      downloadExcelComplete({ rows, years });
      if (counters?.incrementDownloads) await counters.incrementDownloads(1);
    });

    if (downloadAllPngBtn) downloadAllPngBtn.addEventListener("click", async () => {
      setLoading(true);
      setError("");
      try {
        await downloadAllMapsZip({
          rows, years,
          colorscale: colorscaleSel.value,
          geojson,
        });
        if (counters?.incrementDownloads) await counters.incrementDownloads(1);
      } catch (e) {
        setError(e?.stack ?? String(e));
      } finally {
        setLoading(false);
      }
    });

    // Unified sector sync: setor, barView and deltaView always reflect the same choice.
    // "stacked" and "todos" both map to setor="todos" but keep distinct display modes.
    function syncToValue(value) {
      setorSel.value = (value === "stacked" || value === "todos") ? "todos" : value;
      if (barViewSel)   barViewSel.value   = value;
      if (deltaViewSel) deltaViewSel.value = value;
    }

    setorSel.addEventListener("change", () => {
      // setor "todos" defaults to stacked; sus/privado sync directly
      syncToValue(setorSel.value === "todos" ? "stacked" : setorSel.value);
      render();
    });

    if (barViewSel) barViewSel.addEventListener("change", () => {
      syncToValue(barViewSel.value);
      render();
    });

    if (deltaViewSel) deltaViewSel.addEventListener("change", () => {
      syncToValue(deltaViewSel.value);
      render();
    });

    metricSel.addEventListener("change", render);
    colorscaleSel.addEventListener("change", render);
    yearSel.addEventListener("change", render);

    if (toggleDarkBtn) {
      toggleDarkBtn.addEventListener("change", () => {
        applyTheme(toggleDarkBtn.checked ? "dark" : "light");
        render();
      });
    }

    render();
  } catch (e) {
    setError(e?.stack ?? String(e));
  } finally {
    setLoading(false);
  }
}

main();
