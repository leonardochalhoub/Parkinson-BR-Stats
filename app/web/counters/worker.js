/**
 * Cloudflare Worker: simple counters API backed by KV.
 *
 * Endpoints:
 *   GET  /api/counters                 -> { visits, downloads }
 *   POST /api/visit                    -> increments visits, returns { visits, downloads }
 *   POST /api/download                 -> increments downloads, returns { visits, downloads }
 *
 * CORS:
 *   Allows any origin by default; you can restrict via env.ALLOWED_ORIGINS (comma-separated).
 */

function json(data, { status = 200, corsOrigin = "*" } = {}) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
      "access-control-allow-origin": corsOrigin,
      "access-control-allow-methods": "GET,POST,OPTIONS",
      "access-control-allow-headers": "content-type",
    },
  });
}

function pickCorsOrigin(request, env) {
  const origin = request.headers.get("Origin") || "";
  const allow = (env.ALLOWED_ORIGINS || "").trim();
  if (!allow) return "*";
  const allowed = allow.split(",").map((s) => s.trim()).filter(Boolean);
  return allowed.includes(origin) ? origin : allowed[0] || "null";
}

async function getCount(env, key) {
  const raw = await env.COUNTERS.get(key);
  const n = raw === null ? 0 : Number(raw);
  return Number.isFinite(n) ? n : 0;
}

async function setCount(env, key, value) {
  await env.COUNTERS.put(key, String(Math.max(0, Math.trunc(value))));
}

async function inc(env, key, by = 1) {
  // KV doesn't have atomic increment; for a simple low-traffic counter this is OK.
  // If you need strong correctness at high concurrency, switch to Durable Objects.
  const cur = await getCount(env, key);
  const next = cur + by;
  await setCount(env, key, next);
  return next;
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname.replace(/\/+$/, "");
    const method = request.method.toUpperCase();
    const corsOrigin = pickCorsOrigin(request, env);

    if (method === "OPTIONS") return json({ ok: true }, { corsOrigin });

    if (method === "GET" && (path === "/api/counters" || path === "/api/counters/")) {
      const visits = await getCount(env, "visits");
      const downloads = await getCount(env, "downloads");
      return json({ visits, downloads }, { corsOrigin });
    }

    if (method === "POST" && path === "/api/visit") {
      const visits = await inc(env, "visits", 1);
      const downloads = await getCount(env, "downloads");
      return json({ visits, downloads }, { corsOrigin });
    }

    if (method === "POST" && path === "/api/download") {
      const downloads = await inc(env, "downloads", 1);
      const visits = await getCount(env, "visits");
      return json({ visits, downloads }, { corsOrigin });
    }

    return json({ error: "Not Found" }, { status: 404, corsOrigin });
  },
};
