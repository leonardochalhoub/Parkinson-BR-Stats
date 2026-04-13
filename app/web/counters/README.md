# Counters backend (Cloudflare Worker + KV)

This folder contains a tiny Cloudflare Worker used to keep:
- **Visitor counter** (`visits`)
- **Download counter** (`downloads`)

The web page (in `app/web/`) calls this backend:
- when the page loads: `POST /api/visit`
- when a download button is clicked: `POST /api/download`
- to display counts: `GET /api/counters` (polled every ~10s)

## 1) Prereqs

- Node.js 16+ (your machine: Node 20 works)
- Cloudflare account
- Wrangler CLI (already installed)

Check:

```bash
node -v
wrangler --version
```

## 2) Create a KV namespace

From repo root:

```bash
cd app/web/counters
wrangler kv namespace create COUNTERS
```

Copy the produced `id` into `wrangler.toml`:

```toml
kv_namespaces = [
  { binding = "COUNTERS", id = "PASTE_ID_HERE" }
]
```

## 3) Local dev

```bash
cd app/web/counters
wrangler dev
```

The worker will run locally (wrangler prints the URL). Use that URL to test:

```bash
curl -X POST http://127.0.0.1:8787/api/visit
curl http://127.0.0.1:8787/api/counters
curl -X POST http://127.0.0.1:8787/api/download
```

## 4) Deploy

```bash
cd app/web/counters
wrangler deploy
```

Wrangler will output a public URL like:

```
https://getpbfdata-counters.<your-subdomain>.workers.dev
```

## 5) Configure the web page to use it

You can choose one of these:

### Option A (recommended): add a query param

Open:

```
http://localhost:8000/?counters=https://getpbfdata-counters.<your-subdomain>.workers.dev
```

### Option B: set a global variable in `index.html`

Add before `app.js`:

```html
<script>
  window.COUNTERS_API_BASE = "https://getpbfdata-counters.<your-subdomain>.workers.dev";
</script>
```

## CORS restriction (optional)

To restrict which sites can call the worker, set:

```toml
# wrangler.toml
[vars]
ALLOWED_ORIGINS = "https://leonardochalhoub.github.io,http://localhost:8000"
```

Then redeploy.

## Notes

- KV increments are not strongly atomic under heavy concurrency. For typical portfolio/demo traffic it's fine.
- If you expect very high traffic and need strict correctness, migrate to a Durable Object counter.
