FROM nginx:1.27-alpine

# Serve the static web app + gold-only data.
# Everything is self-contained under /usr/share/nginx/html.
WORKDIR /usr/share/nginx/html

# App assets
COPY index.html ./index.html
COPY styles.css ./styles.css
COPY app.js ./app.js
COPY vendor-jszip.min.js ./vendor-jszip.min.js
COPY vendor-xlsx.full.min.js ./vendor-xlsx.full.min.js
COPY brazil-flag.svg ./brazil-flag.svg
COPY brazil-states.geojson ./brazil-states.geojson

# Gold JSON expected by app.js: const DATA_URL = "./gold_pbf_estados_df_geo.json";
# (export_gold_for_web.py currently writes app/web/gold_pbf_estados_df_geo.json)
COPY gold_pbf_estados_df_geo.json ./gold_pbf_estados_df_geo.json

# Optional additional data folder (keep for compatibility / future use)
COPY data/ ./data/
