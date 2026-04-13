FROM nginx:1.27-alpine

# Serve the static web app.
# Run `PYTHONPATH=. python app/src/silver/mri_br.py` first to generate data/.
WORKDIR /usr/share/nginx/html

COPY index.html ./
COPY styles.css ./
COPY app.js ./
COPY vendor-jszip.min.js ./
COPY vendor-xlsx.full.min.js ./
COPY brazil-flag.svg ./
COPY brazil-states.geojson ./
COPY linkedin-blue.svg ./
COPY linkedin-white.svg ./
COPY firebase-config.js ./
COPY data/ ./data/
