FROM python:3.11-slim

WORKDIR /app

# System deps for sentence-transformers + database drivers
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download embedding model (cached in image)
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('BAAI/bge-small-zh-v1.5')"

# Copy project (excluding items in .dockerignore)
COPY . .

# ── Copy deployment files ──
COPY deployment/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Persistent volumes
VOLUME ["/app/.chroma"]
VOLUME ["/root/.cache/huggingface"]

EXPOSE 8000 8501

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/entrypoint.sh"]
